import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import time

# 데이터베이스 연결 ID (Airflow UI > Admin > Connections에서 설정)
MYSQL_CONN_ID = 'blog_posts_db'

def _check_market_hours(**context):
    """
    KST 기준, 주중 장내 시간(09:30 ~ 15:30)인지 확인합니다.
    장내 시간이 아니면 downstream task들을 skip합니다.
    """
    now_kst = pendulum.now('Asia/Seoul')
    
    # 주중(월요일=0, 일요일=6)이고, 장내 시간인지 확인
    is_weekday = 0 <= now_kst.weekday() <= 4
    is_market_time = time(9, 30) <= now_kst.time() <= time(15, 30)
    
    if is_weekday and is_market_time:
        print(f"KST {now_kst}: Market is open. Proceeding with tasks.")
        return True
    else:
        print(f"KST {now_kst}: Market is closed. Skipping tasks.")
        return False

def _fetch_and_calculate_premium(**context):
    """
    KRX 최신 가격을 가져오고, 해당 시점의 KB 가격을 찾아 프리미엄을 계산합니다.
    계산된 결과를 XCom으로 다음 태스크에 전달합니다.
    """
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    # 1. KRX 테이블에서 가장 최신 데이터 조회 (20분 지연 감안)
    sql_krx = """
    SELECT trade_datetime, present_price
    FROM krx_gold_price_minute
    ORDER BY trade_datetime DESC
    LIMIT 1;
    """
    krx_result = mysql_hook.get_first(sql_krx)
    
    if not krx_result:
        raise ValueError("Could not fetch latest KRX gold price.")
        
    krx_trade_datetime, krx_price_per_g = krx_result
    print(f"Latest KRX data: {krx_trade_datetime}, Price_per_g: {krx_price_per_g}")

    # 2. KRX 거래시간과 가장 가까운 KB 데이터 조회
    # [수정] KB 데이터의 recorded_at 시간도 함께 조회하도록 변경
    sql_kb = """
    SELECT recorded_at, base_price_krw_g
    FROM kb_global_gold_price
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, recorded_at, %s))
    LIMIT 1;
    """
    kb_result = mysql_hook.get_first(sql_kb, parameters=(krx_trade_datetime,))
    
    if not kb_result:
        raise ValueError(f"Could not find a matching KB price for KRX time: {krx_trade_datetime}")

    # [수정] 조회 결과를 두 변수로 받음
    kb_recorded_at, kb_price_per_g = kb_result
    print(f"Matching KB data: {kb_recorded_at}, Price_per_g: {kb_price_per_g}")

    # 3. 프리미엄 계산 (g당 가격 기준)
    premium = krx_price_per_g - kb_price_per_g
    print(f"Premium calculated: {krx_price_per_g} - {kb_price_per_g} = {premium}")
    
    # 4. 다음 태스크로 결과 전달
    # [수정] XCom으로 전달하는 데이터에 kb_recorded_at 추가
    result_data = {
        'trade_datetime': krx_trade_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        'kb_recorded_at': kb_recorded_at.strftime('%Y-%m-%d %H:%M:%S'),
        'krx_price_per_g': krx_price_per_g,
        'kb_price_per_g': kb_price_per_g,
        'premium': premium
    }
    context['ti'].xcom_push(key='premium_data', value=result_data)


def _store_premium_result(**context):
    """
    이전 태스크에서 계산된 프리미엄 데이터를 DB에 저장합니다.
    """
    data_to_store = context['ti'].xcom_pull(key='premium_data', task_ids='fetch_and_calculate_premium')
    
    if not data_to_store:
        raise ValueError("No premium data received from the previous task.")

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    # [수정] INSERT 및 UPDATE 쿼리에 kb_recorded_at 컬럼 추가
    sql_insert = """
    INSERT INTO gold_price_premium 
        (trade_datetime, kb_recorded_at, krx_price_per_g, kb_price_per_g, premium)
    VALUES 
        (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        kb_recorded_at = VALUES(kb_recorded_at),
        krx_price_per_g = VALUES(krx_price_per_g),
        kb_price_per_g = VALUES(kb_price_per_g),
        premium = VALUES(premium);
    """
    
    # [수정] SQL 파라미터에 kb_recorded_at 추가
    params = (
        data_to_store['trade_datetime'],
        data_to_store['kb_recorded_at'],
        data_to_store['krx_price_per_g'],
        data_to_store['kb_price_per_g'],
        data_to_store['premium']
    )
    
    mysql_hook.run(sql_insert, parameters=params)
    print(f"Successfully stored premium data for {data_to_store['trade_datetime']}")


with DAG(
    dag_id='gold_price_premium_calculator',
    start_date=pendulum.datetime(2025, 10, 10, tz="Asia/Seoul"),
    schedule='*/1 9-15 * * 1-5',
    catchup=False,
    doc_md="""
    ### KRX-KB 금 시세 프리미엄 계산 DAG
    - **기능**: KRX와 KB의 1g당 금 가격 차이를 계산하여 저장합니다.
    - **실행 시간**: 월-금, 09:30 ~ 15:30 (KST)에 매 분마다 실제 로직을 수행합니다.
    - **기준 데이터**: 20분 지연된 KRX 분봉 데이터를 기준으로 합니다.
    """,
    tags=['gold', 'finance', 'premium'],
) as dag:
    
    check_market_hours = ShortCircuitOperator(
        task_id='check_market_hours',
        python_callable=_check_market_hours,
    )
    
    fetch_and_calculate_premium = PythonOperator(
        task_id='fetch_and_calculate_premium',
        python_callable=_fetch_and_calculate_premium,
    )

    store_premium_result = PythonOperator(
        task_id='store_premium_result',
        python_callable=_store_premium_result,
    )

    check_market_hours >> fetch_and_calculate_premium >> store_premium_result