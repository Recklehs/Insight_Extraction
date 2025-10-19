import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 로거 설정
log = logging.getLogger(__name__)

# --- 상수 정의 (사용자 요청 반영) ---
URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201060204",
}
PAYLOAD = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT15202",
    "ddTp": "1D",
    "locale": "ko_KR",
    "isuCd": "KRD040200002",
    "csvxls_isNo": "false",
}
MYSQL_CONN_ID = "blog_posts_db"
TARGET_TABLE = "krx_gold_price_minute"
REQUIRED_FIELDS = ["TRD_DD", "PRSNT_PRC", "TMZN_OPNPRC", "TMZN_HGPRC", "TMZN_LWPRC", "TM_ACC_TRDVOL"]

# --- DAG 정의 ---
with DAG(
    dag_id="krx_gold_price_ingestion_final",
    start_date=pendulum.datetime(2025, 10, 3, tz="Asia/Seoul"),
    schedule="*/3 9-15 * * 1-5",
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=3)},
    doc_md="""
    ### KRX 금 시세 데이터 수집 DAG (최종版)
    - **효율성**: DB의 최신 데이터를 먼저 조회하여, 새로운 데이터만 삽입 (멱등성 보장)
    - **역할 분리**: 데이터 변환(transform)과 적재(load) 태스크를 분리하여 구조 개선
    - **DB 안정성**: `executemany`를 사용하여 안정적인 bulk insert 처리
    """,
    tags=["krx", "market-data", "production"],
) as dag:

    @task
    def fetch_krx_data() -> list[dict]:
        """KRX API를 호출하여 금 시세 데이터를 JSON으로 가져옵니다."""
        log.info("Requesting data from KRX API...")
        response = requests.post(URL, data=PAYLOAD, headers=HEADERS, timeout=20)
        response.raise_for_status()
        data = response.json()
        if "output" not in data or not data["output"]:
            raise ValueError("API response is empty or invalid.")
        log.info(f"Successfully fetched {len(data['output'])} records from API.")
        return data["output"]
    @task
    def transform_and_filter_data(data: list[dict]) -> list[tuple]:
        """데이터를 검증, 변환하고 DB에 없는 새로운 데이터만 필터링합니다."""
        if not data:
            raise AirflowSkipException("Upstream task provided no data.")

        today_str = pendulum.now("Asia/Seoul").to_date_string()
        
        processed_rows = []
        for record in data:
            if not all(record.get(key) for key in REQUIRED_FIELDS):
                continue
            try:
                processed_rows.append((
                    f"{today_str} {record['TRD_DD']}:00",
                    int(record['PRSNT_PRC'].replace(',', '')),
                    int(record['TMZN_OPNPRC'].replace(',', '')),
                    int(record['TMZN_HGPRC'].replace(',', '')),
                    int(record['TMZN_LWPRC'].replace(',', '')),
                    int(record['TM_ACC_TRDVOL'].replace(',', ''))
                ))
            except (ValueError, KeyError) as e:
                log.warning(f"Skipping a record due to processing error: {e}. Record: {record}")
        
        if not processed_rows:
            raise AirflowSkipException("No valid data rows after processing.")

        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        sql_max_time = f"SELECT MAX(trade_datetime) FROM {TARGET_TABLE} WHERE trade_datetime LIKE %s"
        result = mysql_hook.get_first(sql_max_time, parameters=(f"{today_str}%",))
        latest_timestamp_in_db_naive = result[0] if result else None
        
        log.info(f"Latest timestamp in DB for today: {latest_timestamp_in_db_naive}")

        if not latest_timestamp_in_db_naive:
            log.info("No existing data for today. Preparing to insert all processed rows.")
            return processed_rows

        # --- 💡 핵심 수정 부분 ---
        # 1. DB에서 가져온 naive한 datetime에 'Asia/Seoul' 시간대 정보를 부여합니다.
        latest_timestamp_in_db_aware = pendulum.instance(latest_timestamp_in_db_naive, tz="Asia/Seoul")

        # 2. 비교할 대상(row[0])도 동일한 시간대 기준으로 파싱하여 명확하게 만듭니다.
        new_rows = [
            row for row in processed_rows 
            if pendulum.parse(row[0], tz="Asia/Seoul") > latest_timestamp_in_db_aware
        ]
        # --- 수정 완료 ---

        if not new_rows:
            raise AirflowSkipException("No new data to insert.")
        
        log.info(f"Found {len(new_rows)} new rows to insert.")
        return new_rows

    @task
    def insert_data_to_db(rows: list[tuple]):
        """변환된 데이터를 DB에 삽입합니다."""
        if not rows:
            log.warning("Received no rows to insert.")
            return

        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        sql_insert_ignore = f"""
            INSERT IGNORE INTO {TARGET_TABLE}
            (trade_datetime, present_price, open_price, high_price, low_price, acc_trade_volume)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        conn = mysql_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql_insert_ignore, rows)
                conn.commit()
                log.info(f"{cursor.rowcount} rows were newly inserted into {TARGET_TABLE}.")
        finally:
            conn.close()

    # --- TaskFlow 정의 ---
    raw_data = fetch_krx_data()
    new_data = transform_and_filter_data(raw_data)
    insert_data_to_db(new_data)