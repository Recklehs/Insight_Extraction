import json
import logging
from datetime import timedelta

import pendulum
import requests
from airflow.exceptions import AirflowSkipException
from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task

# 로거 설정
log = logging.getLogger(__name__)

# --- 상수 정의 ---
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
# 데이터 유효성 검증에 필요한 필수 필드 목록
REQUIRED_FIELDS = [
    "TRD_DD", "PRSNT_PRC", "TMZN_OPNPRC",
    "TMZN_HGPRC", "TMZN_LWPRC", "TM_ACC_TRDVOL"
]

# --- DAG 정의 ---
with DAG(
    dag_id="krx_gold_price_ingestion", 
    start_date=pendulum.datetime(2025, 10, 3, tz="Asia/Seoul"),
    schedule_interval="*/3 9-15 * * 1-5",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    doc_md="""
    ### KRX 금 시세 데이터 수집 DAG 
    ### 
    - **DB 저장 로직 변경**: `REPLACE INTO` 대신 `INSERT IGNORE`를 사용하여, 이미 존재하는 데이터는 무시하고 새로운 데이터만 효율적으로 추가합니다.
    """,
    tags=["krx", "market-data", "production"],
) as dag:

    @task
    def fetch_krx_data() -> list[dict]:
        """KRX API를 호출하여 금 시세 데이터를 JSON으로 가져옵니다."""
        log.info("Requesting data from KRX API...")
        try:
            response = requests.post(URL, data=PAYLOAD, headers=HEADERS, timeout=20)
            response.raise_for_status()
            data = response.json()
            if "output" not in data or not data["output"]:
                raise ValueError("API response is empty or invalid.")
            log.info(f"Successfully fetched {len(data['output'])} records from API.")
            return data["output"]
        except Exception as e:
            log.error(f"Error during fetch: {e}")
            raise

    @task
    def process_and_insert_data(data: list[dict]):
        """
        데이터를 검증하고, 새로운 데이터만 필터링하여 MySQL DB에 효율적으로 적재합니다.
        """
        if not data:
            raise AirflowSkipException("Upstream task provided no data.")

        now_kst = pendulum.now("Asia/Seoul")
        today_str = now_kst.to_date_string()

        # 영업일 검증
        for record in data:
            if all(record.get(key) for key in REQUIRED_FIELDS):
                record_time_str = record.get("TRD_DD")
                record_datetime = pendulum.parse(f"{today_str} {record_time_str}", tz="Asia/Seoul")
                if record_datetime > now_kst:
                    raise AirflowSkipException("Detected non-trading day based on future data.")
        log.info("Trading day confirmed. Proceeding with data processing.")

        # 유효한 데이터 필터링 및 DB 저장 준비
        rows_to_insert = []
        for record in data:
            if not all(record.get(key) for key in REQUIRED_FIELDS):
                continue
            try:
                rows_to_insert.append((
                    f"{today_str} {record['TRD_DD']}:00",
                    int(record['PRSNT_PRC'].replace(',', '')),
                    int(record['TMZN_OPNPRC'].replace(',', '')),
                    int(record['TMZN_HGPRC'].replace(',', '')),
                    int(record['TMZN_LWPRC'].replace(',', '')),
                    int(record['TM_ACC_TRDVOL'].replace(',', ''))
                ))
            except (ValueError, KeyError) as e:
                log.warning(f"Skipping a record due to processing error: {e}. Record: {record}")

        if not rows_to_insert:
            log.info("No valid data rows to insert.")
            return

        # --- START: DB 저장 로직 수정 ---
        # `INSERT IGNORE` 쿼리를 사용하여 중복 데이터는 무시하고 새로운 데이터만 삽입
        log.info(f"Attempting to insert {len(rows_to_insert)} new rows into {TARGET_TABLE}.")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        
        sql_insert_ignore = f"""
            INSERT IGNORE INTO {TARGET_TABLE}
            (trade_datetime, present_price, open_price, high_price, low_price, acc_trade_volume)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        # `run` 메서드는 여러 개의 레코드를 한 번의 쿼리로 효율적으로 실행
        result = mysql_hook.run(sql_insert_ignore, parameters=rows_to_insert)
        log.info(f"DB operation complete. Result: {result}")
        # --- END: DB 저장 로직 수정 ---


    # --- TaskFlow 정의 ---
    fetched_data = fetch_krx_data()
    process_and_insert_data(fetched_data)