from __future__ import annotations

import pendulum
import requests
from lxml import html
from bs4 import BeautifulSoup
import datetime
from typing import List, Dict, Any

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

MYSQL_CONN_ID = "blog_posts_db"

@dag(
    dag_id="kb_gold_price_crawler", # 이전 DAG와 구분하기 위해 이름 변경
    start_date=pendulum.datetime(2025, 10, 6, tz="Asia/Seoul"),
    schedule="*/10 * * * *",
    catchup=False,
    doc_md="""
    ### KB 국민은행 골드바 가격 크롤링 및 저장 DAG (분리 버전)
    - **crawl_gold_price**: 웹사이트에서 금 시세를 크롤링하고 가공합니다.
    - **save_data_to_db**: 크롤링된 데이터를 DB에 저장합니다.
    - 두 태스크로 분리하여 에러 추적 및 재실행 용이성을 높였습니다.
    """,
    tags=["crawling", "gold", "kb", "separated"],
)
def kb_gold_price_crawler_dag_separated():
    """
    크롤링과 DB 저장을 별도의 태스크로 분리한 DAG
    """

    @task
    def crawl_gold_price() -> List[Dict[str, Any]]:
        """
        [태스크 1] 웹사이트에서 데이터를 크롤링하고 가공하여 리스트 형태로 반환합니다.
        """
        print("금 시세 크롤링을 시작합니다.")
        
        url = "https://obank.kbstar.com/quics?page=C023489"
        headers = {'User-Agent': 'Mozilla/5.0 ...'} # 헤더 정보
        
        # 실제 운영에서는 Selenium/Playwright 사용이 필요
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        html_content = response.content

        tree = html.fromstring(html_content)
        base_date_element = tree.xpath('//*[@id="IBF"]/div[2]/p')
        table_element = tree.xpath('//*[@id="IBF"]/table[2]')

        if not (base_date_element and table_element):
            print("❌ 기준일(p) 또는 테이블(table) 요소를 찾을 수 없습니다.")
            return [] # 데이터를 찾지 못하면 빈 리스트 반환

        raw_date_string = base_date_element[0].text_content().strip()
        base_date_text = raw_date_string.split(':')[1].strip()
        
        table_html_string = html.tostring(table_element[0], encoding='unicode')
        soup = BeautifulSoup(table_html_string, 'html.parser')
        headers_list = [th.get_text(separator=' ', strip=True) for th in soup.find('thead').find_all('th')]
        rows = soup.find('tbody').find_all('tr')
        
        all_data = []
        for row in rows:
            values = [td.get_text(strip=True).replace(',', '') for td in row.find_all('td')]
            apply_time = values[0]
            try:
                recorded_at = datetime.datetime.strptime(f"{base_date_text} {apply_time}", "%Y.%m.%d %H:%M:%S")
            except ValueError:
                print(f"⚠️ 날짜/시간 변환 오류 발생: {base_date_text} {apply_time}")
                continue

            row_data = dict(zip(headers_list, values))
            processed_data = {
                'recorded_at': recorded_at,
                'base_price_krw_g': row_data.get('기준가격 (원/gram)'),
                'buy_price_krw_g': row_data.get('매입가격 (원/gram) [고객이 금을 살 때]'),
                'sell_price_krw_g': row_data.get('매도가격(원/gram) [고객이 금을 팔 때]'),
                'intl_price_usd_oz': row_data.get('금가격 (USD/트로이온스)'),
                'exchange_rate_krw_usd': row_data.get('원달러환율 (￦/USD)')
            }
            all_data.append(processed_data)

        print(f"✅ 총 {len(all_data)}개의 데이터를 성공적으로 크롤링 및 가공했습니다.")
        return all_data

    @task
    def save_data_to_db(data_to_save: List[Dict[str, Any]]) -> int:
        """
        [태스크 2] 이전 태스크에서 전달받은 데이터를 DB에 저장합니다.
        """
        if not data_to_save:
            print("ℹ️ 저장할 데이터가 없습니다. 태스크를 종료합니다.")
            return 0

        print(f"✅ DB 저장을 시작합니다. (대상 데이터: {len(data_to_save)}건)")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        
        sql_data = [
            (
                d['recorded_at'], d['base_price_krw_g'], d['buy_price_krw_g'],
                d['sell_price_krw_g'], d['intl_price_usd_oz'], d['exchange_rate_krw_usd']
            )
            for d in data_to_save
        ]
        
        sql = """
        INSERT IGNORE INTO kb_global_gold_price 
        (recorded_at, base_price_krw_g, buy_price_krw_g, sell_price_krw_g, intl_price_usd_oz, exchange_rate_krw_usd)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        conn = mysql_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                inserted_rows = cursor.executemany(sql, sql_data)
            conn.commit()
            print(f"✅ 데이터베이스 저장 완료. 총 {len(sql_data)}개 중 {inserted_rows}개의 새로운 데이터가 추가되었습니다.")
            return inserted_rows
        finally:
            if conn:
                conn.close()

    # --- 태스크 실행 순서 정의 ---
    crawled_data = crawl_gold_price()
    save_data_to_db(crawled_data)

# DAG 인스턴스화
kb_gold_price_crawler_dag_separated()