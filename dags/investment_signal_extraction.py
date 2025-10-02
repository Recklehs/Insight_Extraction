from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# langgraph_processor.py 파일이 Airflow의 PYTHONPATH에 포함되어야 합니다.
# 일반적으로 dags 폴더에 함께 위치시키면 자동으로 인식됩니다.
from logics.blog_extract_langgraph_processor import main_processor

# DAG 기본 인수 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    'investment_signal_extraction_dag',
    default_args=default_args,
    description='크롤링된 블로그 포스트에서 투자 시그널을 추출, 분석, 저장하는 DAG',
    # 매 시간 실행
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['investment', 'llm', 'langgraph'],
) as dag:
    # PythonOperator를 사용하여 langgraph_processor의 메인 함수 실행
    run_extraction_task = PythonOperator(
        task_id='run_langgraph_signal_extraction',
        python_callable=main_processor,
    )
