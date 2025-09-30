import logging
from datetime import datetime

import feedparser
import requests
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pendulum import timezone

# 한국 시간대 설정
KST = timezone("Asia/Seoul")

@dag(
    dag_id='rss_blog_post_collector',
    start_date=datetime(2025, 8, 26, tzinfo=KST),
    schedule='@hourly',  # 1시간에 한번씩 실행
    catchup=False,
    max_active_runs=1,
    tags=['blog', 'rss', 'collector'],
    doc_md="""
    ### 블로그 RSS 피드 수집 DAG

    PostgreSQL DB의 `naver_blog_list` 테이블을 참조하여 모니터링할 블로그 목록을 가져옵니다.
    각 블로그의 RSS 피드를 주기적으로 확인하여, 새로운 게시물이 발견되면 `blog_posts_rss` 테이블에 저장합니다.
    
    **✅ 사전 준비사항:**
    1. **Airflow Connection:** ID가 `blog_posts_db`인 DB 연결 설정
    2. **Database Tables:**
        - `naver_blog_list`: 모니터링할 블로그 목록 관리 테이블 (is_active=TRUE)
        - `blog_posts_rss`: 수집한 새 글 정보를 저장할 테이블
    3. **Python Libraries:** `requests`, `feedparser`, `apache-airflow-providers-postgres` 설치
    """
)
def rss_blog_post_collector_dag():
    """
    DB 테이블 기반으로 네이버 블로그 RSS 피드를 확인하여 새로운 글을 수집/저장하는 DAG
    """
    
    @task
    def get_target_blog_ids_from_db() -> list[str]:
        """DB의 naver_blog_list 테이블에서 활성화된(is_active=TRUE) 블로그 ID 목록을 가져옵니다."""
        mysql_hook = MySqlHook(mysql_conn_id='blog_posts_db')
        sql = "SELECT blog_id FROM naver_blog_list WHERE is_active = TRUE;"
        try:
            records = mysql_hook.get_records(sql)
            blog_ids = [row[0] for row in records]
            if not blog_ids:
                logging.warning("모니터링할 활성화된 블로그가 DB에 없습니다.")
            else:
                logging.info(f"DB에서 가져온 모니터링 대상 블로그: {blog_ids}")
            return blog_ids
        except Exception as e:
            logging.error(f"DB에서 블로그 목록 조회 실패: {e}")
            return []

    @task
    def fetch_and_process_rss(blog_id: str):
        """특정 블로그 ID의 RSS 피드를 가져와 파싱하고 새로운 글을 DB에 저장합니다."""
        if not blog_id:
            logging.warning("유효하지 않은 blog_id 입니다.")
            return

        rss_url = f"https://rss.blog.naver.com/{blog_id}.xml"
        logging.info(f"[{blog_id}] RSS 피드를 확인합니다: {rss_url}")

        try:
            response = requests.get(rss_url, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"[{blog_id}] RSS 피드 다운로드 실패: {e}")
            return

        feed = feedparser.parse(response.content)

        if not feed.entries:
            logging.info(f"[{blog_id}] 게시물이 없습니다.")
            return

        mysql_hook = MySqlHook(mysql_conn_id='blog_posts_db')
        new_posts_count = 0

        # 최신 글부터 확인하기 위해 피드 순서를 뒤집음
        for entry in reversed(feed.entries):
            guid = entry.get('guid')
            link = entry.get('link')
            if not (guid and link):
                logging.warning(f"[{blog_id}] GUID 또는 Link가 없는 항목을 건너뜁니다: {entry.get('title')}")
                continue

            # DB에 이미 존재하는지 확인 (테이블명: blog_posts_rss)
            result = mysql_hook.get_first("SELECT guid FROM blog_posts_rss WHERE guid = %s", parameters=(guid,))

            if result is None:
                new_posts_count += 1
                logging.info(f"[{blog_id}] 새로운 글 발견! >> {entry.title}")

                pub_date_struct = entry.get('published_parsed')
                pub_date = datetime(*pub_date_struct[:6]) if pub_date_struct else datetime.now()

                # === 트랜잭션을 사용한 DB 저장 (수정된 부분) ===
                conn = None
                cursor = None
                try:
                    conn = mysql_hook.get_conn()
                    cursor = conn.cursor()

                    # 1. blog_posts_rss 테이블에 새 글 저장
                    sql_insert_post = """
                        INSERT INTO blog_posts_rss (guid, blog_id, title, link, author, pub_date, summary)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """
                    params_post = (
                        guid, blog_id, entry.get('title', 'N/A'), link,
                        entry.get('author', ''), pub_date, entry.get('summary', '')
                    )
                    cursor.execute(sql_insert_post, params_post)

                    # 2. crawl_check 테이블에 해당 guid 추가
                    sql_insert_check = "INSERT INTO crawl_check (guid) VALUES (%s);"
                    cursor.execute(sql_insert_check, (guid,)) # crawled, created_at은 DEFAULT 값 사용

                    # 모든 작업이 성공하면 커밋
                    conn.commit()

                except Exception as e:
                    logging.error(f"[{blog_id}] DB 저장 중 오류 발생, 롤백합니다: {e}")
                    if conn:
                        conn.rollback() # 오류 발생 시 모든 작업을 되돌림
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()

        if new_posts_count == 0:
            logging.info(f"[{blog_id}] 확인 완료. 새로운 글이 없습니다.")
        else:
            logging.info(f"[{blog_id}] 총 {new_posts_count}개의 새로운 글을 저장했습니다.")

    # DAG 실행 흐름 정의
    blog_ids_list = get_target_blog_ids_from_db()
    fetch_and_process_rss.expand(blog_id=blog_ids_list)

# DAG 인스턴스 생성
rss_blog_post_collector_dag()