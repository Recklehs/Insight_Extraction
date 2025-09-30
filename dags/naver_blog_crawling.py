import asyncio
import pendulum
import traceback
from urllib.parse import urlparse

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# -------------------------------------------------------------------
# 0. 설정: Airflow Connection ID
# -------------------------------------------------------------------
POSTGRES_CONN_ID = "postgres_default"


# -------------------------------------------------------------------
# 1. 데이터베이스 상호작용 함수
# -------------------------------------------------------------------
def get_urls_to_crawl() -> list[dict[str, str]]:
    """
    crawl_check 테이블에서 'crawled'가 false인 URL('guid' 컬럼) 목록을 조회합니다.
    Airflow 동적 매핑을 위해 딕셔너리 리스트를 반환합니다.
    e.g., [{'guid': 'http://url1'}, {'guid': 'http://url2'}]
    """
    mysql_hook = MySqlHook(mysql_conn_id='blog_posts_db')
    # crawl_check 테이블의 guid가 바로 크롤링할 URL입니다.
    sql = "SELECT guid FROM crawl_check WHERE crawled = FALSE;"
    records = mysql_hook.get_records(sql)
    if not records:
        print("크롤링할 새로운 URL이 없습니다.")
        return []
    
    # expand(op_kwargs=...)에 맞게 딕셔너리 리스트로 변환합니다.
    # 여기서 'guid' 키는 run_scrape_and_save 함수의 파라미터 이름과 일치합니다.
    tasks = [{"guid": record[0]} for record in records]
    print(f"총 {len(tasks)}개의 URL을 크롤링 대상으로 가져왔습니다.")
    return tasks

def save_result_to_db(source_url: str, crawled_text: str, http_status_code: int):
    """
    크롤링 결과를 crawl_result 테이블에 저장합니다.
    source_url을 기준으로 ON DUPLICATE KEY UPDATE를 수행합니다.
    """
    mysql_hook = MySqlHook(mysql_conn_id='blog_posts_db')
    # MySQL 호환 구문으로 수정
    sql = """
        INSERT INTO crawl_result (source_url, crawled_text, http_status_code, crawled_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            crawled_text = VALUES(crawled_text),
            http_status_code = VALUES(http_status_code),
            crawled_at = CURRENT_TIMESTAMP;
    """
    mysql_hook.run(sql, parameters=(source_url, crawled_text, http_status_code))

def update_check_table(guid: str):
    """
    크롤링이 성공적으로 완료된 후 crawl_check 테이블의 상태를 업데이트합니다.
    이때 guid는 크롤링한 URL입니다.
    """
    mysql_hook = MySqlHook(mysql_conn_id='blog_posts_db')
    sql = "UPDATE crawl_check SET crawled = TRUE WHERE guid = %s;"
    mysql_hook.run(sql, parameters=(guid,))


# -------------------------------------------------------------------
# 2. 기존 스크래핑 로직 (수정 없음)
# -------------------------------------------------------------------
def to_mobile_post_url(url: str) -> str:
    """https://blog.naver.com/{blogId}/{logNo} -> m.blog.naver.com PostView URL로 변환"""
    p = urlparse(url)
    parts = p.path.strip("/").split("/")
    if p.netloc == "blog.naver.com" and len(parts) >= 2:
        blog_id, log_no = parts[0], parts[1]
        return f"https://m.blog.naver.com/PostView.naver?blogId={blog_id}&logNo={log_no}"
    return url

async def scrape_naver_blog_async(blog_url: str) -> dict:
    """
    주어진 URL의 네이버 블로그 포스트를 스크래핑하고 결과를 dict 형태로 반환합니다.
    """
    MOBILE_URL = to_mobile_post_url(blog_url)
    print("Playwright를 시작합니다...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            locale="ko-KR",
            extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
            viewport={"width": 1366, "height": 900}
        )
        page = await context.new_page()

        scraped_data = {}
        try:
            # 1. 모바일 뷰 시도
            print(f"타겟 블로그로 이동합니다(모바일): {MOBILE_URL}")
            try:
                await page.goto(MOBILE_URL, wait_until="domcontentloaded")
                await page.wait_for_selector(".se-main-container, #post-view", timeout=15000)
                title = await page.evaluate("document.querySelector('meta[property=\"og:title\"]')?.content || document.title || ''")
                content = await page.evaluate("(() => { const cand = document.querySelector('.se-main-container') || document.querySelector('#post-view') || document.querySelector('#viewTypeSelector'); return cand ? cand.innerText.trim() : ''; })()")
                if title.strip() and content.strip():
                    print("블로그 데이터 추출(모바일) 완료!")
                    scraped_data = {"title": title.strip(), "content": content}
            except PWTimeout:
                print("모바일 뷰 추출 실패 → 데스크톱 뷰로 폴백합니다.")

            # 2. 모바일 뷰 실패 시 데스크톱 뷰 시도
            if not scraped_data:
                print(f"타겟 블로그로 이동합니다(데스크톱): {blog_url}")
                await page.goto(blog_url, wait_until="domcontentloaded")
                frame_loc = page.frame_locator("iframe#mainFrame")
                await frame_loc.locator(".se-main-container, #postViewArea").first.wait_for(timeout=15000)
                title = await frame_loc.locator("meta[property='og:title']").get_attribute("content", timeout=5000)
                if not title:
                    title_element = frame_loc.locator(".se-title-text, .pcol1, .tit_h3").first
                    title = await title_element.inner_text(timeout=5000)
                content_loc = frame_loc.locator(".se-main-container, #postViewArea").first
                content = await content_loc.inner_text()
                print("블로그 데이터 추출(데스크톱) 완료!")
                scraped_data = {"title": title.strip(), "content": content.strip()}

            if scraped_data:
                return {"source_url": blog_url, "crawled_text": f"{scraped_data['title']}\n\n{scraped_data['content']}", "http_status_code": 200}
            else:
                raise Exception("모바일과 데스크톱 뷰 모두에서 컨텐츠 추출에 실패했습니다.")
        except Exception as e:
            print(f"스크래핑 중 에러 발생: {repr(e)}")
            await page.screenshot(path=f"/tmp/naver_blog_fail_{urlparse(blog_url).path.replace('/', '_')}.png", full_page=True)
            return {"source_url": blog_url, "crawled_text": traceback.format_exc(), "http_status_code": 500}
        finally:
            await browser.close()
            print("Playwright를 종료합니다.")

# -------------------------------------------------------------------
# 3. 비동기 래퍼 및 DB 저장/업데이트를 포함한 단일 실행 함수
# -------------------------------------------------------------------
def run_scrape_and_save(guid: str):
    """
    단일 URL('guid')에 대해 스크래퍼를 실행하고, 결과를 DB에 저장/업데이트합니다.
    """
    url = guid  # 명확성을 위해 변수 할당
    print(f"URL 크롤링 및 저장을 시작합니다: {url}")
    try:
        # 비동기 스크래핑 함수 실행
        result = asyncio.run(scrape_naver_blog_async(url))
        
        # guid 없이 결과 저장
        save_result_to_db(**result)

        if result.get("http_status_code") == 200:
            # crawl_check 테이블 업데이트는 guid(URL)를 사용
            update_check_table(guid)
            print(f"성공적으로 크롤링 및 저장 완료: {url}")
        else:
            print(f"크롤링 실패. 에러 메시지를 저장했습니다: {url}")
    except Exception as e:
        print(f"태스크 실행 중 예외 발생: {url}, 에러: {e}")
        error_details = traceback.format_exc()
        save_result_to_db(source_url=url, crawled_text=error_details, http_status_code=500)

# -------------------------------------------------------------------
# 4. Airflow DAG 정의
# -------------------------------------------------------------------
with DAG(
    dag_id='naver_blog_playwright_scraper_from_db_v4',
    start_date=pendulum.datetime(2025, 9, 10, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=['scraper', 'playwright', 'database'],
    doc_md="crawl_check 테이블의 guid(URL)를 읽어 Playwright로 크롤링하고 crawl_result에 저장하는 최종 DAG"
) as dag:
    get_urls_task = PythonOperator(
        task_id='get_urls_to_crawl',
        python_callable=get_urls_to_crawl,
    )

    scrape_and_save_task = PythonOperator.partial(
        task_id='scrape_and_save_blog_post',
        python_callable=run_scrape_and_save,
    ).expand(
        op_kwargs=get_urls_task.output,
    )

    get_urls_task >> scrape_and_save_task

