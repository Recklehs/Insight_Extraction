import asyncio
import pendulum
import traceback
from urllib.parse import urlparse

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from playwright.async_api import TimeoutError as PWTimeout
from playwright.async_api import async_playwright


BLOG_DB_CONN_ID = "insight_extraction_pg"


def get_urls_to_crawl() -> list[dict[str, str]]:
    """
    crawl_check 테이블에서 아직 크롤링하지 않은 게시물 목록을 조회합니다.
    반환값은 동적 매핑에 바로 사용할 수 있는 dict 리스트입니다.
    """
    hook = PostgresHook(postgres_conn_id=BLOG_DB_CONN_ID)
    sql = """
        SELECT guid, source_url
        FROM crawl_check
        WHERE crawled = FALSE
        ORDER BY created_at ASC;
    """
    records = hook.get_records(sql)
    if not records:
        print("크롤링할 새로운 URL이 없습니다.")
        return []

    tasks = [{"guid": guid, "source_url": source_url} for guid, source_url in records]
    print(f"총 {len(tasks)}개의 URL을 크롤링 대상으로 가져왔습니다.")
    return tasks


def save_result_to_db(guid: str, source_url: str, crawled_text: str, http_status_code: int):
    """
    크롤링 결과를 crawl_result 테이블에 저장합니다.
    guid를 기준으로 upsert하며, 재크롤링된 결과는 다시 미처리 상태로 전환합니다.
    """
    hook = PostgresHook(postgres_conn_id=BLOG_DB_CONN_ID)
    sql = """
        INSERT INTO crawl_result (
            guid, source_url, crawled_text, http_status_code, crawled_at, is_processed, processed_at
        )
        VALUES (%s, %s, %s, %s, NOW(), FALSE, NULL)
        ON CONFLICT (guid) DO UPDATE
        SET source_url = EXCLUDED.source_url,
            crawled_text = EXCLUDED.crawled_text,
            http_status_code = EXCLUDED.http_status_code,
            crawled_at = NOW(),
            is_processed = FALSE,
            processed_at = NULL;
    """
    hook.run(sql, parameters=(guid, source_url, crawled_text, http_status_code))


def update_check_table(guid: str):
    """
    크롤링 성공 시 crawl_check의 상태를 완료로 표시합니다.
    """
    hook = PostgresHook(postgres_conn_id=BLOG_DB_CONN_ID)
    sql = """
        UPDATE crawl_check
        SET crawled = TRUE, updated_at = NOW()
        WHERE guid = %s;
    """
    hook.run(sql, parameters=(guid,))


def to_mobile_post_url(url: str) -> str:
    """https://blog.naver.com/{blogId}/{logNo} -> m.blog.naver.com PostView URL로 변환"""
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    if parsed.netloc == "blog.naver.com" and len(parts) >= 2:
        blog_id, log_no = parts[0], parts[1]
        return f"https://m.blog.naver.com/PostView.naver?blogId={blog_id}&logNo={log_no}"
    return url


async def scrape_naver_blog_async(source_url: str) -> dict:
    """
    주어진 URL의 네이버 블로그 포스트를 스크래핑합니다.
    """
    mobile_url = to_mobile_post_url(source_url)
    print("Playwright를 시작합니다...")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            locale="ko-KR",
            extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
            viewport={"width": 1366, "height": 900},
        )
        page = await context.new_page()

        scraped_data = {}
        try:
            print(f"타겟 블로그로 이동합니다(모바일): {mobile_url}")
            try:
                await page.goto(mobile_url, wait_until="domcontentloaded")
                await page.wait_for_selector(".se-main-container, #post-view", timeout=15000)
                title = await page.evaluate("document.querySelector('meta[property=\"og:title\"]')?.content || document.title || ''")
                content = await page.evaluate(
                    "(() => { const cand = document.querySelector('.se-main-container') || document.querySelector('#post-view') || document.querySelector('#viewTypeSelector'); return cand ? cand.innerText.trim() : ''; })()"
                )
                if title.strip() and content.strip():
                    print("블로그 데이터 추출(모바일) 완료!")
                    scraped_data = {"title": title.strip(), "content": content}
            except PWTimeout:
                print("모바일 뷰 추출 실패 -> 데스크톱 뷰로 폴백합니다.")

            if not scraped_data:
                print(f"타겟 블로그로 이동합니다(데스크톱): {source_url}")
                await page.goto(source_url, wait_until="domcontentloaded")
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
                return {
                    "source_url": source_url,
                    "crawled_text": f"{scraped_data['title']}\n\n{scraped_data['content']}",
                    "http_status_code": 200,
                }

            raise RuntimeError("모바일과 데스크톱 뷰 모두에서 컨텐츠 추출에 실패했습니다.")
        except Exception as exc:
            print(f"스크래핑 중 에러 발생: {repr(exc)}")
            path_fragment = urlparse(source_url).path.replace("/", "_") or "root"
            await page.screenshot(path=f"/tmp/naver_blog_fail_{path_fragment}.png", full_page=True)
            return {
                "source_url": source_url,
                "crawled_text": traceback.format_exc(),
                "http_status_code": 500,
            }
        finally:
            await browser.close()
            print("Playwright를 종료합니다.")


def run_scrape_and_save(guid: str, source_url: str):
    """
    단일 게시물에 대해 스크래핑을 실행하고 결과를 Postgres에 저장합니다.
    """
    print(f"URL 크롤링 및 저장을 시작합니다: {source_url}")
    try:
        result = asyncio.run(scrape_naver_blog_async(source_url))
        save_result_to_db(guid=guid, **result)

        if result.get("http_status_code") == 200:
            update_check_table(guid)
            print(f"성공적으로 크롤링 및 저장 완료: {source_url}")
        else:
            print(f"크롤링 실패. 에러 메시지를 저장했습니다: {source_url}")
    except Exception as exc:
        print(f"태스크 실행 중 예외 발생: {source_url}, 에러: {exc}")
        save_result_to_db(
            guid=guid,
            source_url=source_url,
            crawled_text=traceback.format_exc(),
            http_status_code=500,
        )


with DAG(
    dag_id="naver_blog_playwright_scraper_from_db_v5",
    start_date=pendulum.datetime(2025, 9, 10, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "playwright", "database", "postgres"],
    doc_md="crawl_check의 guid/source_url을 읽어 Playwright로 크롤링하고 crawl_result에 저장합니다.",
) as dag:
    get_urls_task = PythonOperator(
        task_id="get_urls_to_crawl",
        python_callable=get_urls_to_crawl,
    )

    scrape_and_save_task = PythonOperator.partial(
        task_id="scrape_and_save_blog_post",
        python_callable=run_scrape_and_save,
    ).expand(
        op_kwargs=get_urls_task.output,
    )

    trigger_analysis_task = TriggerDagRunOperator(
        task_id="trigger_signal_analysis",
        trigger_dag_id="investment_signal_extraction_dag",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    get_urls_task >> scrape_and_save_task >> trigger_analysis_task
