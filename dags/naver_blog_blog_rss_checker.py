import logging
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import feedparser
import requests
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from pendulum import timezone


KST = timezone("Asia/Seoul")
BLOG_DB_CONN_ID = "insight_extraction_pg"
INITIAL_BACKFILL_WINDOW = timedelta(days=2)
RECOVERY_OVERLAP_WINDOW = timedelta(hours=1)


def _entry_published_at(entry) -> datetime | None:
    published = entry.get("published")
    if published:
        try:
            parsed = parsedate_to_datetime(published)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except (TypeError, ValueError):
            logging.warning("published 값을 파싱하지 못했습니다: %s", published)

    pub_date_struct = entry.get("published_parsed")
    if pub_date_struct:
        return datetime(*pub_date_struct[:6], tzinfo=UTC)
    return None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def plan_recovery_window(
    initial_backfill_completed_at: datetime | None,
    last_successful_rss_checked_at: datetime | None,
    now: datetime,
) -> tuple[str, datetime | None]:
    if initial_backfill_completed_at is None:
        return "initial_backfill", now - INITIAL_BACKFILL_WINDOW
    if last_successful_rss_checked_at is None:
        return "bridge_recovery", now - INITIAL_BACKFILL_WINDOW
    return "incremental", last_successful_rss_checked_at - RECOVERY_OVERLAP_WINDOW


def should_skip_entry_for_window(published_at: datetime | None, cutoff: datetime | None) -> bool:
    return cutoff is not None and published_at is not None and published_at < cutoff


def mark_blog_rss_success(cursor, blog_id: str, recovery_mode: str, checked_at: datetime) -> None:
    cursor.execute(
        """
        UPDATE naver_blog_list
        SET initial_backfill_completed_at = CASE
                WHEN %s = 'initial_backfill' AND initial_backfill_completed_at IS NULL THEN %s
                ELSE initial_backfill_completed_at
            END,
            last_successful_rss_checked_at = %s,
            updated_at = NOW()
        WHERE blog_id = %s;
        """,
        (recovery_mode, checked_at, checked_at, blog_id),
    )


@dag(
    dag_id="rss_blog_post_collector",
    start_date=datetime(2025, 8, 26, tzinfo=KST),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["blog", "rss", "collector"],
    doc_md="""
    ### 블로그 RSS 피드 수집 DAG

    PostgreSQL의 `naver_blog_list` 테이블을 참조하여 모니터링할 블로그 목록을 가져옵니다.
    각 블로그의 RSS 피드를 확인하여 새로운 게시물을 `blog_posts_rss`와 `crawl_check`에 저장합니다.

    **사전 준비사항**
    1. Airflow Connection: `insight_extraction_pg`
    2. 대상 DB: PostgreSQL `insight_extraction`
    3. 스키마: `sql/init_blog_pipeline.sql` 적용
    """,
)
def rss_blog_post_collector_dag():
    @task
    def get_target_blogs_from_db() -> list[dict[str, str | None]]:
        hook = PostgresHook(postgres_conn_id=BLOG_DB_CONN_ID)
        sql = """
            SELECT blog_id, blog_url, initial_backfill_completed_at, last_successful_rss_checked_at
            FROM naver_blog_list
            WHERE is_active = TRUE
            ORDER BY blog_id;
        """
        try:
            records = hook.get_records(sql)
        except Exception as exc:
            logging.error("DB에서 블로그 목록 조회 실패: %s", exc)
            return []

        blogs = [
            {
                "blog_id": row[0],
                "blog_url": row[1],
                "initial_backfill_completed_at": row[2].isoformat() if row[2] else None,
                "last_successful_rss_checked_at": row[3].isoformat() if row[3] else None,
            }
            for row in records
        ]
        if not blogs:
            logging.warning("모니터링할 활성화된 블로그가 DB에 없습니다.")
        else:
            logging.info("DB에서 가져온 모니터링 대상 블로그: %s", [blog["blog_id"] for blog in blogs])
        return blogs

    @task
    def fetch_and_process_rss(blog: dict[str, str | None]):
        blog_id = blog["blog_id"]
        if not blog_id:
            logging.warning("유효하지 않은 blog_id 입니다.")
            return

        initial_backfill_completed_at = _parse_iso_datetime(blog.get("initial_backfill_completed_at"))
        last_successful_rss_checked_at = _parse_iso_datetime(blog.get("last_successful_rss_checked_at"))
        recovery_mode, cutoff = plan_recovery_window(
            initial_backfill_completed_at=initial_backfill_completed_at,
            last_successful_rss_checked_at=last_successful_rss_checked_at,
            now=datetime.now(UTC),
        )

        rss_url = f"https://rss.blog.naver.com/{blog_id}.xml"
        logging.info("[%s] RSS 피드를 확인합니다: %s (mode=%s, cutoff=%s)", blog_id, rss_url, recovery_mode, cutoff)

        try:
            response = requests.get(rss_url, timeout=15)
            response.raise_for_status()
        except requests.RequestException as exc:
            logging.error("[%s] RSS 피드 다운로드 실패: %s", blog_id, exc)
            return

        feed = feedparser.parse(response.content)
        if not feed.entries:
            logging.info("[%s] 게시물이 없습니다.", blog_id)
            if recovery_mode in {"initial_backfill", "bridge_recovery", "incremental"}:
                hook = PostgresHook(postgres_conn_id=BLOG_DB_CONN_ID)
                conn = hook.get_conn()
                cursor = conn.cursor()
                try:
                    mark_blog_rss_success(cursor, blog_id, recovery_mode, datetime.now(UTC))
                    conn.commit()
                finally:
                    cursor.close()
                    conn.close()
            return

        hook = PostgresHook(postgres_conn_id=BLOG_DB_CONN_ID)
        conn = None
        cursor = None
        new_posts_count = 0

        try:
            conn = hook.get_conn()
            cursor = conn.cursor()

            for entry in reversed(feed.entries):
                guid = entry.get("guid") or entry.get("id") or entry.get("link")
                link = entry.get("link")
                if not (guid and link):
                    logging.warning("[%s] GUID 또는 Link가 없는 항목을 건너뜁니다: %s", blog_id, entry.get("title"))
                    continue

                published_at = _entry_published_at(entry)
                if should_skip_entry_for_window(published_at, cutoff):
                    continue

                insert_post_sql = """
                    INSERT INTO blog_posts_rss (guid, blog_id, title, link, author, pub_date, summary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (guid) DO NOTHING
                    RETURNING guid;
                """
                cursor.execute(
                    insert_post_sql,
                    (
                        guid,
                        blog_id,
                        entry.get("title", "N/A"),
                        link,
                        entry.get("author", ""),
                        published_at,
                        entry.get("summary", ""),
                    ),
                )
                inserted = cursor.fetchone()

                if inserted:
                    cursor.execute(
                        """
                        INSERT INTO crawl_check (guid, source_url, crawled, created_at, updated_at)
                        VALUES (%s, %s, FALSE, NOW(), NOW())
                        ON CONFLICT (guid) DO NOTHING;
                        """,
                        (guid, link),
                    )
                    conn.commit()
                    new_posts_count += 1
                    logging.info("[%s] 새로운 글 발견! >> %s", blog_id, entry.get("title", guid))
                else:
                    conn.rollback()

            checked_at = datetime.now(UTC)
            mark_blog_rss_success(cursor, blog_id, recovery_mode, checked_at)
            conn.commit()
            if recovery_mode == "initial_backfill":
                logging.info("[%s] 초기 2일 백필을 완료했습니다.", blog_id)
            elif recovery_mode == "bridge_recovery":
                logging.info("[%s] 재기동 브리지 복구를 완료했습니다.", blog_id)
            else:
                logging.info("[%s] incremental RSS 체크를 완료했습니다.", blog_id)

        except Exception as exc:
            if conn:
                conn.rollback()
            logging.error("[%s] RSS 저장 중 오류 발생: %s", blog_id, exc)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        if new_posts_count == 0:
            logging.info("[%s] 확인 완료. 새로운 글이 없습니다.", blog_id)
        else:
            logging.info("[%s] 총 %s개의 새로운 글을 저장했습니다.", blog_id, new_posts_count)

    blogs = get_target_blogs_from_db()
    processed_blogs = fetch_and_process_rss.expand(blog=blogs)
    trigger_crawler = TriggerDagRunOperator(
        task_id="trigger_blog_crawler",
        trigger_dag_id="naver_blog_playwright_scraper_from_db_v5",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    processed_blogs >> trigger_crawler


rss_blog_post_collector_dag()
