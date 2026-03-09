from __future__ import annotations

import json
import sys
import unittest
from datetime import UTC, datetime
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("DATABASE_URL", "sqlite://")

from dashboard.app.db import Base  # noqa: E402
from dashboard.app.main import app, get_session, trigger_analysis_dag_run  # noqa: E402
from dashboard.app.models import BlogPostRSS, CrawlCheck, CrawlResult, InvestmentSignal, NaverBlog, ProcessingLog  # noqa: E402


class DashboardAnalysisViewsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine(
            "sqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        cls.SessionLocal = sessionmaker(bind=cls.engine, autoflush=False, autocommit=False, future=True)
        Base.metadata.create_all(bind=cls.engine)

        def override_get_session():
            session = cls.SessionLocal()
            try:
                yield session
            finally:
                session.close()

        app.dependency_overrides[get_session] = override_get_session
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        app.dependency_overrides.clear()
        Base.metadata.drop_all(bind=cls.engine)
        cls.engine.dispose()

    def setUp(self):
        with self.SessionLocal() as session:
            for table in reversed(Base.metadata.sorted_tables):
                session.execute(table.delete())
            session.commit()

    def seed_post(
        self,
        *,
        blog_id: str = "sample_blog",
        guid: str = "https://blog.naver.com/sample_blog/123",
        title: str = "샘플 글",
        http_status_code: int = 200,
        is_processed: bool = True,
    ) -> str:
        with self.SessionLocal() as session:
            blog = NaverBlog(
                blog_id=blog_id,
                blog_url=f"https://blog.naver.com/{blog_id}",
                is_active=True,
                initial_backfill_completed_at=datetime(2026, 3, 6, 10, 0, tzinfo=UTC),
                last_successful_rss_checked_at=datetime(2026, 3, 7, 9, 0, tzinfo=UTC),
                created_at=datetime(2026, 3, 6, 9, 0, tzinfo=UTC),
                updated_at=datetime(2026, 3, 7, 9, 0, tzinfo=UTC),
            )
            post = BlogPostRSS(
                guid=guid,
                blog_id=blog_id,
                title=title,
                link=f"{guid}?fromRss=true",
                author="tester",
                pub_date=datetime(2026, 3, 7, 8, 0, tzinfo=UTC),
                summary="요약",
                created_at=datetime(2026, 3, 7, 8, 0, tzinfo=UTC),
            )
            crawl_check = CrawlCheck(
                guid=guid,
                source_url=guid,
                crawled=True,
                created_at=datetime(2026, 3, 7, 8, 1, tzinfo=UTC),
                updated_at=datetime(2026, 3, 7, 8, 1, tzinfo=UTC),
            )
            crawl_result = CrawlResult(
                guid=guid,
                source_url=guid,
                crawled_text="OXY를 더 사도 된다고 본다. GS는 관망 의견이다. 한전KPS는 매수 의견이다.",
                http_status_code=http_status_code,
                crawled_at=datetime(2026, 3, 7, 8, 2, tzinfo=UTC),
                is_processed=is_processed,
                processed_at=datetime(2026, 3, 7, 8, 3, tzinfo=UTC) if is_processed else None,
            )
            session.add_all([blog, post, crawl_check, crawl_result])
            session.commit()
        return guid

    def add_log(
        self,
        *,
        guid: str,
        log_id: int,
        status: str,
        processed_at: datetime,
        llm_model: str = "gemini-3-flash-preview",
        error_message: str | None = None,
        signals: list[dict] | None = None,
    ):
        with self.SessionLocal() as session:
            log = ProcessingLog(
                log_id=log_id,
                crawl_result_guid=guid,
                status=status,
                llm_model=llm_model,
                error_message=error_message,
                processed_at=processed_at,
            )
            session.add(log)
            for index, signal in enumerate(signals or [], start=1):
                session.add(
                    InvestmentSignal(
                        signal_id=(log_id * 100) + index,
                        log_id=log_id,
                        ticker=signal["ticker"],
                        action=signal["action"],
                        evidence=signal["evidence"],
                        confidence_score=signal["confidence_score"],
                    )
                )
            session.commit()

    def set_blog_active_state(self, blog_id: str, *, is_active: bool):
        with self.SessionLocal() as session:
            blog = session.get(NaverBlog, blog_id)
            blog.is_active = is_active
            blog.updated_at = datetime(2026, 3, 7, 9, 30, tzinfo=UTC)
            session.commit()

    def update_post_dates(
        self,
        guid: str,
        *,
        pub_date: datetime | None = None,
        created_at: datetime | None = None,
    ):
        with self.SessionLocal() as session:
            post = session.get(BlogPostRSS, guid)
            if pub_date is not None:
                post.pub_date = pub_date
            if created_at is not None:
                post.created_at = created_at
            session.commit()

    def test_post_detail_route_handles_guid_with_slashes(self):
        guid = self.seed_post(guid="https://blog.naver.com/sample_blog/456", title="슬래시 GUID 글")

        response = self.client.get(f"/posts/{guid}")

        self.assertEqual(response.status_code, 200)
        self.assertIn("슬래시 GUID 글", response.text)

    def test_blog_detail_shows_latest_success_signal_summary(self):
        guid = self.seed_post(blog_id="leader", guid="https://blog.naver.com/leader/1", title="시그널 있는 글")
        self.add_log(
            guid=guid,
            log_id=10,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 30, tzinfo=UTC),
            signals=[
                {
                    "ticker": "OXY",
                    "action": "buy",
                    "evidence": "OXY를 더 사도 된다고 본다.",
                    "confidence_score": 0.95,
                }
            ],
        )

        response = self.client.get("/blogs/leader")

        self.assertEqual(response.status_code, 200)
        self.assertIn("OXY", response.text)
        self.assertIn("buy", response.text)
        self.assertIn("재분석", response.text)

    def test_blog_detail_shows_no_signal_message_for_success_without_signals(self):
        guid = self.seed_post(blog_id="nosignal", guid="https://blog.naver.com/nosignal/1", title="시그널 없는 글")
        self.add_log(
            guid=guid,
            log_id=20,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 40, tzinfo=UTC),
            signals=[],
        )

        response = self.client.get("/blogs/nosignal")

        self.assertEqual(response.status_code, 200)
        self.assertIn("핵심 투자 시그널 없음", response.text)

    def test_blog_detail_prefers_latest_failed_log_over_older_success(self):
        guid = self.seed_post(blog_id="retrycase", guid="https://blog.naver.com/retrycase/1", title="재시도 글")
        self.add_log(
            guid=guid,
            log_id=30,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 10, tzinfo=UTC),
            signals=[
                {
                    "ticker": "GS",
                    "action": "hold",
                    "evidence": "GS는 관망 의견이다.",
                    "confidence_score": 0.7,
                }
            ],
        )
        self.add_log(
            guid=guid,
            log_id=31,
            status="failed",
            processed_at=datetime(2026, 3, 7, 9, 50, tzinfo=UTC),
            error_message="timeout while calling model",
            signals=[],
        )

        response = self.client.get("/blogs/retrycase")

        self.assertEqual(response.status_code, 200)
        self.assertIn("failed", response.text)
        self.assertIn("timeout while calling model", response.text)
        self.assertNotIn("GS", response.text)

    def test_manual_reanalyze_resets_processed_and_triggers_airflow(self):
        guid = self.seed_post(blog_id="manualrun", guid="https://blog.naver.com/manualrun/1", title="재분석 대상")
        self.add_log(
            guid=guid,
            log_id=40,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 0, tzinfo=UTC),
            signals=[],
        )

        with patch("dashboard.app.main.trigger_analysis_dag_run") as trigger_mock:
            response = self.client.post(
                f"/posts/{guid}/reanalyze",
                data={"return_to": f"/posts/{guid}"},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/posts/", response.headers["location"])
        trigger_mock.assert_called_once()

        with self.SessionLocal() as session:
            crawl_result = session.get(CrawlResult, guid)
            self.assertFalse(crawl_result.is_processed)
            self.assertIsNone(crawl_result.processed_at)

    def test_manual_reanalyze_rejects_posts_without_successful_crawl(self):
        guid = self.seed_post(
            blog_id="badcrawl",
            guid="https://blog.naver.com/badcrawl/1",
            title="실패한 크롤링 글",
            http_status_code=500,
            is_processed=True,
        )

        with patch("dashboard.app.main.trigger_analysis_dag_run") as trigger_mock:
            response = self.client.post(
                f"/posts/{guid}/reanalyze",
                data={"return_to": f"/posts/{guid}"},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        trigger_mock.assert_not_called()

        with self.SessionLocal() as session:
            crawl_result = session.get(CrawlResult, guid)
            self.assertTrue(crawl_result.is_processed)
            self.assertIsNotNone(crawl_result.processed_at)

    def test_trigger_analysis_dag_run_includes_required_logical_date(self):
        mock_response = MagicMock()
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = False
        mock_response.read.return_value = b"{}"

        with patch("dashboard.app.main.request_airflow_token", return_value="token"), patch(
            "dashboard.app.main.urlopen",
            return_value=mock_response,
        ) as urlopen_mock:
            trigger_analysis_dag_run({"guid": "https://blog.naver.com/sample_blog/123"})

        request = urlopen_mock.call_args.args[0]
        payload = json.loads(request.data.decode("utf-8"))

        self.assertIn("logical_date", payload)
        self.assertTrue(payload["logical_date"])

    def test_blogs_page_contains_link_to_signals_dashboard(self):
        self.seed_post()

        response = self.client.get("/blogs")

        self.assertEqual(response.status_code, 200)
        self.assertIn("/signals", response.text)

    def test_blogs_page_shows_current_llm_model_and_historical_candidates(self):
        guid = self.seed_post(blog_id="modelblog", guid="https://blog.naver.com/modelblog/1", title="모델 이력 글")
        self.add_log(
            guid=guid,
            log_id=73,
            status="success",
            processed_at=datetime(2026, 3, 7, 10, 0, tzinfo=UTC),
            llm_model="gemini-2.5-flash",
            signals=[],
        )

        with patch(
            "dashboard.app.main.get_current_blog_llm_model",
            return_value="gemini-3-flash-preview",
            create=True,
        ):
            response = self.client.get("/blogs")

        self.assertEqual(response.status_code, 200)
        self.assertIn("현재 LLM 모델", response.text)
        self.assertIn('value="gemini-3-flash-preview"', response.text)
        self.assertIn('option value="gemini-2.5-flash"', response.text)

    def test_llm_model_setting_route_trims_and_saves_model(self):
        with patch("dashboard.app.main.set_current_blog_llm_model", create=True) as set_model_mock:
            response = self.client.post(
                "/settings/llm-model",
                data={"llm_model": "  gemini-2.0-flash  "},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/blogs?message=", response.headers["location"])
        set_model_mock.assert_called_once_with("gemini-2.0-flash")

    def test_llm_model_setting_route_rejects_blank_values(self):
        with patch("dashboard.app.main.set_current_blog_llm_model", create=True) as set_model_mock:
            response = self.client.post(
                "/settings/llm-model",
                data={"llm_model": "   "},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/blogs?error=", response.headers["location"])
        set_model_mock.assert_not_called()

    def test_llm_model_setting_route_surfaces_storage_errors(self):
        with patch(
            "dashboard.app.main.set_current_blog_llm_model",
            side_effect=RuntimeError("Airflow variable update failed"),
            create=True,
        ):
            response = self.client.post(
                "/settings/llm-model",
                data={"llm_model": "gemini-2.5-pro"},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        self.assertIn("/blogs?error=", response.headers["location"])
        self.assertIn("Airflow+variable+update+failed", response.headers["location"])

    def test_signals_page_shows_buy_and_sell_sections_for_default_7d_period(self):
        buy_guid = self.seed_post(
            blog_id="buyblog",
            guid="https://blog.naver.com/buyblog/1",
            title="매수 아이디어",
        )
        sell_guid = self.seed_post(
            blog_id="sellblog",
            guid="https://blog.naver.com/sellblog/1",
            title="매도 아이디어",
        )
        old_guid = self.seed_post(
            blog_id="oldblog",
            guid="https://blog.naver.com/oldblog/1",
            title="오래된 아이디어",
        )
        self.update_post_dates(
            old_guid,
            pub_date=datetime(2026, 2, 10, 8, 0, tzinfo=UTC),
            created_at=datetime(2026, 2, 10, 8, 0, tzinfo=UTC),
        )
        self.add_log(
            guid=buy_guid,
            log_id=50,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 0, tzinfo=UTC),
            signals=[
                {
                    "ticker": "OXY",
                    "action": "buy",
                    "evidence": "OXY를 더 사도 된다고 본다.",
                    "confidence_score": 0.95,
                },
                {
                    "ticker": "QQQ",
                    "action": "hold",
                    "evidence": "QQQ는 보유 의견이다.",
                    "confidence_score": 0.65,
                },
            ],
        )
        self.add_log(
            guid=sell_guid,
            log_id=51,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 10, tzinfo=UTC),
            signals=[
                {
                    "ticker": "TSLA",
                    "action": "sell",
                    "evidence": "TSLA는 일부 차익실현이 맞다.",
                    "confidence_score": 0.81,
                }
            ],
        )
        self.add_log(
            guid=old_guid,
            log_id=52,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 20, tzinfo=UTC),
            signals=[
                {
                    "ticker": "AMZN",
                    "action": "buy",
                    "evidence": "AMZN을 사도 된다고 본다.",
                    "confidence_score": 0.75,
                }
            ],
        )

        response = self.client.get("/signals")

        self.assertEqual(response.status_code, 200)
        self.assertIn("BUY 신호 Top", response.text)
        self.assertIn("SELL 신호 Top", response.text)
        self.assertIn("OXY", response.text)
        self.assertIn("TSLA", response.text)
        self.assertNotIn("QQQ", response.text)
        self.assertNotIn("AMZN", response.text)

    def test_signals_page_uses_latest_success_analysis_even_if_newer_failed_log_exists(self):
        guid = self.seed_post(
            blog_id="retrysignal",
            guid="https://blog.naver.com/retrysignal/1",
            title="재시도된 글",
        )
        self.add_log(
            guid=guid,
            log_id=60,
            status="success",
            processed_at=datetime(2026, 3, 7, 8, 40, tzinfo=UTC),
            signals=[
                {
                    "ticker": "OCI",
                    "action": "buy",
                    "evidence": "OCI는 매수 의견이다.",
                    "confidence_score": 0.88,
                }
            ],
        )
        self.add_log(
            guid=guid,
            log_id=61,
            status="failed",
            processed_at=datetime(2026, 3, 7, 9, 30, tzinfo=UTC),
            error_message="latest retry failed",
            signals=[],
        )

        response = self.client.get("/signals?period=7d")

        self.assertEqual(response.status_code, 200)
        self.assertIn("OCI", response.text)
        self.assertNotIn("latest retry failed", response.text)

    def test_signals_page_excludes_inactive_blog_data_and_keeps_original_ticker_labels(self):
        active_guid = self.seed_post(
            blog_id="activeblog",
            guid="https://blog.naver.com/activeblog/1",
            title="활성 블로그 글",
        )
        inactive_guid = self.seed_post(
            blog_id="inactiveblog",
            guid="https://blog.naver.com/inactiveblog/1",
            title="비활성 블로그 글",
        )
        self.set_blog_active_state("inactiveblog", is_active=False)
        self.add_log(
            guid=active_guid,
            log_id=70,
            status="success",
            processed_at=datetime(2026, 3, 7, 8, 20, tzinfo=UTC),
            signals=[
                {
                    "ticker": "OCI",
                    "action": "buy",
                    "evidence": "OCI는 매수 의견이다.",
                    "confidence_score": 0.8,
                },
                {
                    "ticker": "OCI홀딩스",
                    "action": "buy",
                    "evidence": "OCI홀딩스도 매수 의견이다.",
                    "confidence_score": 0.82,
                },
            ],
        )
        self.add_log(
            guid=inactive_guid,
            log_id=71,
            status="success",
            processed_at=datetime(2026, 3, 7, 8, 25, tzinfo=UTC),
            signals=[
                {
                    "ticker": "NVDA",
                    "action": "buy",
                    "evidence": "NVDA는 매수 의견이다.",
                    "confidence_score": 0.91,
                }
            ],
        )

        response = self.client.get("/signals?period=all")

        self.assertEqual(response.status_code, 200)
        self.assertIn("OCI", response.text)
        self.assertIn("OCI홀딩스", response.text)
        self.assertNotIn("NVDA", response.text)

    def test_signals_page_uses_created_at_when_pub_date_is_missing(self):
        guid = self.seed_post(
            blog_id="fallbackblog",
            guid="https://blog.naver.com/fallbackblog/1",
            title="발행일 없는 글",
        )
        self.update_post_dates(
            guid,
            created_at=datetime(2026, 2, 25, 8, 0, tzinfo=UTC),
        )
        with self.SessionLocal() as session:
            post = session.get(BlogPostRSS, guid)
            post.pub_date = None
            session.commit()
        self.add_log(
            guid=guid,
            log_id=72,
            status="success",
            processed_at=datetime(2026, 3, 7, 8, 30, tzinfo=UTC),
            signals=[
                {
                    "ticker": "META",
                    "action": "sell",
                    "evidence": "META는 매도 의견이다.",
                    "confidence_score": 0.77,
                }
            ],
        )

        response = self.client.get("/signals?period=30d")

        self.assertEqual(response.status_code, 200)
        self.assertIn("META", response.text)

    def test_signals_page_shows_empty_state_per_action_section(self):
        guid = self.seed_post(
            blog_id="buyonly",
            guid="https://blog.naver.com/buyonly/1",
            title="매수만 있는 글",
        )
        self.add_log(
            guid=guid,
            log_id=73,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 5, tzinfo=UTC),
            signals=[
                {
                    "ticker": "AAPL",
                    "action": "buy",
                    "evidence": "AAPL은 매수 의견이다.",
                    "confidence_score": 0.79,
                }
            ],
        )

        response = self.client.get("/signals?period=7d")

        self.assertEqual(response.status_code, 200)
        self.assertIn("AAPL", response.text)
        self.assertIn("해당 기간에 SELL 신호가 없습니다.", response.text)

    def test_signals_page_includes_sample_post_links(self):
        guid = self.seed_post(
            blog_id="samplelinks",
            guid="https://blog.naver.com/samplelinks/1",
            title="샘플 링크 글",
        )
        self.add_log(
            guid=guid,
            log_id=74,
            status="success",
            processed_at=datetime(2026, 3, 7, 9, 15, tzinfo=UTC),
            signals=[
                {
                    "ticker": "MSFT",
                    "action": "buy",
                    "evidence": "MSFT는 매수 의견이다.",
                    "confidence_score": 0.86,
                }
            ],
        )

        response = self.client.get("/signals?period=all")

        self.assertEqual(response.status_code, 200)
        self.assertIn("샘플 링크 글", response.text)
        self.assertIn(f"/posts/{guid}", response.text)
        self.assertIn(f"{guid}?fromRss=true", response.text)
