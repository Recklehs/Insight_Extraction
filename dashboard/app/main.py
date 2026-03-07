from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urlparse, urlsplit, urlunsplit
from urllib.request import Request as UrlRequest, urlopen

from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import case, desc, func, select
from sqlalchemy.orm import Session

from .db import get_session
from .models import BlogPostRSS, CrawlCheck, CrawlResult, InvestmentSignal, NaverBlog, ProcessingLog


BASE_DIR = Path(__file__).resolve().parent
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-apiserver:8080").rstrip("/")
ANALYSIS_DAG_ID = "investment_signal_extraction_dag"
SIGNAL_PERIODS = {
    "7d": {"label": "최근 7일", "delta": timedelta(days=7)},
    "30d": {"label": "최근 1달", "delta": timedelta(days=30)},
    "all": {"label": "전체 기간", "delta": None},
}
ACTION_LABELS = {"buy": "BUY", "sell": "SELL"}
SIGNAL_ACTIONS = tuple(ACTION_LABELS)
SIGNAL_SAMPLE_LIMIT = 3
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="Insight Extraction Dashboard")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def format_datetime(value: datetime | None) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


templates.env.filters["datetime"] = format_datetime


def truncate_text(value: str | None, limit: int = 180) -> str | None:
    if value is None:
        return None
    collapsed = re.sub(r"\s+", " ", value).strip()
    if len(collapsed) <= limit:
        return collapsed
    return f"{collapsed[:limit - 1].rstrip()}..."


def build_core_flash_url(base_path: str, *, message: str | None = None, error: str | None = None) -> str:
    split = urlsplit(base_path)
    query = dict(parse_qsl(split.query, keep_blank_values=True))
    if message is not None:
        query["message"] = message
    if error is not None:
        query["error"] = error
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))


def resolve_return_to(return_to: str | None, fallback: str) -> str:
    candidate = (return_to or "").strip()
    if candidate.startswith("/") and not candidate.startswith("//"):
        return candidate
    return fallback


def build_core_ideas_latest_log_subquery():
    ranked_logs = (
        select(
            ProcessingLog.log_id.label("log_id"),
            ProcessingLog.crawl_result_guid.label("guid"),
            ProcessingLog.status.label("status"),
            ProcessingLog.llm_model.label("llm_model"),
            ProcessingLog.error_message.label("error_message"),
            ProcessingLog.processed_at.label("processed_at"),
            func.row_number()
            .over(
                partition_by=ProcessingLog.crawl_result_guid,
                order_by=(ProcessingLog.processed_at.desc(), ProcessingLog.log_id.desc()),
            )
            .label("row_number"),
        )
        .subquery()
    )

    return (
        select(
            ranked_logs.c.guid.label("guid"),
            ranked_logs.c.log_id.label("latest_log_id"),
            ranked_logs.c.status.label("latest_status"),
            ranked_logs.c.llm_model.label("latest_llm_model"),
            ranked_logs.c.error_message.label("latest_error_message"),
            ranked_logs.c.processed_at.label("latest_processed_at"),
        )
        .where(ranked_logs.c.row_number == 1)
        .subquery()
    )


def build_latest_success_log_subquery():
    ranked_logs = (
        select(
            ProcessingLog.log_id.label("log_id"),
            ProcessingLog.crawl_result_guid.label("guid"),
            ProcessingLog.llm_model.label("llm_model"),
            ProcessingLog.processed_at.label("processed_at"),
            func.row_number()
            .over(
                partition_by=ProcessingLog.crawl_result_guid,
                order_by=(ProcessingLog.processed_at.desc(), ProcessingLog.log_id.desc()),
            )
            .label("row_number"),
        )
        .where(ProcessingLog.status == "success")
        .subquery()
    )

    return (
        select(
            ranked_logs.c.guid.label("guid"),
            ranked_logs.c.log_id.label("latest_log_id"),
            ranked_logs.c.llm_model.label("latest_llm_model"),
            ranked_logs.c.processed_at.label("latest_processed_at"),
        )
        .where(ranked_logs.c.row_number == 1)
        .subquery()
    )


def fetch_signal_map(session: Session, log_ids: list[int]) -> dict[int, list[InvestmentSignal]]:
    signal_map: dict[int, list[InvestmentSignal]] = defaultdict(list)
    if not log_ids:
        return signal_map

    signals = session.scalars(
        select(InvestmentSignal)
        .where(InvestmentSignal.log_id.in_(log_ids))
        .order_by(InvestmentSignal.log_id.asc(), InvestmentSignal.confidence_score.desc(), InvestmentSignal.signal_id.asc())
    ).all()
    for signal in signals:
        signal_map[signal.log_id].append(signal)
    return signal_map


def resolve_signal_period(period: str | None) -> str:
    return period if period in SIGNAL_PERIODS else "7d"


def get_signal_period_cutoff(period_key: str) -> datetime | None:
    delta = SIGNAL_PERIODS[period_key]["delta"]
    if delta is None:
        return None
    return datetime.now(UTC) - delta


def build_signal_period_links(current_period: str) -> list[dict[str, str | bool]]:
    return [
        {
            "key": key,
            "label": spec["label"],
            "href": f"/signals?period={key}",
            "active": key == current_period,
        }
        for key, spec in SIGNAL_PERIODS.items()
    ]


def fetch_signal_dashboard_sections(session: Session, period_key: str) -> dict[str, list[dict]]:
    latest_success_subquery = build_latest_success_log_subquery()
    post_timestamp = func.coalesce(BlogPostRSS.pub_date, BlogPostRSS.created_at)
    cutoff = get_signal_period_cutoff(period_key)

    aggregate_stmt = (
        select(
            InvestmentSignal.ticker.label("ticker"),
            InvestmentSignal.action.label("action"),
            func.count(func.distinct(BlogPostRSS.guid)).label("post_count"),
            func.avg(InvestmentSignal.confidence_score).label("avg_confidence"),
            func.max(post_timestamp).label("last_mentioned_at"),
        )
        .join(latest_success_subquery, latest_success_subquery.c.latest_log_id == InvestmentSignal.log_id)
        .join(BlogPostRSS, BlogPostRSS.guid == latest_success_subquery.c.guid)
        .join(NaverBlog, NaverBlog.blog_id == BlogPostRSS.blog_id)
        .where(
            NaverBlog.is_active.is_(True),
            InvestmentSignal.action.in_(SIGNAL_ACTIONS),
        )
        # /signals uses the latest successful analysis per post, not the latest overall log.
    )
    if cutoff is not None:
        aggregate_stmt = aggregate_stmt.where(post_timestamp >= cutoff)

    aggregate_rows = session.execute(
        aggregate_stmt
        .group_by(InvestmentSignal.ticker, InvestmentSignal.action)
        .order_by(
            desc(func.count(func.distinct(BlogPostRSS.guid))),
            desc(func.avg(InvestmentSignal.confidence_score)),
            desc(func.max(post_timestamp)),
            InvestmentSignal.ticker.asc(),
        )
    ).all()

    keys = {(row.ticker, row.action) for row in aggregate_rows}
    sample_stmt = (
        select(
            InvestmentSignal.ticker.label("ticker"),
            InvestmentSignal.action.label("action"),
            BlogPostRSS.guid.label("guid"),
            BlogPostRSS.title.label("title"),
            BlogPostRSS.link.label("source_link"),
            BlogPostRSS.blog_id.label("blog_id"),
            post_timestamp.label("published_at"),
            latest_success_subquery.c.latest_processed_at.label("latest_processed_at"),
        )
        .join(latest_success_subquery, latest_success_subquery.c.latest_log_id == InvestmentSignal.log_id)
        .join(BlogPostRSS, BlogPostRSS.guid == latest_success_subquery.c.guid)
        .join(NaverBlog, NaverBlog.blog_id == BlogPostRSS.blog_id)
        .where(
            NaverBlog.is_active.is_(True),
            InvestmentSignal.action.in_(SIGNAL_ACTIONS),
        )
        .order_by(
            InvestmentSignal.action.asc(),
            InvestmentSignal.ticker.asc(),
            post_timestamp.desc(),
            latest_success_subquery.c.latest_processed_at.desc(),
            BlogPostRSS.guid.asc(),
        )
    )
    if cutoff is not None:
        sample_stmt = sample_stmt.where(post_timestamp >= cutoff)

    sample_rows = session.execute(sample_stmt).all()
    sample_map: dict[tuple[str, str], list[dict]] = defaultdict(list)
    seen_guids: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in sample_rows:
        key = (row.ticker, row.action)
        if key not in keys or row.guid in seen_guids[key] or len(sample_map[key]) >= SIGNAL_SAMPLE_LIMIT:
            continue
        seen_guids[key].add(row.guid)
        sample_map[key].append(
            {
                "guid": row.guid,
                "title": row.title,
                "source_link": row.source_link,
                "blog_id": row.blog_id,
                "published_at": row.published_at,
            }
        )

    sections = {action: [] for action in SIGNAL_ACTIONS}
    for row in aggregate_rows:
        sections[row.action].append(
            {
                "ticker": row.ticker,
                "action": row.action,
                "action_label": ACTION_LABELS[row.action],
                "post_count": row.post_count,
                "avg_confidence": float(row.avg_confidence or 0.0),
                "last_mentioned_at": row.last_mentioned_at,
                "samples": sample_map.get((row.ticker, row.action), []),
            }
        )
    return sections


def request_airflow_token() -> str:
    username = os.getenv("_AIRFLOW_WWW_USER_USERNAME")
    password = os.getenv("_AIRFLOW_WWW_USER_PASSWORD")
    if not username or not password:
        raise RuntimeError("Airflow API 계정 정보가 dashboard 컨테이너에 설정되지 않았습니다.")

    payload = json.dumps({"username": username, "password": password}).encode("utf-8")
    request = UrlRequest(
        f"{AIRFLOW_API_URL}/auth/token",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", "ignore")
        raise RuntimeError(f"Airflow 토큰 발급 실패: HTTP {exc.code} {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Airflow 토큰 발급 실패: {exc.reason}") from exc

    token = body.get("access_token")
    if not token:
        raise RuntimeError("Airflow 토큰 응답에 access_token이 없습니다.")
    return token


def trigger_analysis_dag_run(conf: dict | None = None):
    token = request_airflow_token()
    logical_date = datetime.now(UTC).isoformat()
    dag_run_id = f"manual__dashboard__{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}"
    payload = json.dumps(
        {
            "dag_run_id": dag_run_id,
            "logical_date": logical_date,
            "conf": conf or {},
            "note": "Triggered from Insight Extraction dashboard",
        }
    ).encode("utf-8")
    request = UrlRequest(
        f"{AIRFLOW_API_URL}/api/v2/dags/{ANALYSIS_DAG_ID}/dagRuns",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=10):
            return
    except HTTPError as exc:
        body = exc.read().decode("utf-8", "ignore")
        raise RuntimeError(f"Airflow DAG 실행 요청 실패: HTTP {exc.code} {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Airflow DAG 실행 요청 실패: {exc.reason}") from exc


def parse_naver_blog_url(raw_value: str) -> tuple[str, str]:
    candidate = raw_value.strip()
    if not candidate:
        raise ValueError("블로그 URL을 입력해주세요.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"

    parsed = urlparse(candidate)
    host = parsed.netloc.lower()
    query = parse_qs(parsed.query)
    parts = [part for part in parsed.path.split("/") if part]

    blog_id = None
    if "blogid" in {key.lower() for key in query}:
        for key, values in query.items():
            if key.lower() == "blogid" and values:
                blog_id = values[0]
                break
    elif host in {"blog.naver.com", "m.blog.naver.com", "rss.blog.naver.com"} and parts:
        if parts[0].lower() != "postview.naver":
            blog_id = parts[0]

    if not blog_id or not re.fullmatch(r"[A-Za-z0-9_.-]+", blog_id):
        raise ValueError("네이버 블로그 URL에서 blog_id를 파싱하지 못했습니다.")

    canonical_url = f"https://blog.naver.com/{blog_id}"
    return blog_id, canonical_url


def fetch_blog_rows(session: Session, is_active: bool):
    processed_case = case((CrawlResult.is_processed.is_(True), CrawlResult.guid), else_=None)
    stmt = (
        select(
            NaverBlog.blog_id,
            NaverBlog.blog_url,
            NaverBlog.is_active,
            NaverBlog.initial_backfill_completed_at,
            NaverBlog.last_successful_rss_checked_at,
            NaverBlog.created_at,
            func.count(func.distinct(BlogPostRSS.guid)).label("post_count"),
            func.count(func.distinct(processed_case)).label("processed_count"),
            func.max(BlogPostRSS.pub_date).label("last_post_at"),
            func.max(CrawlResult.processed_at).label("last_processed_at"),
        )
        .outerjoin(BlogPostRSS, BlogPostRSS.blog_id == NaverBlog.blog_id)
        .outerjoin(CrawlResult, CrawlResult.guid == BlogPostRSS.guid)
        .where(NaverBlog.is_active.is_(is_active))
        .group_by(
            NaverBlog.blog_id,
            NaverBlog.blog_url,
            NaverBlog.is_active,
            NaverBlog.initial_backfill_completed_at,
            NaverBlog.last_successful_rss_checked_at,
            NaverBlog.created_at,
        )
        .order_by(desc(func.max(BlogPostRSS.pub_date)), NaverBlog.blog_id)
    )
    return session.execute(stmt).all()


def render_blog_sections(request: Request, session: Session):
    context = {
        "request": request,
        "active_blogs": fetch_blog_rows(session, True),
        "inactive_blogs": fetch_blog_rows(session, False),
    }
    return templates.TemplateResponse(request=request, name="partials/blog_sections.html", context=context)


@app.get("/", response_class=HTMLResponse)
def root() -> RedirectResponse:
    return RedirectResponse(url="/blogs", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/signals", response_class=HTMLResponse)
def signals_dashboard(request: Request, session: Session = Depends(get_session)):
    current_period = resolve_signal_period(request.query_params.get("period"))
    sections = fetch_signal_dashboard_sections(session, current_period)
    return templates.TemplateResponse(
        request=request,
        name="signals.html",
        context={
            "request": request,
            "current_period": current_period,
            "period_links": build_signal_period_links(current_period),
            "buy_signals": sections["buy"],
            "sell_signals": sections["sell"],
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.get("/blogs", response_class=HTMLResponse)
def list_blogs(request: Request, session: Session = Depends(get_session)):
    context = {
        "request": request,
        "active_blogs": fetch_blog_rows(session, True),
        "inactive_blogs": fetch_blog_rows(session, False),
        "error": request.query_params.get("error"),
        "message": request.query_params.get("message"),
    }
    return templates.TemplateResponse(request=request, name="blogs.html", context=context)


@app.post("/blogs", response_class=HTMLResponse)
def create_blog(
    request: Request,
    blog_url: str = Form(...),
    session: Session = Depends(get_session),
):
    try:
        blog_id, canonical_url = parse_naver_blog_url(blog_url)
    except ValueError as exc:
        return RedirectResponse(
            url=f"/blogs?error={quote(str(exc))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    blog = session.get(NaverBlog, blog_id)
    now = datetime.now(UTC)
    if blog is None:
        blog = NaverBlog(
            blog_id=blog_id,
            blog_url=canonical_url,
            is_active=True,
            initial_backfill_completed_at=None,
            last_successful_rss_checked_at=None,
            created_at=now,
            updated_at=now,
        )
        session.add(blog)
    else:
        blog.blog_url = canonical_url
        blog.is_active = True
        blog.initial_backfill_completed_at = None
        blog.last_successful_rss_checked_at = None
        blog.updated_at = now

    session.commit()
    return RedirectResponse(url="/blogs", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/blogs/{blog_id}/deactivate", response_class=HTMLResponse)
def deactivate_blog(
    request: Request,
    blog_id: str,
    session: Session = Depends(get_session),
):
    blog = session.get(NaverBlog, blog_id)
    if blog is None:
        raise HTTPException(status_code=404, detail="블로그를 찾을 수 없습니다.")

    blog.is_active = False
    blog.updated_at = datetime.now(UTC)
    session.commit()

    if request.headers.get("HX-Request") == "true":
        return render_blog_sections(request, session)
    return RedirectResponse(url="/blogs", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/blogs/{blog_id}", response_class=HTMLResponse)
def blog_detail(
    request: Request,
    blog_id: str,
    session: Session = Depends(get_session),
):
    blog = session.get(NaverBlog, blog_id)
    if blog is None:
        raise HTTPException(status_code=404, detail="블로그를 찾을 수 없습니다.")

    latest_log_subquery = build_core_ideas_latest_log_subquery()
    signal_counts_subquery = (
        select(
            InvestmentSignal.log_id.label("log_id"),
            func.count(InvestmentSignal.signal_id).label("signal_count"),
        )
        .group_by(InvestmentSignal.log_id)
        .subquery()
    )

    stmt = (
        select(
            BlogPostRSS.guid,
            BlogPostRSS.title,
            BlogPostRSS.link,
            BlogPostRSS.pub_date,
            BlogPostRSS.created_at,
            CrawlCheck.crawled,
            CrawlResult.is_processed,
            CrawlResult.http_status_code,
            CrawlResult.crawled_at,
            CrawlResult.processed_at,
            latest_log_subquery.c.latest_log_id,
            latest_log_subquery.c.latest_status,
            latest_log_subquery.c.latest_llm_model,
            latest_log_subquery.c.latest_error_message,
            latest_log_subquery.c.latest_processed_at,
            func.coalesce(signal_counts_subquery.c.signal_count, 0).label("latest_signal_count"),
        )
        .outerjoin(CrawlCheck, CrawlCheck.guid == BlogPostRSS.guid)
        .outerjoin(CrawlResult, CrawlResult.guid == BlogPostRSS.guid)
        .outerjoin(latest_log_subquery, latest_log_subquery.c.guid == BlogPostRSS.guid)
        .outerjoin(signal_counts_subquery, signal_counts_subquery.c.log_id == latest_log_subquery.c.latest_log_id)
        .where(BlogPostRSS.blog_id == blog_id)
        .order_by(desc(BlogPostRSS.pub_date), desc(BlogPostRSS.created_at))
    )
    rows = session.execute(stmt).all()
    signal_map = fetch_signal_map(
        session,
        [row.latest_log_id for row in rows if row.latest_log_id is not None],
    )
    posts = []
    for row in rows:
        post = dict(row._mapping)
        latest_signals = signal_map.get(post["latest_log_id"], [])
        post["latest_signals"] = latest_signals
        post["latest_signal_preview"] = latest_signals[:3]
        post["latest_error_preview"] = truncate_text(post["latest_error_message"], limit=140)
        posts.append(post)

    return templates.TemplateResponse(
        request=request,
        name="blog_detail.html",
        context={
            "request": request,
            "blog": blog,
            "posts": posts,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/posts/{guid:path}/reanalyze", response_class=HTMLResponse)
def reanalyze_post(
    request: Request,
    guid: str,
    return_to: str | None = Form(default=None),
    session: Session = Depends(get_session),
):
    fallback = str(request.url_for("post_detail", guid=guid))
    redirect_target = resolve_return_to(return_to, fallback)

    post = session.execute(
        select(BlogPostRSS, CrawlResult)
        .outerjoin(CrawlResult, CrawlResult.guid == BlogPostRSS.guid)
        .where(BlogPostRSS.guid == guid)
    ).first()
    if post is None:
        raise HTTPException(status_code=404, detail="게시물을 찾을 수 없습니다.")

    _blog_post, crawl_result = post
    if crawl_result is None or crawl_result.http_status_code != 200 or not crawl_result.crawled_text:
        return RedirectResponse(
            url=build_core_flash_url(redirect_target, error="성공적으로 크롤링된 본문이 있어야 재분석할 수 있습니다."),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    previous_is_processed = crawl_result.is_processed
    previous_processed_at = crawl_result.processed_at
    crawl_result.is_processed = False
    crawl_result.processed_at = None
    session.commit()

    try:
        trigger_analysis_dag_run({"guid": guid, "source": "dashboard_manual_reanalyze"})
    except Exception as exc:
        crawl_result = session.get(CrawlResult, guid)
        if crawl_result is not None:
            crawl_result.is_processed = previous_is_processed
            crawl_result.processed_at = previous_processed_at
            session.commit()
        return RedirectResponse(
            url=build_core_flash_url(redirect_target, error=str(exc)),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    return RedirectResponse(
        url=build_core_flash_url(redirect_target, message="재분석 요청을 큐에 넣었습니다."),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.get("/posts/{guid:path}", response_class=HTMLResponse)
def post_detail(
    request: Request,
    guid: str,
    session: Session = Depends(get_session),
):
    post = session.execute(
        select(BlogPostRSS, NaverBlog, CrawlCheck, CrawlResult)
        .join(NaverBlog, NaverBlog.blog_id == BlogPostRSS.blog_id)
        .outerjoin(CrawlCheck, CrawlCheck.guid == BlogPostRSS.guid)
        .outerjoin(CrawlResult, CrawlResult.guid == BlogPostRSS.guid)
        .where(BlogPostRSS.guid == guid)
    ).first()
    if post is None:
        raise HTTPException(status_code=404, detail="게시물을 찾을 수 없습니다.")

    blog_post, blog, crawl_check, crawl_result = post

    logs = session.scalars(
        select(ProcessingLog)
        .where(ProcessingLog.crawl_result_guid == guid)
        .order_by(desc(ProcessingLog.processed_at), desc(ProcessingLog.log_id))
    ).all()

    log_ids = [log.log_id for log in logs]
    signal_map = fetch_signal_map(session, log_ids)
    latest_log = logs[0] if logs else None
    latest_signals = signal_map.get(latest_log.log_id, []) if latest_log else []

    context = {
        "request": request,
        "blog": blog,
        "post": blog_post,
        "crawl_check": crawl_check,
        "crawl_result": crawl_result,
        "logs": logs,
        "signal_map": signal_map,
        "latest_log": latest_log,
        "latest_signals": latest_signals,
        "message": request.query_params.get("message"),
        "error": request.query_params.get("error"),
    }
    return templates.TemplateResponse(request=request, name="post_detail.html", context=context)
