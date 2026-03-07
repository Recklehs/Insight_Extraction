from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, CheckConstraint, DateTime, Float, ForeignKey, Index, String, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class NaverBlog(Base):
    __tablename__ = "naver_blog_list"

    blog_id: Mapped[str] = mapped_column(String, primary_key=True)
    blog_url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("TRUE"))
    initial_backfill_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_successful_rss_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    posts: Mapped[list["BlogPostRSS"]] = relationship(back_populates="blog")


class BlogPostRSS(Base):
    __tablename__ = "blog_posts_rss"
    __table_args__ = (
        Index("idx_blog_posts_rss_blog_pub_date", "blog_id", "pub_date"),
    )

    guid: Mapped[str] = mapped_column(Text, primary_key=True)
    blog_id: Mapped[str] = mapped_column(ForeignKey("naver_blog_list.blog_id"), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    link: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pub_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    blog: Mapped["NaverBlog"] = relationship(back_populates="posts")
    crawl_check: Mapped[Optional["CrawlCheck"]] = relationship(back_populates="post", uselist=False)
    crawl_result: Mapped[Optional["CrawlResult"]] = relationship(back_populates="post", uselist=False)


class CrawlCheck(Base):
    __tablename__ = "crawl_check"
    __table_args__ = (
        Index("idx_crawl_check_crawled_created_at", "crawled", "created_at"),
    )

    guid: Mapped[str] = mapped_column(ForeignKey("blog_posts_rss.guid"), primary_key=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    crawled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("FALSE"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    post: Mapped["BlogPostRSS"] = relationship(back_populates="crawl_check")


class CrawlResult(Base):
    __tablename__ = "crawl_result"
    __table_args__ = (
        Index("idx_crawl_result_processed_crawled_at", "is_processed", "crawled_at"),
    )

    guid: Mapped[str] = mapped_column(ForeignKey("blog_posts_rss.guid"), primary_key=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    crawled_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    http_status_code: Mapped[Optional[int]] = mapped_column(nullable=True)
    crawled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_processed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("FALSE"))
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    post: Mapped["BlogPostRSS"] = relationship(back_populates="crawl_result")
    logs: Mapped[list["ProcessingLog"]] = relationship(back_populates="crawl_result")


class ProcessingLog(Base):
    __tablename__ = "processing_logs"
    __table_args__ = (
        Index("idx_processing_logs_guid_processed_at", "crawl_result_guid", "processed_at"),
    )

    log_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crawl_result_guid: Mapped[str] = mapped_column(ForeignKey("crawl_result.guid"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    llm_model: Mapped[str] = mapped_column(Text, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    crawl_result: Mapped["CrawlResult"] = relationship(back_populates="logs")
    signals: Mapped[list["InvestmentSignal"]] = relationship(back_populates="log")


class InvestmentSignal(Base):
    __tablename__ = "investment_signals"
    __table_args__ = (
        CheckConstraint("action IN ('buy', 'hold', 'sell')", name="ck_investment_signals_action"),
        CheckConstraint(
            "confidence_score >= 0.0 AND confidence_score <= 1.0",
            name="ck_investment_signals_confidence_score",
        ),
    )

    signal_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    log_id: Mapped[int] = mapped_column(ForeignKey("processing_logs.log_id"), nullable=False)
    ticker: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(String, nullable=False)
    evidence: Mapped[str] = mapped_column(Text, nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False)

    log: Mapped["ProcessingLog"] = relationship(back_populates="signals")
