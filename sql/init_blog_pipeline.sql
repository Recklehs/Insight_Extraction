CREATE TABLE IF NOT EXISTS naver_blog_list (
    blog_id TEXT PRIMARY KEY,
    blog_url TEXT NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    initial_backfill_completed_at TIMESTAMPTZ,
    last_successful_rss_checked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE IF EXISTS naver_blog_list
    ADD COLUMN IF NOT EXISTS last_successful_rss_checked_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS blog_posts_rss (
    guid TEXT PRIMARY KEY,
    blog_id TEXT NOT NULL REFERENCES naver_blog_list (blog_id),
    title TEXT NOT NULL,
    link TEXT NOT NULL,
    author TEXT,
    pub_date TIMESTAMPTZ,
    summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crawl_check (
    guid TEXT PRIMARY KEY REFERENCES blog_posts_rss (guid) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    crawled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crawl_result (
    guid TEXT PRIMARY KEY REFERENCES blog_posts_rss (guid) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    crawled_text TEXT,
    http_status_code INTEGER,
    crawled_at TIMESTAMPTZ,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS processing_logs (
    log_id BIGSERIAL PRIMARY KEY,
    crawl_result_guid TEXT NOT NULL REFERENCES crawl_result (guid) ON DELETE CASCADE,
    status TEXT NOT NULL,
    llm_model TEXT NOT NULL,
    error_message TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS investment_signals (
    signal_id BIGSERIAL PRIMARY KEY,
    log_id BIGINT NOT NULL REFERENCES processing_logs (log_id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('buy', 'hold', 'sell')),
    evidence TEXT NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0)
);

CREATE INDEX IF NOT EXISTS idx_blog_posts_rss_blog_pub_date
    ON blog_posts_rss (blog_id, pub_date DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_check_crawled_created_at
    ON crawl_check (crawled, created_at);

CREATE INDEX IF NOT EXISTS idx_crawl_result_processed_crawled_at
    ON crawl_result (is_processed, crawled_at);

CREATE INDEX IF NOT EXISTS idx_processing_logs_guid_processed_at
    ON processing_logs (crawl_result_guid, processed_at DESC);
