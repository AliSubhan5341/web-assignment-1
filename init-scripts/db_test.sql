-- ============================================================
-- WIER PA1 - crawldb schema
-- Base schema kept intact, only extended
-- PostgreSQL
-- ============================================================

CREATE SCHEMA IF NOT EXISTS crawldb;

-- ============================================================
-- Base model (MUST remain intact)
-- ============================================================

CREATE TABLE IF NOT EXISTS crawldb.site (
    id              SERIAL PRIMARY KEY,
    domain          VARCHAR(500) UNIQUE NOT NULL,
    robots_content  TEXT,
    sitemap_content TEXT
);

CREATE TABLE IF NOT EXISTS crawldb.page_type (
    code VARCHAR(20) PRIMARY KEY
);

INSERT INTO crawldb.page_type(code) VALUES
    ('HTML'),
    ('BINARY'),
    ('DUPLICATE'),
    ('FRONTIER')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS crawldb.page (
    id               SERIAL PRIMARY KEY,
    site_id          INTEGER REFERENCES crawldb.site(id),
    page_type_code   VARCHAR(20) NOT NULL REFERENCES crawldb.page_type(code),
    url              VARCHAR(3000) UNIQUE NOT NULL,
    html_content     TEXT,
    http_status_code INTEGER,
    accessed_time    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crawldb.link (
    from_page INTEGER REFERENCES crawldb.page(id),
    to_page   INTEGER REFERENCES crawldb.page(id),
    PRIMARY KEY (from_page, to_page)
);

CREATE TABLE IF NOT EXISTS crawldb.data_type (
    code VARCHAR(20) PRIMARY KEY
);

INSERT INTO crawldb.data_type(code) VALUES
    ('PDF'),
    ('DOC'),
    ('DOCX'),
    ('PPT'),
    ('PPTX'),
    ('XLS'),
    ('XLSX'),
    ('OTHER')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS crawldb.page_data (
    id              SERIAL PRIMARY KEY,
    page_id         INTEGER NOT NULL REFERENCES crawldb.page(id),
    data_type_code  VARCHAR(20) NOT NULL REFERENCES crawldb.data_type(code),
    data            BYTEA
);

CREATE TABLE IF NOT EXISTS crawldb.image (
    id            SERIAL PRIMARY KEY,
    page_id       INTEGER NOT NULL REFERENCES crawldb.page(id),
    filename      VARCHAR(255),
    content_type  VARCHAR(50),
    data          BYTEA,
    accessed_time TIMESTAMP
);

-- ============================================================
-- Safe extensions
-- ============================================================

ALTER TABLE crawldb.site
    ADD COLUMN IF NOT EXISTS crawl_delay INTEGER;

ALTER TABLE crawldb.page
    ADD COLUMN IF NOT EXISTS priority DOUBLE PRECISION DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS processing BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS content_hash CHAR(32),
    ADD COLUMN IF NOT EXISTS canonical_page_id INTEGER REFERENCES crawldb.page(id),
    ADD COLUMN IF NOT EXISTS discovered_time TIMESTAMP DEFAULT NOW(),
    -- extension: track outcome without changing base page_type_code vocabulary
    ADD COLUMN IF NOT EXISTS fetch_status VARCHAR(30),
    ADD COLUMN IF NOT EXISTS redirect_url VARCHAR(3000);

-- Optional crawl-run metadata
CREATE TABLE IF NOT EXISTS crawldb.crawl_run (
    id                  SERIAL PRIMARY KEY,
    started_at          TIMESTAMP DEFAULT NOW(),
    finished_at         TIMESTAMP,
    target_description  TEXT,
    status              VARCHAR(20) DEFAULT 'running',
    pages_crawled       INTEGER DEFAULT 0
);

ALTER TABLE crawldb.page
    ADD COLUMN IF NOT EXISTS crawl_run_id INTEGER REFERENCES crawldb.crawl_run(id);

-- Optional storage for near-duplicate signatures
CREATE TABLE IF NOT EXISTS crawldb.page_signature (
    page_id        INTEGER PRIMARY KEY REFERENCES crawldb.page(id) ON DELETE CASCADE,
    signature_text TEXT NOT NULL
);

-- Optional: store image URL for dedup/reporting (base model doesn't include it)
ALTER TABLE crawldb.image
    ADD COLUMN IF NOT EXISTS url VARCHAR(3000);

-- ============================================================
-- Helpful indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_site_domain
    ON crawldb.site(domain);

CREATE INDEX IF NOT EXISTS idx_page_url
    ON crawldb.page(url);

CREATE INDEX IF NOT EXISTS idx_page_type_code
    ON crawldb.page(page_type_code);

CREATE INDEX IF NOT EXISTS idx_page_frontier_priority
    ON crawldb.page(priority ASC, id ASC)
    WHERE page_type_code = 'FRONTIER' AND processing = FALSE AND fetch_status IS NULL;

CREATE INDEX IF NOT EXISTS idx_page_fetch_status
    ON crawldb.page(fetch_status);

CREATE INDEX IF NOT EXISTS idx_page_content_hash
    ON crawldb.page(content_hash);

CREATE INDEX IF NOT EXISTS idx_page_site_id
    ON crawldb.page(site_id);

CREATE INDEX IF NOT EXISTS idx_link_from_page
    ON crawldb.link(from_page);

CREATE INDEX IF NOT EXISTS idx_link_to_page
    ON crawldb.link(to_page);

CREATE INDEX IF NOT EXISTS idx_page_data_page_id
    ON crawldb.page_data(page_id);

CREATE INDEX IF NOT EXISTS idx_image_page_id
    ON crawldb.image(page_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_image_page_url_unique
    ON crawldb.image(page_id, url)
    WHERE url IS NOT NULL;