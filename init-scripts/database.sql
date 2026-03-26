-- ============================================================
-- Web Crawler Database Schema
-- Extended for GitHub FOSS Repository Crawling
-- ============================================================

CREATE SCHEMA IF NOT EXISTS crawldb;

-- ------------------------------------------------------------
-- Page type (HTML or binary/other)
-- ------------------------------------------------------------
CREATE TYPE crawldb.page_type_enum AS ENUM ('HTML', 'BINARY', 'DUPLICATE', 'FRONTIER');


CREATE TABLE crawldb.site (
    id              SERIAL PRIMARY KEY,
    domain          VARCHAR(500) UNIQUE NOT NULL,
    robots_content  TEXT,
    sitemap_content TEXT,
    crawl_delay     INTEGER DEFAULT 60   -- seconds between requests to GitHub
);

-- ------------------------------------------------------------
-- Pages
-- ------------------------------------------------------------
CREATE TABLE crawldb.page (
    id               SERIAL PRIMARY KEY,
    site_id          INTEGER REFERENCES crawldb.site(id),
    page_type        crawldb.page_type_enum NOT NULL DEFAULT 'FRONTIER',
    url              VARCHAR(3000) UNIQUE NOT NULL,
    html_content     TEXT,
    http_status_code INTEGER,
    accessed_time    TIMESTAMP,
    -- preferential crawling priority (lower = crawl sooner)
    priority         FLOAT DEFAULT 1.0
);

-- ------------------------------------------------------------
-- Links between pages
-- ------------------------------------------------------------
CREATE TABLE crawldb.link (
    from_page INTEGER REFERENCES crawldb.page(id),
    to_page   INTEGER REFERENCES crawldb.page(id),
    PRIMARY KEY (from_page, to_page)
);

-- ------------------------------------------------------------
-- Binary/downloadable page data (PDFs, images, etc.)
-- ------------------------------------------------------------
CREATE TYPE crawldb.data_type_enum AS ENUM ('PDF', 'DOC', 'DOCX', 'PPT', 'PPTX', 'XLS', 'XLSX', 'OTHER');

CREATE TABLE crawldb.page_data (
    id        SERIAL PRIMARY KEY,
    page_id   INTEGER REFERENCES crawldb.page(id),
    data_type crawldb.data_type_enum,
    data      BYTEA
);

-- ------------------------------------------------------------
-- Images found on pages
-- ------------------------------------------------------------
CREATE TABLE crawldb.image (
    id            SERIAL PRIMARY KEY,
    page_id       INTEGER REFERENCES crawldb.page(id),
    filename      VARCHAR(255),
    content_type  VARCHAR(50),
    data          BYTEA,
    accessed_time TIMESTAMP
);


-- ============================================================
-- GITHUB FOSS EXTENSION
-- ============================================================

-- ------------------------------------------------------------
-- FOSS License vocabulary
-- Normalises the many ways GitHub spells the same licence.
-- ------------------------------------------------------------
CREATE TABLE crawldb.foss_license (
    id        SERIAL PRIMARY KEY,
    spdx_id   VARCHAR(100) UNIQUE NOT NULL,  -- e.g. 'MIT', 'Apache-2.0', 'GPL-3.0'
    full_name VARCHAR(255)                   -- e.g. 'MIT License'
);

-- Seed with common FOSS licences so foreign keys resolve immediately.
INSERT INTO crawldb.foss_license (spdx_id, full_name) VALUES
    ('MIT',          'MIT License'),
    ('Apache-2.0',   'Apache License 2.0'),
    ('GPL-2.0',      'GNU General Public License v2.0'),
    ('GPL-3.0',      'GNU General Public License v3.0'),
    ('LGPL-2.1',     'GNU Lesser General Public License v2.1'),
    ('LGPL-3.0',     'GNU Lesser General Public License v3.0'),
    ('AGPL-3.0',     'GNU Affero General Public License v3.0'),
    ('MPL-2.0',      'Mozilla Public License 2.0'),
    ('BSD-2-Clause', 'BSD 2-Clause "Simplified" License'),
    ('BSD-3-Clause', 'BSD 3-Clause "New" or "Revised" License'),
    ('ISC',          'ISC License'),
    ('CC0-1.0',      'Creative Commons Zero v1.0 Universal'),
    ('Unlicense',    'The Unlicense'),
    ('EUPL-1.2',     'European Union Public License 1.2'),
    ('OTHER',        'Other / Non-standard')
ON CONFLICT (spdx_id) DO NOTHING;

-- ------------------------------------------------------------
-- GitHub repositories
-- One row per unique repo discovered during the crawl.
-- Linked back to crawldb.page so the original HTML/API response
-- is always accessible.
-- ------------------------------------------------------------
CREATE TABLE crawldb.repo (
    id              SERIAL PRIMARY KEY,

    -- back-reference to the page that was crawled for this repo
    page_id         INTEGER REFERENCES crawldb.page(id) ON DELETE SET NULL,

    -- GitHub identity
    github_id       BIGINT UNIQUE,              -- GitHub's own numeric repo ID
    owner           VARCHAR(255) NOT NULL,       -- login of owner (user or org)
    name            VARCHAR(255) NOT NULL,       -- repo name
    full_name       VARCHAR(511) GENERATED ALWAYS AS (owner || '/' || name) STORED,
    html_url        VARCHAR(1000),               -- https://github.com/{owner}/{name}
    api_url         VARCHAR(1000),               -- https://api.github.com/repos/{owner}/{name}

    -- descriptive metadata
    description     TEXT,
    homepage_url    VARCHAR(1000),               -- project website (if set)
    license_id      INTEGER REFERENCES crawldb.foss_license(id),
    primary_language VARCHAR(100),

    -- community signals
    stars           INTEGER DEFAULT 0,
    forks           INTEGER DEFAULT 0,
    open_issues     INTEGER DEFAULT 0,
    watchers        INTEGER DEFAULT 0,

    -- repository state
    is_fork         BOOLEAN DEFAULT FALSE,
    is_archived     BOOLEAN DEFAULT FALSE,
    is_disabled     BOOLEAN DEFAULT FALSE,
    default_branch  VARCHAR(100) DEFAULT 'main',

    -- timestamps (from GitHub, stored as text then cast)
    repo_created_at TIMESTAMP,
    repo_updated_at TIMESTAMP,
    repo_pushed_at  TIMESTAMP,

    -- when we crawled it
    crawled_at      TIMESTAMP DEFAULT NOW(),

    UNIQUE (owner, name)
);

-- ------------------------------------------------------------
-- Topics / tags attached to a repo  (many-to-many)
-- e.g. 'open-source', 'foss', 'machine-learning', …
-- ------------------------------------------------------------
CREATE TABLE crawldb.topic (
    id    SERIAL PRIMARY KEY,
    name  VARCHAR(100) UNIQUE NOT NULL   -- lowercased, hyphenated GitHub topic
);

CREATE TABLE crawldb.repo_topic (
    repo_id  INTEGER REFERENCES crawldb.repo(id)  ON DELETE CASCADE,
    topic_id INTEGER REFERENCES crawldb.topic(id) ON DELETE CASCADE,
    PRIMARY KEY (repo_id, topic_id)
);

-- ------------------------------------------------------------
-- README files
-- A repo may have multiple READMEs over time (re-crawls),
-- but only one is marked current.
-- ------------------------------------------------------------
CREATE TABLE crawldb.readme (
    id          SERIAL PRIMARY KEY,
    repo_id     INTEGER NOT NULL REFERENCES crawldb.repo(id) ON DELETE CASCADE,

    -- raw content as returned by the GitHub API (base64-decoded)
    content     TEXT NOT NULL,
    encoding    VARCHAR(20) DEFAULT 'utf-8',

    -- GitHub metadata
    sha         VARCHAR(100),                    -- blob SHA for change detection
    file_path   VARCHAR(500) DEFAULT 'README.md',
    download_url VARCHAR(1000),

    is_current  BOOLEAN DEFAULT TRUE,            -- FALSE for historical snapshots
    fetched_at  TIMESTAMP DEFAULT NOW()
);

-- Only one README per repo can be current at a time.
CREATE UNIQUE INDEX idx_readme_current
    ON crawldb.readme (repo_id)
    WHERE is_current = TRUE;

-- ------------------------------------------------------------
-- Documentation pages linked from a repo
-- (links found inside the README or repo description)
-- ------------------------------------------------------------
CREATE TABLE crawldb.doc_link (
    id          SERIAL PRIMARY KEY,
    repo_id     INTEGER NOT NULL REFERENCES crawldb.repo(id) ON DELETE CASCADE,
    page_id     INTEGER REFERENCES crawldb.page(id) ON DELETE SET NULL,  -- crawled page, if visited

    url         VARCHAR(3000) NOT NULL,
    link_text   VARCHAR(500),                    -- anchor text from the README
    source      VARCHAR(50) DEFAULT 'readme',    -- 'readme' | 'description' | 'homepage'

    discovered_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (repo_id, url)
);

-- ------------------------------------------------------------
-- Issues
-- Stores open issues fetched from the GitHub API.
-- ------------------------------------------------------------
CREATE TYPE crawldb.issue_state_enum AS ENUM ('open', 'closed');

CREATE TABLE crawldb.issue (
    id              SERIAL PRIMARY KEY,
    repo_id         INTEGER NOT NULL REFERENCES crawldb.repo(id) ON DELETE CASCADE,

    github_issue_id BIGINT NOT NULL,             -- GitHub's numeric issue ID
    issue_number    INTEGER NOT NULL,            -- #123
    title           TEXT,
    body            TEXT,
    state           crawldb.issue_state_enum DEFAULT 'open',

    -- author (GitHub login)
    author_login    VARCHAR(255),

    -- reactions summary
    reactions_total INTEGER DEFAULT 0,

    -- timestamps
    issue_created_at TIMESTAMP,
    issue_updated_at TIMESTAMP,
    issue_closed_at  TIMESTAMP,

    fetched_at      TIMESTAMP DEFAULT NOW(),

    UNIQUE (repo_id, github_issue_id)
);

-- ------------------------------------------------------------
-- Issue labels  (many-to-many)
-- e.g. 'bug', 'enhancement', 'good first issue', 'help wanted'
-- ------------------------------------------------------------
CREATE TABLE crawldb.label (
    id    SERIAL PRIMARY KEY,
    name  VARCHAR(100) UNIQUE NOT NULL,
    color VARCHAR(10)                  -- hex colour from GitHub, e.g. 'e4e669'
);

CREATE TABLE crawldb.issue_label (
    issue_id INTEGER REFERENCES crawldb.issue(id) ON DELETE CASCADE,
    label_id INTEGER REFERENCES crawldb.label(id) ON DELETE CASCADE,
    PRIMARY KEY (issue_id, label_id)
);

-- ------------------------------------------------------------
-- Issue comments
-- ------------------------------------------------------------
CREATE TABLE crawldb.issue_comment (
    id                SERIAL PRIMARY KEY,
    issue_id          INTEGER NOT NULL REFERENCES crawldb.issue(id) ON DELETE CASCADE,

    github_comment_id BIGINT UNIQUE NOT NULL,
    author_login      VARCHAR(255),
    body              TEXT,

    comment_created_at TIMESTAMP,
    comment_updated_at TIMESTAMP,

    fetched_at        TIMESTAMP DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Crawl runs
-- Tracks each time the crawler is executed so results from
-- different runs can be compared or incrementally updated.
-- ------------------------------------------------------------
CREATE TABLE crawldb.crawl_run (
    id              SERIAL PRIMARY KEY,
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    seed_topics     TEXT[],                      -- e.g. '{foss, open-source}'
    target_description TEXT,                     -- preferential crawling description
    pages_crawled   INTEGER DEFAULT 0,
    repos_found     INTEGER DEFAULT 0,
    status          VARCHAR(50) DEFAULT 'running' -- 'running' | 'done' | 'interrupted'
);

-- Link each repo back to the run that discovered it
ALTER TABLE crawldb.repo
    ADD COLUMN crawl_run_id INTEGER REFERENCES crawldb.crawl_run(id) ON DELETE SET NULL;


-- ============================================================
-- Indexes
-- ============================================================

-- Original indexes (unchanged)
CREATE INDEX idx_page_type     ON crawldb.page(page_type);
CREATE INDEX idx_page_url      ON crawldb.page(url);
CREATE INDEX idx_page_priority ON crawldb.page(priority ASC) WHERE page_type = 'FRONTIER';
CREATE INDEX idx_link_from     ON crawldb.link(from_page);
CREATE INDEX idx_link_to       ON crawldb.link(to_page);

-- Repo lookups
CREATE INDEX idx_repo_owner         ON crawldb.repo(owner);
CREATE INDEX idx_repo_stars         ON crawldb.repo(stars DESC);
CREATE INDEX idx_repo_language      ON crawldb.repo(primary_language);
CREATE INDEX idx_repo_license       ON crawldb.repo(license_id);
CREATE INDEX idx_repo_crawl_run     ON crawldb.repo(crawl_run_id);
CREATE INDEX idx_repo_updated       ON crawldb.repo(repo_updated_at DESC);

-- README lookups
CREATE INDEX idx_readme_repo        ON crawldb.readme(repo_id);

-- Issue lookups
CREATE INDEX idx_issue_repo         ON crawldb.issue(repo_id);
CREATE INDEX idx_issue_state        ON crawldb.issue(state);
CREATE INDEX idx_issue_created      ON crawldb.issue(issue_created_at DESC);

-- Issue comment lookups
CREATE INDEX idx_comment_issue      ON crawldb.issue_comment(issue_id);

-- Doc link lookups
CREATE INDEX idx_doc_link_repo      ON crawldb.doc_link(repo_id);

-- Topic lookups
CREATE INDEX idx_repo_topic_repo    ON crawldb.repo_topic(repo_id);
CREATE INDEX idx_repo_topic_topic   ON crawldb.repo_topic(topic_id);