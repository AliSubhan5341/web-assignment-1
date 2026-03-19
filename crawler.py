"""
Web Crawler - Complete Implementation
======================================
Combines:
  - Selenium (JS-enabled fetching) + requests (fast fallback)
  - PostgreSQL storage (psycopg2)
  - Multi-threaded workers with locking
  - Preferential crawling (BoW + cosine similarity priority)
  - robots.txt + sitemap.xml parsing
  - Duplicate detection via URL normalisation
"""

import time
import threading
import hashlib
import re
import logging
import os
import random
random.seed(42)
from datetime import datetime
from urllib.parse import urlsplit, urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
import psycopg2
from psycopg2 import pool
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Selenium imports (comment out if not installed)
try:
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.firefox.service import Service
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning("Selenium not installed – falling back to requests only.")


# ============================================================
# Configuration
# ============================================================

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "user":     "user",
    "password": "SecretPassword",
    "database": "wier",
}

# GitHub auth — read from environment, never hardcode
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_HEADERS = {
    "Accept": "application/vnd.github+json",
    **({"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}),
}

CRAWLER_CONFIG = {
    "user_agent":       "fri-wier-group-e/1.0",
    "num_workers":      5,
    "default_delay":    5 if GITHUB_TOKEN else 60,  # 5s authenticated, 60s anonymous
    "request_timeout":  10,
    "use_selenium":     True,
    "selenium_timeout": 8,
    "gecko_driver":     "./geckodriver",
    "max_pages":        5000,
    "target_description": "",
}


# Binary file extensions to download but not parse for links
BINARY_EXTENSIONS = {
    ".pdf":  "PDF",  ".doc":  "DOC",  ".docx": "DOCX",
    ".ppt":  "PPT",  ".pptx": "PPTX", ".xls":  "XLS",
    ".xlsx": "XLSX",
}

# Image extensions
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp"}

# GITHUB ACCESS
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

if GITHUB_TOKEN:
    log.info("GitHub token found — rate limit: 5,000 req/hr (1 req/s)")
else:
    log.warning("No GITHUB_TOKEN found — rate limit: 60 req/hr (1 req/60s). Set GITHUB_TOKEN to speed up.")


# ============================================================
# Database layer
# ============================================================

class CrawlerDB:
    """Thread-safe PostgreSQL access via a connection pool."""

    def __init__(self, config: dict, pool_size: int = 10):
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=pool_size, **config
        )
        self._lock = threading.Lock()

    def _conn(self):
        return self._pool.getconn()

    def _release(self, conn):
        self._pool.putconn(conn)

    # ----------------------------------------------------------
    # Sites
    # ----------------------------------------------------------

    def get_or_create_site(self, domain: str) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id FROM crawldb.site WHERE domain = %s", (domain,)
                )
                row = cur.fetchone()
                if row:
                    return row[0]
                cur.execute(
                    "INSERT INTO crawldb.site (domain) VALUES (%s) RETURNING id",
                    (domain,),
                )
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    def update_site_robots(self, site_id: int, robots: str, sitemap: str, delay: int):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE crawldb.site
                       SET robots_content = %s, sitemap_content = %s, crawl_delay = %s
                       WHERE id = %s""",
                    (robots, sitemap, delay, site_id),
                )
        finally:
            self._release(conn)

    def get_crawl_delay(self, site_id: int) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT crawl_delay FROM crawldb.site WHERE id = %s", (site_id,)
                )
                row = cur.fetchone()
                return row[0] if row else CRAWLER_CONFIG["default_delay"]
        finally:
            self._release(conn)

    # ----------------------------------------------------------
    # Pages – frontier management
    # ----------------------------------------------------------

    def add_frontier_url(self, url: str, site_id: int, priority: float = 1.0) -> int | None:
        """Insert a FRONTIER page. Returns new id or None if URL already known."""
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM crawldb.page WHERE url = %s", (url,))
                if cur.fetchone():
                    return None  # already seen
                cur.execute(
                    """INSERT INTO crawldb.page (site_id, page_type, url, priority)
                       VALUES (%s, 'FRONTIER', %s, %s)
                       ON CONFLICT (url) DO NOTHING
                       RETURNING id""",
                    (site_id, url, priority),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self._release(conn)

    def pop_next_frontier(self) -> dict | None:
        """Atomically claim the highest-priority FRONTIER page."""
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE crawldb.page
                       SET page_type = 'HTML'       -- mark as in-progress
                       WHERE id = (
                           SELECT id FROM crawldb.page
                           WHERE page_type = 'FRONTIER'
                           ORDER BY priority ASC
                           LIMIT 1
                           FOR UPDATE SKIP LOCKED
                       )
                       RETURNING id, url, site_id""",
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {"id": row[0], "url": row[1], "site_id": row[2]}
        finally:
            self._release(conn)

    def frontier_count(self) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM crawldb.page WHERE page_type = 'FRONTIER'"
                )
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    def crawled_count(self) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM crawldb.page WHERE page_type != 'FRONTIER'"
                )
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    # ----------------------------------------------------------
    # Pages – storing results
    # ----------------------------------------------------------

    def store_page(self, page_id: int, html: str, status: int, page_type: str = "HTML"):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE crawldb.page
                       SET html_content = %s,
                           http_status_code = %s,
                           page_type = %s,
                           accessed_time = %s
                       WHERE id = %s""",
                    (html, status, page_type, datetime.now(), page_id),
                )
        finally:
            self._release(conn)

    def mark_duplicate(self, page_id: int, canonical_id: int, status: int):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                # Store reference to canonical via a link
                cur.execute(
                    """UPDATE crawldb.page
                       SET page_type = 'DUPLICATE',
                           http_status_code = %s,
                           accessed_time = %s
                       WHERE id = %s""",
                    (status, datetime.now(), page_id),
                )
                # Record duplicate -> canonical link
                cur.execute(
                    """INSERT INTO crawldb.link (from_page, to_page)
                       VALUES (%s, %s)
                       ON CONFLICT DO NOTHING""",
                    (page_id, canonical_id),
                )
        finally:
            self._release(conn)

    def store_binary(self, page_id: int, data: bytes, data_type: str, status: int):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE crawldb.page
                       SET page_type = 'BINARY',
                           http_status_code = %s,
                           accessed_time = %s
                       WHERE id = %s""",
                    (status, datetime.now(), page_id),
                )
                cur.execute(
                    """INSERT INTO crawldb.page_data (page_id, data_type, data)
                       VALUES (%s, %s, %s)""",
                    (page_id, data_type, psycopg2.Binary(data)),
                )
        finally:
            self._release(conn)

    def store_image(self, page_id: int, filename: str, content_type: str, data: bytes):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO crawldb.image (page_id, filename, content_type, data, accessed_time)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (page_id, filename, content_type, psycopg2.Binary(data), datetime.now()),
                )
        finally:
            self._release(conn)

    # ----------------------------------------------------------
    # Links
    # ----------------------------------------------------------

    def store_link(self, from_id: int, to_url: str, site_id: int, priority: float = 1.0):
        """Store a link and ensure the target page exists in the frontier."""
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                # Upsert target page
                cur.execute(
                    """INSERT INTO crawldb.page (site_id, page_type, url, priority)
                       VALUES (%s, 'FRONTIER', %s, %s)
                       ON CONFLICT (url) DO NOTHING
                       RETURNING id""",
                    (site_id, to_url, priority),
                )
                row = cur.fetchone()
                if row:
                    to_id = row[0]
                else:
                    cur.execute(
                        "SELECT id FROM crawldb.page WHERE url = %s", (to_url,)
                    )
                    to_id = cur.fetchone()[0]

                cur.execute(
                    """INSERT INTO crawldb.link (from_page, to_page)
                       VALUES (%s, %s)
                       ON CONFLICT DO NOTHING""",
                    (from_id, to_id),
                )
        finally:
            self._release(conn)

    # ----------------------------------------------------------
    # README / priority helpers
    # ----------------------------------------------------------

    def update_page_priority(self, page_id: int, priority: float):
        """Update crawl priority — called after README scoring."""
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE crawldb.page SET priority = %s WHERE id = %s",
                    (priority, page_id),
                )
        finally:
            self._release(conn)

    # ----------------------------------------------------------
    # Duplicate detection
    # ----------------------------------------------------------

    def find_page_by_hash(self, content_hash: str) -> int | None:
        """Return page_id of a page whose content hash matches, or None."""
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """SELECT id FROM crawldb.page
                       WHERE page_type = 'HTML'
                         AND md5(html_content) = %s
                       LIMIT 1""",
                    (content_hash,),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self._release(conn)


# ============================================================
# robots.txt helper
# ============================================================

class RobotsCache:
    """Per-domain robots.txt parser with caching."""

    def __init__(self, user_agent: str):
        self._cache: dict[str, RobotFileParser] = {}
        self._lock = threading.Lock()
        self._ua = user_agent

    def can_fetch(self, url: str) -> bool:
        parsed = urlsplit(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        with self._lock:
            if base not in self._cache:
                rp = RobotFileParser()
                rp.set_url(base + "/robots.txt")
                try:
                    rp.read()
                    log.debug(f"robots.txt loaded for {base}")
                except Exception as e:
                    log.warning(f"Could not fetch robots.txt for {base}: {e} — assuming allow-all")
                self._cache[base] = rp
            allowed = self._cache[base].can_fetch(self._ua, url)
            if not allowed:
                log.info(f"robots.txt BLOCKED: {url}")
            return allowed

    def get_raw(self, base_url: str) -> str:
        """Return raw robots.txt text."""
        try:
            r = requests.get(
                base_url + "/robots.txt",
                timeout=CRAWLER_CONFIG["request_timeout"],
                headers={"User-Agent": CRAWLER_CONFIG["user_agent"]},
            )
            return r.text if r.status_code == 200 else ""
        except Exception:
            return ""

    def get_sitemaps(self, base_url: str, robots_text: str) -> list[str]:
        """Parse Sitemap: lines from robots.txt."""
        sitemaps = []
        for line in robots_text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())
        return sitemaps


# ============================================================
# Priority / preferential crawling
# ============================================================

class PriorityCalculator:
    """Computes crawl priority for a URL given its surrounding context."""

    def __init__(self, target_description: str):
        self._target = target_description.strip()

    def compute(self, url: str, link_tag) -> float:
        """
        Returns a priority float. Lower = crawl sooner.
        0.0 = highest relevance, 1.0 = no relevance.
        """
        if not self._target:
            return 1.0  # no preference – FIFO

        window = 120
        surrounding = ""
        if link_tag is not None:
            text = link_tag.parent.get_text(" ", strip=True) if link_tag.parent else ""
            idx = text.find(link_tag.get_text(strip=True))
            start = max(0, idx - window)
            end = min(len(text), idx + window)
            surrounding = text[start:end]

        # Also include URL tokens
        url_text = re.sub(r"[/_\-.]", " ", urlsplit(url).path)
        combined = surrounding + " " + url_text

        if not combined.strip():
            return 1.0

        try:
            vec = CountVectorizer(stop_words="english")
            mat = vec.fit_transform([self._target, combined])
            sim = cosine_similarity(mat[0], mat[1])[0][0]
            return float(1.0 - sim)
        except Exception:
            return 1.0

    def score_readme(self, readme_text: str) -> float:
        """
        Score a repo directly from its README text against TARGET_DESCRIPTION.
        Called by GitHubExtractor after storing README — replaces the rough
        link-text estimate with real semantic signal (Option A).
        """
        if not self._target or not readme_text.strip():
            return 1.0
        try:
            vec = CountVectorizer(stop_words="english")
            mat = vec.fit_transform([self._target, readme_text])
            sim = cosine_similarity(mat[0], mat[1])[0][0]
            return float(1.0 - sim)
        except Exception:
            return 1.0


# ============================================================
# Page fetcher
# ============================================================

class Fetcher:
    """Downloads pages with requests (fast) or Selenium (JS)."""

    def __init__(self):
        self._driver_lock = threading.Lock()
        self._drivers: list = []  # pool of Selenium drivers

    def _make_driver(self):
        opts = FirefoxOptions()
        opts.add_argument("--headless")
        opts.set_preference("general.useragent.override", CRAWLER_CONFIG["user_agent"])
        service = Service(executable_path=CRAWLER_CONFIG["gecko_driver"])
        return webdriver.Firefox(service=service, options=opts)

    def _get_driver(self):
        with self._driver_lock:
            if self._drivers:
                return self._drivers.pop()
        if SELENIUM_AVAILABLE and CRAWLER_CONFIG["use_selenium"]:
            return self._make_driver()
        return None

    def _return_driver(self, driver):
        with self._driver_lock:
            self._drivers.append(driver)

    def fetch(self, url: str) -> dict:
        """
        Returns dict with keys:
          html, status, content_type, is_binary, binary_data, final_url
        """
        result = {
            "html": None,
            "status": 0,
            "content_type": "",
            "is_binary": False,
            "binary_data": None,
            "final_url": url,
        }

        # Check extension first
        path = urlsplit(url).path.lower()
        ext = "." + path.rsplit(".", 1)[-1] if "." in path.split("/")[-1] else ""
        if ext in BINARY_EXTENSIONS:
            return self._fetch_binary(url, ext, result)

        # Try requests first (fast, no JS)
        try:
            resp = requests.get(
                url,
                timeout=CRAWLER_CONFIG["request_timeout"],
                headers={"User-Agent": CRAWLER_CONFIG["user_agent"]},
                allow_redirects=True,
            )
            result["status"] = resp.status_code
            result["final_url"] = resp.url
            ct = resp.headers.get("Content-Type", "")
            result["content_type"] = ct

            if any(b in ct for b in ("application/pdf", "application/msword",
                                      "application/vnd", "application/zip")):
                result["is_binary"] = True
                result["binary_data"] = resp.content
                return result

            if resp.status_code == 200:
                html = resp.text
                # If page is JS-heavy (minimal body), re-fetch with Selenium
                if SELENIUM_AVAILABLE and CRAWLER_CONFIG["use_selenium"]:
                    soup = BeautifulSoup(html, "html.parser")
                    body_text = soup.get_text(strip=True)
                    if len(body_text) < 200:
                        return self._fetch_selenium(url, result)
                result["html"] = html
        except requests.RequestException as e:
            log.debug(f"requests failed for {url}: {e}")
            if SELENIUM_AVAILABLE and CRAWLER_CONFIG["use_selenium"]:
                return self._fetch_selenium(url, result)

        return result

    def _fetch_selenium(self, url: str, result: dict) -> dict:
        driver = self._get_driver()
        if driver is None:
            return result
        try:
            driver.get(url)
            time.sleep(CRAWLER_CONFIG["selenium_timeout"])
            result["html"] = driver.page_source
            result["status"] = 200
            result["final_url"] = driver.current_url
        except Exception as e:
            log.debug(f"Selenium failed for {url}: {e}")
        finally:
            self._return_driver(driver)
        return result

    def _fetch_binary(self, url: str, ext: str, result: dict) -> dict:
        try:
            resp = requests.get(
                url,
                timeout=CRAWLER_CONFIG["request_timeout"],
                headers={"User-Agent": CRAWLER_CONFIG["user_agent"]},
            )
            result["status"] = resp.status_code
            result["is_binary"] = True
            result["binary_data"] = resp.content
            result["content_type"] = resp.headers.get("Content-Type", "")
        except Exception as e:
            log.debug(f"Binary fetch failed for {url}: {e}")
        return result

    def shutdown(self):
        with self._driver_lock:
            for d in self._drivers:
                try:
                    d.quit()
                except Exception:
                    pass
            self._drivers.clear()


# ============================================================
# URL utilities
# ============================================================

def normalize_url(url: str) -> str:
    """Normalize a URL: lowercase scheme+host, remove fragment, sort params."""
    try:
        p = urlsplit(url)
        return p._replace(
            scheme=p.scheme.lower(),
            netloc=p.netloc.lower(),
            fragment="",
        ).geturl()
    except Exception:
        return url


def extract_links(html: str, base_url: str) -> list[tuple[str, object]]:
    """Return list of (absolute_url, a_tag) from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    parsed_base = urlsplit(base_url)
    base_origin = f"{parsed_base.scheme}://{parsed_base.netloc}"
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.startswith("javascript:") or href.startswith("mailto:"):
            continue
        abs_url = urljoin(base_url, href)
        abs_url = normalize_url(abs_url)
        links.append((abs_url, tag))
    return links


def extract_images(html: str, base_url: str) -> list[str]:
    """Return list of absolute image URLs."""
    soup = BeautifulSoup(html, "html.parser")
    imgs = []
    for tag in soup.find_all("img", src=True):
        src = urljoin(base_url, tag["src"])
        imgs.append(normalize_url(src))
    return imgs


def get_domain(url: str) -> str:
    p = urlsplit(url)
    return f"{p.scheme}://{p.netloc}"


def content_hash(html: str) -> str:
    """MD5 of stripped page text for duplicate detection."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(strip=True)
    return hashlib.md5(text.encode()).hexdigest()


def parse_sitemap(text: str, base_url: str) -> list[str]:
    """Extract <loc> URLs from a sitemap XML."""
    soup = BeautifulSoup(text, "xml")
    return [tag.text.strip() for tag in soup.find_all("loc")]



# ============================================================
# GitHub API extractor
# ============================================================

import re as _re
_REPO_URL_RE = _re.compile(
    r"^https://github\.com/([A-Za-z0-9_.\-]+)/([A-Za-z0-9_.\-]+)/?$"
)

def _is_repo_url(url: str):
    """Return (owner, repo) if url is a GitHub repo root, else None."""
    m = _REPO_URL_RE.match(url.rstrip("/"))
    return (m.group(1), m.group(2)) if m else None


class GitHubExtractor:
    """
    Calls GitHub REST API to extract structured FOSS repo data.
    Tables populated: repo, readme, issue, issue_comment,
                      topic, repo_topic, doc_link, foss_license.
    After storing README, re-scores the page priority via BoW (Option A).
    """
    API_BASE = "https://api.github.com"

    def __init__(self, db, priority_calc):
        self.db = db
        self._priority = priority_calc
        self._session = requests.Session()
        self._session.headers.update(GITHUB_HEADERS)
        self._session.headers.update({"User-Agent": CRAWLER_CONFIG["user_agent"]})

    # ── public ──────────────────────────────────────────────────────────────

    def extract(self, owner: str, repo: str, page_id: int):
        try:
            repo_id = self._extract_repo(owner, repo, page_id)
            if repo_id is None:
                return
            self._extract_readme(owner, repo, repo_id, page_id)
            self._extract_topics(owner, repo, repo_id)
            self._extract_issues(owner, repo, repo_id)
            log.info(f"GitHub extracted: {owner}/{repo} (repo_id={repo_id})")
        except Exception as e:
            log.warning(f"GitHub extraction failed for {owner}/{repo}: {e}")

    # ── repo metadata ────────────────────────────────────────────────────────

    def _extract_repo(self, owner, repo, page_id):
        r = self._get(f"/repos/{owner}/{repo}")
        if r is None:
            return None
        d = r.json()
        lic = self._resolve_license((d.get("license") or {}).get("spdx_id") or "OTHER")
        conn = self.db._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO crawldb.repo (
                        page_id, github_id, owner, name,
                        html_url, api_url, description, homepage_url,
                        license_id, primary_language,
                        stars, forks, open_issues, watchers,
                        is_fork, is_archived, is_disabled, default_branch,
                        repo_created_at, repo_updated_at, repo_pushed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (owner, name) DO UPDATE SET
                        stars=EXCLUDED.stars, forks=EXCLUDED.forks,
                        open_issues=EXCLUDED.open_issues,
                        repo_updated_at=EXCLUDED.repo_updated_at,
                        crawled_at=NOW()
                    RETURNING id""",
                    (page_id, d.get("id"), owner, repo,
                     d.get("html_url"), d.get("url"),
                     d.get("description"), d.get("homepage") or None,
                     lic, d.get("language"),
                     d.get("stargazers_count",0), d.get("forks_count",0),
                     d.get("open_issues_count",0), d.get("watchers_count",0),
                     d.get("fork",False), d.get("archived",False),
                     d.get("disabled",False), d.get("default_branch","main"),
                     d.get("created_at"), d.get("updated_at"), d.get("pushed_at")),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self.db._release(conn)

    # ── README + Option-A re-scoring ─────────────────────────────────────────

    def _extract_readme(self, owner, repo, repo_id, page_id):
        r = self._get(f"/repos/{owner}/{repo}/readme")
        if r is None:
            return
        d = r.json()
        import base64
        try:
            content = base64.b64decode(d.get("content","")).decode("utf-8", errors="replace")
        except Exception:
            return
        if not content:
            return

        conn = self.db._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("UPDATE crawldb.readme SET is_current=FALSE WHERE repo_id=%s", (repo_id,))
                cur.execute("""
                    INSERT INTO crawldb.readme (repo_id, content, sha, file_path, download_url, is_current)
                    VALUES (%s,%s,%s,%s,%s,TRUE)""",
                    (repo_id, content, d.get("sha"),
                     d.get("path","README.md"), d.get("download_url")),
                )
                # doc links from README
                for link_text, link_url in re.findall(r"\[([^\]]*)\]\((https?://[^)]+)\)", content)[:50]:
                    link_url = link_url.strip()
                    if len(link_url) < 3000:
                        cur.execute("""
                            INSERT INTO crawldb.doc_link (repo_id, url, link_text, source)
                            VALUES (%s,%s,%s,'readme') ON CONFLICT (repo_id, url) DO NOTHING""",
                            (repo_id, link_url, link_text[:499] if link_text else None),
                        )
        finally:
            self.db._release(conn)

        # Option A: re-score this repo page using README BoW
        score = self._priority.score_readme(content)
        self.db.update_page_priority(page_id, score)
        log.debug(f"README re-scored {owner}/{repo}: priority={score:.4f}")

    # ── topics ───────────────────────────────────────────────────────────────

    def _extract_topics(self, owner, repo, repo_id):
        r = self._get(f"/repos/{owner}/{repo}/topics")
        if r is None:
            return
        conn = self.db._conn()
        try:
            with conn:
                cur = conn.cursor()
                for name in r.json().get("names", []):
                    name = name.lower().strip()
                    cur.execute("INSERT INTO crawldb.topic (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", (name,))
                    row = cur.fetchone()
                    if not row:
                        cur.execute("SELECT id FROM crawldb.topic WHERE name=%s", (name,))
                        row = cur.fetchone()
                    cur.execute("INSERT INTO crawldb.repo_topic (repo_id,topic_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", (repo_id, row[0]))
        finally:
            self.db._release(conn)

    # ── issues ───────────────────────────────────────────────────────────────

    def _extract_issues(self, owner, repo, repo_id):
        for page in range(1, 3):
            r = self._get(f"/repos/{owner}/{repo}/issues", params={"state":"open","per_page":50,"page":page})
            if r is None:
                break
            issues = r.json()
            if not issues:
                break
            for issue in issues:
                if "pull_request" in issue:
                    continue
                self._store_issue(repo_id, owner, repo, issue)

    def _store_issue(self, repo_id, owner, repo, issue):
        conn = self.db._conn()
        issue_id = None
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO crawldb.issue (
                        repo_id, github_issue_id, issue_number,
                        title, body, state, author_login, reactions_total,
                        issue_created_at, issue_updated_at, issue_closed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (repo_id, github_issue_id) DO UPDATE
                        SET state=EXCLUDED.state, issue_updated_at=EXCLUDED.issue_updated_at
                    RETURNING id""",
                    (repo_id, issue["id"], issue["number"],
                     issue.get("title"), issue.get("body"),
                     issue.get("state","open"),
                     (issue.get("user") or {}).get("login"),
                     (issue.get("reactions") or {}).get("total_count",0),
                     issue.get("created_at"), issue.get("updated_at"), issue.get("closed_at")),
                )
                row = cur.fetchone()
                if not row:
                    return
                issue_id = row[0]
                for label in issue.get("labels", []):
                    cur.execute("INSERT INTO crawldb.label (name,color) VALUES (%s,%s) ON CONFLICT (name) DO NOTHING RETURNING id", (label["name"], label.get("color")))
                    lrow = cur.fetchone()
                    if not lrow:
                        cur.execute("SELECT id FROM crawldb.label WHERE name=%s", (label["name"],))
                        lrow = cur.fetchone()
                    cur.execute("INSERT INTO crawldb.issue_label (issue_id,label_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", (issue_id, lrow[0]))
        finally:
            self.db._release(conn)
        if issue_id:
            self._extract_comments(owner, repo, issue["number"], issue_id)

    def _extract_comments(self, owner, repo, issue_number, issue_id):
        r = self._get(f"/repos/{owner}/{repo}/issues/{issue_number}/comments", params={"per_page":10})
        if r is None:
            return
        conn = self.db._conn()
        try:
            with conn:
                cur = conn.cursor()
                for c in r.json():
                    cur.execute("""
                        INSERT INTO crawldb.issue_comment
                            (issue_id, github_comment_id, author_login, body, comment_created_at, comment_updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (github_comment_id) DO NOTHING""",
                        (issue_id, c["id"], (c.get("user") or {}).get("login"),
                         c.get("body"), c.get("created_at"), c.get("updated_at")),
                    )
        finally:
            self.db._release(conn)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _resolve_license(self, spdx_id):
        conn = self.db._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM crawldb.foss_license WHERE spdx_id=%s", (spdx_id,))
                row = cur.fetchone()
                if row:
                    return row[0]
                cur.execute("INSERT INTO crawldb.foss_license (spdx_id,full_name) VALUES (%s,%s) RETURNING id", (spdx_id, spdx_id))
                return cur.fetchone()[0]
        finally:
            self.db._release(conn)

    def _get(self, path, params=None):
        try:
            r = self._session.get(self.API_BASE + path, params=params, timeout=CRAWLER_CONFIG["request_timeout"])
            if r.status_code == 404:
                return None
            if r.status_code == 403:
                log.warning(f"GitHub rate limited: {path}")
                return None
            if r.status_code != 200:
                log.warning(f"GitHub API {r.status_code}: {path}")
                return None
            return r
        except Exception as e:
            log.warning(f"GitHub API error {path}: {e}")
            return None

# ============================================================
# Crawler worker
# ============================================================

class CrawlerWorker(threading.Thread):
    """One crawl thread. Pops URLs from DB frontier and processes them."""

    def __init__(
        self,
        db: CrawlerDB,
        fetcher: Fetcher,
        robots: RobotsCache,
        priority_calc: PriorityCalculator,
        domain_timestamps: dict,
        domain_lock: threading.Lock,
        stop_event: threading.Event,
        allowed_domains: list[str] | None,
        github_extractor: GitHubExtractor,
        name: str,
    ):
        super().__init__(name=name, daemon=True)
        self.db = db
        self.fetcher = fetcher
        self.robots = robots
        self.priority = priority_calc
        self._domain_timestamps = domain_timestamps
        self._domain_lock = domain_lock
        self._stop_event = stop_event
        self._allowed = [d.lower() for d in (allowed_domains or [])]
        self._github = github_extractor

    def run(self):
        while not self._stop_event.is_set():
            page = self.db.pop_next_frontier()
            if page is None:
                time.sleep(1)
                continue
            self._process(page)

    # ----------------------------------------------------------

    def _process(self, page: dict):
        url = page["url"]
        page_id = page["id"]
        site_id = page["site_id"]
        log.info(f"Crawling: {url}")

        # Domain restriction
        domain = get_domain(url)
        if self._allowed and not any(url.lower().startswith(d) for d in self._allowed):
            log.debug(f"Skipping (out of scope): {url}")
            return

        # Skip GitHub noise (file trees, commits, blame, etc.)
        GITHUB_SKIP_PATTERNS = [
            "/tree/", "/blob/", "/commits/", "/commit/",
            "/blame/", "/raw/", "/compare/", "/releases/",
            "/tags/", "/branches/", "/network/", "/graphs/",
            "/pulls/", "/milestone/", "/packages/", "/actions/",
        ]
        if "github.com" in url:
            if any(p in url for p in GITHUB_SKIP_PATTERNS):
                log.debug(f"Skipping GitHub noise: {url}")
                self.db.store_page(page_id, None, 0, "HTML")
                return

        # robots.txt check
        if not self.robots.can_fetch(url):
            log.debug(f"Blocked by robots.txt: {url}")
            self.db.store_page(page_id, None, 403, "HTML")
            return
            
        # Don't go deeper than 4 slashes past the domain
        path_depth = len([p for p in urlsplit(url).path.split("/") if p])
        if path_depth > 4:
            log.debug(f"Skipping deep URL: {url}")
            self.db.store_page(page_id, None, 0, "HTML")
            return

        # Crawl delay
        self._respect_delay(domain, site_id)

        # Fetch
        result = self.fetcher.fetch(url)
        status = result["status"]
        final_url = normalize_url(result["final_url"])

        # Redirect to different URL
        if final_url != url:
            redirect_site_id = self._ensure_site(final_url)
            new_id = self.db.add_frontier_url(final_url, redirect_site_id)
            if new_id:
                self.db.store_link(page_id, final_url, redirect_site_id)
            self.db.store_page(page_id, None, status, "HTML")
            return

        # Binary file
        if result["is_binary"]:
            path = urlsplit(url).path.lower()
            ext = "." + path.rsplit(".", 1)[-1] if "." in path else ""
            dtype = BINARY_EXTENSIONS.get(ext, "OTHER")
            if result["binary_data"]:
                self.db.store_binary(page_id, result["binary_data"], dtype, status)
            return

        # No HTML
        if not result["html"]:
            self.db.store_page(page_id, None, status, "HTML")
            return

        html = result["html"]

        # Duplicate detection
        chash = content_hash(html)
        existing = self.db.find_page_by_hash(chash)
        if existing and existing != page_id:
            log.debug(f"Duplicate of page {existing}: {url}")
            self.db.mark_duplicate(page_id, existing, status)
            return

        # Store page
        self.db.store_page(page_id, html, status)

        # GitHub structured extraction + README re-scoring (Option A)
        repo_match = _is_repo_url(url)
        if repo_match:
            owner, repo_name = repo_match
            self._github.extract(owner, repo_name, page_id)

        # Extract and queue links — GitHub noise filtered BEFORE entering frontier
        FRONTIER_SKIP_PATTERNS = [
            "/tree/", "/blob/", "/commits/", "/commit/",
            "/blame/", "/raw/", "/compare/", "/releases/",
            "/tags/", "/branches/", "/network/", "/graphs/",
            "/pulls", "/pull/", "/milestone/", "/packages/",
            "/actions", "/discussions", "/sponsors", "/login",
            "/team", "/collections", "/resources/", "/settings",
            "/stargazers", "/watchers", "/forks", "/deployments",
            "/projects", "/security", "/pulse", "/marketplace",
            "/explore", "/trending", "/issues",
        ]
        links = extract_links(html, url)
        random.shuffle(links)
        links = links[:random.randint(20, 60)]
        for link_url, link_tag in links:
            # Domain restriction
            if self._allowed and not any(
                link_url.lower().startswith(d) for d in self._allowed
            ):
                continue

            if "github.com" in link_url:
                # Strip ALL query strings — always noise on GitHub
                if "?" in link_url:
                    link_url = link_url.split("?")[0].rstrip("/")
                    link_url = normalize_url(link_url)

                # Skip noise paths
                if any(p in link_url for p in FRONTIER_SKIP_PATTERNS):
                    continue

                # Only allow: github.com/owner or github.com/owner/repo (max depth 2)
                path_parts = [p for p in urlsplit(link_url).path.split("/") if p]
                if len(path_parts) > 2:
                    continue

            link_site_id = self._ensure_site(link_url)
            priority = self.priority.compute(link_url, link_tag)
            self.db.store_link(page_id, link_url, link_site_id, priority)

        # Store images
        for img_url in extract_images(html, url):
            try:
                r = requests.get(
                    img_url,
                    timeout=CRAWLER_CONFIG["request_timeout"],
                    headers={"User-Agent": CRAWLER_CONFIG["user_agent"]},
                )
                if r.status_code == 200:
                    filename = img_url.split("/")[-1].split("?")[0] or "image"
                    ct = r.headers.get("Content-Type", "image/unknown")
                    self.db.store_image(page_id, filename, ct, r.content)
            except Exception:
                pass

    # ----------------------------------------------------------

    def _respect_delay(self, domain: str, site_id: int):
        delay = self.db.get_crawl_delay(site_id)
        with self._domain_lock:
            last = self._domain_timestamps.get(domain, 0)
            wait = delay - (time.time() - last)
            if wait > 0:
                time.sleep(wait)
            self._domain_timestamps[domain] = time.time()

    def _ensure_site(self, url: str) -> int:
        domain = get_domain(url)
        return self.db.get_or_create_site(domain)


# ============================================================
# Main Crawler orchestrator
# ============================================================

class Crawler:
    """
    Orchestrates the full crawl:
      - Seeds the frontier
      - Fetches robots.txt / sitemaps for each new domain
      - Spawns worker threads
    """

    def __init__(
        self,
        seed_urls: list[str],
        allowed_domains: list[str] | None = None,
        target_description: str = "",
    ):
        self.seed_urls = [normalize_url(u) for u in seed_urls]
        self.allowed_domains = allowed_domains
        self.target_description = target_description or CRAWLER_CONFIG["target_description"]

        self.db = CrawlerDB(DB_CONFIG, pool_size=CRAWLER_CONFIG["num_workers"] * 2 + 2)
        self.fetcher = Fetcher()
        self.robots = RobotsCache(CRAWLER_CONFIG["user_agent"])
        self.priority_calc = PriorityCalculator(self.target_description)
        self.github_extractor = GitHubExtractor(self.db, self.priority_calc)

        self._domain_timestamps: dict[str, float] = {}
        self._domain_lock = threading.Lock()
        self._stop_event = threading.Event()

    # ----------------------------------------------------------

    def _bootstrap_domain(self, url: str) -> int:
        """Fetch robots.txt + sitemap for a domain and return site_id."""
        domain = get_domain(url)
        site_id = self.db.get_or_create_site(domain)

        robots_text = self.robots.get_raw(domain)
        sitemap_urls = self.robots.get_sitemaps(domain, robots_text)

        # Fetch first sitemap
        sitemap_text = ""
        for sm_url in sitemap_urls[:1]:
            try:
                r = requests.get(
                    sm_url,
                    timeout=CRAWLER_CONFIG["request_timeout"],
                    headers={"User-Agent": CRAWLER_CONFIG["user_agent"]},
                )
                if r.status_code == 200:
                    sitemap_text = r.text
                    for loc in parse_sitemap(sitemap_text, domain):
                        loc = normalize_url(loc)
                        self.db.add_frontier_url(loc, site_id, priority=0.5)
            except Exception:
                pass

        # Extract crawl delay from robots
        delay = CRAWLER_CONFIG["default_delay"]
        for line in robots_text.splitlines():
            if line.lower().startswith("crawl-delay:"):
                try:
                    delay = int(line.split(":", 1)[1].strip())
                except ValueError:
                    pass

        self.db.update_site_robots(site_id, robots_text, sitemap_text, delay)
        return site_id

    # ----------------------------------------------------------

    def run(self):
        log.info("=== Crawler starting ===")

        # Seed the frontier
        for url in self.seed_urls:
            site_id = self._bootstrap_domain(url)
            self.db.add_frontier_url(url, site_id, priority=0.0)
            log.info(f"Seeded: {url}")

        # Spawn workers
        workers = []
        for i in range(CRAWLER_CONFIG["num_workers"]):
            w = CrawlerWorker(
                db=self.db,
                fetcher=self.fetcher,
                robots=self.robots,
                priority_calc=self.priority_calc,
                domain_timestamps=self._domain_timestamps,
                domain_lock=self._domain_lock,
                stop_event=self._stop_event,
                allowed_domains=self.allowed_domains,
                github_extractor=self.github_extractor,
                name=f"Worker-{i+1}",
            )
            w.start()
            workers.append(w)
            log.info(f"Started {w.name}")

        # Monitor loop
        max_pages = CRAWLER_CONFIG["max_pages"]
        try:
            while True:
                crawled = self.db.crawled_count()
                frontier = self.db.frontier_count()
                log.info(f"Status: crawled={crawled}, frontier={frontier}")

                if max_pages and crawled >= max_pages:
                    log.info(f"Reached max_pages={max_pages}. Stopping.")
                    break
                if frontier == 0:
                    time.sleep(10)
                    if self.db.frontier_count() == 0:
                        time.sleep(10)  # second check — workers may still be adding links
                        if self.db.frontier_count() == 0:
                            log.info("Frontier empty. Stopping.")
                            break
                time.sleep(10)
        except KeyboardInterrupt:
            log.info("Interrupted by user.")
        finally:
            self._stop_event.set()
            for w in workers:
                w.join(timeout=15)
            self.fetcher.shutdown()
            log.info("=== Crawler stopped ===")
            log.info(f"Total crawled: {self.db.crawled_count()}")


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    SEED_URLS = [
        "https://github.com/topics/foss",
        "https://github.com/topics/open-source",
        "https://github.com/topics/free-software",
    ]
    ALLOWED_DOMAINS = ["https://github.com"]

    TARGET_DESCRIPTION = "open source foss free software repository project"

    crawler = Crawler(
        seed_urls=SEED_URLS,
        allowed_domains=ALLOWED_DOMAINS,
        target_description=TARGET_DESCRIPTION,
    )
    crawler.run()
