"""
WIER PA1 - Preferential Web Crawler
Assignment-compliant version

Features
--------
- Multi-threaded workers
- PostgreSQL crawldb backend
- Base schema preserved; only extensions used
- robots.txt + sitemap support
- IP-based politeness (min 5s per IP, configurable per robots crawl-delay)
- Canonicalized URLs only
- Preferential crawling with BoW cosine similarity
- Exact duplicate detection via content hash
- Near-duplicate detection via MinHash LSH + Jaccard (bonus)
- href + onclick link extraction
- img/src image extraction
- HTML storage in page.html_content
- Binary pages stored in page_data with NULL payload by default
"""

from __future__ import annotations

import argparse
import hashlib
import os
import logging
import math
import random
import re
import socket
import struct
import threading
import time
from datetime import datetime
from typing import Optional
from urllib.parse import parse_qsl, urlencode, unquote, urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import psycopg2
from psycopg2 import pool
import requests
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.firefox.service import Service
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False


# ============================================================
# Configuration
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "user",
    "password": "SecretPassword",
    "database": "wier",
}

CRAWLER_CONFIG = {
    "user_agent": "fri-wier-GroupE",
    "num_workers": 5,
    "default_delay": 5,   # assignment requires at most 1 request / 5 sec per IP
    "request_timeout": 10,
    "use_selenium": True,
    "selenium_timeout": 8,
    "gecko_driver": "./geckodriver",
    "max_html_pages": 5000,
    "target_description": "",
    "store_image_payload": False,
    "store_binary_payload": False,
    "store_pdf_payload": False,
    "near_duplicate_lsh": True,
    "jaccard_threshold": 0.88,
    "lsh_bands": 16,
    "lsh_rows": 4,
    "near_dup_min_unigrams": 20,
    "near_dup_max_unigrams": 8000,
    "max_url_depth": 6,
    "max_links_per_page": 100,
}

BINARY_EXTENSIONS = {
    ".pdf":  "PDF",
    ".doc":  "DOC",
    ".docx": "DOCX",
    ".ppt":  "PPT",
    ".pptx": "PPTX",
    ".xls":  "XLS",
    ".xlsx": "XLSX",
}

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp"}

# GitHub: optional token for higher rate limits on github.com / api.github.com (same idea as crawler.py).
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()


def http_request_headers(url: str) -> dict:
    """
    User-Agent is always set. If GITHUB_TOKEN is set, add Bearer auth for GitHub hosts
    (HTML pages on github.com and any api.github.com calls).
    """
    headers = {"User-Agent": CRAWLER_CONFIG["user_agent"]}
    host = (urlsplit(url).hostname or "").lower()
    if GITHUB_TOKEN and (
        host == "github.com"
        or host.endswith(".github.com")
        or host == "api.github.com"
    ):
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    if host == "api.github.com":
        headers["Accept"] = "application/vnd.github+json"
    return headers


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

if GITHUB_TOKEN:
    log.info("GITHUB_TOKEN set — using Bearer auth for github.com / api.github.com HTTP requests")
else:
    log.warning(
        "No GITHUB_TOKEN — GitHub requests are unauthenticated (stricter rate limits). "
        "Set GITHUB_TOKEN for better limits on GitHub-hosted pages."
    )


# ============================================================
# Utilities
# ============================================================

def _strip_default_port(scheme: str, netloc: str) -> str:
    netloc = netloc.lower()
    if scheme == "http" and netloc.endswith(":80"):
        return netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        return netloc[:-4]
    return netloc


def _normalize_path(path: str) -> str:
    raw = unquote(path or "")
    if not raw.startswith("/"):
        raw = "/" + raw
    parts = []
    for part in raw.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
        else:
            parts.append(part)
    out = "/" + "/".join(parts) if parts else "/"
    if len(out) > 1 and out.endswith("/"):
        out = out.rstrip("/")
    return out


def normalize_url(url: str) -> str:
    url = url.strip()
    try:
        p = urlsplit(url)
        scheme = (p.scheme or "").lower()
        netloc = p.netloc or ""
        if not scheme and netloc:
            scheme = "https"
        if not scheme or not netloc:
            return url
        netloc = _strip_default_port(scheme, netloc)
        path = _normalize_path(p.path)
        params = parse_qsl(p.query, keep_blank_values=True)
        params.sort(key=lambda kv: (kv[0], kv[1]))
        query = urlencode(params, doseq=True)
        return urlunsplit((scheme, netloc, path, query, ""))
    except Exception:
        return url


def get_domain(url: str) -> str:
    p = urlsplit(normalize_url(url))
    return f"{p.scheme}://{p.netloc}"


def content_hash(html: str) -> str:
    return hashlib.md5(html.encode("utf-8", errors="replace")).hexdigest()


def visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


# ============================================================
# Onclick extraction
# ============================================================

_ONCLICK_URL_RES = (
    re.compile(r"""location\.href\s*=\s*["']([^"'\\]+)["']""", re.I),
    re.compile(r"""window\.location(?:\.href)?\s*=\s*["']([^"'\\]+)["']""", re.I),
    re.compile(r"""document\.location(?:\.href)?\s*=\s*["']([^"'\\]+)["']""", re.I),
    re.compile(r"""(?:top|parent|self)\.location(?:\.href)?\s*=\s*["']([^"'\\]+)["']""", re.I),
    re.compile(r"""location\.(?:assign|replace)\s*\(\s*["']([^"'\\]+)["']""", re.I),
    re.compile(r"""window\.open\s*\(\s*["']([^"'\\]+)["']""", re.I),
)


def _raw_targets_from_onclick(handler: str) -> list[str]:
    out = []
    seen = set()
    if not handler:
        return out
    for pat in _ONCLICK_URL_RES:
        for m in pat.finditer(handler):
            u = m.group(1).strip()
            if u and u not in seen:
                seen.add(u)
                out.append(u)
    return out


def _is_extractable_href(raw: str) -> bool:
    if not raw:
        return False
    low = raw.strip().lower()
    if low.startswith(("javascript:", "mailto:", "tel:", "#")):
        return False
    return True


def extract_links(html: str, base_url: str) -> list[tuple[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not _is_extractable_href(href):
            continue
        abs_url = normalize_url(urljoin(base_url, href))
        if urlsplit(abs_url).netloc:
            links.append((abs_url, tag))

    for tag in soup.find_all(onclick=True):
        handler = tag.get("onclick")
        if handler is None:
            continue
        for raw in _raw_targets_from_onclick(str(handler)):
            if not _is_extractable_href(raw):
                continue
            abs_url = normalize_url(urljoin(base_url, raw))
            if urlsplit(abs_url).netloc:
                links.append((abs_url, tag))

    return links


def extract_images(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    imgs = []
    for tag in soup.find_all("img", src=True):
        src = normalize_url(urljoin(base_url, tag["src"]))
        if urlsplit(src).netloc:
            imgs.append(src)
    return imgs


def parse_sitemap(xml_text: str) -> list[str]:
    soup = BeautifulSoup(xml_text, "xml")
    urls = []
    for loc in soup.find_all("loc"):
        u = loc.text.strip()
        if u:
            urls.append(normalize_url(u))
    return urls


# ============================================================
# Near-duplicate detection
# ============================================================

def html_unigrams(html: str, max_tokens: int | None = None) -> frozenset[str]:
    if max_tokens is None:
        max_tokens = CRAWLER_CONFIG["near_dup_max_unigrams"]
    text = visible_text(html).lower()
    words = re.findall(r"[a-z0-9]+", text)
    if len(words) > max_tokens:
        step = max(1, len(words) // max_tokens)
        words = words[::step][:max_tokens]
    return frozenset(words)


def jaccard_similarity(a: frozenset[str], b: frozenset[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _minhash_component(j: int, token: str) -> int:
    digest = hashlib.md5(
        str(j).encode("ascii") + b"\x00" + token.encode("utf-8", errors="replace")
    ).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def minhash_signature(unigrams: frozenset[str], num_perm: int) -> tuple[int, ...]:
    if not unigrams:
        return tuple(0xFFFFFFFFFFFFFFFF for _ in range(num_perm))
    sig = []
    for j in range(num_perm):
        sig.append(min(_minhash_component(j, t) for t in unigrams))
    return tuple(sig)


def _band_bucket_hash(sub_signature: tuple[int, ...]) -> int:
    buf = b"".join(struct.pack(">Q", x & 0xFFFFFFFFFFFFFFFF) for x in sub_signature)
    return int.from_bytes(hashlib.md5(buf).digest()[:8], "big", signed=False)


class MinHashLSHIndex:
    def __init__(self, num_bands: int, rows_per_band: int):
        self.num_bands = num_bands
        self.rows_per_band = rows_per_band
        self.num_perm = num_bands * rows_per_band
        self._lock = threading.Lock()
        self._buckets: dict[tuple[int, int], list[int]] = {}

    def find_candidate_ids(self, signature: tuple[int, ...]) -> set[int]:
        out = set()
        r = self.rows_per_band
        with self._lock:
            for b in range(self.num_bands):
                sub = signature[b * r:(b + 1) * r]
                key = (b, _band_bucket_hash(sub))
                for pid in self._buckets.get(key, ()):
                    out.add(pid)
        return out

    def insert(self, page_id: int, signature: tuple[int, ...]) -> None:
        r = self.rows_per_band
        with self._lock:
            for b in range(self.num_bands):
                sub = signature[b * r:(b + 1) * r]
                key = (b, _band_bucket_hash(sub))
                self._buckets.setdefault(key, []).append(page_id)


# ============================================================
# Priority calculator
# ============================================================

class PriorityCalculator:
    def __init__(self, target_description: str):
        self.target = target_description.strip()

    def _sim(self, a: str, b: str) -> float:
        a = (a or "").strip()
        b = (b or "").strip()
        if not self.target or not a and not b:
            return 0.0
        try:
            vec = CountVectorizer(stop_words="english")
            mat = vec.fit_transform([a, b])
            return float(cosine_similarity(mat[0], mat[1])[0][0])
        except Exception:
            return 0.0

    def compute(self, url: str, link_tag) -> float:
        if not self.target:
            return 1.0

        anchor_text = ""
        context = ""
        if link_tag is not None:
            anchor_text = link_tag.get_text(" ", strip=True)
            parent_text = link_tag.parent.get_text(" ", strip=True) if link_tag.parent else ""
            context = parent_text[:300]

        url_text = re.sub(r"[/_\-.]", " ", urlsplit(url).path.lower())

        s1 = self._sim(self.target, f"{anchor_text} {context}")
        s2 = self._sim(self.target, url_text)

        depth = len([p for p in urlsplit(url).path.split("/") if p])
        depth_penalty = min(depth / 10.0, 1.0)

        score = 0.65 * (1.0 - s1) + 0.25 * (1.0 - s2) + 0.10 * depth_penalty
        return max(0.0, min(1.0, score))


# ============================================================
# Robots cache
# ============================================================

class RobotsCache:
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self._cache: dict[str, RobotFileParser] = {}
        self._lock = threading.Lock()

    def _ensure(self, base_url: str) -> RobotFileParser:
        with self._lock:
            if base_url not in self._cache:
                rp = RobotFileParser()
                rp.set_url(base_url + "/robots.txt")
                try:
                    rp.read()
                except Exception:
                    rp = RobotFileParser()
                self._cache[base_url] = rp
            return self._cache[base_url]

    def can_fetch(self, url: str) -> bool:
        base = get_domain(url)
        rp = self._ensure(base)
        try:
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            return True

    def get_raw(self, base_url: str) -> str:
        try:
            r = requests.get(
                base_url + "/robots.txt",
                timeout=CRAWLER_CONFIG["request_timeout"],
                headers=http_request_headers(base_url + "/robots.txt"),
            )
            robots_text = r.text if r.status_code == 200 else ""
        except Exception:
            robots_text = ""

        # Parse and cache so later calls (crawl-delay / can_fetch) do not trigger
        # an additional unthrottled rp.read() HTTP request.
        with self._lock:
            if base_url not in self._cache:
                rp = RobotFileParser()
                rp.set_url(base_url + "/robots.txt")
                try:
                    rp.parse(robots_text.splitlines())
                except Exception:
                    rp = RobotFileParser()
                    rp.set_url(base_url + "/robots.txt")
                    try:
                        rp.parse(robots_text.splitlines())
                    except Exception:
                        pass
                self._cache[base_url] = rp
        return robots_text

    def get_crawl_delay(self, base_url: str) -> int:
        rp = self._ensure(base_url)
        try:
            delay = rp.crawl_delay(self.user_agent)
            if delay is None:
                delay = rp.crawl_delay("*")
            if delay is None:
                return CRAWLER_CONFIG["default_delay"]
            return max(CRAWLER_CONFIG["default_delay"], int(math.ceil(float(delay))))
        except Exception:
            return CRAWLER_CONFIG["default_delay"]

    def get_sitemaps(self, robots_text: str) -> list[str]:
        out = []
        for line in robots_text.splitlines():
            if line.lower().startswith("sitemap:"):
                out.append(line.split(":", 1)[1].strip())
        return out


# ============================================================
# Fetcher
# ============================================================

class Fetcher:
    def __init__(self):
        self._driver_lock = threading.Lock()
        self._drivers = []

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
        result = {
            "html": None,
            "status": 0,
            "content_type": "",
            "is_binary": False,
            "binary_data": None,
            "data_type_code": None,
            "final_url": url,
        }

        path = urlsplit(url).path.lower()
        ext = "." + path.rsplit(".", 1)[-1] if "." in path.split("/")[-1] else ""
        if ext in BINARY_EXTENSIONS:
            data_type = BINARY_EXTENSIONS.get(ext) or "OTHER"
            store_payload = (
                CRAWLER_CONFIG["store_pdf_payload"]
                if data_type == "PDF"
                else CRAWLER_CONFIG["store_binary_payload"]
            )
            result["data_type_code"] = data_type
            return self._fetch_binary(url, result, store_payload=store_payload)

        try:
            resp = requests.get(
                url,
                timeout=CRAWLER_CONFIG["request_timeout"],
                headers=http_request_headers(url),
                allow_redirects=True,
                stream=True,  # important for "HTML only": don't download binaries
            )
            result["status"] = resp.status_code
            result["final_url"] = resp.url
            result["content_type"] = resp.headers.get("Content-Type", "")

            ct = (result["content_type"] or "").lower()
            if any(
                x in ct
                for x in [
                    "application/pdf",
                    "application/msword",
                    "application/vnd",
                    "application/octet-stream",
                ]
            ):
                # Binary detected by Content-Type: infer a best-effort data type
                # so we can download payload bytes only if explicitly enabled.
                inferred_type = "OTHER"
                if "application/pdf" in ct:
                    inferred_type = "PDF"
                elif "application/msword" in ct:
                    inferred_type = "DOC"
                elif "wordprocessingml.document" in ct:
                    inferred_type = "DOCX"
                elif "powerpoint" in ct or "application/vnd.ms-powerpoint" in ct:
                    inferred_type = "PPTX" if "openxmlformats-officedocument.presentationml.presentation" in ct else "PPT"
                elif "excel" in ct or "spreadsheetml.sheet" in ct:
                    # XLS / XLSX are optional extensions in db_test.sql.
                    inferred_type = "XLSX" if "spreadsheetml.sheet" in ct else "XLS"

                store_payload = (
                    CRAWLER_CONFIG["store_pdf_payload"]
                    if inferred_type == "PDF"
                    else CRAWLER_CONFIG["store_binary_payload"]
                )

                result["is_binary"] = True
                result["data_type_code"] = inferred_type
                if store_payload:
                    result["binary_data"] = resp.content
                else:
                    result["binary_data"] = None
                try:
                    resp.close()
                except Exception:
                    pass
                return result

            if resp.status_code == 200:
                # HTML: read full body.
                html = resp.text
                body_text = visible_text(html)
                try:
                    resp.close()
                except Exception:
                    pass
                if SELENIUM_AVAILABLE and CRAWLER_CONFIG["use_selenium"] and len(body_text) < 200:
                    return self._fetch_selenium(url, result)
                result["html"] = html
                return result
        except requests.RequestException:
            pass

        if SELENIUM_AVAILABLE and CRAWLER_CONFIG["use_selenium"]:
            return self._fetch_selenium(url, result)

        return result

    def _fetch_binary(self, url: str, result: dict, store_payload: bool) -> dict:
        try:
            resp = requests.get(
                url,
                timeout=CRAWLER_CONFIG["request_timeout"],
                headers=http_request_headers(url),
                allow_redirects=True,
                stream=True,  # do not download unless explicitly requested
            )
            result["status"] = resp.status_code
            result["final_url"] = resp.url
            result["content_type"] = resp.headers.get("Content-Type", "")
            result["is_binary"] = True
            if store_payload:
                result["binary_data"] = resp.content
            else:
                result["binary_data"] = None
            try:
                resp.close()
            except Exception:
                pass
        except Exception:
            pass
        return result

    def _fetch_selenium(self, url: str, result: dict) -> dict:
        driver = self._get_driver()
        if driver is None:
            return result
        try:
            driver.set_page_load_timeout(CRAWLER_CONFIG["selenium_timeout"])
            driver.get(url)
            time.sleep(2)
            result["html"] = driver.page_source
            if not result["status"]:
                result["status"] = 200
            result["final_url"] = driver.current_url
        except Exception:
            pass
        finally:
            self._return_driver(driver)
        return result

    def shutdown(self):
        with self._driver_lock:
            for driver in self._drivers:
                try:
                    driver.quit()
                except Exception:
                    pass
            self._drivers.clear()


# ============================================================
# Database layer
# ============================================================

class CrawlerDB:
    def __init__(self, config: dict, pool_size: int = 10):
        self.pool = pool.ThreadedConnectionPool(1, pool_size, **config)

    def _conn(self):
        return self.pool.getconn()

    def _release(self, conn):
        self.pool.putconn(conn)

    # -------------------------------
    # Crawl run
    # -------------------------------

    def create_crawl_run(self, target_description: str) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO crawldb.crawl_run (target_description, status)
                    VALUES (%s, 'running')
                    RETURNING id
                    """,
                    (target_description,),
                )
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    def finish_crawl_run(self, crawl_run_id: int, pages_crawled: int):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.crawl_run
                    SET finished_at = NOW(),
                        status = 'done',
                        pages_crawled = %s
                    WHERE id = %s
                    """,
                    (pages_crawled, crawl_run_id),
                )
        finally:
            self._release(conn)

    # -------------------------------
    # Site
    # -------------------------------

    def get_or_create_site(self, domain: str) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM crawldb.site WHERE domain = %s", (domain,))
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

    def update_site_meta(self, site_id: int, robots: str, sitemap: str, crawl_delay: int):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.site
                    SET robots_content = %s,
                        sitemap_content = %s,
                        crawl_delay = %s
                    WHERE id = %s
                    """,
                    (robots, sitemap, crawl_delay, site_id),
                )
        finally:
            self._release(conn)

    def get_crawl_delay(self, site_id: int) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT crawl_delay FROM crawldb.site WHERE id = %s", (site_id,))
                row = cur.fetchone()
                if row and row[0]:
                    return max(CRAWLER_CONFIG["default_delay"], int(row[0]))
                return CRAWLER_CONFIG["default_delay"]
        finally:
            self._release(conn)

    # -------------------------------
    # Frontier
    # -------------------------------

    def add_frontier_url(self, url: str, site_id: int, crawl_run_id: int, priority: float = 1.0) -> Optional[int]:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM crawldb.page WHERE url = %s", (url,))
                row = cur.fetchone()
                if row:
                    return None
                cur.execute(
                    """
                    INSERT INTO crawldb.page (
                        site_id, page_type_code, url, priority, crawl_run_id
                    )
                    VALUES (%s, 'FRONTIER', %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    RETURNING id
                    """,
                    (site_id, url, priority, crawl_run_id),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self._release(conn)

    def pop_next_frontier(self) -> Optional[dict]:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.page
                    SET processing = TRUE
                    WHERE id = (
                        SELECT id
                        FROM crawldb.page
                        WHERE page_type_code = 'FRONTIER'
                          AND processing = FALSE
                          AND fetch_status IS NULL
                        ORDER BY priority ASC, id ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, url, site_id, crawl_run_id
                    """
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "id": row[0],
                    "url": row[1],
                    "site_id": row[2],
                    "crawl_run_id": row[3],
                }
        finally:
            self._release(conn)

    def frontier_count(self) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM crawldb.page
                    WHERE page_type_code = 'FRONTIER'
                      AND processing = FALSE
                      AND fetch_status IS NULL
                    """
                )
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    def crawled_html_count(self) -> int:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM crawldb.page
                    WHERE page_type_code = 'HTML'
                      AND accessed_time IS NOT NULL
                    """
                )
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    # -------------------------------
    # Page storage
    # -------------------------------

    def store_html_page(self, page_id: int, html: str, status: int, chash: str):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.page
                    SET page_type_code = 'HTML',
                        html_content = %s,
                        http_status_code = %s,
                        content_hash = %s,
                        accessed_time = %s,
                        fetch_status = NULL,
                        redirect_url = NULL,
                        canonical_page_id = NULL,
                        processing = FALSE
                    WHERE id = %s
                    """,
                    (html, status, chash, datetime.now(), page_id),
                )
        finally:
            self._release(conn)

    def mark_attempt_finished(
        self,
        page_id: int,
        status: int,
        fetch_status: str,
        redirect_url: str | None = None,
    ):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.page
                    SET html_content = NULL,
                        content_hash = NULL,
                        canonical_page_id = NULL,
                        http_status_code = %s,
                        accessed_time = %s,
                        fetch_status = %s,
                        redirect_url = %s,
                        processing = FALSE
                    WHERE id = %s
                    """,
                    (status, datetime.now(), fetch_status, redirect_url, page_id),
                )
        finally:
            self._release(conn)

    def mark_duplicate(self, page_id: int, canonical_id: int, status: int):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.page
                    SET page_type_code = 'DUPLICATE',
                        html_content = NULL,
                        http_status_code = %s,
                        accessed_time = %s,
                        canonical_page_id = %s,
                        content_hash = NULL,
                        fetch_status = NULL,
                        redirect_url = NULL,
                        processing = FALSE
                    WHERE id = %s
                    """,
                    (status, datetime.now(), canonical_id, page_id),
                )
        finally:
            self._release(conn)

    def cleanup_stale_processing(self, max_age_minutes: int = 30) -> int:
        """
        Reset stuck `processing=TRUE` frontier rows back to FALSE.
        Uses discovered_time (schema extension) as a conservative age signal.
        """
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.page
                    SET processing = FALSE
                    WHERE page_type_code = 'FRONTIER'
                      AND processing = TRUE
                      AND fetch_status IS NULL
                      AND (discovered_time IS NULL OR discovered_time < NOW() - (%s * INTERVAL '1 minute'))
                    """,
                    (int(max_age_minutes),),
                )
                return cur.rowcount or 0
        finally:
            self._release(conn)

    def iter_signatures(self) -> list[tuple[int, str]]:
        """Return (page_id, signature_text) rows for rebuilding in-memory LSH after restart."""
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT page_id, signature_text
                    FROM crawldb.page_signature
                    ORDER BY page_id ASC
                    """
                )
                return [(int(r[0]), str(r[1])) for r in cur.fetchall()]
        finally:
            self._release(conn)

    def store_binary_page(
        self,
        page_id: int,
        data_type_code: str,
        status: int,
        data: bytes | None = None,
        store_payload: bool = False,
    ):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE crawldb.page
                    SET page_type_code = 'BINARY',
                        html_content = NULL,
                        http_status_code = %s,
                        accessed_time = %s,
                        fetch_status = NULL,
                        redirect_url = NULL,
                        canonical_page_id = NULL,
                        content_hash = NULL,
                        processing = FALSE
                    WHERE id = %s
                    """,
                    (status, datetime.now(), page_id),
                )
                cur.execute(
                    """
                    INSERT INTO crawldb.page_data (page_id, data_type_code, data)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        page_id,
                        data_type_code,
                        psycopg2.Binary(data) if (store_payload and data) else None,
                    ),
                )
        finally:
            self._release(conn)

    def store_image(
        self,
        page_id: int,
        filename: str,
        content_type: str,
        url: str | None = None,
        data: bytes | None = None,
        store_payload: bool = False,
    ):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO crawldb.image (page_id, filename, content_type, url, data, accessed_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (page_id, url) WHERE url IS NOT NULL DO NOTHING
                    """,
                    (
                        page_id,
                        filename,
                        content_type,
                        url,
                        psycopg2.Binary(data) if (store_payload and data) else None,
                        datetime.now(),
                    ),
                )
        finally:
            self._release(conn)

    # -------------------------------
    # Links
    # -------------------------------

    def store_link(self, from_page_id: int, to_url: str, site_id: int, crawl_run_id: int, priority: float = 1.0):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO crawldb.page (site_id, page_type_code, url, priority, crawl_run_id)
                    VALUES (%s, 'FRONTIER', %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    RETURNING id
                    """,
                    (site_id, to_url, priority, crawl_run_id),
                )
                row = cur.fetchone()
                if row:
                    to_page_id = row[0]
                else:
                    cur.execute("SELECT id FROM crawldb.page WHERE url = %s", (to_url,))
                    to_page_id = cur.fetchone()[0]

                # If the URL already exists as an uncrawled frontier entry, improve its
                # priority when we encounter it again with stronger relevance.
                cur.execute(
                    """
                    UPDATE crawldb.page
                    SET priority = LEAST(priority, %s)
                    WHERE id = %s
                      AND page_type_code = 'FRONTIER'
                      AND fetch_status IS NULL
                    """,
                    (priority, to_page_id),
                )

                cur.execute(
                    """
                    INSERT INTO crawldb.link (from_page, to_page)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (from_page_id, to_page_id),
                )
        finally:
            self._release(conn)

    # -------------------------------
    # Priority helpers
    # -------------------------------

    def update_page_priority(self, page_id: int, priority: float):
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

    # -------------------------------
    # Duplicate helpers
    # -------------------------------

    def find_page_by_content_hash(self, chash: str) -> Optional[int]:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id
                    FROM crawldb.page
                    WHERE page_type_code = 'HTML'
                      AND content_hash = %s
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (chash,),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self._release(conn)

    def get_html_for_page(self, page_id: int) -> Optional[str]:
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT html_content
                    FROM crawldb.page
                    WHERE id = %s
                      AND page_type_code = 'HTML'
                    """,
                    (page_id,),
                )
                row = cur.fetchone()
                return row[0] if row and row[0] else None
        finally:
            self._release(conn)

    def store_signature(self, page_id: int, signature: tuple[int, ...]):
        conn = self._conn()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO crawldb.page_signature (page_id, signature_text)
                    VALUES (%s, %s)
                    ON CONFLICT (page_id) DO UPDATE
                    SET signature_text = EXCLUDED.signature_text
                    """,
                    (page_id, ",".join(str(x) for x in signature)),
                )
        finally:
            self._release(conn)


# ============================================================
# Worker
# ============================================================

class CrawlerWorker(threading.Thread):
    def __init__(
        self,
        db: CrawlerDB,
        fetcher: Fetcher,
        robots: RobotsCache,
        priority_calc: PriorityCalculator,
        ip_timestamps: dict,
        ip_lock: threading.Lock,
        dns_cache: dict,
        dns_lock: threading.Lock,
        stop_event: threading.Event,
        allowed_domains: list[str] | None,
        near_dup_index: MinHashLSHIndex | None,
        name: str,
    ):
        super().__init__(name=name, daemon=True)
        self.db = db
        self.fetcher = fetcher
        self.robots = robots
        self.priority_calc = priority_calc
        self.ip_timestamps = ip_timestamps
        self.ip_lock = ip_lock
        self.dns_cache = dns_cache
        self.dns_lock = dns_lock
        self.stop_event = stop_event
        self.allowed_domains = [d.lower() for d in (allowed_domains or [])]
        self.near_dup_index = near_dup_index

    def run(self):
        while not self.stop_event.is_set():
            page = self.db.pop_next_frontier()
            if page is None:
                time.sleep(1)
                continue
            try:
                self._process(page)
            except Exception as e:
                log.exception("Worker error on %s: %s", page["url"], e)
                self.db.mark_attempt_finished(page["id"], 0, "WORKER_ERROR")

    def _process(self, page: dict):
        page_id = page["id"]
        url = page["url"]
        site_id = page["site_id"]
        crawl_run_id = page["crawl_run_id"]

        log.info("Crawling %s", url)

        if self.allowed_domains and not any(url.lower().startswith(d) for d in self.allowed_domains):
            self.db.mark_attempt_finished(page_id, 0, "OUT_OF_SCOPE")
            return

        if not self.robots.can_fetch(url):
            log.info("Blocked by robots.txt: %s", url)
            # Not an actual HTTP 403; record as policy decision
            self.db.mark_attempt_finished(page_id, 0, "ROBOTS_BLOCKED")
            return

        depth = len([p for p in urlsplit(url).path.split("/") if p])
        if depth > CRAWLER_CONFIG["max_url_depth"]:
            self.db.mark_attempt_finished(page_id, 0, "DEPTH_SKIPPED")
            return

        self._respect_ip_delay(url, site_id)

        result = self.fetcher.fetch(url)
        status = result["status"]
        final_url = normalize_url(result["final_url"])

        if final_url != url:
            redirect_site_id = self._ensure_site(final_url)
            self.db.store_link(page_id, final_url, redirect_site_id, crawl_run_id, priority=0.2)
            self.db.mark_attempt_finished(page_id, status, "REDIRECTED", redirect_url=final_url)
            return

        if result["is_binary"]:
            data_type = result.get("data_type_code") or "OTHER"
            store_payload = (
                CRAWLER_CONFIG["store_pdf_payload"]
                if data_type == "PDF"
                else CRAWLER_CONFIG["store_binary_payload"]
            )
            self.db.store_binary_page(
                page_id=page_id,
                data_type_code=data_type,
                status=status,
                data=result["binary_data"],
                store_payload=store_payload,
            )
            return

        html = result["html"]
        if not html:
            # Attempt finished but no HTML body retrieved
            self.db.mark_attempt_finished(page_id, status, "NO_HTML")
            return

        chash = content_hash(html)
        existing = self.db.find_page_by_content_hash(chash)
        if existing and existing != page_id:
            self.db.mark_duplicate(page_id, existing, status)
            return

        sig_nd = None
        uni_nd = None
        if CRAWLER_CONFIG["near_duplicate_lsh"] and self.near_dup_index is not None:
            uni_nd = html_unigrams(html)
            if len(uni_nd) >= CRAWLER_CONFIG["near_dup_min_unigrams"]:
                sig_nd = minhash_signature(uni_nd, self.near_dup_index.num_perm)
                candidates = self.near_dup_index.find_candidate_ids(sig_nd)
                near_matches = []
                for cand_id in candidates:
                    if cand_id == page_id:
                        continue
                    old_html = self.db.get_html_for_page(cand_id)
                    if not old_html:
                        continue
                    uni2 = html_unigrams(old_html)
                    j = jaccard_similarity(uni_nd, uni2)
                    if j >= CRAWLER_CONFIG["jaccard_threshold"]:
                        near_matches.append((j, cand_id))
                if near_matches:
                    near_matches.sort(key=lambda x: (-x[0], x[1]))
                    best_j, best_id = near_matches[0]
                    log.info("Near duplicate %.3f: %s -> %s", best_j, page_id, best_id)
                    self.db.mark_duplicate(page_id, best_id, status)
                    return

        self.db.store_html_page(page_id, html, status, chash)

        if sig_nd is not None:
            self.near_dup_index.insert(page_id, sig_nd)
            self.db.store_signature(page_id, sig_nd)

        links = extract_links(html, url)
        random.shuffle(links)
        links = links[:CRAWLER_CONFIG["max_links_per_page"]]

        # Deduplicate outgoing links from this page (efficiency)
        seen_links: set[str] = set()
        deduped_links: list[tuple[str, object]] = []
        for link_url, link_tag in links:
            if link_url in seen_links:
                continue
            seen_links.add(link_url)
            deduped_links.append((link_url, link_tag))
        links = deduped_links

        for link_url, link_tag in links:
            if self.allowed_domains and not any(link_url.lower().startswith(d) for d in self.allowed_domains):
                continue
            link_site_id = self._ensure_site(link_url)
            pr = self.priority_calc.compute(link_url, link_tag)
            self.db.store_link(page_id, link_url, link_site_id, crawl_run_id, priority=pr)

        # Store image metadata; only download image bytes if explicitly enabled.
        for img_url in extract_images(html, url):
            filename = img_url.split("/")[-1].split("?")[0] or "image"
            ct = "image/unknown"

            if CRAWLER_CONFIG["store_image_payload"]:
                # Respect robots + IP politeness for image payload downloads.
                if not self.robots.can_fetch(img_url):
                    continue
                try:
                    img_site_id = self._ensure_site(img_url)
                    self._respect_ip_delay(img_url, img_site_id)
                    r = requests.get(
                        img_url,
                        timeout=CRAWLER_CONFIG["request_timeout"],
                        headers=http_request_headers(img_url),
                        stream=True,
                        allow_redirects=True,
                    )
                    try:
                        if r.status_code == 200:
                            ct = r.headers.get("Content-Type", ct) or ct
                            self.db.store_image(
                                page_id=page_id,
                                filename=filename[:255],
                                content_type=ct[:50],
                                url=img_url[:3000],
                                data=r.content,
                                store_payload=True,
                            )
                    finally:
                        try:
                            r.close()
                        except Exception:
                            pass
                except Exception:
                    pass
            else:
                # No binary download: store only metadata with NULL payload.
                try:
                    self.db.store_image(
                        page_id=page_id,
                        filename=filename[:255],
                        content_type=ct[:50],
                        url=img_url[:3000],
                        data=None,
                        store_payload=False,
                    )
                except Exception:
                    pass

    def _ensure_site(self, url: str) -> int:
        return self.db.get_or_create_site(get_domain(url))

    def _resolve_hostname_ips(self, hostname: str) -> list[str]:
        if not hostname:
            return []
        hostname = hostname.lower()
        with self.dns_lock:
            if hostname in self.dns_cache:
                return list(self.dns_cache[hostname])

        ips = set()
        try:
            infos = socket.getaddrinfo(hostname, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM)
            for info in infos:
                sockaddr = info[4]
                if sockaddr and sockaddr[0]:
                    ips.add(sockaddr[0])
        except Exception:
            ips = set()

        with self.dns_lock:
            self.dns_cache[hostname] = ips
        return list(ips)

    def _respect_ip_delay(self, url: str, site_id: int):
        delay = self.db.get_crawl_delay(site_id)
        now = time.time()
        host = urlsplit(url).hostname
        ips = self._resolve_hostname_ips(host)
        if not ips:
            ips = [host.lower() if host else url]

        with self.ip_lock:
            next_allowed = now
            for ip in ips:
                last = self.ip_timestamps.get(ip, 0)
                wait = delay - (now - last)
                if wait > 0:
                    next_allowed = max(next_allowed, now + wait)
            for ip in ips:
                self.ip_timestamps[ip] = next_allowed

        sleep_for = next_allowed - now
        if sleep_for > 0:
            time.sleep(sleep_for)


# ============================================================
# Orchestrator
# ============================================================

class Crawler:
    def __init__(self, seed_urls: list[str], allowed_domains: list[str] | None, target_description: str):
        self.seed_urls = [normalize_url(u) for u in seed_urls]
        self.allowed_domains = [normalize_url(d) for d in (allowed_domains or [])]
        self.target_description = target_description

        self.db = CrawlerDB(DB_CONFIG, pool_size=CRAWLER_CONFIG["num_workers"] * 2 + 2)
        self.fetcher = Fetcher()
        self.robots = RobotsCache(CRAWLER_CONFIG["user_agent"])
        self.priority_calc = PriorityCalculator(target_description)
        self.stop_event = threading.Event()
        self.ip_timestamps = {}
        self.ip_lock = threading.Lock()
        self.dns_cache = {}
        self.dns_lock = threading.Lock()
        self.crawl_run_id = self.db.create_crawl_run(target_description)

        if CRAWLER_CONFIG["near_duplicate_lsh"]:
            self.near_dup_index = MinHashLSHIndex(
                num_bands=CRAWLER_CONFIG["lsh_bands"],
                rows_per_band=CRAWLER_CONFIG["lsh_rows"],
            )
        else:
            self.near_dup_index = None

        # Cleanup + warm-start helpers
        try:
            reset = self.db.cleanup_stale_processing(max_age_minutes=30)
            if reset:
                log.info("Reset %s stale processing FRONTIER rows", reset)
        except Exception:
            pass

        # Warm-start near-duplicate index from DB so restarts keep dedup working.
        if self.near_dup_index is not None:
            expected_len = self.near_dup_index.num_perm
            loaded = 0
            try:
                for page_id, sig_text in self.db.iter_signatures():
                    parts = [p for p in sig_text.split(",") if p.strip()]
                    if len(parts) != expected_len:
                        continue
                    try:
                        sig = tuple(int(p) for p in parts)
                    except Exception:
                        continue
                    self.near_dup_index.insert(int(page_id), sig)
                    loaded += 1
                if loaded:
                    log.info("Warm-started near-duplicate index with %s signatures", loaded)
            except Exception:
                pass

    def _resolve_hostname_ips(self, hostname: str) -> list[str]:
        """Resolve hostname to IPs for shared IP politeness throttling (bootstrap only)."""
        if not hostname:
            return []
        hostname = hostname.lower()

        with self.dns_lock:
            if hostname in self.dns_cache:
                return list(self.dns_cache[hostname])

        ips: set[str] = set()
        try:
            infos = socket.getaddrinfo(
                hostname, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
            )
            for info in infos:
                sockaddr = info[4]
                if sockaddr and sockaddr[0]:
                    ips.add(sockaddr[0])
        except Exception:
            ips = set()

        with self.dns_lock:
            self.dns_cache[hostname] = ips
        return list(ips)

    def _respect_ip_delay_bootstrap(self, url: str):
        """
        Enforce minimum politeness even during bootstrap:
        "no more often than once in 5 seconds per IP".
        """
        delay = max(5.0, float(CRAWLER_CONFIG["default_delay"]))
        now = time.time()
        host = urlsplit(url).hostname
        ips = self._resolve_hostname_ips(host or "")

        # If DNS fails, fall back to hostname-based throttling key.
        if not ips:
            ips = [host.lower() if host else url]

        with self.ip_lock:
            next_allowed = now
            for ip in ips:
                last = self.ip_timestamps.get(ip, 0)
                wait = delay - (now - last)
                if wait > 0:
                    next_allowed = max(next_allowed, now + wait)
            for ip in ips:
                self.ip_timestamps[ip] = next_allowed

        sleep_for = next_allowed - now
        if sleep_for > 0:
            time.sleep(sleep_for)

    def _bootstrap_domain(self, url: str) -> int:
        domain = get_domain(url)
        site_id = self.db.get_or_create_site(domain)

        # Rate-limit bootstrap requests as well (robots + sitemaps).
        self._respect_ip_delay_bootstrap(domain)
        robots_text = self.robots.get_raw(domain)
        sitemap_text = ""
        sitemap_urls = self.robots.get_sitemaps(robots_text)

        for sm_url in sitemap_urls[:2]:
            try:
                self._respect_ip_delay_bootstrap(sm_url)
                r = requests.get(
                    sm_url,
                    timeout=CRAWLER_CONFIG["request_timeout"],
                    headers=http_request_headers(sm_url),
                )
                if r.status_code == 200:
                    sitemap_text = r.text
                    for loc in parse_sitemap(sitemap_text):
                        self.db.add_frontier_url(loc, site_id, self.crawl_run_id, priority=0.5)
                try:
                    r.close()
                except Exception:
                    pass
            except Exception:
                pass

        crawl_delay = self.robots.get_crawl_delay(domain)
        self.db.update_site_meta(site_id, robots_text, sitemap_text, crawl_delay)
        return site_id

    def run(self):
        log.info("Crawler starting")

        for seed in self.seed_urls:
            site_id = self._bootstrap_domain(seed)
            self.db.add_frontier_url(seed, site_id, self.crawl_run_id, priority=0.0)
            log.info("Seeded %s", seed)

        workers = []
        for i in range(CRAWLER_CONFIG["num_workers"]):
            w = CrawlerWorker(
                db=self.db,
                fetcher=self.fetcher,
                robots=self.robots,
                priority_calc=self.priority_calc,
                ip_timestamps=self.ip_timestamps,
                ip_lock=self.ip_lock,
                dns_cache=self.dns_cache,
                dns_lock=self.dns_lock,
                stop_event=self.stop_event,
                allowed_domains=self.allowed_domains,
                near_dup_index=self.near_dup_index,
                name=f"Worker-{i+1}",
            )
            w.start()
            workers.append(w)

        try:
            while True:
                crawled = self.db.crawled_html_count()
                frontier = self.db.frontier_count()
                log.info("Status: html=%s frontier=%s", crawled, frontier)

                if crawled >= CRAWLER_CONFIG["max_html_pages"]:
                    log.info("Reached max_html_pages=%s", CRAWLER_CONFIG["max_html_pages"])
                    break

                if frontier == 0:
                    time.sleep(10)
                    if self.db.frontier_count() == 0:
                        break

                time.sleep(10)
        except KeyboardInterrupt:
            log.info("Interrupted")
        finally:
            self.stop_event.set()
            for w in workers:
                w.join(timeout=15)
            self.fetcher.shutdown()
            total = self.db.crawled_html_count()
            self.db.finish_crawl_run(self.crawl_run_id, total)
            log.info("Crawler stopped. Total HTML pages: %s", total)


# ============================================================
# Entry point
# ============================================================

def _csv_list(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WIER PA1 Preferential Crawler")

    parser.add_argument("--seed", action="append", default=[], help="Seed URL, repeatable")
    parser.add_argument("--seed-csv", type=str, default="", help="Comma-separated seed URLs")
    parser.add_argument("--allowed-domain", action="append", default=[], help="Allowed domain/prefix, repeatable")
    parser.add_argument("--allowed-domain-csv", type=str, default="", help="Comma-separated allowed domains")
    parser.add_argument("--workers", type=int, default=CRAWLER_CONFIG["num_workers"])
    parser.add_argument("--max-pages", type=int, default=CRAWLER_CONFIG["max_html_pages"])
    parser.add_argument("--target-description", type=str, default="open source machine learning repositories")
    parser.add_argument("--use-selenium", type=int, choices=[0, 1], default=1 if CRAWLER_CONFIG["use_selenium"] else 0)
    parser.add_argument("--gecko-driver", type=str, default=CRAWLER_CONFIG["gecko_driver"])

    parser.add_argument("--db-host", type=str, default=DB_CONFIG["host"])
    parser.add_argument("--db-port", type=int, default=DB_CONFIG["port"])
    parser.add_argument("--db-user", type=str, default=DB_CONFIG["user"])
    parser.add_argument("--db-password", type=str, default=DB_CONFIG["password"])
    parser.add_argument("--db-name", type=str, default=DB_CONFIG["database"])

    args = parser.parse_args()

    CRAWLER_CONFIG["num_workers"] = max(1, int(args.workers))
    CRAWLER_CONFIG["max_html_pages"] = max(1, int(args.max_pages))
    CRAWLER_CONFIG["use_selenium"] = bool(args.use_selenium)
    CRAWLER_CONFIG["gecko_driver"] = args.gecko_driver

    DB_CONFIG.update({
        "host": args.db_host,
        "port": int(args.db_port),
        "user": args.db_user,
        "password": args.db_password,
        "database": args.db_name,
    })

    seed_urls = list(args.seed)
    if args.seed_csv:
        seed_urls.extend(_csv_list(args.seed_csv))
    if not seed_urls:
        seed_urls = ["https://github.com/topics/machine-learning"]

    allowed_domains = list(args.allowed_domain)
    if args.allowed_domain_csv:
        allowed_domains.extend(_csv_list(args.allowed_domain_csv))
    if not allowed_domains:
        allowed_domains = ["https://github.com"]

    crawler = Crawler(
        seed_urls=seed_urls,
        allowed_domains=allowed_domains,
        target_description=args.target_description,
    )
    crawler.run()