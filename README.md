# WIER PA1 – Preferential Web Crawler

Single-file crawler (`crawler.py`) backed by PostgreSQL **`crawldb`**: multi-threaded workers, frontier in the DB, robots.txt + sitemaps, IP politeness (min **5 s** per IP by default), URL canonicalization, preferential priority (BoW cosine similarity), exact duplicates (content hash), optional near-duplicates (MinHash LSH + Jaccard), `href` + `onclick` links, `img` extraction, HTML in `page.html_content`, binary metadata in `page_data` with **NULL** payloads by default.

## Folder structure

```
web-assignment-1/
├── crawler.py              ← PA1 crawler (all logic in one file)
├── requirements.txt
├── init-scripts/
│   └── database.sql        ← crawldb schema (Docker init or manual load)
├── pgdata/                 ← Postgres data (Docker volume)
└── geckodriver             ← optional; for Selenium + Firefox
```

---

## 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

---

## 2. GeckoDriver + Firefox (optional)

Only needed if you run with **`--use-selenium 1`** (default in config). Download GeckoDriver: https://github.com/mozilla/geckodriver/releases  

Place the binary next to `crawler.py` and set **`--gecko-driver`** (default in code: `./geckodriver`; on Windows you may use `geckodriver.exe`).

Use **`--use-selenium 0`** for **requests-only** mode (no Firefox, faster, no JS rendering in the browser).

---

## 3. `GITHUB_TOKEN` (optional)

If set, HTTP requests to **`github.com`**, **`*.github.com`**, and **`api.github.com`** send **`Authorization: Bearer …`** (and `Accept` for the API host). This can help with GitHub rate limits on HTML and API traffic; **politeness remains `default_delay` (5 s) per IP** unless you change config.

```powershell
# Windows PowerShell (current session)
$env:GITHUB_TOKEN="ghp_yourtoken"
```

```cmd
set "GITHUB_TOKEN=ghp_yourtoken"
```

---

## 4. PostgreSQL (Docker)

```bash
# Linux / macOS
docker run --name postgresql-wier \
    -e POSTGRES_PASSWORD=SecretPassword \
    -e POSTGRES_USER=user \
    -e POSTGRES_DB=wier \
    -v "$PWD/pgdata:/var/lib/postgresql/data" \
    -v "$PWD/init-scripts:/docker-entrypoint-initdb.d" \
    -p 5432:5432 \
    -d pgvector/pgvector:pg17
```

Windows **cmd** (use `%CD%`, not `$PWD`):

```cmd
docker run --name postgresql-wier ^
    -e POSTGRES_PASSWORD=SecretPassword ^
    -e POSTGRES_USER=user ^
    -e POSTGRES_DB=wier ^
    -v "%CD%\pgdata:/var/lib/postgresql/data" ^
    -v "%CD%\init-scripts:/docker-entrypoint-initdb.d" ^
    -p 5432:5432 ^
    -d pgvector/pgvector:pg17
```

Load or refresh the schema (if not applied by init). The DDL file is **`init-scripts/database.sql`** (PA1 `crawldb`; this replaces the older `db_test.sql` name).

```bash
docker exec -i postgresql-wier psql -U user -d wier < init-scripts/database.sql
```

Credentials match `DB_CONFIG` in `crawler.py` (`user` / `SecretPassword` / `wier`), overridable with **`--db-*`** flags.

---

## 5. Configure / CLI

```bash
python crawler.py --help
```

| Flag / setting | Default (typical) | Notes |
|----------------|-------------------|--------|
| `--workers` | `5` | Worker threads |
| `--max-pages` | `5000` | Stop after this many **HTML** pages stored |
| `--use-selenium` | `1` | `0` = requests only, `1` = Firefox when needed |
| `--gecko-driver` | `./geckodriver` | Path to GeckoDriver |
| `--seed` | (repeatable) | Default seed if none: `https://github.com/topics/machine-learning` |
| `--allowed-domain` | (repeatable) | Default if none: `https://github.com` |
| `--target-description` | ML-related string | BoW priority target (see `--help` default) |
| `--db-host` … `--db-name` | `localhost`, `5432`, `user`, `SecretPassword`, `wier` | |

Key entries in `CRAWLER_CONFIG` inside the file: `default_delay` (**5**), `max_html_pages`, `near_duplicate_lsh`, `jaccard_threshold`, `store_*_payload` (all **False** = metadata only for binaries/images).

---

## 6. Run

Minimal:

```bash
python crawler.py
```

Example with explicit seeds and domain:

```powershell
python crawler.py `
  --workers 5 `
  --max-pages 5000 `
  --use-selenium 0 `
  --allowed-domain https://github.com `
  --seed https://github.com/topics/machine-learning `
  --target-description "open source machine learning repositories"
```

Typical log lines:

```
Status: html=120 frontier=3400
[Worker-1] INFO - Crawling https://github.com/...
[Worker-3] INFO - Near duplicate 0.880: 267718 -> 365
```

---

## 7. Reset `crawldb` (PA1 tables only)

This matches the current **`database.sql`** schema (no GitHub FOSS `repo` / `issue` tables):

```bash
docker exec -it postgresql-wier psql -U user -d wier -c "
TRUNCATE crawldb.link,
         crawldb.page_data,
         crawldb.image,
         crawldb.page_signature,
         crawldb.page,
         crawldb.crawl_run,
         crawldb.site
RESTART IDENTITY CASCADE;
"
```

If `TRUNCATE` fails on self-FK on `page`, run the same with only the tables your Postgres version accepts, or drop/recreate schema from `init-scripts/database.sql`.
