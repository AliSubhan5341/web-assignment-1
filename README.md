# GitHub FOSS Crawler – Setup & Run Guide

## Folder structure

```
crawler/
├── crawler.py              ← main crawler (all logic in one file)
├── requirements.txt        ← Python dependencies
├── init-scripts/
│   └── database.sql        ← PostgreSQL schema (auto-run by Docker)
├── pgdata/                 ← Postgres data volume (created by Docker)
└── geckodriver             ← Firefox WebDriver binary (download separately)
```

---

## 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

---

## 2. Download GeckoDriver (Firefox WebDriver)

Download from: https://github.com/mozilla/geckodriver/releases

Place the binary next to `crawler.py`:
```bash
# Linux / macOS
tar -xzf geckodriver-*.tar.gz
chmod +x geckodriver
mv geckodriver /path/to/crawler/geckodriver

# then set "gecko_driver": "geckodriver" in CRAWLER_CONFIG
```

Set `use_selenium: False` in `CRAWLER_CONFIG` if you want requests-only mode (faster, no JS rendering).

---

## 3. Set GitHub token (recommended)

Without a token the GitHub API allows only **60 requests/hour** (1 req/min).
With a token it allows **5,000 requests/hour**.

Generate one at: https://github.com/settings/tokens
No scopes needed — leave everything unchecked for public repo access.

```fish
# Fish shell — persists across sessions
set -Ux GITHUB_TOKEN "ghp_yourtoken"

# Bash / Zsh
echo 'export GITHUB_TOKEN="ghp_yourtoken"' >> ~/.bashrc
source ~/.bashrc
```

The crawler detects the token automatically at startup and sets the crawl delay accordingly (`5s` authenticated, `60s` anonymous).

---

## 4. Start PostgreSQL with Docker

```bash
# Linux / macOS
docker run --name postgresql-wier \
    -e POSTGRES_PASSWORD=SecretPassword \
    -e POSTGRES_USER=user \
    -e POSTGRES_DB=wier \
    -v $PWD/pgdata:/var/lib/postgresql/data \
    -v $PWD/init-scripts:/docker-entrypoint-initdb.d \
    -p 5432:5432 \
    -d pgvector/pgvector:pg17

# Windows Command Prompt
docker run --name postgresql-wier ^
    -e POSTGRES_PASSWORD=SecretPassword ^
    -e POSTGRES_USER=user ^
    -e POSTGRES_DB=wier ^
    -v "%CD%\pgdata:/var/lib/postgresql/data" ^
    -v "%CD%\init-scripts:/docker-entrypoint-initdb.d" ^
    -p 5432:5432 ^
    -d pgvector/pgvector:pg17
```

Check it's running:
```bash
docker logs -f postgresql-wier
```

Execute the following command to start the tables and schemas:
```bash
docker exec -i postgresql-wier psql -U user -d wier < init-scripts/database.sql
```

---

## 5. Configure the crawler

Edit the entry point at the bottom of `crawler.py`, DEFAULTS:

```python
SEED_URLS = [
    "https://github.com/topics/foss",
    "https://github.com/topics/open-source",
    "https://github.com/topics/free-software",
]

ALLOWED_DOMAINS = ["https://github.com"]

TARGET_DESCRIPTION = "open source foss free software repository project"
```

Key settings in `CRAWLER_CONFIG`:

| Setting | Default | Description |
|---|---|---|
| `num_workers` | 5 | Parallel crawl threads |
| `default_delay` | 5 / 60 | Seconds between requests (5 with token, 60 without) |
| `use_selenium` | True | Use Firefox for JS rendering |
| `max_pages` | 5000 | Stop after N pages (0 = no limit) |
| `gecko_driver` | `./geckodriver` | Path to GeckoDriver binary |

---

## 6. Run

```bash
python crawler.py
```

The crawler prints INFO:
```
[Worker-1] INFO - Crawling: https://github.com/topics/foss
[Worker-5] INFO - GitHub extracted: torvalds/linux (repo_id=1)
INFO - Status: crawled=120, frontier=3400
```

To wipe the database and start fresh:
```bash
docker exec -it postgresql-wier psql -U user -d wier -c "
  TRUNCATE crawldb.issue_comment, crawldb.issue_label, crawldb.issue,
           crawldb.doc_link, crawldb.repo_topic, crawldb.readme,
           crawldb.repo, crawldb.image, crawldb.page_data,
           crawldb.link, crawldb.page, crawldb.site CASCADE;
"
```
