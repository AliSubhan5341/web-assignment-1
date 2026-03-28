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

Windows (PowerShell, current session):

```powershell
$env:GITHUB_TOKEN="ghp_yourtoken"
```

Windows (Command Prompt, current session):

```cmd
set "GITHUB_TOKEN=ghp_yourtoken"
```

The crawler uses **5 seconds** between requests per IP by default (politeness), same with or without a token. The token only affects **GitHub REST API** quota for structured extraction (higher limits when authenticated).

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

The crawler can be configured via command-line arguments (recommended). Run this to see all options:

```bash
python crawler.py --help
```

Key settings in `CRAWLER_CONFIG`:

| Setting | Default | Description |
|---|---|---|
| `num_workers` | 5 | Parallel crawl threads |
| `default_delay` | 5 | Minimum seconds between requests per IP (politeness) |
| `use_selenium` | True | Use Firefox for JS rendering |
| `max_pages` | 100 | Stop after N pages (0 = no limit) |
| `gecko_driver` | `./geckodriver.exe` | Path to GeckoDriver binary |

---

## 6. Run

Minimal:

```bash
python crawler.py
```

Recommended (arguments): set workers / max pages / seeds / allowed domains.

Windows (Command Prompt):

```cmd
set "GITHUB_TOKEN=ghp_yourtoken"
python crawler.py ^
  --workers 10 ^
  --max-pages 5000 ^
  --use-selenium 0 ^
  --allowed-domain https://github.com ^
  --seed https://github.com/topics/foss ^
  --seed https://github.com/topics/open-source ^
  --seed https://github.com/topics/free-software ^
  --target-description "open source foss free software repository project"
```

Windows (PowerShell):

```powershell
$env:GITHUB_TOKEN="ghp_yourtoken"
python crawler.py `
  --workers 10 `
  --max-pages 5000 `
  --use-selenium 0 `
  --allowed-domain https://github.com `
  --seed https://github.com/topics/foss `
  --seed https://github.com/topics/open-source `
  --seed https://github.com/topics/free-software `
  --target-description "open source foss free software repository project"
```

Selenium mode (requires Firefox + `geckodriver.exe` next to `crawler.py`):

```bash
python crawler.py --use-selenium 1 --gecko-driver ./geckodriver.exe
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
