# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ROLE
Lead Data Engineer & ML Architect. Focus: **High-Precision Grid Imbalance Forecasting.**
Supervised by a Senior Architect. Do not implement complex logic or structural changes without a confirmed plan.

---

## COMMON COMMANDS

### Local Development (bare docker compose)
```bash
# Run a script inside the container
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --debug --dry-run

# Run CEPS pipeline for specific dataset
docker compose exec entsoe-ote-data-uploader python3 -m ceps.ceps_soap_pipeline --dataset imbalance --debug

# Run OTE upload script
docker compose exec entsoe-ote-data-uploader python3 upload_dam_curves.py 2026/03 --debug

# Run Alembic migrations
docker compose exec entsoe-ote-data-uploader python3 -m alembic -c /app/alembic.ini upgrade head

# Check current migration revision
docker compose exec entsoe-ote-data-uploader python3 -m alembic -c /app/alembic.ini current

# View logs
docker compose logs -f --tail 100 entsoe-ote-data-uploader
```

### Production (MUST use both compose files)
```bash
# Rebuild image
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build entsoe-ote-data-uploader

# Recreate container (pick up new files/cron/env without rebuilding)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --force-recreate entsoe-ote-data-uploader

# After pushing cron changes
git pull && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --force-recreate entsoe-ote-data-uploader
```
Omitting `docker-compose.prod.yml` puts the container on the wrong network and breaks pgbouncer DNS. Never suggest a bare `docker compose` command for production.

### Runner CLI Pattern (all runners share this)
```bash
python3 -m runners.<runner_name> [--debug] [--dry-run] [--start YYYY-MM-DD --end YYYY-MM-DD]
```
- No args: fetches last 3 hours (cron mode)
- `--start/--end`: backfill mode, auto-chunked into 7-day windows
- `--dry-run`: fetch and parse but skip DB upload
- `--debug`: verbose logging

---

## PERFORMANCE & EXECUTION
* **30-Second Threshold:** If a command is expected to take >30 seconds (migrations, heavy data fetch), do not run it in the chat. Provide the formatted Docker command for manual user execution.
* **Efficient I/O:** Use bulk inserts (`psycopg2.extras.execute_values`) for all database uploads. No individual inserts.
* Always read `docker-compose.yml` to know the name of services.
* To load bidding zones and codes, look for the `entsoe_areas` table.
* If table partitioning is required, always partition based on country code (e.g., `entsoe_generation_actual` -> `entsoe_generation_actual_de`).

---

## DOCKER & ENVIRONMENT
* **Source of Truth:** All runtime behavior occurs inside Docker containers.
* **No `down -v`:** Never suggest `docker compose down -v` to avoid wiping `./ote_files` or DB volumes.
* **Cron Environment:** Access env vars via `export $(cat /etc/environment_for_cron | xargs)`.
* **Timezone:** All operations must respect `TZ=Europe/Prague`.
* Always provide single line commands to avoid indentation issues.
* **`.env` changes require container recreation:** `docker compose restart` does NOT reload `.env`. Use `docker compose up -d --force-recreate entsoe-ote-data-uploader`.
* **Cron changes require `git pull` + recreate on production.** The crontab is a mounted volume. Do NOT try to reload crontab manually inside the container.
* **Cron staggering:** Jobs are deliberately staggered to avoid resource contention. OTE intraday at `*/15`, ENTSO-E group 1 at `1,16,31,46`, group 2 at `3,18,33,48`, group 3 at `5,20,35,50`, CEPS at `12,27,42,57`. Do NOT move all jobs back to `*/15`.

---

## DATABASE & MIGRATIONS
* **Alembic Only:** All schema changes must be performed via Alembic migrations. The ini is mounted at `/app/alembic.ini` (not `/app/scripts/`).
* **Legacy Warning:** Do not use or copy the `create_tables_if_not_exist` logic found in `app/entsoe/entsoe_pipeline.py`.
* **Persistence:** Use the mounted volumes (`./ote_files`, `./downloads`) for persistent file storage.
* **Connection Management:** Use a context manager for database connections to ensure they are closed or returned to the pool.
* **Schema:** All tables live in the `finance` schema. `search_path` is set per-connection.
* **No tests exist.** The project relies on `--dry-run` and `--debug` flags for verification.

---

## ARCHITECTURE

### Three Data Pipelines

**ENTSO-E** (`app/entsoe/` + `app/runners/`): European grid data (imbalance, generation, load, flows, prices) for 6 countries (CZ, DE, AT, PL, SK, HU).
- `client.py` — HTTP client with retry/backoff, fetches XML from ENTSO-E Transparency Platform
- `parsers.py` — XML parsers per document type
- `constants.py` — EIC codes, `AREA_IDS` dict, `ACTIVE_*_AREAS` lists controlling which areas each runner fetches
- All runners inherit from `BaseRunner` (`app/runners/base_runner.py`) which provides DB connection, bulk upsert, time windowing, backfill chunking, and the standard CLI
- ENTSO-E API enforces 7-day max per request; `get_backfill_chunks()` handles this automatically
- Tables are LIST-partitioned by `country_code`. Always include `country_code` in queries for partition pruning.

**CEPS** (`app/ceps/`): Czech grid operator real-time data (1-min and 15-min resolution) via SOAP/XML API.
- `ceps_soap_pipeline.py` — orchestrates fetch → parse → upsert → aggregate for 8 datasets
- `ceps_soap_uploader.py` — per-dataset upsert functions (raw SQL, not ORM)
- `preprocess_ceps_data.py` — computes derived 15-min features from 1-min source data
- Default date range: yesterday through today (catches late-arriving midnight-boundary data)

**OTE-CR** (`app/` top-level): Czech market operator data (day-ahead prices, intraday, imbalance, DAM curves).
- Scripts are standalone `download_*.py` and `upload_*.py` files in `app/`
- `common.py` — shared download utilities, date range logic
- Downloaded files stored in year-based directories: `app/2026/MM/DD/`

### Shared Infrastructure
- `app/config.py` — loads all env vars from `.env`; raises `ValueError` if `DB_USER`/`DB_PASSWORD` missing
- `app/models.py` — SQLAlchemy models for Alembic autogenerate only; not used at runtime
- `app/sentry_init.py` — must be imported first in every entry point; no-op unless `SENTRY_DSN` is set
- `app/` is mounted as `/app/scripts` in the container; `PYTHONPATH=/app/scripts` makes all files importable

### BaseRunner Pattern (`app/runners/base_runner.py`)
Subclass contract: override `RUNNER_NAME`, `TABLE_NAME`, `COLUMNS`, `CONFLICT_COLUMNS`, and implement `run() -> bool`. The base class provides `database_connection()`, `bulk_upsert()` (execute_values, page_size=1000), `get_time_range()`, `get_backfill_chunks()`, and `_run_with_availability_check()`.

---

## DA MARKET TABLES PHILOSOPHY

Three tables store OTE day-ahead auction data at increasing abstraction levels:

### `da_bid` — Raw bid stack
- Source: OTE matching curve XML (`MC_DD_MM_YYYY_EN.xml`)
- One row per bid step x period x side (sell/buy)
- `volume_matched > 0` = accepted at clearing price; `= 0` = rejected
- ~17,000 rows/day. Never query directly for ML features.

### `da_period_summary` — Clearing summary
- One row per period (96/day)
- Captures: clearing price/volume, first unmatched step above/below MCP, price/volume gaps
- Key flag: `supply_volume_gap = 0` means the first unmatched sell bid sits immediately above clearing — present in ALL extreme imbalance price events
- Limitation: describes only the *first* step, not the full curve shape

### `da_curve_depth` — Curve steepness at fixed MW offsets
- One row per period x side x offset_mw (960/day = 96 x 2 x 5)
- Offsets: 50, 100, 200, 500, 1000 MW (defined in `CURVE_DEPTH_OFFSETS_MW` constant)
- `price_at_offset`: price where cumulative unmatched volume first reaches offset_mw
- `NULL price_at_offset`: curve exhausted before that offset
- `volume_available`: total unmatched volume on this side
- Adding new offsets = Python constant change only, no migration needed

### Key findings from analysis (2026-03-04)
- DA curve steepness alone has weak linear correlation (0.13) with imbalance prices
- Strongest predictor found: `intraday_premium x sell_100mw` (corr 0.248)
- Steepness is only meaningful signal during business hours (7-19); hour 11 corr=0.80
- Surplus periods: imbalance prices near zero regardless of buy curve shape
- See `DA_CURVE_ANALYSIS_FINDINGS.md` in imbalance-predictor repo for full analysis

---

## MODULARITY
* **File Size Cap:** Python files must be ≤ 1500 lines. Refactor before hitting this limit.
* **Parallel Processing:** Use native algorithm settings (e.g., LightGBM `n_jobs=-1`) or standard libraries.

---

## WORKFLOW
1. **Plan:** Propose logic/schema/feature set first.
2. **Confirm:** Wait for Architect approval.
3. **Implement:** Write modular, typed Python code.
4. **Verify:** Provide `docker compose exec` commands for testing.

---

## SECURITY & ROBUSTNESS
* **Token Masking:** Never log security tokens or API keys. Mask them as `***` or `[CONFIGURED]`.
* **Retry Logic:** All external API calls must use exponential backoff (e.g., `tenacity` or `urllib3` retries).
* **Validation:** Validate all API inputs (ranges, EIC codes) and XML outputs (XSD validation where possible).
