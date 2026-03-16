# CLAUDE.md - CZ Electricity Imbalance Prediction Project

## ROLE
Lead Data Engineer & ML Architect. Focus: **High-Precision Grid Imbalance Forecasting.**
Supervised by a Senior Architect. Do not implement complex logic or structural changes without a confirmed plan.

---

## ⚡ PERFORMANCE & EXECUTION
* **30-Second Threshold:** If a command is expected to take >30 seconds (migrations, heavy data fetch), do not run it in the chat. Provide the formatted Docker command for manual user execution.
* **Parallel Processing:** Use native algorithm settings (e.g., LightGBM `n_jobs=-1`) or standard libraries. *Note: Add joblib to requirements.txt if complex parallel feature engineering is required.*
* [cite_start]**Efficient I/O:** Use bulk inserts (`psycopg2.extras.execute_values`) for all database uploads.
* always read "docker-compose.yml" to know the name of services
* to load bidding zones are codes, look for `entsoe_areas` table
* if table partitioning is required, always partition based on country code, like `entsoe_generation_actual -> ntsoe_generation_actual_de`
---

## 🐳 DOCKER & ENVIRONMENT
* [cite_start]**Source of Truth:** All runtime behavior occurs inside Docker containers.
* **No `down -v`:** Never suggest `docker compose down -v` to avoid wiping `./ote_files` or DB volumes.
* [cite_start]**Cron Environment:** Access env vars via `export $(cat /etc/environment_for_cron | xargs)`[cite: 3].
* [cite_start]**Timezone:** All operations must respect `TZ=Europe/Prague`.
* always provide single line commands to avoid indentation issues
* if we need to completely rebuild in production environment: `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build entsoe-ote-data-uploader`
* **`.env` changes require container recreation:** `docker compose restart` does NOT reload `.env`. Use `docker compose up -d --force-recreate entsoe-ote-data-uploader` to pick up new env vars (e.g., ENTSOE_SECURITY_TOKEN).

---

## ⚠️ DATABASE & MIGRATIONS
* **Alembic Only:** All schema changes must be performed via Alembic migrations. 
* [cite_start]**Legacy Warning:** Do not use or copy the `create_tables_if_not_exist` logic found in `app/entsoe/entsoe_pipeline.py` for any new modules.
* [cite_start]**Persistence:** Use the mounted volumes (`./ote_files`, `./downloads`) for persistent file storage.
* **No Individual Inserts:** Use `execute_values` for all batch operations.
* **Connection Management:** Use a context manager for database connections to ensure they are closed or returned to the pool.
---

## 🛠 MODULARITY & ARCHITECTURE (REFLECTED)
* **Pipeline Structure:**
  - [cite_start]`app/entsoe/`: ENTSO-E specific clients and parsers.
  - [cite_start]`app/`: OTE-CR scrapers, downloaders, and uploaders[cite: 3].
  - `app/models.py`: SQLAlchemy/Alembic model definitions.
  - `app/alembic/`: Database migrations.
* **File Size Cap:** Python files must be **≤ 1500 lines**. Refactor before hitting this limit.

---

## 🔄 WORKFLOW
1. **Plan:** Propose logic/schema/feature set first.
2. **Confirm:** Wait for Architect approval.
3. **Implement:** Write modular, typed Python code.
4. **Verify:** Provide `docker compose exec` commands for testing.

## 📊 DA MARKET TABLES PHILOSOPHY

Three tables store OTE day-ahead auction data at increasing abstraction levels:

### `da_bid` — Raw bid stack
- Source: OTE matching curve XML (`MC_DD_MM_YYYY_EN.xml`)
- One row per bid step × period × side (sell/buy)
- `volume_matched > 0` = accepted at clearing price; `= 0` = rejected
- ~17,000 rows/day. Never query directly for ML features.

### `da_period_summary` — Clearing summary
- One row per period (96/day)
- Captures: clearing price/volume, first unmatched step above/below MCP, price/volume gaps
- Key flag: `supply_volume_gap = 0` means the first unmatched sell bid sits immediately above clearing — present in ALL extreme imbalance price events
- Limitation: describes only the *first* step, not the full curve shape

### `da_curve_depth` — Curve steepness at fixed MW offsets
- One row per period × side × offset_mw (960/day = 96 × 2 × 5)
- Offsets: 50, 100, 200, 500, 1000 MW (defined in `CURVE_DEPTH_OFFSETS_MW` constant)
- `price_at_offset`: price where cumulative unmatched volume first reaches offset_mw
- `NULL price_at_offset`: curve exhausted before that offset
- `volume_available`: total unmatched volume on this side
- Adding new offsets = Python constant change only, no migration needed

### Key findings from analysis (2026-03-04)
- DA curve steepness alone has weak linear correlation (0.13) with imbalance prices
- Strongest predictor found: `intraday_premium × sell_100mw` (corr 0.248)
- Steepness is only meaningful signal during business hours (7–19); hour 11 corr=0.80
- Surplus periods: imbalance prices near zero regardless of buy curve shape
- See `DA_CURVE_ANALYSIS_FINDINGS.md` in imbalance-predictor repo for full analysis

---

## 🔒 SECURITY & ROBUSTNESS
* **Token Masking:** Never log security tokens or API keys. Mask them as `***` or `[CONFIGURED]`.
* **Retry Logic:** All external API calls must use exponential backoff (e.g., `tenacity` or `urllib3` retries).
* **Validation:** Validate all API inputs (ranges, EIC codes) and XML outputs (XSD validation where possible).

