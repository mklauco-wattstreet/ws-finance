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

## 🔒 SECURITY & ROBUSTNESS
* **Token Masking:** Never log security tokens or API keys. Mask them as `***` or `[CONFIGURED]`.
* **Retry Logic:** All external API calls must use exponential backoff (e.g., `tenacity` or `urllib3` retries).
* **Validation:** Validate all API inputs (ranges, EIC codes) and XML outputs (XSD validation where possible).

