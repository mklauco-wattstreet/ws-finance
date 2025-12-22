# ENTSO-E Pipeline Runners

Manual commands for running ENTSO-E data pipelines.

## Runners

| Runner | Table | Data | Doc Type | Areas |
|--------|-------|------|----------|-------|
| `entsoe_imbalance_runner.py` | `entsoe_imbalance_prices` | Imbalance prices & volumes | A85, A86 | CZ |
| `entsoe_load_runner.py` | `entsoe_load` | Actual load & DA forecast | A65 | CZ |
| `entsoe_unified_gen_runner.py` | `entsoe_generation_actual` | Generation by fuel type | A75 | CZ, DE, AT, PL, SK |
| `entsoe_flow_runner.py` | `entsoe_cross_border_flows` | Physical cross-border flows | A11 | CZ |
| `entsoe_gen_forecast_runner.py` | `entsoe_generation_forecast` | Wind/Solar DA forecasts | A69 | CZ |
| `entsoe_balancing_runner.py` | `entsoe_balancing_energy` | Activated aFRR/mFRR reserves | A84 | CZ |
| `entsoe_gen_scheduled_runner.py` | `entsoe_generation_scheduled` | Scheduled generation (DA) | A71 | CZ |
| `entsoe_sched_flow_runner.py` | `entsoe_scheduled_cross_border_flows` | Scheduled cross-border exchanges | A09 | CZ |

### Unified Generation Runner

The `entsoe_unified_gen_runner.py` fetches generation data for **all 5 areas** (CZ, DE, AT, PL, SK) and stores them in a partitioned `entsoe_generation_actual` table with `area_id` column.

**Partitions:**
- `area_id=1`: Czech Republic (CZ)
- `area_id=2`: Germany TenneT (DE)
- `area_id=3`: Austria (AT)
- `area_id=4`: Poland (PL)
- `area_id=5`: Slovakia (SK)

## Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--debug` | Enable verbose debug logging |
| `--dry-run` | Fetch and parse data but skip database upload |
| `--start YYYY-MM-DD` | Start date for backfill (enables backfill mode) |
| `--end YYYY-MM-DD` | End date for backfill (defaults to today) |

---

## Production Deployment Guide

### Step 1: Run Alembic Migrations

```bash
docker compose exec entsoe-ote-data-uploader alembic upgrade head
```

### Step 2: Verify Tables Exist

```bash
docker compose exec entsoe-ote-data-uploader alembic current
```

### Step 3: Test Unified Generation Runner (Dry Run)

```bash
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py --dry-run --debug
```

### Step 4: Backfill Historical Data

Run all runners to backfill from Dec 1, 2024:

```bash
# 1. Imbalance prices & volumes
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_imbalance_runner.py --start 2024-12-01

# 2. Load data
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_load_runner.py --start 2024-12-01

# 3. Generation actual (ALL 5 AREAS: CZ, DE, AT, PL, SK)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py --start 2024-12-01

# 4. Physical cross-border flows
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_flow_runner.py --start 2024-12-01

# 5. Generation forecast (wind/solar)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_forecast_runner.py --start 2024-12-01

# 6. Balancing energy (aFRR/mFRR)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_balancing_runner.py --start 2024-12-01

# 7. Scheduled generation
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_scheduled_runner.py --start 2024-12-01

# 8. Scheduled cross-border flows
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_sched_flow_runner.py --start 2024-12-01
```

### Step 5: Restart Container (Reload Crontab)

```bash
docker compose restart entsoe-ote-data-uploader
```

### Step 6: Verify Cron is Running

```bash
docker compose exec entsoe-ote-data-uploader tail -50 /var/log/cron.log
```

---

## Normal Mode (Cron)

Runners fetch the last 3 hours of data when run without arguments:

```bash
# Imbalance
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_imbalance_runner.py

# Load
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_load_runner.py

# Generation actual (all 5 areas)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py

# Physical flows
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_flow_runner.py

# Generation forecast
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_forecast_runner.py

# Balancing energy
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_balancing_runner.py

# Scheduled generation
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_scheduled_runner.py

# Scheduled flows
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_sched_flow_runner.py
```

---

## Backfill Mode

Use `--start` and `--end` to backfill historical data:

```bash
# Backfill specific range (all 5 areas)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py --start 2024-12-01 --end 2024-12-15

# Dry run (no DB upload)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py --start 2024-12-01 --dry-run --debug
```

### Backfill Behavior

- **Auto-chunking**: Splits into 7-day blocks (ENTSO-E API limit)
- **Fault tolerance**: Continues to next chunk if one fails
- **Idempotent**: Safe to re-run (`ON CONFLICT DO UPDATE`)
- **Sequential areas**: Processes CZ → DE → AT → PL → SK to avoid API rate limits

---

## Historical Data Backfill Commands

### Backfill Generation Data (All Areas)

```bash
# Last 30 days
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py --start 2024-11-22

# Specific month
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py --start 2024-11-01 --end 2024-11-30

# Full year (will take a while - 5 areas × 52 weeks)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_unified_gen_runner.py --start 2024-01-01
```

### Verify Data Counts by Area

```bash
docker compose exec entsoe-ote-data-uploader python3 -c "
import psycopg2
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT)
cur = conn.cursor()
cur.execute('''
    SELECT a.country_code, COUNT(g.area_id) as records,
           MIN(g.trade_date) as first_date, MAX(g.trade_date) as last_date
    FROM finance.entsoe_areas a
    LEFT JOIN finance.entsoe_generation_actual g ON a.id = g.area_id
    GROUP BY a.id, a.country_code
    ORDER BY a.id
''')
print('Area | Records | First Date | Last Date')
print('-----|---------|------------|----------')
for row in cur.fetchall():
    print(f'{row[0]:4} | {row[1]:7} | {row[2]} | {row[3]}')
conn.close()
"
```

---

## Cron Schedule

All runners execute every 15 minutes (configured in `/crontab`).

## Schema Notes

The `entsoe_generation_actual` table is **partitioned by LIST(area_id)**:
- Queries should include `area_id` for partition pruning
- Each area has its own partition for efficient storage
- Composite PK: `(trade_date, period, area_id)`
