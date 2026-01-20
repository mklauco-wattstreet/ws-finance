# ENTSO-E Pipeline Runners

Manual commands for running ENTSO-E data pipelines.

## Runners

| Runner | Table | Data | Doc Type | Areas |
|--------|-------|------|----------|-------|
| `entsoe_unified_imbalance_runner.py` | `entsoe_imbalance_prices` | Imbalance prices & volumes | A85, A86 | CZ |
| `entsoe_unified_load_runner.py` | `entsoe_load` | Actual load & DA forecast | A65 | CZ |
| `entsoe_unified_gen_runner.py` | `entsoe_generation_actual` | Generation by fuel type | A75 | CZ, DE, AT, PL, SK |
| `entsoe_unified_flow_runner.py` | `entsoe_cross_border_flows` | Physical cross-border flows | A11 | CZ |
| `entsoe_unified_forecast_runner.py` | `entsoe_generation_forecast` | Wind/Solar DA forecasts | A69 | CZ |
| `entsoe_unified_balancing_runner.py` | `entsoe_balancing_energy` | Activated aFRR/mFRR reserves | A84 | CZ |
| `entsoe_unified_scheduled_runner.py` | `entsoe_generation_scheduled` | Scheduled generation (DA) | A71 | CZ |
| `entsoe_unified_sched_flow_runner.py` | `entsoe_scheduled_cross_border_flows` | Scheduled cross-border exchanges | A09 | CZ |

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

### Step 3: Test Runner (Dry Run)

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --dry-run --debug
```

### Step 4: Backfill Historical Data

Run all runners to backfill from Dec 1, 2024:

```bash
# 1. Imbalance prices & volumes
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2024-12-01

# 2. Load data
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_load_runner --start 2024-12-01

# 3. Generation actual (ALL 5 AREAS: CZ, DE, AT, PL, SK)
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --start 2024-12-01

# 4. Physical cross-border flows
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_flow_runner --start 2024-12-01

# 5. Generation forecast (wind/solar)
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_forecast_runner --start 2024-12-01

# 6. Balancing energy (aFRR/mFRR)
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_balancing_runner --start 2024-12-01

# 7. Scheduled generation
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_scheduled_runner --start 2024-12-01

# 8. Scheduled cross-border flows
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_sched_flow_runner --start 2024-12-01
```

### Step 5: Restart Container (Reload Crontab)

```bash
docker compose restart entsoe-ote-data-uploader
```

### Step 6: Verify Cron is Running

```bash
docker compose exec entsoe-ote-data-uploader tail -50 /var/log/entsoe_imbalance.log
```

---

## Normal Mode (Cron)

Runners fetch the last 3 hours of data when run without arguments:

```bash
# Imbalance
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner

# Load
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_load_runner

# Generation actual (all 5 areas)
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner

# Physical flows
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_flow_runner

# Generation forecast
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_forecast_runner

# Balancing energy
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_balancing_runner

# Scheduled generation
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_scheduled_runner

# Scheduled flows
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_sched_flow_runner
```

---

## Backfill Mode

Use `--start` and `--end` to backfill historical data:

```bash
# Backfill specific range (all 5 areas)
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --start 2024-12-01 --end 2024-12-15

# Dry run (no DB upload)
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --start 2024-12-01 --dry-run --debug
```

### Backfill Behavior

- **Auto-chunking**: Splits into 7-day blocks (ENTSO-E API limit)
- **Fault tolerance**: Continues to next chunk if one fails
- **Idempotent**: Safe to re-run (`ON CONFLICT DO UPDATE`)
- **Sequential areas**: Processes CZ -> DE -> AT -> PL -> SK to avoid API rate limits

---

## Consistency Check

Run the data consistency check utility:

```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_consistency_check
```

---

## Cron Schedule

All runners execute every 15 minutes (configured in `/crontab`).

## Schema Notes

The `entsoe_generation_actual` table is **partitioned by LIST(country_code)**:
- Queries should include `country_code` for partition pruning
- Each country has its own partition for efficient storage
- Composite PK: `(trade_date, period, area_id, country_code)`
