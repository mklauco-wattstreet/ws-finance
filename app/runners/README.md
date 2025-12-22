# ENTSO-E Pipeline Runners

Manual commands for running ENTSO-E data pipelines.

## Runners

| Runner | Table | Data | Doc Type |
|--------|-------|------|----------|
| `entsoe_imbalance_runner.py` | `entsoe_imbalance_prices` | Imbalance prices & volumes | A85, A86 |
| `entsoe_load_runner.py` | `entsoe_load` | Actual load & DA forecast | A65 |
| `entsoe_gen_runner.py` | `entsoe_generation_actual` | Generation by fuel type | A75 |
| `entsoe_flow_runner.py` | `entsoe_cross_border_flows` | Physical cross-border flows | A11 |
| `entsoe_gen_forecast_runner.py` | `entsoe_generation_forecast` | Wind/Solar DA forecasts | A69 |
| `entsoe_balancing_runner.py` | `entsoe_balancing_energy` | Activated aFRR/mFRR reserves | A84 |
| `entsoe_gen_scheduled_runner.py` | `entsoe_generation_scheduled` | Scheduled generation (DA) | A71 |
| `entsoe_sched_flow_runner.py` | `entsoe_scheduled_cross_border_flows` | Scheduled cross-border exchanges | A09 |

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
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app && python3 -m alembic upgrade head"
```

### Step 2: Verify Tables Exist

```bash
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app && python3 -m alembic current"
```

### Step 3: Test One Runner (Dry Run)

```bash
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --dry-run --debug
```

### Step 4: Backfill Historical Data

Run all 8 runners to backfill from Dec 1, 2024:

```bash
# 1. Imbalance prices & volumes
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_imbalance_runner.py --start 2024-12-01

# 2. Load data
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_load_runner.py --start 2024-12-01

# 3. Generation actual
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --start 2024-12-01

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
docker compose exec entsoe-ote-data-uploader cat /var/log/cron.log | tail -50
```

---

## Normal Mode (Cron)

Runners fetch the last 3 hours of data when run without arguments:

```bash
# Imbalance
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_imbalance_runner.py

# Load
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_load_runner.py

# Generation actual
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py

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
# Backfill specific range
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --start 2024-12-01 --end 2024-12-15

# Dry run (no DB upload)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --start 2024-12-01 --dry-run
```

### Backfill Behavior

- **Auto-chunking**: Splits into 7-day blocks (ENTSO-E API limit)
- **Fault tolerance**: Continues to next chunk if one fails
- **Idempotent**: Safe to re-run (`ON CONFLICT DO UPDATE`)

---

## Cron Schedule

All runners execute every 15 minutes (configured in `/crontab`).
