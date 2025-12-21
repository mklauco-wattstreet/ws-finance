# ENTSO-E Pipeline Runners

Manual commands for running ENTSO-E data pipelines.

## Runners

| Runner | Table | Data |
|--------|-------|------|
| `entsoe_imbalance_runner.py` | `entsoe_imbalance_prices` | Imbalance prices & volumes |
| `entsoe_load_runner.py` | `entsoe_load` | Actual load & forecast |
| `entsoe_gen_runner.py` | `entsoe_generation_actual` | Generation by fuel type (wide format) |
| `entsoe_flow_runner.py` | `entsoe_cross_border_flows` | Cross-border physical flows (wide format) |

## Command-Line Arguments

All runners support the following arguments:

| Argument | Description |
|----------|-------------|
| `--debug` | Enable verbose debug logging |
| `--dry-run` | Fetch and parse data but skip database upload |
| `--start YYYY-MM-DD` | Start date for backfill (enables backfill mode) |
| `--end YYYY-MM-DD` | End date for backfill (defaults to today if `--start` is provided) |

## Normal Mode (Cron)

When run without `--start`/`--end`, runners fetch the last 3 hours of data. This is the default behavior used by cron jobs.

```bash
# Generation (last 3 hours)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py

# Load (last 3 hours)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_load_runner.py

# Cross-border flows (last 3 hours)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_flow_runner.py

# Imbalance (last 3 hours)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_imbalance_runner.py
```

## Backfill Mode

Use `--start` and optionally `--end` to backfill historical data. The runner automatically chunks long date ranges into 7-day blocks to comply with ENTSO-E API limits.

### Backfill Examples

```bash
# Backfill from Dec 1, 2024 to today
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --start 2024-12-01

# Backfill specific date range
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --start 2024-12-01 --end 2024-12-15

# Dry run to preview without uploading
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --start 2024-12-01 --end 2024-12-07 --dry-run
```

### Full Backfill Commands (Dec 1, 2024 to Present)

```bash
# Generation data
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --start 2024-12-01

# Load data
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_load_runner.py --start 2024-12-01

# Cross-border flows
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_flow_runner.py --start 2024-12-01

# Imbalance data
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_imbalance_runner.py --start 2024-12-01
```

## Backfill Behavior

- **Auto-chunking**: Date ranges are split into 7-day blocks (ENTSO-E API limit)
- **Fault tolerance**: If one chunk fails, the runner logs the error and continues with the next chunk
- **Single connection**: Backfill mode uses one database connection for all chunks (efficient)
- **Idempotent**: Uses `ON CONFLICT DO UPDATE` so re-running is safe

## Dry Run (No Database Upload)

```bash
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --dry-run
```

## Debug Mode (Verbose Logging)

```bash
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/runners/entsoe_gen_runner.py --debug
```

## Alembic Migrations

```bash
# Check current version
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app && python3 -m alembic current"

# Upgrade to latest
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app && python3 -m alembic upgrade head"
```

## Cron Schedule

All runners execute every 15 minutes via cron (configured in `/crontab`).
