# ENTSO-E Pipeline Runners

Manual commands for running ENTSO-E data pipelines.

## Runners

| Runner | Table | Data |
|--------|-------|------|
| `entsoe_imbalance_runner.py` | `entsoe_imbalance_prices` | Imbalance prices & volumes |
| `entsoe_load_runner.py` | `entsoe_load` | Actual load & forecast |
| `entsoe_gen_runner.py` | `entsoe_generation_actual` | Generation by fuel type |

## Manual Execution

### Production Run
```bash
# Generation
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app/scripts && python3 runners/entsoe_gen_runner.py"

# Load
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app/scripts && python3 runners/entsoe_load_runner.py"

# Imbalance
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app/scripts && python3 runners/entsoe_imbalance_runner.py"
```

### Dry Run (no database upload)
```bash
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app/scripts && python3 runners/entsoe_gen_runner.py --dry-run"
```

### Debug Mode (verbose logging)
```bash
docker compose exec entsoe-ote-data-uploader bash -c "export \$(cat /etc/environment_for_cron | xargs) && cd /app/scripts && python3 runners/entsoe_gen_runner.py --debug"
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
