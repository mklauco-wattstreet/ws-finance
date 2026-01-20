# ENTSO-E Commands Reference

## Data Runners

### Run Single Runner (Today)
```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_load_runner
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_flow_runner
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_forecast_runner
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_balancing_runner
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_scheduled_runner
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_sched_flow_runner
```

### Backfill Date Range
```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2026-01-13 --end 2026-01-14
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_load_runner --start 2026-01-13 --end 2026-01-14
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --start 2026-01-13 --end 2026-01-14
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_flow_runner --start 2026-01-13 --end 2026-01-14
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_forecast_runner --start 2026-01-13 --end 2026-01-14
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_balancing_runner --start 2026-01-13 --end 2026-01-14
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_scheduled_runner --start 2026-01-13 --end 2026-01-14
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_sched_flow_runner --start 2026-01-13 --end 2026-01-14
```

### Backfill All Runners (24h Outage)
```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_imbalance_runner --start 2026-01-13 --end 2026-01-14 && \
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_load_runner --start 2026-01-13 --end 2026-01-14 && \
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --start 2026-01-13 --end 2026-01-14 && \
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_flow_runner --start 2026-01-13 --end 2026-01-14 && \
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_forecast_runner --start 2026-01-13 --end 2026-01-14 && \
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_balancing_runner --start 2026-01-13 --end 2026-01-14 && \
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_scheduled_runner --start 2026-01-13 --end 2026-01-14 && \
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_sched_flow_runner --start 2026-01-13 --end 2026-01-14
```

### Debug / Dry Run
```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --debug
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_unified_gen_runner --dry-run
```

---

## Consistency Check
```bash
docker compose exec entsoe-ote-data-uploader python3 -m runners.entsoe_consistency_check
```

---

## Runner → Table Mapping

| Runner | Table | Doc Type |
|--------|-------|----------|
| `entsoe_unified_gen_runner` | `entsoe_generation_actual` | A75 |
| `entsoe_unified_forecast_runner` | `entsoe_generation_forecast` | A69 |
| `entsoe_unified_scheduled_runner` | `entsoe_generation_scheduled` | A71 |
| `entsoe_unified_load_runner` | `entsoe_load` | A65 |
| `entsoe_unified_flow_runner` | `entsoe_cross_border_flows` | A11 |
| `entsoe_unified_sched_flow_runner` | `entsoe_scheduled_cross_border_flows` | A09 |
| `entsoe_unified_balancing_runner` | `entsoe_balancing_energy` | A84 |
| `entsoe_unified_imbalance_runner` | `entsoe_imbalance_prices` | A85/A86 |

---

## Cron Schedule
```bash
# All runners run every 15 minutes (*/15)
# Logs: /var/log/entsoe_*.log
```

---

## Schema
See: `app/runners/ENTSOE_SCHEMA.md`
