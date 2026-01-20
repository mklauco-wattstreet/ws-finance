# CEPS Commands Reference

## SOAP Pipeline (Primary Method)

### Supported Datasets (8 total)

| Key | Description | Resolution | Records/Day |
|-----|-------------|------------|-------------|
| `imbalance` | System Imbalance | 1-min → 15-min | 1440 |
| `re_price` | RE Prices (aFRR/mFRR) | 1-min → 15-min | 1440 |
| `svr_activation` | SVR Activation | 1-min → 15-min | 1440 |
| `export_import_svr` | Cross-Border Balancing | 1-min → 15-min | 1440 |
| `generation_res` | Generation RES (Wind/Solar) | 1-min → 15-min | 1440 |
| `generation` | Generation by Plant Type | Native 15-min | 96 |
| `generation_plan` | Planned Generation | Native 15-min | 96 |
| `estimated_imbalance_price` | Estimated Imbalance Price | Native 15-min | 96 |

### Download + Upload Today's Data
```bash
# Download and upload all datasets for today (default)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset all

# Single dataset only
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset re_price
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset svr_activation
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset export_import_svr
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset generation_res
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset generation
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset generation_plan
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset estimated_imbalance_price
```

### Download + Upload for Specific Date
```bash
# All datasets for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset all --start-date 2026-01-13

# Single dataset for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance --start-date 2026-01-13
```

### Backfill Date Range
```bash
# Backfill specific dataset (auto-chunks to 7-day requests)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance --start-date 2025-11-01 --end-date 2025-11-30

# Backfill all datasets for large range
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset all --start-date 2024-12-01 --end-date 2025-12-31

# Note: SOAP pipeline uses UPSERT logic - safe for re-running
```

### Debug Mode
```bash
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance --start-date 2026-01-13 --debug
```

## SOAP API Test Scripts
```bash
# Test SOAP download only (no upload)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset imbalance --start-date 2026-01-13

# Save XML response to file
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_downloader.py --dataset imbalance --start-date 2026-01-13 --save
```

## Check Data Consistency
```bash
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_consistency_check.py
```

## Manual CSV Upload (Legacy)
```bash
# Upload imbalance CSV (if you have a CSV file from CEPS website)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_uploader.py --file /path/to/file.csv

# Upload RE price CSV
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_re_price_uploader.py --file /path/to/file.csv

# Upload SVR activation CSV
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_svr_activation_uploader.py --file /path/to/file.csv

# Upload Export/Import SVR CSV
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_export_import_svr_uploader.py --file /path/to/file.csv
```

## Cron Schedule
```bash
# CEPS runs automatically at minutes 12, 27, 42, 57 every hour via SOAP
# Edit crontab to change schedule
nano crontab
docker compose restart entsoe-ote-data-uploader
```

## Performance
- SOAP API: ~0.1-0.2s per dataset per day
- Full pipeline for all 8 datasets: ~14 seconds
- Backfill 30 days: ~3 seconds
