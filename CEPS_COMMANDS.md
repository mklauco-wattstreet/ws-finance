# CEPS Commands Reference

## SOAP Pipeline (Primary Method)

### Download + Upload Today's Data
```bash
# Download and upload all datasets for today (default)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset all

# Single dataset only
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset imbalance
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset re_price
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset svr_activation
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset export_import_svr
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
- SOAP API: ~0.2s per day (50-100x faster than old Selenium)
- Full pipeline for all 4 datasets: ~7 seconds
- Backfill 30 days: ~1.7 seconds
