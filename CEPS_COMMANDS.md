# CEPS Commands Reference

## Check Data Consistency
```bash
# Check completeness for all CEPS datasets (2024-12-01 to now)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_consistency_check.py
```

## Upload Single CSV File
```bash
# Upload imbalance CSV
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_uploader.py --file /app/downloads/ceps/2026/01/data_AktualniSystemovaOdchylkaCR_20260107_120000.csv

# Upload RE price CSV
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_re_price_uploader.py --file /app/downloads/ceps/2026/01/data_AktualniCenaRE_20260107_120000.csv

# Upload SVR activation CSV
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_svr_activation_uploader.py --file /app/downloads/ceps/2026/01/data_AktivaceSVRvCR_20260107_120000.csv
```

## Upload All CSV Files from Folder
```bash
# Upload all imbalance CSVs from folder
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_uploader.py --folder /app/downloads/ceps/2026/01

# Upload all RE price CSVs from folder
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_re_price_uploader.py --folder /app/downloads/ceps/2026/01

# Upload all SVR activation CSVs from folder
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_svr_activation_uploader.py --folder /app/downloads/ceps/2026/01
```

## Download + Upload for Specific Date
```bash
# Download and upload all datasets for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py --start-date 2025-12-15

# Download and upload only RE prices for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py --start-date 2025-12-15 --dataset re_price

# Download and upload only SVR activation for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py --start-date 2025-12-15 --dataset svr_activation

# Download and upload only imbalance for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py --start-date 2025-12-15 --dataset imbalance
```

## Download + Upload for Date Range
```bash
# Download and upload all datasets for date range
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py --start-date 2025-12-01 --end-date 2025-12-31

# Download and upload only RE prices for date range
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py --start-date 2025-12-01 --end-date 2025-12-31 --dataset re_price
```

## Download + Upload Today's Data
```bash
# Download and upload all datasets for today (default)
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py
```

## Download Only (No Upload)
```bash
# Download imbalance for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_hybrid_downloader.py --start-date 2025-12-15

# Download RE prices for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_re_price_downloader.py --start-date 2025-12-15

# Download SVR activation for specific date
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_svr_activation_downloader.py --start-date 2025-12-15
```

## Debug Mode
```bash
# Add --debug flag to any command for verbose output
docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_runner.py --start-date 2025-12-15 --debug
```

## Cron Schedule
```bash
# CEPS runs automatically at minutes 4, 19, 34, 49 every hour
# Edit crontab to change schedule
nano crontab
docker compose restart entsoe-ote-data-uploader
```
