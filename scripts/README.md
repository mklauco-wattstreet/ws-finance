# Scripts Directory

**NOTE:** The actual Python scripts are located in the `../app/` directory.

This directory is no longer used for storing scripts. The Docker container mounts
`./app/` to `/app/scripts/` in the container.

## Current Scripts (in ../app/)
- `download_day_ahead_prices.py` - Download day-ahead electricity prices
- `download_imbalance_prices.py` - Download imbalance prices
- `download_intraday_prices.py` - Download intraday market prices
- `upload_day_ahead_prices.py` - Upload day-ahead prices to database
- `upload_imbalance_prices.py` - Upload imbalance prices to database
- `upload_intraday_prices.py` - Upload intraday prices to database

All scripts support both manual and automatic modes. See individual script headers for usage.
