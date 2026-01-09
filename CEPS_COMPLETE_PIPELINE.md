# CEPS Complete Data Pipeline

## Overview

Complete pipeline for Czech Electricity Power System (CEPS) actual system imbalance data:

1. **Download** → CSV files from CEPS website
2. **Upload** → PostgreSQL database (1-minute + 15-minute aggregated)
3. **Query** → Analyze imbalance patterns

---

## Quick Start

### 1. Run Migrations

```bash
# Check current state
docker compose exec entsoe-ote-data-uploader alembic -c /app/alembic.ini current

# Run migrations 027 and 028
docker compose exec entsoe-ote-data-uploader alembic -c /app/alembic.ini upgrade head
```

### 2. Download Historical Data

```bash
# Download specific date
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
  --tag AktualniSystemovaOdchylkaCR \
  --start-date 2026-01-04 \
  --end-date 2026-01-04

# Download multiple days (run in loop)
for day in {01..07}; do
  docker compose exec entsoe-ote-data-uploader \
    python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
    --tag AktualniSystemovaOdchylkaCR \
    --start-date 2026-01-$day \
    --end-date 2026-01-$day
  sleep 5
done
```

**Output**: CSV files in `/app/scripts/ceps/YYYY/MM/`

### 3. Upload to Database

```bash
# Upload all CSV files from January 2026
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --folder /app/scripts/ceps/2026/01
```

**Result**: Data in both `ceps_actual_imbalance_1min` and `ceps_actual_imbalance_15min` tables

### 4. Verify Data

```sql
-- Check data coverage
SELECT
    DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS trade_date,
    COUNT(*) AS records,
    MIN(delivery_timestamp) AS first_timestamp,
    MAX(delivery_timestamp) AS last_timestamp
FROM finance.ceps_actual_imbalance_1min
GROUP BY trade_date
ORDER BY trade_date DESC;

-- Check 15-minute aggregated data
SELECT
    trade_date,
    COUNT(*) AS intervals,
    AVG(load_mean_mw) AS daily_avg_mw
FROM finance.ceps_actual_imbalance_15min
GROUP BY trade_date
ORDER BY trade_date DESC;
```

---

## Components

### 1. Downloader (`ceps_hybrid_downloader.py`)

**Technology**: Selenium + JavaScript injection

**What it does**:
- Opens CEPS website in headless Chrome
- Establishes PHP session with PHPSESSID cookie
- Injects JavaScript to set filter parameters
- Triggers download via website's own `filterData()` function
- Saves CSV files to organized directory structure

**Key Features**:
- ✅ Works for historical dates
- ✅ Handles session management
- ✅ Downloads correct data type (system imbalance)
- ✅ Organizes files by year/month

**Location**: `app/ceps/ceps_hybrid_downloader.py`

**Documentation**: `app/ceps/IMPLEMENTATION_SUMMARY.md`

### 2. Uploader (`ceps_uploader.py`)

**Technology**: Python + psycopg2 + PostgreSQL

**What it does**:
- Parses CEPS CSV files (semicolon-separated, Czech format)
- Converts timestamps to Europe/Prague timezone
- Bulk uploads to 1-minute table (UPSERT)
- Aggregates to 15-minute intervals (UPSERT)

**Key Features**:
- ✅ UPSERT logic (safe to re-run)
- ✅ Bulk inserts for performance
- ✅ Timezone-aware datetime handling
- ✅ Automatic aggregation to 15-minute intervals

**Location**: `app/ceps/ceps_uploader.py`

**Documentation**: `app/ceps/CEPS_UPLOADER_GUIDE.md`

### 3. Database Tables

#### `finance.ceps_actual_imbalance_1min`
- **Raw minute-level data**
- Partitioned by year (RANGE on `delivery_timestamp`)
- Primary key: `(delivery_timestamp, id)`
- Unique constraint: `delivery_timestamp` (for UPSERT)

#### `finance.ceps_actual_imbalance_15min`
- **Aggregated 15-minute data**
- Partitioned by year (RANGE on `trade_date`)
- Primary key: `(trade_date, time_interval, id)`
- Unique constraint: `(trade_date, time_interval)` (for UPSERT)
- Columns:
  - `load_mean_mw` - Average load
  - `load_median_mw` - Median load
  - `last_load_at_interval_mw` - Last load value in interval

**Migrations**:
- 027: Create tables with partitions
- 028: Add `last_load_at_interval_mw` column

**Documentation**: `CEPS_DATABASE_SCHEMA.md`, `MIGRATION_027_SUMMARY.md`, `MIGRATION_028_SUMMARY.md`

---

## Automation

### Daily Cron Job

Add to `crontab` file:

```cron
# Download and upload CEPS data daily at 02:00 (after day completes)
0 2 * * * export $(cat /etc/environment_for_cron | xargs) && \
          /usr/local/bin/python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
          --tag AktualniSystemovaOdchylkaCR >> /var/log/ceps_download.log 2>&1 && \
          /usr/local/bin/python3 /app/scripts/ceps/ceps_uploader.py \
          --folder /app/scripts/ceps/$(date +\%Y)/$(date +\%m) >> /var/log/ceps_upload.log 2>&1
```

This will:
1. Download yesterday's data (downloader defaults to today if no date specified)
2. Upload all CSV files from current month
3. Log to `/var/log/ceps_download.log` and `/var/log/ceps_upload.log`

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────┐
│ CEPS Website                                            │
│ https://www.ceps.cz/cs/data                            │
│ (Minute-level system imbalance data)                   │
└───────────────────┬─────────────────────────────────────┘
                    │
                    │ ceps_hybrid_downloader.py
                    │ (Selenium + JavaScript)
                    ▼
┌─────────────────────────────────────────────────────────┐
│ CSV Files                                               │
│ /app/scripts/ceps/YYYY/MM/data_*.csv                   │
│ Format: DD.MM.YYYY HH:mm;load_mw;                      │
└───────────────────┬─────────────────────────────────────┘
                    │
                    │ ceps_uploader.py
                    │ (Parse + UPSERT)
                    ▼
┌─────────────────────────────────────────────────────────┐
│ PostgreSQL: finance.ceps_actual_imbalance_1min         │
│ - Raw minute-level data                                 │
│ - Partitioned by year                                   │
│ - UPSERT on delivery_timestamp                         │
└───────────────────┬─────────────────────────────────────┘
                    │
                    │ SQL Aggregation
                    │ (15-minute buckets)
                    ▼
┌─────────────────────────────────────────────────────────┐
│ PostgreSQL: finance.ceps_actual_imbalance_15min        │
│ - Aggregated 15-minute intervals                       │
│ - Partitioned by year                                   │
│ - Mean, Median, Last value                             │
│ - UPSERT on (trade_date, time_interval)               │
└─────────────────────────────────────────────────────────┘
```

---

## Example Queries

### Daily Imbalance Summary

```sql
SELECT
    trade_date,
    COUNT(*) AS intervals,
    AVG(load_mean_mw) AS avg_imbalance,
    MIN(load_mean_mw) AS min_imbalance,
    MAX(load_mean_mw) AS max_imbalance,
    STDDEV(load_mean_mw) AS std_dev
FROM finance.ceps_actual_imbalance_15min
WHERE trade_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY trade_date
ORDER BY trade_date;
```

### Peak Imbalance Times

```sql
SELECT
    time_interval,
    AVG(load_mean_mw) AS avg_imbalance,
    COUNT(*) AS days
FROM finance.ceps_actual_imbalance_15min
WHERE trade_date >= '2026-01-01'
GROUP BY time_interval
ORDER BY ABS(AVG(load_mean_mw)) DESC
LIMIT 10;
```

### Compare Mean vs Last Value

```sql
SELECT
    time_interval,
    load_mean_mw,
    last_load_at_interval_mw,
    (last_load_at_interval_mw - load_mean_mw) AS difference
FROM finance.ceps_actual_imbalance_15min
WHERE trade_date = '2026-01-04'
ORDER BY ABS(last_load_at_interval_mw - load_mean_mw) DESC
LIMIT 10;
```

---

## Troubleshooting

### Download Issues

**Problem**: "No PHPSESSID cookie"
- **Cause**: CDN caching or browser profile issues
- **Solution**: Script already handles this by clearing cookies. If persists, check browser-profile directory.

**Problem**: Wrong date in downloaded file
- **Cause**: Browser cached filter settings
- **Solution**: Script uses fresh session each time. Delete `/app/browser-profile/` if issues persist.

### Upload Issues

**Problem**: "Could not parse timestamp"
- **Cause**: CSV format doesn't match expected format
- **Solution**: Verify CSV has format `DD.MM.YYYY HH:mm` (not `YYYY-MM-DD`)

**Problem**: Duplicate key violation
- **Cause**: Unique constraint on delivery_timestamp
- **Solution**: Script uses UPSERT, so this shouldn't happen. Check migration 027 was applied.

### Database Issues

**Problem**: "Partition not found for date X"
- **Cause**: Trying to insert data for year without partition
- **Solution**: Add partition for that year (see `CEPS_DATABASE_SCHEMA.md`)

---

## File Structure

```
app/
├── ceps/
│   ├── ceps_hybrid_downloader.py       # Download from CEPS website
│   ├── ceps_uploader.py                # Upload CSV to PostgreSQL
│   ├── constants.py                    # CEPS data type definitions
│   ├── IMPLEMENTATION_SUMMARY.md       # Downloader development history
│   ├── CEPS_UPLOADER_GUIDE.md          # Uploader documentation
│   └── 2026/                           # Downloaded CSV files
│       └── 01/
│           └── data_*.csv
├── alembic/
│   └── versions/
│       ├── 20260107_0027_027_add_ceps_imbalance_tables.py
│       └── 20260107_0028_028_add_last_load_to_ceps_15min.py
└── config.py                           # Database configuration

CEPS_DATABASE_SCHEMA.md                 # Database schema documentation
CEPS_COMPLETE_PIPELINE.md               # This file
MIGRATION_027_SUMMARY.md                # Migration 027 guide
MIGRATION_028_SUMMARY.md                # Migration 028 guide
```

---

## Next Steps

1. ✅ **Run migrations** (027, 028)
2. ✅ **Download historical data** for desired date range
3. ✅ **Upload to database** using uploader script
4. ✅ **Verify data** with SQL queries
5. ✅ **Set up cron job** for daily automation
6. 📊 **Build dashboards/reports** using the aggregated data

---

## Support

- **Downloader Issues**: See `app/ceps/IMPLEMENTATION_SUMMARY.md`
- **Uploader Issues**: See `app/ceps/CEPS_UPLOADER_GUIDE.md`
- **Database Schema**: See `CEPS_DATABASE_SCHEMA.md`
- **Migration Issues**: See `MIGRATION_027_SUMMARY.md` and `MIGRATION_028_SUMMARY.md`
