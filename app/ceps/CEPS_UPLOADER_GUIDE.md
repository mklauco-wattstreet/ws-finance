# CEPS Uploader Guide

## Overview

The **`ceps_uploader.py`** script uploads CEPS (Czech Electricity Power System) actual system imbalance data from CSV files to PostgreSQL.

It performs two operations:
1. **Upload raw 1-minute data** → `finance.ceps_actual_imbalance_1min`
2. **Aggregate to 15-minute intervals** → `finance.ceps_actual_imbalance_15min`

---

## CSV File Format

The uploader expects CSV files downloaded by `ceps_hybrid_downloader.py`:

```csv
Verze dat;Od;Do;Agregační funkce;Agregace;
;04.01.2026 00:00:00;04.01.2026 23:59:59;agregace průměr;minuta;
Datum;Aktuální odchylka [MW];
04.01.2026 00:00;-160.5321;
04.01.2026 00:01;-98.32527;
04.01.2026 00:02;-82.04894;
```

**Format Details**:
- **Line 1-3**: Headers and metadata (skipped)
- **Line 4+**: Data rows with semicolon separator
- **Column 1**: Date/time in format `DD.MM.YYYY HH:mm` (Europe/Prague timezone)
- **Column 2**: Load in MW (can be negative for imbalance)

---

## Usage

### Upload Single File

```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --file /app/scripts/ceps/2026/01/data_AktualniSystemovaOdchylkaCR_20260104_141035.csv
```

### Upload All Files from Folder

```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --folder /app/scripts/ceps/2026/01
```

This will upload **all `*.csv` files** in the specified folder.

### With Debug Logging

```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --folder /app/scripts/ceps/2026/01 \
  --debug
```

---

## What It Does

### Step 1: Parse CSV Files

- Reads CSV file starting from line 4
- Parses timestamp in `DD.MM.YYYY HH:mm` format
- Converts to timezone-aware datetime (Europe/Prague)
- Extracts load value in MW

### Step 2: Upload to 1-Minute Table

- Bulk inserts using `psycopg2.extras.execute_values`
- Uses **UPSERT** logic: `ON CONFLICT (delivery_timestamp) DO UPDATE`
- Updates existing records if timestamp already exists
- Table: `finance.ceps_actual_imbalance_1min`

### Step 3: Aggregate to 15-Minute Intervals

For each unique trade date in the uploaded data:

- Groups 1-minute records into 15-minute buckets
- Calculates:
  - **`load_mean_mw`**: Average load in interval
  - **`load_median_mw`**: Median load in interval
  - **`last_load_at_interval_mw`**: Last (most recent) load in interval
- Creates time intervals: `00:00-00:15`, `00:15-00:30`, etc.
- Uses **UPSERT** logic: `ON CONFLICT (trade_date, time_interval) DO UPDATE`
- Table: `finance.ceps_actual_imbalance_15min`

---

## Example Output

```
======================================================================
Processing: /app/scripts/ceps/2026/01/data_AktualniSystemovaOdchylkaCR_20260104_141035.csv
======================================================================
2026-01-07T15:30:15 - INFO - Parsing CSV: data_AktualniSystemovaOdchylkaCR_20260104_141035.csv
2026-01-07T15:30:15 - INFO - ✓ Parsed 1440 records from data_AktualniSystemovaOdchylkaCR_20260104_141035.csv
2026-01-07T15:30:15 - INFO - Uploading 1440 records to ceps_actual_imbalance_1min...
2026-01-07T15:30:16 - INFO - ✓ Uploaded 1440 records to 1min table
2026-01-07T15:30:16 - INFO - Aggregating data for 2026-01-04 to 15min intervals...
2026-01-07T15:30:16 - INFO - ✓ Created/updated 96 15-minute intervals for 2026-01-04
======================================================================
✓ Completed: data_AktualniSystemovaOdchylkaCR_20260104_141035.csv
  1min records: 1440
  15min intervals: 96
======================================================================

======================================================================
UPLOAD SUMMARY
======================================================================
Files processed: 1
  Successful: 1
  Failed: 0
Total 1min records: 1,440
Total 15min intervals: 96
======================================================================
```

---

## Complete Workflow

### 1. Download Data

```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
  --tag AktualniSystemovaOdchylkaCR \
  --start-date 2026-01-04 \
  --end-date 2026-01-04
```

This creates a CSV file in `/app/scripts/ceps/2026/01/`

### 2. Upload to Database

```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --folder /app/scripts/ceps/2026/01
```

### 3. Verify Data

```sql
-- Check 1-minute data
SELECT COUNT(*), MIN(delivery_timestamp), MAX(delivery_timestamp)
FROM finance.ceps_actual_imbalance_1min
WHERE DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') = '2026-01-04';

-- Check 15-minute aggregated data
SELECT COUNT(*), MIN(time_interval), MAX(time_interval)
FROM finance.ceps_actual_imbalance_15min
WHERE trade_date = '2026-01-04';

-- Sample 15-minute data
SELECT trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw
FROM finance.ceps_actual_imbalance_15min
WHERE trade_date = '2026-01-04'
ORDER BY time_interval
LIMIT 10;
```

---

## Error Handling

### Duplicate Data

The uploader uses **UPSERT** logic, so re-running it on the same data is safe:
- Existing 1-minute records are **updated** with new values
- Existing 15-minute intervals are **recalculated** with current 1-minute data

### Failed Files

If processing multiple files and one fails:
- Script continues with remaining files
- Failed files are logged
- Exit code 1 if any failures occurred

### Invalid Data

- Rows with invalid timestamps are skipped with a warning
- Rows with invalid load values are skipped with a warning
- Empty rows are ignored

---

## Timezone Handling

**Critical**: All timestamps are handled in **Europe/Prague** timezone:

1. CSV contains naive timestamps (no timezone info)
2. Script localizes them to `Europe/Prague` using `pytz`
3. Database stores as `TIMESTAMP WITH TIME ZONE`
4. Aggregation uses `AT TIME ZONE 'Europe/Prague'` for correct bucketing

This ensures correct handling of:
- DST transitions (UTC+1 → UTC+2)
- Cross-midnight intervals
- Date calculations

---

## Performance

- **Bulk inserts**: Uses `execute_values` for efficient batch operations
- **Single transaction per file**: Commits after both 1min and 15min uploads
- **Partition-aware**: Data automatically routed to correct year partition

**Typical performance**:
- 1 day (1,440 records): ~1-2 seconds
- 1 month (~43,200 records): ~30-45 seconds

---

## Automation with Cron

Add to `/app/crontab` for daily uploads:

```cron
# Download and upload CEPS data daily at 02:00
0 2 * * * export $(cat /etc/environment_for_cron | xargs) && \
          /usr/local/bin/python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
          --tag AktualniSystemovaOdchylkaCR >> /var/log/ceps_download.log 2>&1 && \
          /usr/local/bin/python3 /app/scripts/ceps/ceps_uploader.py \
          --folder /app/scripts/ceps/$(date +\%Y)/$(date +\%m) >> /var/log/ceps_upload.log 2>&1
```

---

## Troubleshooting

### No PHPSESSID cookie warning

This is from the downloader, not the uploader. As long as the CSV files contain correct data, the uploader will work.

### "No data found in CSV file"

Check:
1. File is not empty
2. Data starts on line 4
3. Semicolon-separated format
4. UTF-8 encoding (with or without BOM)

### "Could not parse timestamp"

Check date format matches: `DD.MM.YYYY HH:mm`

Example: `04.01.2026 00:00` (correct)
Not: `2026-01-04 00:00` (wrong)

### Database connection failed

Verify `.env` file has correct credentials:
```bash
DB_HOST=your_host
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=your_database
DB_PORT=5432
```

---

## Related Documentation

- **Downloader**: `app/ceps/IMPLEMENTATION_SUMMARY.md`
- **Database Schema**: `CEPS_DATABASE_SCHEMA.md`
- **Migrations**: `MIGRATION_027_SUMMARY.md`, `MIGRATION_028_SUMMARY.md`
