# CEPS Data Consistency Check Guide

## Overview

The **`CEPS_DATA_CONSISTENCY_CHECK.sql`** script performs comprehensive validation of CEPS imbalance data to identify:
- Missing dates (gaps in time series)
- Incomplete days (missing minutes/intervals)
- Data mismatches between 1min and 15min tables
- NULL values
- Summary statistics

---

## Running the Check

### Method 1: From Docker Container (Recommended)

```bash
docker compose exec entsoe-ote-data-uploader \
  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME \
  -f /app/scripts/CEPS_DATA_CONSISTENCY_CHECK.sql
```

### Method 2: Interactive psql

```bash
# Connect to database
docker compose exec entsoe-ote-data-uploader \
  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME

# Run the script
\i /app/scripts/CEPS_DATA_CONSISTENCY_CHECK.sql
```

### Method 3: Copy SQL Directly

Copy the contents of `CEPS_DATA_CONSISTENCY_CHECK.sql` and paste into your SQL client (DBeaver, pgAdmin, etc.).

---

## What It Checks

### 1. Data Coverage Summary
Shows overall statistics for both tables:
- Total records
- Date range (first to last)
- Number of unique days

**Expected Output**:
```
 table_name      | total_records | first_date | last_date  | unique_days
-----------------+---------------+------------+------------+-------------
 1-minute table  |     50385     | 2025-12-01 | 2026-01-04 |     35
 15-minute table |      3360     | 2025-12-01 | 2026-01-04 |     35
```

### 2. Daily Record Counts (1-minute table)
Checks if each day has exactly **1440 records** (24 hours × 60 minutes).

**Expected Output**:
```
 trade_date | record_count | expected_count | missing_records | status
------------+--------------+----------------+-----------------+------------
 2026-01-04 |     1440     |      1440      |        0        | ✓ COMPLETE
 2026-01-03 |     1440     |      1440      |        0        | ✓ COMPLETE
 2026-01-02 |     1438     |      1440      |        2        | ✗ INCOMPLETE
```

**Status Meanings**:
- `✓ COMPLETE` - All 1440 records present
- `✗ INCOMPLETE` - Missing some minutes
- `⚠ DUPLICATES` - More than 1440 records (shouldn't happen after deduplication)

### 3. Daily Interval Counts (15-minute table)
Checks if each day has exactly **96 intervals** (24 hours × 4 intervals per hour).

**Expected Output**:
```
 trade_date | interval_count | expected_count | missing_intervals | status
------------+----------------+----------------+-------------------+------------
 2026-01-04 |       96       |       96       |         0         | ✓ COMPLETE
 2026-01-03 |       94       |       96       |         2         | ✗ INCOMPLETE
```

### 4. Missing Dates (Gaps in Sequence)
Identifies any dates missing from the time series.

**Expected Output** (if gaps exist):
```
 missing_date | status_1min               | status_15min
--------------+---------------------------+----------------------------
 2025-12-15   | ✗ Missing in 1min table  | ✗ Missing in 15min table
 2025-12-20   | ✓ Present in 1min table  | ✗ Missing in 15min table
```

**If no gaps**: "If no rows returned, there are no gaps in the date sequence."

### 5. Missing Minutes (Detailed Gap Analysis)
Shows specific time ranges with missing data within each day.

**Expected Output** (if gaps exist):
```
 trade_date | gap_start_time | gap_end_time | minutes_missing
------------+----------------+--------------+-----------------
 2026-01-02 |     03:15      |    03:17     |        3
 2026-01-02 |     14:30      |    14:45     |       16
```

This indicates:
- On 2026-01-02, 3 minutes missing from 03:15 to 03:17
- On 2026-01-02, 16 minutes missing from 14:30 to 14:45

### 6. Missing 15-Minute Intervals
Shows which specific 15-minute intervals are missing within each day.

**Expected Output** (if gaps exist):
```
 trade_date | missing_interval
------------+------------------
 2026-01-03 | 03:00-03:15
 2026-01-03 | 14:30-14:45
```

### 7. Data Mismatch Between Tables
Identifies dates that exist in one table but not the other.

**Expected Output**:
```
 trade_date | in_1min_table | in_15min_table | status
------------+---------------+----------------+----------------------------------
 2025-12-31 |       ✓       |       ✗        | ⚠ Missing in 15min - NEEDS AGGREGATION
```

**Action Required**: If a date exists in 1min but not 15min, you need to run aggregation.

### 8. NULL Values Check
Ensures no NULL values in critical fields.

**Expected Output**:
```
 field                           | null_count
---------------------------------+------------
 1min: load_mw                   |     0
 15min: load_mean_mw             |     0
 15min: load_median_mw           |     0
 15min: last_load_at_interval_mw |     0
```

**Expected**: All null_count should be 0.

### 9. Summary Statistics
Overall data quality metrics.

**Expected Output**:
```
 first_date | last_date  | days_in_range | days_with_data | missing_days | coverage_percent
------------+------------+---------------+----------------+--------------+------------------
 2025-12-01 | 2026-01-04 |      35       |       35       |      0       |     100.00
```

**Ideal**: coverage_percent = 100.00%

---

## Interpreting Results

### ✅ Healthy Data (Example)
```
✓ All days have 1440 records in 1min table
✓ All days have 96 intervals in 15min table
✓ No missing dates in the sequence
✓ No gaps within days
✓ Both tables have same date coverage
✓ No NULL values
✓ 100% coverage
```

### ⚠️ Common Issues

#### Issue 1: Incomplete Days
```
trade_date | record_count | missing_records | status
2026-01-02 |     1438     |        2        | ✗ INCOMPLETE
```

**Cause**: CSV file was incomplete or had errors during parsing.

**Action**:
1. Check the CSV file: `/app/scripts/ceps/2026/01/data_*.csv`
2. Re-download the date:
   ```bash
   docker compose exec entsoe-ote-data-uploader \
     python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
     --tag AktualniSystemovaOdchylkaCR \
     --start-date 2026-01-02 \
     --end-date 2026-01-02
   ```
3. Re-upload:
   ```bash
   docker compose exec entsoe-ote-data-uploader \
     python3 /app/scripts/ceps/ceps_uploader.py \
     --folder /app/scripts/ceps/2026/01
   ```

#### Issue 2: Missing 15min Aggregation
```
trade_date | in_1min_table | in_15min_table | status
2025-12-31 |       ✓       |       ✗        | ⚠ Missing in 15min - NEEDS AGGREGATION
```

**Cause**: Data uploaded to 1min table but aggregation failed or wasn't run.

**Action**: Re-run uploader (it will aggregate automatically):
```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --folder /app/scripts/ceps/2025/12
```

Or run manual aggregation (SQL):
```sql
-- See section "Manual Aggregation" below
```

#### Issue 3: Date Gaps
```
missing_date | status_1min              | status_15min
2025-12-15   | ✗ Missing in 1min table | ✗ Missing in 15min table
```

**Cause**: Data was never downloaded for this date.

**Action**: Download and upload the missing date:
```bash
# Download
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
  --tag AktualniSystemovaOdchylkaCR \
  --start-date 2025-12-15 \
  --end-date 2025-12-15

# Upload
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --folder /app/scripts/ceps/2025/12
```

---

## Manual Aggregation

If you need to manually aggregate specific dates to 15-minute table:

```sql
-- Aggregate a specific date
INSERT INTO finance.ceps_actual_imbalance_15min
    (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
WITH interval_data AS (
    SELECT
        DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS trade_date,
        DATE_TRUNC('hour', delivery_timestamp AT TIME ZONE 'Europe/Prague') +
        INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp AT TIME ZONE 'Europe/Prague') / 15) AS interval_start,
        delivery_timestamp,
        load_mw
    FROM finance.ceps_actual_imbalance_1min
    WHERE DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') = '2025-12-31'  -- Change this date
)
SELECT
    trade_date,
    TO_CHAR(interval_start, 'HH24:MI') || '-' ||
    TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
    AVG(load_mw) AS load_mean_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY load_mw) AS load_median_mw,
    (ARRAY_AGG(load_mw ORDER BY delivery_timestamp DESC))[1] AS last_load_at_interval_mw
FROM interval_data
GROUP BY trade_date, interval_start
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    load_mean_mw = EXCLUDED.load_mean_mw,
    load_median_mw = EXCLUDED.load_median_mw,
    last_load_at_interval_mw = EXCLUDED.last_load_at_interval_mw,
    created_at = CURRENT_TIMESTAMP;
```

---

## Automation

Add consistency checks to your daily cron:

```cron
# Run consistency check daily at 03:00 (after data upload)
0 3 * * * export $(cat /etc/environment_for_cron | xargs) && \
          psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
          -f /app/scripts/CEPS_DATA_CONSISTENCY_CHECK.sql \
          >> /var/log/ceps_consistency_check.log 2>&1
```

---

## Quick Reference Commands

```bash
# Run consistency check
docker compose exec entsoe-ote-data-uploader \
  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME \
  -f /app/scripts/CEPS_DATA_CONSISTENCY_CHECK.sql

# Count total records
docker compose exec entsoe-ote-data-uploader \
  psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
  -c "SELECT COUNT(*) FROM finance.ceps_actual_imbalance_1min;"

# Check specific date
docker compose exec entsoe-ote-data-uploader \
  psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
  -c "SELECT COUNT(*) FROM finance.ceps_actual_imbalance_1min WHERE DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') = '2026-01-04';"

# List all dates with data
docker compose exec entsoe-ote-data-uploader \
  psql -h $DB_HOST -U $DB_USER -d $DB_NAME \
  -c "SELECT DISTINCT DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS date FROM finance.ceps_actual_imbalance_1min ORDER BY date DESC;"
```

---

## Expected Results for Complete Data

For a fully consistent dataset:

1. ✅ All days have exactly 1440 records (1min table)
2. ✅ All days have exactly 96 intervals (15min table)
3. ✅ No missing dates between first and last date
4. ✅ No gaps within individual days
5. ✅ Both tables cover the same dates
6. ✅ All load fields have no NULL values
7. ✅ 100% data coverage

---

## Related Documentation

- **Upload Data**: `app/ceps/CEPS_UPLOADER_GUIDE.md`
- **Database Schema**: `CEPS_DATABASE_SCHEMA.md`
- **Complete Pipeline**: `CEPS_COMPLETE_PIPELINE.md`
