# CEPS Actual Imbalance Data - Database Schema

## Overview

Two tables store CEPS (Czech Electricity Power System) actual system imbalance data:
1. **`ceps_actual_imbalance_1min`** - Raw minute-level imbalance measurements
2. **`ceps_actual_imbalance_15min`** - Aggregated 15-minute statistics

**IMPORTANT**: All timestamps are stored as **naive TIMESTAMP** (no timezone) in **Europe/Prague local time**.

## Table: `finance.ceps_actual_imbalance_1min`

### Schema
```sql
CREATE TABLE finance.ceps_actual_imbalance_1min (
    id BIGSERIAL,
    delivery_timestamp TIMESTAMP NOT NULL,  -- Naive timestamp in Europe/Prague local time
    load_mw NUMERIC(12,5) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_1min_delivery_timestamp UNIQUE (delivery_timestamp)
) PARTITION BY RANGE (delivery_timestamp);
```

### Partitioning
- **Strategy**: RANGE partitioning by `delivery_timestamp`
- **Partition Key**: Direct partitioning by `delivery_timestamp` column
- **Partitions**: One per year (2024, 2025, 2026, 2027, 2028)
- **Timezone**: Data stored as naive timestamps in Europe/Prague local time (no automatic conversion)

**Partition Boundaries**:
```sql
-- 2024 partition
CREATE TABLE finance.ceps_actual_imbalance_1min_2024
PARTITION OF finance.ceps_actual_imbalance_1min
FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00');

-- 2025 partition
CREATE TABLE finance.ceps_actual_imbalance_1min_2025
PARTITION OF finance.ceps_actual_imbalance_1min
FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00');

-- ... etc
```

### UPSERT Logic

**Unique Constraint**: `uq_ceps_1min_delivery_timestamp UNIQUE (delivery_timestamp)`

```sql
INSERT INTO finance.ceps_actual_imbalance_1min (delivery_timestamp, load_mw)
VALUES ('2026-01-04 00:00:00', -160.5321)
ON CONFLICT (delivery_timestamp) DO UPDATE SET
    load_mw = EXCLUDED.load_mw,
    created_at = CURRENT_TIMESTAMP;
```

### Bulk Insert with `psycopg2`

```python
from psycopg2.extras import execute_values
from datetime import datetime

# Parse timestamps as naive (no timezone)
data = [
    (datetime(2026, 1, 4, 0, 0, 0), -160.5321),
    (datetime(2026, 1, 4, 0, 1, 0), -98.3253),
    # ... more rows
]

with conn.cursor() as cur:
    execute_values(
        cur,
        """
        INSERT INTO finance.ceps_actual_imbalance_1min (delivery_timestamp, load_mw)
        VALUES %s
        ON CONFLICT (delivery_timestamp) DO UPDATE SET
            load_mw = EXCLUDED.load_mw,
            created_at = CURRENT_TIMESTAMP
        """,
        data
    )
    conn.commit()
```

**IMPORTANT**: Always use naive `datetime` objects when inserting. Do NOT use timezone-aware timestamps.

```python
# CORRECT - Naive timestamp
delivery_timestamp = datetime.strptime("04.01.2026 00:00", "%d.%m.%Y %H:%M")

# WRONG - Timezone-aware timestamp (will cause errors)
import pytz
prague_tz = pytz.timezone('Europe/Prague')
delivery_timestamp = prague_tz.localize(datetime(2026, 1, 4, 0, 0, 0))  # Don't do this!
```

## Table: `finance.ceps_actual_imbalance_15min`

### Schema
```sql
CREATE TABLE finance.ceps_actual_imbalance_15min (
    id BIGSERIAL,
    trade_date DATE NOT NULL,
    time_interval VARCHAR(11) NOT NULL,  -- e.g., "00:00-00:15"
    load_mean_mw NUMERIC(12,5),
    load_median_mw NUMERIC(12,5),
    last_load_at_interval_mw NUMERIC(12,5),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ceps_15min_trade_date_interval UNIQUE (trade_date, time_interval)
) PARTITION BY RANGE (trade_date);
```

### Partitioning
- **Strategy**: RANGE partitioning by `trade_date`
- **Partition Key**: Year extracted from `trade_date`
- **Partitions**: One per year (2024, 2025, 2026, 2027, 2028)

**Partition Boundaries**:
```sql
-- 2024 partition
CREATE TABLE finance.ceps_actual_imbalance_15min_2024
PARTITION OF finance.ceps_actual_imbalance_15min
FOR VALUES FROM (2024) TO (2025);

-- 2025 partition
CREATE TABLE finance.ceps_actual_imbalance_15min_2025
PARTITION OF finance.ceps_actual_imbalance_15min
FOR VALUES FROM (2025) TO (2026);

-- ... etc
```

### UPSERT Logic

**Unique Constraint**: `uq_ceps_15min_trade_date_interval UNIQUE (trade_date, time_interval)`

```sql
INSERT INTO finance.ceps_actual_imbalance_15min
    (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
VALUES ('2026-01-04', '00:00-00:15', -150.2, -145.8, -142.1)
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    load_mean_mw = EXCLUDED.load_mean_mw,
    load_median_mw = EXCLUDED.load_median_mw,
    last_load_at_interval_mw = EXCLUDED.last_load_at_interval_mw,
    created_at = CURRENT_TIMESTAMP;
```

### Bulk Insert with `psycopg2`

```python
from psycopg2.extras import execute_values
from datetime import date

data = [
    (date(2026, 1, 4), '00:00-00:15', -150.2, -145.8, -142.1),
    (date(2026, 1, 4), '00:15-00:30', -120.5, -118.3, -115.2),
    # ... more rows
]

with conn.cursor() as cur:
    execute_values(
        cur,
        """
        INSERT INTO finance.ceps_actual_imbalance_15min
            (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
        VALUES %s
        ON CONFLICT (trade_date, time_interval) DO UPDATE SET
            load_mean_mw = EXCLUDED.load_mean_mw,
            load_median_mw = EXCLUDED.load_median_mw,
            last_load_at_interval_mw = EXCLUDED.last_load_at_interval_mw,
            created_at = CURRENT_TIMESTAMP
        """,
        data
    )
    conn.commit()
```

## Aggregation Query

To populate the 15-minute table from 1-minute data (with naive timestamps):

```sql
INSERT INTO finance.ceps_actual_imbalance_15min
    (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
WITH interval_data AS (
    SELECT
        DATE(delivery_timestamp) AS trade_date,
        DATE_TRUNC('hour', delivery_timestamp) +
        INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
        delivery_timestamp,
        load_mw
    FROM finance.ceps_actual_imbalance_1min
    WHERE DATE(delivery_timestamp) = '2026-01-04'
),
aggregated AS (
    SELECT
        trade_date,
        TO_CHAR(interval_start, 'HH24:MI') || '-' ||
        TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
        AVG(load_mw) AS load_mean_mw,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY load_mw) AS load_median_mw,
        (ARRAY_AGG(load_mw ORDER BY delivery_timestamp DESC))[1] AS last_load_at_interval_mw
    FROM interval_data
    GROUP BY trade_date, interval_start
)
SELECT
    trade_date,
    time_interval,
    load_mean_mw,
    load_median_mw,
    last_load_at_interval_mw
FROM aggregated
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    load_mean_mw = EXCLUDED.load_mean_mw,
    load_median_mw = EXCLUDED.load_median_mw,
    last_load_at_interval_mw = EXCLUDED.last_load_at_interval_mw,
    created_at = CURRENT_TIMESTAMP;
```

**Note**: No `AT TIME ZONE` conversions needed since timestamps are already stored as naive in Europe/Prague local time.

## Rebuilding 15min Table from 1min Data

To completely rebuild the 15min table:

```bash
# Truncate 15min table
docker compose exec entsoe-ote-data-uploader psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "TRUNCATE TABLE finance.ceps_actual_imbalance_15min;"

# Re-aggregate all data
docker compose exec entsoe-ote-data-uploader psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
INSERT INTO finance.ceps_actual_imbalance_15min
    (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
WITH interval_data AS (
    SELECT
        DATE(delivery_timestamp) AS trade_date,
        DATE_TRUNC('hour', delivery_timestamp) +
        INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
        delivery_timestamp,
        load_mw
    FROM finance.ceps_actual_imbalance_1min
),
aggregated AS (
    SELECT
        trade_date,
        TO_CHAR(interval_start, 'HH24:MI') || '-' ||
        TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
        AVG(load_mw) AS load_mean_mw,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY load_mw) AS load_median_mw,
        (ARRAY_AGG(load_mw ORDER BY delivery_timestamp DESC))[1] AS last_load_at_interval_mw
    FROM interval_data
    GROUP BY trade_date, interval_start
)
SELECT
    trade_date,
    time_interval,
    load_mean_mw,
    load_median_mw,
    last_load_at_interval_mw
FROM aggregated
ORDER BY trade_date, time_interval;
"
```

## Adding Future Partitions

When a new year approaches, add new partitions:

```sql
-- For 1-minute table (naive timestamps)
CREATE TABLE finance.ceps_actual_imbalance_1min_2029
PARTITION OF finance.ceps_actual_imbalance_1min
FOR VALUES FROM ('2029-01-01 00:00:00') TO ('2030-01-01 00:00:00');

-- For 15-minute table
CREATE TABLE finance.ceps_actual_imbalance_15min_2029
PARTITION OF finance.ceps_actual_imbalance_15min
FOR VALUES FROM (2029) TO (2030);
```

## Data Pipeline

### 1. Combined Download and Upload (Recommended)

Use `ceps_runner.py` to download and immediately upload:

```bash
# Download and upload today's data (for cron)
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_runner.py

# Download and upload specific date
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_runner.py \
  --start-date 2026-01-07 \
  --end-date 2026-01-07
```

The runner:
- Downloads CSV from CEPS website
- Parses CSV (naive timestamps in Prague local time)
- Uploads to `ceps_actual_imbalance_1min` (UPSERT)
- Aggregates to `ceps_actual_imbalance_15min` (UPSERT)

### 2. Manual: Download Only

Downloaded from CEPS website using `ceps_hybrid_downloader.py`:
```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_hybrid_downloader.py \
  --tag AktualniSystemovaOdchylkaCR \
  --start-date 2026-01-04 \
  --end-date 2026-01-04
```

Files are saved to: `/app/scripts/ceps/YYYY/MM/data_*.csv`

### 3. Manual: Upload Only

Use `ceps_uploader.py` to upload CSV files to PostgreSQL:

```bash
# Upload all CSV files from a folder
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --folder /app/scripts/ceps/2026/01

# Upload single file
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_uploader.py \
  --file /app/scripts/ceps/2026/01/data_AktualniSystemovaOdchylkaCR_20260104_141035.csv
```

The uploader:
- Parses CSV files (starting from line 4)
- Stores timestamps as naive (no timezone conversion)
- Uploads to `ceps_actual_imbalance_1min` (UPSERT)
- Aggregates to `ceps_actual_imbalance_15min` (UPSERT)

See: `app/ceps/CEPS_UPLOADER_GUIDE.md` for complete documentation.

## Automated Data Collection (Cron)

CEPS data is automatically fetched every 15 minutes via cron:

```bash
# From crontab
*/15 * * * * export $(cat /etc/environment_for_cron | xargs) && cd /app/scripts && /usr/local/bin/python3 ceps/ceps_runner.py >> /var/log/cron.log 2>&1
```

This keeps the database updated with the latest imbalance data throughout the day.

## Data Consistency Check

Run consistency checks to identify gaps and missing data:

```bash
docker compose exec entsoe-ote-data-uploader \
  python3 /app/scripts/ceps/ceps_consistency_check.py
```

The check validates:
- All days have 1440 records (1min table)
- All days have 96 intervals (15min table)
- No missing dates in sequence
- No NULL values
- Both tables have same date coverage

See: `CEPS_CONSISTENCY_CHECK_GUIDE.md` for details.

## Migration History

### Migration 027 (2026-01-07)
- **File**: `20260107_0027_027_add_ceps_imbalance_tables.py`
- **Action**: Created initial CEPS tables with TIMESTAMPTZ

### Migration 028 (2026-01-07)
- **File**: `20260107_0028_028_add_last_load_to_ceps_15min.py`
- **Action**: Added `last_load_at_interval_mw` column to 15min table

### Migration 029 (2026-01-07)
- **File**: `20260107_0029_029_change_ceps_timestamp_to_naive.py`
- **Action**: Converted `delivery_timestamp` from TIMESTAMPTZ to TIMESTAMP (naive)
- **Reason**: Eliminate timezone conversion disruptions
- **Data**: All timestamps converted from UTC to Europe/Prague local time

## Timezone Handling Summary

| Aspect | Details |
|--------|---------|
| **Storage** | Naive TIMESTAMP (no timezone) |
| **Timezone** | Europe/Prague local time |
| **CSV Format** | `DD.MM.YYYY HH:mm` (already in Prague time) |
| **Python Parsing** | Use naive `datetime.strptime()` - NO pytz |
| **SQL Queries** | No `AT TIME ZONE` conversions needed |
| **Partitioning** | Direct by `delivery_timestamp` column |

**Key Principle**: All timestamps represent Europe/Prague local time. No timezone conversions occur in the application or database layer.
