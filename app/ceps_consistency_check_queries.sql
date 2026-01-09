-- ============================================================================
-- CEPS Data Consistency Check - Pure SQL (No psql metacommands)
-- ============================================================================

-- 1. DATA COVERAGE SUMMARY
SELECT
    '1-minute table' AS table_name,
    COUNT(*) AS total_records,
    MIN(DATE(delivery_timestamp)) AS first_date,
    MAX(DATE(delivery_timestamp)) AS last_date,
    COUNT(DISTINCT DATE(delivery_timestamp)) AS unique_days
FROM finance.ceps_actual_imbalance_1min
UNION ALL
SELECT
    '15-minute table' AS table_name,
    COUNT(*) AS total_records,
    MIN(trade_date) AS first_date,
    MAX(trade_date) AS last_date,
    COUNT(DISTINCT trade_date) AS unique_days
FROM finance.ceps_actual_imbalance_15min;

-- 2. DAILY RECORD COUNTS (1-minute table)
-- Expected: 1440 records per day (24 hours * 60 minutes)
WITH daily_counts AS (
    SELECT
        DATE(delivery_timestamp) AS trade_date,
        COUNT(*) AS record_count,
        1440 AS expected_count,
        1440 - COUNT(*) AS missing_records
    FROM finance.ceps_actual_imbalance_1min
    GROUP BY DATE(delivery_timestamp)
)
SELECT
    trade_date,
    record_count,
    expected_count,
    missing_records,
    CASE
        WHEN record_count = expected_count THEN '✓ COMPLETE'
        WHEN record_count > expected_count THEN '⚠ DUPLICATES'
        ELSE '✗ INCOMPLETE'
    END AS status
FROM daily_counts
ORDER BY trade_date DESC;

-- 3. DAILY INTERVAL COUNTS (15-minute table)
-- Expected: 96 intervals per day (24 hours * 4 intervals per hour)
WITH daily_counts AS (
    SELECT
        trade_date,
        COUNT(*) AS interval_count,
        96 AS expected_count,
        96 - COUNT(*) AS missing_intervals
    FROM finance.ceps_actual_imbalance_15min
    GROUP BY trade_date
)
SELECT
    trade_date,
    interval_count,
    expected_count,
    missing_intervals,
    CASE
        WHEN interval_count = expected_count THEN '✓ COMPLETE'
        WHEN interval_count > expected_count THEN '⚠ DUPLICATES'
        ELSE '✗ INCOMPLETE'
    END AS status
FROM daily_counts
ORDER BY trade_date DESC;

-- 4. MISSING DATES (Gaps in Date Sequence)
WITH RECURSIVE date_range AS (
    SELECT
        MIN(DATE(delivery_timestamp)) AS check_date,
        MAX(DATE(delivery_timestamp)) AS max_date
    FROM finance.ceps_actual_imbalance_1min
    UNION ALL
    SELECT
        (check_date + INTERVAL '1 day')::DATE,
        max_date
    FROM date_range
    WHERE check_date < max_date
),
actual_dates_1min AS (
    SELECT DISTINCT DATE(delivery_timestamp) AS trade_date
    FROM finance.ceps_actual_imbalance_1min
),
actual_dates_15min AS (
    SELECT DISTINCT trade_date
    FROM finance.ceps_actual_imbalance_15min
)
SELECT
    dr.check_date::DATE AS missing_date,
    CASE
        WHEN ad1.trade_date IS NULL THEN '✗ Missing in 1min'
        ELSE '✓ In 1min'
    END AS status_1min,
    CASE
        WHEN ad15.trade_date IS NULL THEN '✗ Missing in 15min'
        ELSE '✓ In 15min'
    END AS status_15min
FROM date_range dr
LEFT JOIN actual_dates_1min ad1 ON dr.check_date::DATE = ad1.trade_date
LEFT JOIN actual_dates_15min ad15 ON dr.check_date::DATE = ad15.trade_date
WHERE ad1.trade_date IS NULL OR ad15.trade_date IS NULL
ORDER BY dr.check_date;

-- 5. DATA MISMATCH: 1min vs 15min tables
WITH dates_1min AS (
    SELECT DISTINCT DATE(delivery_timestamp) AS trade_date
    FROM finance.ceps_actual_imbalance_1min
),
dates_15min AS (
    SELECT DISTINCT trade_date
    FROM finance.ceps_actual_imbalance_15min
)
SELECT
    COALESCE(d1.trade_date, d15.trade_date) AS trade_date,
    CASE WHEN d1.trade_date IS NOT NULL THEN '✓' ELSE '✗' END AS in_1min,
    CASE WHEN d15.trade_date IS NOT NULL THEN '✓' ELSE '✗' END AS in_15min,
    CASE
        WHEN d1.trade_date IS NULL THEN '⚠ Missing in 1min'
        WHEN d15.trade_date IS NULL THEN '⚠ Missing in 15min'
        ELSE '✓ Both tables'
    END AS status
FROM dates_1min d1
FULL OUTER JOIN dates_15min d15 ON d1.trade_date = d15.trade_date
WHERE d1.trade_date IS NULL OR d15.trade_date IS NULL
ORDER BY trade_date DESC;

-- 6. NULL VALUES CHECK
SELECT
    '1min: load_mw' AS field,
    COUNT(*) AS null_count
FROM finance.ceps_actual_imbalance_1min
WHERE load_mw IS NULL
UNION ALL
SELECT
    '15min: load_mean_mw' AS field,
    COUNT(*) AS null_count
FROM finance.ceps_actual_imbalance_15min
WHERE load_mean_mw IS NULL
UNION ALL
SELECT
    '15min: load_median_mw' AS field,
    COUNT(*) AS null_count
FROM finance.ceps_actual_imbalance_15min
WHERE load_median_mw IS NULL
UNION ALL
SELECT
    '15min: last_load_at_interval_mw' AS field,
    COUNT(*) AS null_count
FROM finance.ceps_actual_imbalance_15min
WHERE last_load_at_interval_mw IS NULL;

-- 7. SUMMARY STATISTICS
WITH date_coverage AS (
    SELECT
        MIN(DATE(delivery_timestamp)) AS first_date,
        MAX(DATE(delivery_timestamp)) AS last_date,
        MAX(DATE(delivery_timestamp)) -
        MIN(DATE(delivery_timestamp)) + 1 AS expected_days,
        COUNT(DISTINCT DATE(delivery_timestamp)) AS actual_days
    FROM finance.ceps_actual_imbalance_1min
)
SELECT
    first_date,
    last_date,
    expected_days AS days_in_range,
    actual_days AS days_with_data,
    expected_days - actual_days AS missing_days,
    ROUND(100.0 * actual_days / expected_days, 2) AS coverage_percent
FROM date_coverage;
