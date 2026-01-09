-- ============================================================================
-- CEPS Data Consistency Check
-- ============================================================================
-- Checks for gaps, missing data, and inconsistencies in CEPS imbalance tables
--
-- Tables checked:
-- - finance.ceps_actual_imbalance_1min
-- - finance.ceps_actual_imbalance_15min
--
-- Run with: \i CEPS_DATA_CONSISTENCY_CHECK.sql
-- Or copy-paste into psql
-- ============================================================================

\echo '============================================================================'
\echo 'CEPS DATA CONSISTENCY CHECK'
\echo '============================================================================'
\echo ''

-- ============================================================================
-- 1. DATA COVERAGE SUMMARY
-- ============================================================================
\echo '1. DATA COVERAGE SUMMARY'
\echo '============================================================================'

SELECT
    '1-minute table' AS table_name,
    COUNT(*) AS total_records,
    MIN(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS first_date,
    MAX(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS last_date,
    COUNT(DISTINCT DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS unique_days
FROM finance.ceps_actual_imbalance_1min

UNION ALL

SELECT
    '15-minute table' AS table_name,
    COUNT(*) AS total_records,
    MIN(trade_date) AS first_date,
    MAX(trade_date) AS last_date,
    COUNT(DISTINCT trade_date) AS unique_days
FROM finance.ceps_actual_imbalance_15min;

\echo ''

-- ============================================================================
-- 2. DAILY RECORD COUNTS (1-minute table)
-- ============================================================================
\echo '2. DAILY RECORD COUNTS - 1-minute table'
\echo '============================================================================'
\echo 'Expected: 1440 records per day (24 hours * 60 minutes)'
\echo ''

WITH daily_counts AS (
    SELECT
        DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS trade_date,
        COUNT(*) AS record_count,
        1440 AS expected_count,
        1440 - COUNT(*) AS missing_records
    FROM finance.ceps_actual_imbalance_1min
    GROUP BY DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')
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

\echo ''

-- ============================================================================
-- 3. DAILY INTERVAL COUNTS (15-minute table)
-- ============================================================================
\echo '3. DAILY INTERVAL COUNTS - 15-minute table'
\echo '============================================================================'
\echo 'Expected: 96 intervals per day (24 hours * 4 intervals per hour)'
\echo ''

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

\echo ''

-- ============================================================================
-- 4. MISSING DATES (Gaps in Date Sequence)
-- ============================================================================
\echo '4. MISSING DATES - Gaps in Date Sequence'
\echo '============================================================================'

WITH RECURSIVE date_range AS (
    -- Get min and max dates from 1-minute table
    SELECT
        MIN(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS check_date,
        MAX(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS max_date
    FROM finance.ceps_actual_imbalance_1min

    UNION ALL

    -- Generate all dates in range
    SELECT
        check_date + INTERVAL '1 day',
        max_date
    FROM date_range
    WHERE check_date < max_date
),
actual_dates_1min AS (
    SELECT DISTINCT DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS trade_date
    FROM finance.ceps_actual_imbalance_1min
),
actual_dates_15min AS (
    SELECT DISTINCT trade_date
    FROM finance.ceps_actual_imbalance_15min
)
SELECT
    dr.check_date::DATE AS missing_date,
    CASE
        WHEN ad1.trade_date IS NULL THEN '✗ Missing in 1min table'
        ELSE '✓ Present in 1min table'
    END AS status_1min,
    CASE
        WHEN ad15.trade_date IS NULL THEN '✗ Missing in 15min table'
        ELSE '✓ Present in 15min table'
    END AS status_15min
FROM date_range dr
LEFT JOIN actual_dates_1min ad1 ON dr.check_date::DATE = ad1.trade_date
LEFT JOIN actual_dates_15min ad15 ON dr.check_date::DATE = ad15.trade_date
WHERE ad1.trade_date IS NULL OR ad15.trade_date IS NULL
ORDER BY dr.check_date;

\echo ''
\echo 'If no rows returned, there are no gaps in the date sequence.'
\echo ''

-- ============================================================================
-- 5. MISSING MINUTES (within each day)
-- ============================================================================
\echo '5. MISSING MINUTES - Detailed Gap Analysis (1-minute table)'
\echo '============================================================================'
\echo 'Shows days with missing minutes and their time ranges'
\echo ''

WITH RECURSIVE minutes AS (
    SELECT 0 AS minute_offset
    UNION ALL
    SELECT minute_offset + 1
    FROM minutes
    WHERE minute_offset < 1439  -- 0 to 1439 = 1440 minutes
),
date_list AS (
    SELECT DISTINCT DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS trade_date
    FROM finance.ceps_actual_imbalance_1min
),
expected_timestamps AS (
    SELECT
        dl.trade_date,
        (dl.trade_date + INTERVAL '1 minute' * m.minute_offset)::TIMESTAMP AS expected_timestamp
    FROM date_list dl
    CROSS JOIN minutes m
),
actual_timestamps AS (
    SELECT
        DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS trade_date,
        (delivery_timestamp AT TIME ZONE 'Europe/Prague')::TIMESTAMP AS actual_timestamp
    FROM finance.ceps_actual_imbalance_1min
),
missing_minutes AS (
    SELECT
        et.trade_date,
        et.expected_timestamp
    FROM expected_timestamps et
    LEFT JOIN actual_timestamps at ON
        et.trade_date = at.trade_date AND
        et.expected_timestamp = at.actual_timestamp
    WHERE at.actual_timestamp IS NULL
),
gaps_with_ranges AS (
    SELECT
        trade_date,
        MIN(expected_timestamp) AS gap_start,
        MAX(expected_timestamp) AS gap_end,
        COUNT(*) AS missing_minutes
    FROM (
        SELECT
            trade_date,
            expected_timestamp,
            expected_timestamp - (ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY expected_timestamp) * INTERVAL '1 minute') AS gap_group
        FROM missing_minutes
    ) grouped
    GROUP BY trade_date, gap_group
)
SELECT
    trade_date,
    TO_CHAR(gap_start, 'HH24:MI') AS gap_start_time,
    TO_CHAR(gap_end, 'HH24:MI') AS gap_end_time,
    missing_minutes AS minutes_missing
FROM gaps_with_ranges
ORDER BY trade_date DESC, gap_start;

\echo ''
\echo 'If no rows returned, all days have complete minute-level data.'
\echo ''

-- ============================================================================
-- 6. MISSING 15-MINUTE INTERVALS (within each day)
-- ============================================================================
\echo '6. MISSING 15-MINUTE INTERVALS - Detailed Gap Analysis'
\echo '============================================================================'

WITH RECURSIVE intervals AS (
    SELECT 0 AS interval_num
    UNION ALL
    SELECT interval_num + 1
    FROM intervals
    WHERE interval_num < 95  -- 0 to 95 = 96 intervals
),
date_list AS (
    SELECT DISTINCT trade_date
    FROM finance.ceps_actual_imbalance_15min
),
expected_intervals AS (
    SELECT
        dl.trade_date,
        TO_CHAR(INTERVAL '15 minutes' * i.interval_num, 'HH24:MI') || '-' ||
        TO_CHAR(INTERVAL '15 minutes' * (i.interval_num + 1), 'HH24:MI') AS expected_interval
    FROM date_list dl
    CROSS JOIN intervals i
),
actual_intervals AS (
    SELECT trade_date, time_interval
    FROM finance.ceps_actual_imbalance_15min
),
missing_intervals AS (
    SELECT
        ei.trade_date,
        ei.expected_interval
    FROM expected_intervals ei
    LEFT JOIN actual_intervals ai ON
        ei.trade_date = ai.trade_date AND
        ei.expected_interval = ai.time_interval
    WHERE ai.time_interval IS NULL
)
SELECT
    trade_date,
    expected_interval AS missing_interval
FROM missing_intervals
ORDER BY trade_date DESC, expected_interval;

\echo ''
\echo 'If no rows returned, all days have complete 15-minute intervals.'
\echo ''

-- ============================================================================
-- 7. DATA MISMATCH: 1min vs 15min tables
-- ============================================================================
\echo '7. DATA MISMATCH - Dates in 1min but not in 15min (or vice versa)'
\echo '============================================================================'

WITH dates_1min AS (
    SELECT DISTINCT DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague') AS trade_date
    FROM finance.ceps_actual_imbalance_1min
),
dates_15min AS (
    SELECT DISTINCT trade_date
    FROM finance.ceps_actual_imbalance_15min
)
SELECT
    COALESCE(d1.trade_date, d15.trade_date) AS trade_date,
    CASE WHEN d1.trade_date IS NOT NULL THEN '✓' ELSE '✗' END AS in_1min_table,
    CASE WHEN d15.trade_date IS NOT NULL THEN '✓' ELSE '✗' END AS in_15min_table,
    CASE
        WHEN d1.trade_date IS NULL THEN '⚠ Missing in 1min table'
        WHEN d15.trade_date IS NULL THEN '⚠ Missing in 15min table - NEEDS AGGREGATION'
        ELSE '✓ Present in both'
    END AS status
FROM dates_1min d1
FULL OUTER JOIN dates_15min d15 ON d1.trade_date = d15.trade_date
WHERE d1.trade_date IS NULL OR d15.trade_date IS NULL
ORDER BY trade_date DESC;

\echo ''
\echo 'If no rows returned, both tables have the same date coverage.'
\echo ''

-- ============================================================================
-- 8. NULL VALUES CHECK
-- ============================================================================
\echo '8. NULL VALUES CHECK'
\echo '============================================================================'

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

\echo ''
\echo 'Expected: 0 null values in all fields'
\echo ''

-- ============================================================================
-- 9. SUMMARY STATISTICS
-- ============================================================================
\echo '9. SUMMARY STATISTICS'
\echo '============================================================================'

WITH date_coverage AS (
    SELECT
        MIN(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS first_date,
        MAX(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS last_date,
        MAX(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) -
        MIN(DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) + 1 AS expected_days,
        COUNT(DISTINCT DATE(delivery_timestamp AT TIME ZONE 'Europe/Prague')) AS actual_days
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

\echo ''
\echo '============================================================================'
\echo 'END OF CONSISTENCY CHECK'
\echo '============================================================================'
