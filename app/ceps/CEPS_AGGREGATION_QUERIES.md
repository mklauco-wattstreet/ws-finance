# CEPS 15-Minute Aggregation Queries

SQL queries for aggregating 1-minute CEPS data into 15-minute intervals.

Each query:
- Groups 1-minute data into 15-minute intervals
- Calculates **mean**, **median**, and **last value at interval** for each metric
- Uses `time_interval` format: `"HH:MM-HH:MM"` (e.g., `"14:00-14:15"`)

---

## 1. System Imbalance

**Tables:** `ceps_actual_imbalance_1min` → `ceps_actual_imbalance_15min`

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
)
SELECT
    trade_date,
    TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
    AVG(load_mw) AS load_mean_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY load_mw) AS load_median_mw,
    (ARRAY_AGG(load_mw ORDER BY delivery_timestamp DESC))[1] AS last_load_at_interval_mw
FROM interval_data
GROUP BY trade_date, interval_start
ORDER BY trade_date, interval_start;
```

---

## 2. RE Price (Balancing Energy Prices)

**Tables:** `ceps_actual_re_price_1min` → `ceps_actual_re_price_15min`

```sql
INSERT INTO finance.ceps_actual_re_price_15min
    (trade_date, time_interval,
     price_afrr_plus_mean_eur_mwh, price_afrr_minus_mean_eur_mwh,
     price_mfrr_plus_mean_eur_mwh, price_mfrr_minus_mean_eur_mwh, price_mfrr_5_mean_eur_mwh,
     price_afrr_plus_median_eur_mwh, price_afrr_minus_median_eur_mwh,
     price_mfrr_plus_median_eur_mwh, price_mfrr_minus_median_eur_mwh, price_mfrr_5_median_eur_mwh,
     price_afrr_plus_last_at_interval_eur_mwh, price_afrr_minus_last_at_interval_eur_mwh,
     price_mfrr_plus_last_at_interval_eur_mwh, price_mfrr_minus_last_at_interval_eur_mwh, price_mfrr_5_last_at_interval_eur_mwh)
WITH interval_data AS (
    SELECT
        DATE(delivery_timestamp) AS trade_date,
        DATE_TRUNC('hour', delivery_timestamp) +
            INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
        delivery_timestamp,
        price_afrr_plus_eur_mwh, price_afrr_minus_eur_mwh,
        price_mfrr_plus_eur_mwh, price_mfrr_minus_eur_mwh, price_mfrr_5_eur_mwh
    FROM finance.ceps_actual_re_price_1min
)
SELECT
    trade_date,
    TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
    -- Means
    AVG(price_afrr_plus_eur_mwh) AS price_afrr_plus_mean_eur_mwh,
    AVG(price_afrr_minus_eur_mwh) AS price_afrr_minus_mean_eur_mwh,
    AVG(price_mfrr_plus_eur_mwh) AS price_mfrr_plus_mean_eur_mwh,
    AVG(price_mfrr_minus_eur_mwh) AS price_mfrr_minus_mean_eur_mwh,
    AVG(price_mfrr_5_eur_mwh) AS price_mfrr_5_mean_eur_mwh,
    -- Medians
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_afrr_plus_eur_mwh) AS price_afrr_plus_median_eur_mwh,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_afrr_minus_eur_mwh) AS price_afrr_minus_median_eur_mwh,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_plus_eur_mwh) AS price_mfrr_plus_median_eur_mwh,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_minus_eur_mwh) AS price_mfrr_minus_median_eur_mwh,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_5_eur_mwh) AS price_mfrr_5_median_eur_mwh,
    -- Last at interval
    (ARRAY_AGG(price_afrr_plus_eur_mwh ORDER BY delivery_timestamp DESC))[1] AS price_afrr_plus_last_at_interval_eur_mwh,
    (ARRAY_AGG(price_afrr_minus_eur_mwh ORDER BY delivery_timestamp DESC))[1] AS price_afrr_minus_last_at_interval_eur_mwh,
    (ARRAY_AGG(price_mfrr_plus_eur_mwh ORDER BY delivery_timestamp DESC))[1] AS price_mfrr_plus_last_at_interval_eur_mwh,
    (ARRAY_AGG(price_mfrr_minus_eur_mwh ORDER BY delivery_timestamp DESC))[1] AS price_mfrr_minus_last_at_interval_eur_mwh,
    (ARRAY_AGG(price_mfrr_5_eur_mwh ORDER BY delivery_timestamp DESC))[1] AS price_mfrr_5_last_at_interval_eur_mwh
FROM interval_data
GROUP BY trade_date, interval_start
ORDER BY trade_date, interval_start;
```

---

## 3. SVR Activation (Balancing Reserve Activation)

**Tables:** `ceps_svr_activation_1min` → `ceps_svr_activation_15min`

```sql
INSERT INTO finance.ceps_svr_activation_15min
    (trade_date, time_interval,
     afrr_plus_mean_mw, afrr_minus_mean_mw, mfrr_plus_mean_mw, mfrr_minus_mean_mw, mfrr_5_mean_mw,
     afrr_plus_median_mw, afrr_minus_median_mw, mfrr_plus_median_mw, mfrr_minus_median_mw, mfrr_5_median_mw,
     afrr_plus_last_at_interval_mw, afrr_minus_last_at_interval_mw, mfrr_plus_last_at_interval_mw, mfrr_minus_last_at_interval_mw, mfrr_5_last_at_interval_mw)
WITH interval_data AS (
    SELECT
        DATE(delivery_timestamp) AS trade_date,
        DATE_TRUNC('hour', delivery_timestamp) +
            INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
        delivery_timestamp,
        afrr_plus_mw, afrr_minus_mw, mfrr_plus_mw, mfrr_minus_mw, mfrr_5_mw
    FROM finance.ceps_svr_activation_1min
)
SELECT
    trade_date,
    TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
    -- Means
    AVG(afrr_plus_mw) AS afrr_plus_mean_mw,
    AVG(afrr_minus_mw) AS afrr_minus_mean_mw,
    AVG(mfrr_plus_mw) AS mfrr_plus_mean_mw,
    AVG(mfrr_minus_mw) AS mfrr_minus_mean_mw,
    AVG(mfrr_5_mw) AS mfrr_5_mean_mw,
    -- Medians
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY afrr_plus_mw) AS afrr_plus_median_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY afrr_minus_mw) AS afrr_minus_median_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_plus_mw) AS mfrr_plus_median_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_minus_mw) AS mfrr_minus_median_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_5_mw) AS mfrr_5_median_mw,
    -- Last at interval
    (ARRAY_AGG(afrr_plus_mw ORDER BY delivery_timestamp DESC))[1] AS afrr_plus_last_at_interval_mw,
    (ARRAY_AGG(afrr_minus_mw ORDER BY delivery_timestamp DESC))[1] AS afrr_minus_last_at_interval_mw,
    (ARRAY_AGG(mfrr_plus_mw ORDER BY delivery_timestamp DESC))[1] AS mfrr_plus_last_at_interval_mw,
    (ARRAY_AGG(mfrr_minus_mw ORDER BY delivery_timestamp DESC))[1] AS mfrr_minus_last_at_interval_mw,
    (ARRAY_AGG(mfrr_5_mw ORDER BY delivery_timestamp DESC))[1] AS mfrr_5_last_at_interval_mw
FROM interval_data
GROUP BY trade_date, interval_start
ORDER BY trade_date, interval_start;
```

---

## 4. Export/Import SVR (Cross-Border Balancing)

**Tables:** `ceps_export_import_svr_1min` → `ceps_export_import_svr_15min`

```sql
INSERT INTO finance.ceps_export_import_svr_15min
    (trade_date, time_interval,
     imbalance_netting_mean_mw, mari_mfrr_mean_mw, picasso_afrr_mean_mw, sum_exchange_mean_mw,
     imbalance_netting_median_mw, mari_mfrr_median_mw, picasso_afrr_median_mw, sum_exchange_median_mw,
     imbalance_netting_last_at_interval_mw, mari_mfrr_last_at_interval_mw, picasso_afrr_last_at_interval_mw, sum_exchange_last_at_interval_mw)
WITH interval_data AS (
    SELECT
        DATE(delivery_timestamp) AS trade_date,
        DATE_TRUNC('hour', delivery_timestamp) +
            INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
        delivery_timestamp,
        imbalance_netting_mw, mari_mfrr_mw, picasso_afrr_mw, sum_exchange_european_platforms_mw
    FROM finance.ceps_export_import_svr_1min
)
SELECT
    trade_date,
    TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
    -- Means
    AVG(imbalance_netting_mw) AS imbalance_netting_mean_mw,
    AVG(mari_mfrr_mw) AS mari_mfrr_mean_mw,
    AVG(picasso_afrr_mw) AS picasso_afrr_mean_mw,
    AVG(sum_exchange_european_platforms_mw) AS sum_exchange_mean_mw,
    -- Medians
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY imbalance_netting_mw) AS imbalance_netting_median_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mari_mfrr_mw) AS mari_mfrr_median_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY picasso_afrr_mw) AS picasso_afrr_median_mw,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sum_exchange_european_platforms_mw) AS sum_exchange_median_mw,
    -- Last at interval
    (ARRAY_AGG(imbalance_netting_mw ORDER BY delivery_timestamp DESC))[1] AS imbalance_netting_last_at_interval_mw,
    (ARRAY_AGG(mari_mfrr_mw ORDER BY delivery_timestamp DESC))[1] AS mari_mfrr_last_at_interval_mw,
    (ARRAY_AGG(picasso_afrr_mw ORDER BY delivery_timestamp DESC))[1] AS picasso_afrr_last_at_interval_mw,
    (ARRAY_AGG(sum_exchange_european_platforms_mw ORDER BY delivery_timestamp DESC))[1] AS sum_exchange_last_at_interval_mw
FROM interval_data
GROUP BY trade_date, interval_start
ORDER BY trade_date, interval_start;
```

---

## Key Concepts

### Time Interval Calculation
```sql
DATE_TRUNC('hour', delivery_timestamp) +
    INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15)
```
Maps any timestamp to its 15-minute interval start:
- `14:03` → `14:00`
- `14:17` → `14:15`
- `14:59` → `14:45`

### Time Interval Format
```sql
TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI')
```
Produces: `"14:00-14:15"`, `"14:15-14:30"`, etc.

### Aggregation Functions
| Function | Description |
|----------|-------------|
| `AVG(col)` | Mean value across 15 minutes |
| `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)` | Median value |
| `(ARRAY_AGG(col ORDER BY delivery_timestamp DESC))[1]` | Last value in the interval |

### UPSERT for Incremental Updates
Add `ON CONFLICT` clause when re-aggregating changed data:
```sql
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    load_mean_mw = EXCLUDED.load_mean_mw,
    load_median_mw = EXCLUDED.load_median_mw,
    last_load_at_interval_mw = EXCLUDED.last_load_at_interval_mw,
    created_at = CURRENT_TIMESTAMP
```
