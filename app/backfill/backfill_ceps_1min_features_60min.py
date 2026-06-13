"""Backfill ceps_1min_features_60min by re-aggregating from the 1-min source.

Mirrors app/ceps/preprocess_ceps_data.py::aggregate_1min_features with
two surgical changes:
  1. bucket  : DATE_TRUNC('hour', …)        (was 15-min FLOOR bucket)
  2. coverage: strict HAVING COUNT(*) = 60  (no partial hours)

Per docs/60min_tables_plan.md §4.4: distributional stats (min/max/std/
skew, threshold counts, slopes) cannot be aggregated from the already-
aggregated 15-min stats. They must be recomputed from the 1-min source
over a 60-min window.

Completeness gate is enforced inside the `agg` CTE — an hour with fewer
than 60 distinct minutes never reaches the INSERT (mirrors the
HAVING COUNT(DISTINCT time_interval) = 4 used by the 15-min aggregators).

Source tables (all in finance schema):
  - ceps_actual_re_price_1min       (driver — required)
  - ceps_actual_imbalance_1min      (system_imbalance_mw)
  - ceps_export_import_svr_1min     (picasso/mari for saturation flag)
  - ceps_svr_activation_1min        (afrr+/mfrr+ for total-active)

Usage:
    python3 -m backfill.backfill_ceps_1min_features_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
    python3 -m backfill.backfill_ceps_1min_features_60min --auto              [--debug] [--dry-run]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import (
    get_db_connection,
    parse_args,
    print_banner,
    setup_logging,
    daterange,
)


PRICE_FLOOR_EUR = -500
PRICE_PEAK_EUR = 500


# The SQL takes the same parameter (the day) three times: once for daily_sat,
# twice for the interval_data lower/upper bounds.
QUERY = f"""
WITH daily_sat AS (
    SELECT
        delivery_timestamp::date AS td,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ABS(picasso_afrr_mw)) AS sat_limit
    FROM finance.ceps_export_import_svr_1min
    WHERE delivery_timestamp::date = %s
    GROUP BY delivery_timestamp::date
),
interval_data AS (
    SELECT
        re.delivery_timestamp::date AS trade_date,
        DATE_TRUNC('hour', re.delivery_timestamp) AS interval_start,
        re.delivery_timestamp,
        re.price_afrr_plus_eur_mwh,
        re.price_afrr_minus_eur_mwh,
        re.price_mfrr_plus_eur_mwh,
        re.price_mfrr_minus_eur_mwh,
        imb.system_imbalance_mw,
        act.afrr_plus_mw,
        act.mfrr_plus_mw,
        svr.picasso_afrr_mw,
        svr.mari_mfrr_mw,
        CASE WHEN ABS(svr.picasso_afrr_mw) >= COALESCE(ds.sat_limit, 999999) * 0.95
             THEN TRUE ELSE FALSE END AS is_saturated
    FROM finance.ceps_actual_re_price_1min re
    LEFT JOIN finance.ceps_actual_imbalance_1min imb
        ON imb.delivery_timestamp = re.delivery_timestamp
    LEFT JOIN finance.ceps_export_import_svr_1min svr
        ON svr.delivery_timestamp = re.delivery_timestamp
    LEFT JOIN finance.ceps_svr_activation_1min act
        ON act.delivery_timestamp = re.delivery_timestamp
    LEFT JOIN daily_sat ds
        ON ds.td = re.delivery_timestamp::date
    WHERE re.delivery_timestamp >= %s::date
      AND re.delivery_timestamp < %s::date + INTERVAL '1 day'
),
agg AS (
    SELECT
        trade_date,
        TO_CHAR(interval_start, 'HH24:MI') || '-' ||
            TO_CHAR(interval_start + INTERVAL '1 hour', 'HH24:MI') AS time_interval,
        COUNT(*)::smallint AS n,

        -- aFRR+ price distribution
        MIN(price_afrr_plus_eur_mwh) AS afrr_plus_min,
        MAX(price_afrr_plus_eur_mwh) AS afrr_plus_max,
        STDDEV_SAMP(price_afrr_plus_eur_mwh) AS afrr_plus_std,
        CASE WHEN COUNT(price_afrr_plus_eur_mwh) >= 3 THEN
            (COUNT(price_afrr_plus_eur_mwh)::float
                / ((COUNT(price_afrr_plus_eur_mwh) - 1)
                   * (COUNT(price_afrr_plus_eur_mwh) - 2)))
            * (SUM(POWER(price_afrr_plus_eur_mwh, 3))
                - 3 * AVG(price_afrr_plus_eur_mwh)
                    * SUM(POWER(price_afrr_plus_eur_mwh, 2))
                + 2 * COUNT(price_afrr_plus_eur_mwh)
                    * POWER(AVG(price_afrr_plus_eur_mwh), 3))
            / NULLIF(POWER(STDDEV_SAMP(price_afrr_plus_eur_mwh), 3), 0)
        END AS afrr_plus_skew,

        -- aFRR- price distribution
        MIN(price_afrr_minus_eur_mwh) AS afrr_minus_min,
        MAX(price_afrr_minus_eur_mwh) AS afrr_minus_max,
        STDDEV_SAMP(price_afrr_minus_eur_mwh) AS afrr_minus_std,
        CASE WHEN COUNT(price_afrr_minus_eur_mwh) >= 3 THEN
            (COUNT(price_afrr_minus_eur_mwh)::float
                / ((COUNT(price_afrr_minus_eur_mwh) - 1)
                   * (COUNT(price_afrr_minus_eur_mwh) - 2)))
            * (SUM(POWER(price_afrr_minus_eur_mwh, 3))
                - 3 * AVG(price_afrr_minus_eur_mwh)
                    * SUM(POWER(price_afrr_minus_eur_mwh, 2))
                + 2 * COUNT(price_afrr_minus_eur_mwh)
                    * POWER(AVG(price_afrr_minus_eur_mwh), 3))
            / NULLIF(POWER(STDDEV_SAMP(price_afrr_minus_eur_mwh), 3), 0)
        END AS afrr_minus_skew,

        -- mFRR+ price distribution
        MIN(price_mfrr_plus_eur_mwh) AS mfrr_plus_min,
        MAX(price_mfrr_plus_eur_mwh) AS mfrr_plus_max,
        STDDEV_SAMP(price_mfrr_plus_eur_mwh) AS mfrr_plus_std,
        CASE WHEN COUNT(price_mfrr_plus_eur_mwh) >= 3 THEN
            (COUNT(price_mfrr_plus_eur_mwh)::float
                / ((COUNT(price_mfrr_plus_eur_mwh) - 1)
                   * (COUNT(price_mfrr_plus_eur_mwh) - 2)))
            * (SUM(POWER(price_mfrr_plus_eur_mwh, 3))
                - 3 * AVG(price_mfrr_plus_eur_mwh)
                    * SUM(POWER(price_mfrr_plus_eur_mwh, 2))
                + 2 * COUNT(price_mfrr_plus_eur_mwh)
                    * POWER(AVG(price_mfrr_plus_eur_mwh), 3))
            / NULLIF(POWER(STDDEV_SAMP(price_mfrr_plus_eur_mwh), 3), 0)
        END AS mfrr_plus_skew,

        -- mFRR- price distribution
        MIN(price_mfrr_minus_eur_mwh) AS mfrr_minus_min,
        MAX(price_mfrr_minus_eur_mwh) AS mfrr_minus_max,
        STDDEV_SAMP(price_mfrr_minus_eur_mwh) AS mfrr_minus_std,
        CASE WHEN COUNT(price_mfrr_minus_eur_mwh) >= 3 THEN
            (COUNT(price_mfrr_minus_eur_mwh)::float
                / ((COUNT(price_mfrr_minus_eur_mwh) - 1)
                   * (COUNT(price_mfrr_minus_eur_mwh) - 2)))
            * (SUM(POWER(price_mfrr_minus_eur_mwh, 3))
                - 3 * AVG(price_mfrr_minus_eur_mwh)
                    * SUM(POWER(price_mfrr_minus_eur_mwh, 2))
                + 2 * COUNT(price_mfrr_minus_eur_mwh)
                    * POWER(AVG(price_mfrr_minus_eur_mwh), 3))
            / NULLIF(POWER(STDDEV_SAMP(price_mfrr_minus_eur_mwh), 3), 0)
        END AS mfrr_minus_skew,

        -- Imbalance distribution
        MAX(system_imbalance_mw) - MIN(system_imbalance_mw) AS imb_range,
        STDDEV_SAMP(system_imbalance_mw) AS imb_std,
        REGR_SLOPE(
            system_imbalance_mw,
            EXTRACT(EPOCH FROM delivery_timestamp - interval_start)::int / 60
        ) AS imb_slope,

        -- Threshold counts
        COUNT(*) FILTER (WHERE price_afrr_minus_eur_mwh <= {PRICE_FLOOR_EUR})::smallint AS minutes_at_floor,
        COUNT(*) FILTER (WHERE price_afrr_plus_eur_mwh >= {PRICE_PEAK_EUR})::smallint AS minutes_near_peak,
        COUNT(*) FILTER (WHERE is_saturated)::smallint AS saturation_count,

        -- Total active MW (upward regulation: aFRR+ + mFRR+)
        AVG(afrr_plus_mw + mfrr_plus_mw) AS total_active_mean,
        STDDEV_SAMP(afrr_plus_mw + mfrr_plus_mw) AS total_active_std,

        -- Platform active count (PICASSO or MARI non-zero)
        COUNT(*) FILTER (
            WHERE picasso_afrr_mw != 0 OR mari_mfrr_mw != 0
        )::smallint AS platform_active_count,

        -- Marginal slope: aFRR vs mFRR price spread
        AVG(price_afrr_plus_eur_mwh - price_mfrr_plus_eur_mwh) AS afrr_mfrr_plus_spread_mean,
        STDDEV_SAMP(price_afrr_plus_eur_mwh - price_mfrr_plus_eur_mwh) AS afrr_mfrr_plus_spread_std,
        AVG(price_afrr_minus_eur_mwh - price_mfrr_minus_eur_mwh) AS afrr_mfrr_minus_spread_mean,
        STDDEV_SAMP(price_afrr_minus_eur_mwh - price_mfrr_minus_eur_mwh) AS afrr_mfrr_minus_spread_std

    FROM interval_data
    GROUP BY trade_date, interval_start
    HAVING COUNT(*) = 60
)
INSERT INTO finance.ceps_1min_features_60min (
    trade_date, time_interval, minute_count,
    afrr_plus_min_eur, afrr_plus_max_eur, afrr_plus_std_eur, afrr_plus_skew,
    afrr_minus_min_eur, afrr_minus_max_eur, afrr_minus_std_eur, afrr_minus_skew,
    mfrr_plus_min_eur, mfrr_plus_max_eur, mfrr_plus_std_eur, mfrr_plus_skew,
    mfrr_minus_min_eur, mfrr_minus_max_eur, mfrr_minus_std_eur, mfrr_minus_skew,
    imbalance_range_mw, imbalance_std_mw, imbalance_slope,
    minutes_at_floor, minutes_near_peak, saturation_count,
    total_active_mean_mw, total_active_std_mw, platform_active_count,
    afrr_mfrr_plus_spread_mean_eur, afrr_mfrr_plus_spread_std_eur,
    afrr_mfrr_minus_spread_mean_eur, afrr_mfrr_minus_spread_std_eur
)
SELECT
    trade_date, time_interval, n,
    afrr_plus_min, afrr_plus_max, afrr_plus_std, afrr_plus_skew,
    afrr_minus_min, afrr_minus_max, afrr_minus_std, afrr_minus_skew,
    mfrr_plus_min, mfrr_plus_max, mfrr_plus_std, mfrr_plus_skew,
    mfrr_minus_min, mfrr_minus_max, mfrr_minus_std, mfrr_minus_skew,
    imb_range, imb_std, imb_slope,
    minutes_at_floor, minutes_near_peak, saturation_count,
    total_active_mean, total_active_std, platform_active_count,
    afrr_mfrr_plus_spread_mean, afrr_mfrr_plus_spread_std,
    afrr_mfrr_minus_spread_mean, afrr_mfrr_minus_spread_std
FROM agg
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    minute_count = EXCLUDED.minute_count,
    afrr_plus_min_eur = EXCLUDED.afrr_plus_min_eur,
    afrr_plus_max_eur = EXCLUDED.afrr_plus_max_eur,
    afrr_plus_std_eur = EXCLUDED.afrr_plus_std_eur,
    afrr_plus_skew = EXCLUDED.afrr_plus_skew,
    afrr_minus_min_eur = EXCLUDED.afrr_minus_min_eur,
    afrr_minus_max_eur = EXCLUDED.afrr_minus_max_eur,
    afrr_minus_std_eur = EXCLUDED.afrr_minus_std_eur,
    afrr_minus_skew = EXCLUDED.afrr_minus_skew,
    mfrr_plus_min_eur = EXCLUDED.mfrr_plus_min_eur,
    mfrr_plus_max_eur = EXCLUDED.mfrr_plus_max_eur,
    mfrr_plus_std_eur = EXCLUDED.mfrr_plus_std_eur,
    mfrr_plus_skew = EXCLUDED.mfrr_plus_skew,
    mfrr_minus_min_eur = EXCLUDED.mfrr_minus_min_eur,
    mfrr_minus_max_eur = EXCLUDED.mfrr_minus_max_eur,
    mfrr_minus_std_eur = EXCLUDED.mfrr_minus_std_eur,
    mfrr_minus_skew = EXCLUDED.mfrr_minus_skew,
    imbalance_range_mw = EXCLUDED.imbalance_range_mw,
    imbalance_std_mw = EXCLUDED.imbalance_std_mw,
    imbalance_slope = EXCLUDED.imbalance_slope,
    minutes_at_floor = EXCLUDED.minutes_at_floor,
    minutes_near_peak = EXCLUDED.minutes_near_peak,
    saturation_count = EXCLUDED.saturation_count,
    total_active_mean_mw = EXCLUDED.total_active_mean_mw,
    total_active_std_mw = EXCLUDED.total_active_std_mw,
    platform_active_count = EXCLUDED.platform_active_count,
    afrr_mfrr_plus_spread_mean_eur = EXCLUDED.afrr_mfrr_plus_spread_mean_eur,
    afrr_mfrr_plus_spread_std_eur = EXCLUDED.afrr_mfrr_plus_spread_std_eur,
    afrr_mfrr_minus_spread_mean_eur = EXCLUDED.afrr_mfrr_minus_spread_mean_eur,
    afrr_mfrr_minus_spread_std_eur = EXCLUDED.afrr_mfrr_minus_spread_std_eur,
    updated_at = CURRENT_TIMESTAMP
"""


def main():
    args = parse_args("CEPS 1-min features 60-min")
    logger = setup_logging("backfill_ceps_1min_features_60min", args.debug)
    print_banner(f"CEPS 1-min features 60-min backfill {args.start_date} to {args.end_date}")

    total_days = (args.end_date - args.start_date).days + 1
    total_rows = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for i, day in enumerate(daterange(args.start_date, args.end_date), 1):
                if args.dry_run:
                    logger.info(f"  DRY-RUN day {day}: would re-aggregate from 1-min source")
                    continue
                try:
                    cur.execute(QUERY, (day, day, day))
                    n = cur.rowcount
                    total_rows += n
                    conn.commit()
                    if args.debug:
                        logger.info(f"  {day}: {n} hour-rows upserted")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"  {day} failed: {e}")
                    raise

                if i % 30 == 0:
                    logger.info(f"  Progress: {i}/{total_days} days")

    logger.info(f"  ceps_1min_features_60min: {total_rows} rows upserted")
    logger.info("CEPS 1-min features 60-min backfill complete")


if __name__ == "__main__":
    main()
