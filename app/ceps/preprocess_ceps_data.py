#!/usr/bin/env python3
"""
CEPS 1-minute Feature Aggregation to 15-minute intervals.

Computes distributional, threshold, and temporal trend features from three
1-minute source tables (RE prices, imbalance, export/import SVR) and stores
them in ceps_1min_features_15min.

Called after each batch of 1-min upserts in ceps_soap_uploader.py.
The query is idempotent (ON CONFLICT DO UPDATE), so it is safe to call
multiple times for the same intervals as different source tables are loaded.

Driver table: ceps_actual_re_price_1min (LEFT JOINs the others).
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import date, timedelta
from typing import Set

import psycopg2

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT

# Threshold constants
PRICE_FLOOR_EUR = -500
PRICE_PEAK_EUR = 500
MIN_MINUTE_COUNT = 12


def aggregate_1min_features(affected_intervals: Set[tuple], conn, logger) -> int:
    """Aggregate 1-min data into distributional features for 15-min intervals.

    Skewness: adjusted Fisher-Pearson via raw moments (handles n < 3 → NULL).
    Saturation: dynamic 95th percentile of |picasso_afrr_mw| per trade_date.
    Slope: REGR_SLOPE with normalized x (minute index 0..14).
    Coverage: all features NULLed when minute_count < MIN_MINUTE_COUNT.
    """
    if not affected_intervals:
        return 0

    dates = sorted(set(d for d, _ in affected_intervals))
    min_date, max_date = dates[0], dates[-1]
    affected_dates = tuple(dates)

    query = f"""
        WITH daily_sat AS (
            SELECT
                delivery_timestamp::date AS td,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ABS(picasso_afrr_mw)) AS sat_limit
            FROM finance.ceps_export_import_svr_1min
            WHERE delivery_timestamp::date IN %s
            GROUP BY delivery_timestamp::date
        ),
        interval_data AS (
            SELECT
                re.delivery_timestamp::date AS trade_date,
                DATE_TRUNC('hour', re.delivery_timestamp) +
                    INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM re.delivery_timestamp) / 15)
                    AS interval_start,
                re.delivery_timestamp,
                re.price_afrr_plus_eur_mwh,
                re.price_afrr_minus_eur_mwh,
                re.price_mfrr_plus_eur_mwh,
                re.price_mfrr_minus_eur_mwh,
                imb.load_mw,
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
            WHERE re.delivery_timestamp >= %s AND re.delivery_timestamp < %s::date + INTERVAL '1 day'
        ),
        agg AS (
            SELECT
                trade_date,
                TO_CHAR(interval_start, 'HH24:MI') || '-' ||
                    TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
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
                MAX(load_mw) - MIN(load_mw) AS imb_range,
                STDDEV_SAMP(load_mw) AS imb_std,
                -- Normalized x: minute index 0..14 avoids float precision issues with epoch
                REGR_SLOPE(
                    load_mw,
                    EXTRACT(EPOCH FROM delivery_timestamp - interval_start)::int / 60
                ) AS imb_slope,

                -- Threshold counts
                COUNT(*) FILTER (
                    WHERE price_afrr_minus_eur_mwh <= {PRICE_FLOOR_EUR}
                )::smallint AS minutes_at_floor,
                COUNT(*) FILTER (
                    WHERE price_afrr_plus_eur_mwh >= {PRICE_PEAK_EUR}
                )::smallint AS minutes_near_peak,
                COUNT(*) FILTER (
                    WHERE is_saturated
                )::smallint AS saturation_count,

                -- Total active MW (upward regulation: aFRR+ + mFRR+)
                AVG(afrr_plus_mw + mfrr_plus_mw) AS total_active_mean,
                STDDEV_SAMP(afrr_plus_mw + mfrr_plus_mw) AS total_active_std,

                -- Platform active count (PICASSO or MARI non-zero = importing from EU)
                COUNT(*) FILTER (
                    WHERE picasso_afrr_mw != 0 OR mari_mfrr_mw != 0
                )::smallint AS platform_active_count,

                -- Marginal slope: aFRR vs mFRR price spread (wide = thin market)
                AVG(price_afrr_plus_eur_mwh - price_mfrr_plus_eur_mwh) AS afrr_mfrr_plus_spread_mean,
                STDDEV_SAMP(price_afrr_plus_eur_mwh - price_mfrr_plus_eur_mwh) AS afrr_mfrr_plus_spread_std,
                AVG(price_afrr_minus_eur_mwh - price_mfrr_minus_eur_mwh) AS afrr_mfrr_minus_spread_mean,
                STDDEV_SAMP(price_afrr_minus_eur_mwh - price_mfrr_minus_eur_mwh) AS afrr_mfrr_minus_spread_std

            FROM interval_data
            WHERE (
                trade_date,
                TO_CHAR(interval_start, 'HH24:MI') || '-' ||
                    TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI')
            ) IN %s
            GROUP BY trade_date, interval_start
        )
        INSERT INTO finance.ceps_1min_features_15min (
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
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_plus_min END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_plus_max END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_plus_std END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_plus_skew END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_minus_min END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_minus_max END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_minus_std END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_minus_skew END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_plus_min END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_plus_max END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_plus_std END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_plus_skew END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_minus_min END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_minus_max END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_minus_std END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN mfrr_minus_skew END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN imb_range END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN imb_std END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN imb_slope END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN minutes_at_floor END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN minutes_near_peak END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN saturation_count END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN total_active_mean END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN total_active_std END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN platform_active_count END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_mfrr_plus_spread_mean END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_mfrr_plus_spread_std END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_mfrr_minus_spread_mean END,
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN afrr_mfrr_minus_spread_std END
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

    with conn.cursor() as cur:
        cur.execute(query, (affected_dates, min_date, max_date, tuple(affected_intervals)))
        rows = cur.rowcount
        conn.commit()

    return rows


def aggregate_derived_features(affected_intervals: Set[tuple], conn, logger) -> int:
    """Compute rolling memory and forecast surprise features for 15-min intervals.

    Rolling memory: AVG/SUM of load_mean_mw over trailing 2h (8) and 4h (16) intervals.
    Window functions require lookback data, so the query includes the previous day.

    Forecast surprise: actual generation minus forecast/plan.
    - solar_error = pvpp_mw (CEPS actual) - forecast_solar_mw (ENTSO-E DA forecast, CZ)
    - gen_total_error = actual_total - plan.total_mw
    Note: wind_error dropped — CZ has negligible wind capacity.
    """
    if not affected_intervals:
        return 0

    # Expand dates to include 1 day lookback for rolling window context
    affected_dates_set = set()
    for d_str, _ in affected_intervals:
        d = date.fromisoformat(d_str) if isinstance(d_str, str) else d_str
        affected_dates_set.add(d_str if isinstance(d_str, str) else d_str.isoformat())
        prev = (d - timedelta(days=1)).isoformat() if isinstance(d_str, str) else (d_str - timedelta(days=1)).isoformat()
        affected_dates_set.add(prev)
    lookback_dates = tuple(affected_dates_set)

    query = """
        WITH rolling AS (
            SELECT
                trade_date,
                time_interval,
                load_mean_mw,
                AVG(load_mean_mw) OVER w8 AS imb_roll_2h,
                AVG(load_mean_mw) OVER w16 AS imb_roll_4h,
                SUM(load_mean_mw) OVER w16 AS imb_integral_4h
            FROM finance.ceps_actual_imbalance_15min
            WHERE trade_date IN %s
            WINDOW
                w8  AS (ORDER BY trade_date, time_interval ROWS 7 PRECEDING),
                w16 AS (ORDER BY trade_date, time_interval ROWS 15 PRECEDING)
        ),
        forecast AS (
            SELECT
                g.trade_date,
                g.time_interval,
                COALESCE(g.tpp_mw, 0) + COALESCE(g.ccgt_mw, 0) + COALESCE(g.npp_mw, 0) +
                    COALESCE(g.hpp_mw, 0) + COALESCE(g.pspp_mw, 0) + COALESCE(g.altpp_mw, 0) +
                    COALESCE(g.appp_mw, 0) + COALESCE(g.wpp_mw, 0) + COALESCE(g.pvpp_mw, 0)
                    AS actual_total_mw,
                g.pvpp_mw AS actual_solar_mw,
                ef.forecast_solar_mw AS da_forecast_solar_mw,
                plan.total_mw AS plan_total_mw
            FROM finance.ceps_generation_15min g
            LEFT JOIN finance.entsoe_generation_forecast ef
                ON ef.trade_date = g.trade_date AND ef.time_interval = g.time_interval
                AND ef.country_code = 'CZ'
            LEFT JOIN finance.ceps_generation_plan_15min plan
                ON plan.trade_date = g.trade_date AND plan.time_interval = g.time_interval
            WHERE g.trade_date IN %s
        )
        INSERT INTO finance.ceps_derived_features_15min (
            trade_date, time_interval,
            imb_roll_2h, imb_roll_4h, imb_integral_4h,
            solar_error_mw, wind_error_mw, gen_total_error_mw
        )
        SELECT
            r.trade_date, r.time_interval,
            r.imb_roll_2h, r.imb_roll_4h, r.imb_integral_4h,
            f.actual_solar_mw - f.da_forecast_solar_mw,
            NULL,
            f.actual_total_mw - f.plan_total_mw
        FROM rolling r
        LEFT JOIN forecast f
            ON f.trade_date = r.trade_date AND f.time_interval = r.time_interval
        WHERE (r.trade_date::text, r.time_interval) IN %s
        ON CONFLICT (trade_date, time_interval) DO UPDATE SET
            imb_roll_2h = EXCLUDED.imb_roll_2h,
            imb_roll_4h = EXCLUDED.imb_roll_4h,
            imb_integral_4h = EXCLUDED.imb_integral_4h,
            solar_error_mw = EXCLUDED.solar_error_mw,
            wind_error_mw = EXCLUDED.wind_error_mw,
            gen_total_error_mw = EXCLUDED.gen_total_error_mw,
            updated_at = CURRENT_TIMESTAMP
    """

    # affected_dates used for both rolling and forecast CTEs
    affected_dates = tuple(set(d for d, _ in affected_intervals))

    with conn.cursor() as cur:
        cur.execute(query, (lookback_dates, affected_dates, tuple(affected_intervals)))
        rows = cur.rowcount
        conn.commit()

    return rows


def backfill_derived_features(start_date: date, end_date: date, conn, logger) -> int:
    """Backfill derived features from existing 15-min tables.

    Processes one day at a time. No API calls.
    """
    total = 0
    current = start_date

    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')

        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT trade_date::text, time_interval
                FROM finance.ceps_actual_imbalance_15min
                WHERE trade_date = %s
            """, (date_str,))
            intervals = set(cur.fetchall())

        if not intervals:
            logger.debug(f"  {date_str}: no imbalance data, skipping")
            current += timedelta(days=1)
            continue

        rows = aggregate_derived_features(intervals, conn, logger)
        total += rows
        logger.info(f"  {date_str}: {rows} derived intervals computed")

        current += timedelta(days=1)

    return total


def backfill_features(start_date: date, end_date: date, conn, logger) -> int:
    """Backfill features by querying all existing 15-min intervals from RE price data.

    Processes one day at a time to keep memory and transaction size bounded.
    No re-fetch needed — computes directly from existing 1-min tables.
    """
    total = 0
    current = start_date

    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')

        # Get all 15-min intervals that exist in RE price 1-min data for this day
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                    delivery_timestamp::date::text AS trade_date,
                    TO_CHAR(
                        DATE_TRUNC('hour', delivery_timestamp) +
                            INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15),
                        'HH24:MI'
                    ) || '-' ||
                    TO_CHAR(
                        DATE_TRUNC('hour', delivery_timestamp) +
                            INTERVAL '15 minutes' * (FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) + 1),
                        'HH24:MI'
                    ) AS time_interval
                FROM finance.ceps_actual_re_price_1min
                WHERE delivery_timestamp::date = %s
            """, (date_str,))
            intervals = set(cur.fetchall())

        if not intervals:
            logger.debug(f"  {date_str}: no RE price data, skipping")
            current += timedelta(days=1)
            continue

        rows = aggregate_1min_features(intervals, conn, logger)
        total += rows
        logger.info(f"  {date_str}: {rows} intervals computed")

        current += timedelta(days=1)

    return total


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill CEPS feature tables from existing DB data')
    parser.add_argument('--start', type=str, required=True, metavar='YYYY-MM-DD')
    parser.add_argument('--end', type=str, required=True, metavar='YYYY-MM-DD')
    parser.add_argument('--table', type=str, default='all',
                        choices=['1min_features', 'derived', 'all'],
                        help='Which feature table to backfill (default: all)')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    logger = logging.getLogger(__name__)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        dbname=DB_NAME, port=DB_PORT, connect_timeout=15
    )
    try:
        if args.table in ('1min_features', 'all'):
            logger.info(f"Backfilling ceps_1min_features_15min: {start} to {end}")
            total = backfill_features(start, end, conn, logger)
            logger.info(f"Done. 1min_features intervals: {total}")

        if args.table in ('derived', 'all'):
            logger.info(f"Backfilling ceps_derived_features_15min: {start} to {end}")
            total = backfill_derived_features(start, end, conn, logger)
            logger.info(f"Done. derived intervals: {total}")
    finally:
        conn.close()
