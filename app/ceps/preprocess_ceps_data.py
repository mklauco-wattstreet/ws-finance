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

    affected_dates = tuple(set(d for d, _ in affected_intervals))

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
                CASE WHEN ABS(svr.picasso_afrr_mw) >= COALESCE(ds.sat_limit, 999999) * 0.95
                     THEN TRUE ELSE FALSE END AS is_saturated
            FROM finance.ceps_actual_re_price_1min re
            LEFT JOIN finance.ceps_actual_imbalance_1min imb
                ON imb.delivery_timestamp = re.delivery_timestamp
            LEFT JOIN finance.ceps_export_import_svr_1min svr
                ON svr.delivery_timestamp = re.delivery_timestamp
            LEFT JOIN daily_sat ds
                ON ds.td = re.delivery_timestamp::date
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
                )::smallint AS saturation_count

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
            minutes_at_floor, minutes_near_peak, saturation_count
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
            CASE WHEN n >= {MIN_MINUTE_COUNT} THEN saturation_count END
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
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(query, (affected_dates, tuple(affected_intervals)))
        rows = cur.rowcount
        conn.commit()

    return rows


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
    parser = argparse.ArgumentParser(description='Backfill CEPS 1-min features to 15-min intervals')
    parser.add_argument('--start', type=str, required=True, metavar='YYYY-MM-DD')
    parser.add_argument('--end', type=str, required=True, metavar='YYYY-MM-DD')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    logger = logging.getLogger(__name__)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    logger.info(f"Backfilling ceps_1min_features_15min: {start} to {end}")

    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        dbname=DB_NAME, port=DB_PORT
    )
    try:
        total = backfill_features(start, end, conn, logger)
        logger.info(f"Done. Total intervals: {total}")
    finally:
        conn.close()
