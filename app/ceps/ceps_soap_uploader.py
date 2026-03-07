#!/usr/bin/env python3
"""
CEPS SOAP Data Uploader

Uploads parsed XML data to PostgreSQL with UPSERT logic.
Uses INSERT ... ON CONFLICT DO UPDATE for safe backfills.
Aggregates 1-minute data to 15-minute intervals.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set
import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
from ceps.preprocess_ceps_data import aggregate_1min_features, aggregate_derived_features


def get_affected_intervals(records: List[Dict]) -> Set[tuple]:
    """
    Get unique (trade_date, time_interval) pairs from records.
    Used to determine which 15min intervals need re-aggregation.
    Time interval format: "HH:MM-HH:MM" (e.g., "14:00-14:15")
    """
    intervals = set()
    for r in records:
        ts = r['delivery_timestamp']
        trade_date = ts.strftime('%Y-%m-%d')
        interval_minute = (ts.minute // 15) * 15
        next_minute = (interval_minute + 15) % 60
        next_hour = ts.hour + (1 if interval_minute == 45 else 0)
        if next_hour == 24:
            next_hour = 0
        time_interval = f"{ts.hour:02d}:{interval_minute:02d}-{next_hour:02d}:{next_minute:02d}"
        intervals.add((trade_date, time_interval))
    return intervals


def aggregate_imbalance_15min(affected_intervals: Set[tuple], conn, logger) -> int:
    """Aggregate imbalance data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        WITH interval_data AS (
            SELECT
                delivery_timestamp::date AS trade_date,
                DATE_TRUNC('hour', delivery_timestamp) +
                    INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
                load_mw
            FROM finance.ceps_actual_imbalance_1min
        )
        INSERT INTO finance.ceps_actual_imbalance_15min
            (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
        SELECT
            trade_date,
            TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
            AVG(load_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY load_mw),
            (ARRAY_AGG(load_mw ORDER BY interval_start DESC))[1]
        FROM interval_data
        WHERE (trade_date, TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI')) IN %s
        GROUP BY trade_date, interval_start
        ON CONFLICT (trade_date, time_interval) DO UPDATE SET
            load_mean_mw = EXCLUDED.load_mean_mw,
            load_median_mw = EXCLUDED.load_median_mw,
            last_load_at_interval_mw = EXCLUDED.last_load_at_interval_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(query, (tuple(affected_intervals),))
        rows = cur.rowcount
        conn.commit()

    return rows


def aggregate_re_price_15min(affected_intervals: Set[tuple], conn, logger) -> int:
    """Aggregate RE price data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        WITH interval_data AS (
            SELECT
                delivery_timestamp::date AS trade_date,
                DATE_TRUNC('hour', delivery_timestamp) +
                    INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
                delivery_timestamp,
                price_afrr_plus_eur_mwh, price_afrr_minus_eur_mwh,
                price_mfrr_plus_eur_mwh, price_mfrr_minus_eur_mwh, price_mfrr_5_eur_mwh
            FROM finance.ceps_actual_re_price_1min
        )
        INSERT INTO finance.ceps_actual_re_price_15min
            (trade_date, time_interval,
             price_afrr_plus_mean_eur_mwh, price_afrr_minus_mean_eur_mwh,
             price_mfrr_plus_mean_eur_mwh, price_mfrr_minus_mean_eur_mwh, price_mfrr_5_mean_eur_mwh,
             price_afrr_plus_median_eur_mwh, price_afrr_minus_median_eur_mwh,
             price_mfrr_plus_median_eur_mwh, price_mfrr_minus_median_eur_mwh, price_mfrr_5_median_eur_mwh,
             price_afrr_plus_last_at_interval_eur_mwh, price_afrr_minus_last_at_interval_eur_mwh,
             price_mfrr_plus_last_at_interval_eur_mwh, price_mfrr_minus_last_at_interval_eur_mwh, price_mfrr_5_last_at_interval_eur_mwh)
        SELECT
            trade_date,
            TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
            AVG(price_afrr_plus_eur_mwh), AVG(price_afrr_minus_eur_mwh),
            AVG(price_mfrr_plus_eur_mwh), AVG(price_mfrr_minus_eur_mwh), AVG(price_mfrr_5_eur_mwh),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_afrr_plus_eur_mwh),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_afrr_minus_eur_mwh),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_plus_eur_mwh),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_minus_eur_mwh),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_mfrr_5_eur_mwh),
            (ARRAY_AGG(price_afrr_plus_eur_mwh ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(price_afrr_minus_eur_mwh ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(price_mfrr_plus_eur_mwh ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(price_mfrr_minus_eur_mwh ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(price_mfrr_5_eur_mwh ORDER BY delivery_timestamp DESC))[1]
        FROM interval_data
        WHERE (trade_date, TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI')) IN %s
        GROUP BY trade_date, interval_start
        ON CONFLICT (trade_date, time_interval) DO UPDATE SET
            price_afrr_plus_mean_eur_mwh = EXCLUDED.price_afrr_plus_mean_eur_mwh,
            price_afrr_minus_mean_eur_mwh = EXCLUDED.price_afrr_minus_mean_eur_mwh,
            price_mfrr_plus_mean_eur_mwh = EXCLUDED.price_mfrr_plus_mean_eur_mwh,
            price_mfrr_minus_mean_eur_mwh = EXCLUDED.price_mfrr_minus_mean_eur_mwh,
            price_mfrr_5_mean_eur_mwh = EXCLUDED.price_mfrr_5_mean_eur_mwh,
            price_afrr_plus_median_eur_mwh = EXCLUDED.price_afrr_plus_median_eur_mwh,
            price_afrr_minus_median_eur_mwh = EXCLUDED.price_afrr_minus_median_eur_mwh,
            price_mfrr_plus_median_eur_mwh = EXCLUDED.price_mfrr_plus_median_eur_mwh,
            price_mfrr_minus_median_eur_mwh = EXCLUDED.price_mfrr_minus_median_eur_mwh,
            price_mfrr_5_median_eur_mwh = EXCLUDED.price_mfrr_5_median_eur_mwh,
            price_afrr_plus_last_at_interval_eur_mwh = EXCLUDED.price_afrr_plus_last_at_interval_eur_mwh,
            price_afrr_minus_last_at_interval_eur_mwh = EXCLUDED.price_afrr_minus_last_at_interval_eur_mwh,
            price_mfrr_plus_last_at_interval_eur_mwh = EXCLUDED.price_mfrr_plus_last_at_interval_eur_mwh,
            price_mfrr_minus_last_at_interval_eur_mwh = EXCLUDED.price_mfrr_minus_last_at_interval_eur_mwh,
            price_mfrr_5_last_at_interval_eur_mwh = EXCLUDED.price_mfrr_5_last_at_interval_eur_mwh,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(query, (tuple(affected_intervals),))
        rows = cur.rowcount
        conn.commit()

    return rows


def aggregate_svr_activation_15min(affected_intervals: Set[tuple], conn, logger) -> int:
    """Aggregate SVR activation data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        WITH interval_data AS (
            SELECT
                delivery_timestamp::date AS trade_date,
                DATE_TRUNC('hour', delivery_timestamp) +
                    INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
                delivery_timestamp,
                afrr_plus_mw, afrr_minus_mw, mfrr_plus_mw, mfrr_minus_mw, mfrr_5_mw
            FROM finance.ceps_svr_activation_1min
        )
        INSERT INTO finance.ceps_svr_activation_15min
            (trade_date, time_interval,
             afrr_plus_mean_mw, afrr_minus_mean_mw, mfrr_plus_mean_mw, mfrr_minus_mean_mw, mfrr_5_mean_mw,
             afrr_plus_median_mw, afrr_minus_median_mw, mfrr_plus_median_mw, mfrr_minus_median_mw, mfrr_5_median_mw,
             afrr_plus_last_at_interval_mw, afrr_minus_last_at_interval_mw, mfrr_plus_last_at_interval_mw, mfrr_minus_last_at_interval_mw, mfrr_5_last_at_interval_mw)
        SELECT
            trade_date,
            TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
            AVG(afrr_plus_mw), AVG(afrr_minus_mw), AVG(mfrr_plus_mw), AVG(mfrr_minus_mw), AVG(mfrr_5_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY afrr_plus_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY afrr_minus_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_plus_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_minus_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfrr_5_mw),
            (ARRAY_AGG(afrr_plus_mw ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(afrr_minus_mw ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(mfrr_plus_mw ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(mfrr_minus_mw ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(mfrr_5_mw ORDER BY delivery_timestamp DESC))[1]
        FROM interval_data
        WHERE (trade_date, TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI')) IN %s
        GROUP BY trade_date, interval_start
        ON CONFLICT (trade_date, time_interval) DO UPDATE SET
            afrr_plus_mean_mw = EXCLUDED.afrr_plus_mean_mw,
            afrr_minus_mean_mw = EXCLUDED.afrr_minus_mean_mw,
            mfrr_plus_mean_mw = EXCLUDED.mfrr_plus_mean_mw,
            mfrr_minus_mean_mw = EXCLUDED.mfrr_minus_mean_mw,
            mfrr_5_mean_mw = EXCLUDED.mfrr_5_mean_mw,
            afrr_plus_median_mw = EXCLUDED.afrr_plus_median_mw,
            afrr_minus_median_mw = EXCLUDED.afrr_minus_median_mw,
            mfrr_plus_median_mw = EXCLUDED.mfrr_plus_median_mw,
            mfrr_minus_median_mw = EXCLUDED.mfrr_minus_median_mw,
            mfrr_5_median_mw = EXCLUDED.mfrr_5_median_mw,
            afrr_plus_last_at_interval_mw = EXCLUDED.afrr_plus_last_at_interval_mw,
            afrr_minus_last_at_interval_mw = EXCLUDED.afrr_minus_last_at_interval_mw,
            mfrr_plus_last_at_interval_mw = EXCLUDED.mfrr_plus_last_at_interval_mw,
            mfrr_minus_last_at_interval_mw = EXCLUDED.mfrr_minus_last_at_interval_mw,
            mfrr_5_last_at_interval_mw = EXCLUDED.mfrr_5_last_at_interval_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(query, (tuple(affected_intervals),))
        rows = cur.rowcount
        conn.commit()

    return rows


def aggregate_export_import_svr_15min(affected_intervals: Set[tuple], conn, logger) -> int:
    """Aggregate Export/Import SVR data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        WITH interval_data AS (
            SELECT
                delivery_timestamp::date AS trade_date,
                DATE_TRUNC('hour', delivery_timestamp) +
                    INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
                delivery_timestamp,
                imbalance_netting_mw, mari_mfrr_mw, picasso_afrr_mw, sum_exchange_european_platforms_mw
            FROM finance.ceps_export_import_svr_1min
        )
        INSERT INTO finance.ceps_export_import_svr_15min
            (trade_date, time_interval,
             imbalance_netting_mean_mw, mari_mfrr_mean_mw, picasso_afrr_mean_mw, sum_exchange_mean_mw,
             imbalance_netting_median_mw, mari_mfrr_median_mw, picasso_afrr_median_mw, sum_exchange_median_mw,
             imbalance_netting_last_at_interval_mw, mari_mfrr_last_at_interval_mw, picasso_afrr_last_at_interval_mw, sum_exchange_last_at_interval_mw)
        SELECT
            trade_date,
            TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
            AVG(imbalance_netting_mw), AVG(mari_mfrr_mw), AVG(picasso_afrr_mw), AVG(sum_exchange_european_platforms_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY imbalance_netting_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mari_mfrr_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY picasso_afrr_mw),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sum_exchange_european_platforms_mw),
            (ARRAY_AGG(imbalance_netting_mw ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(mari_mfrr_mw ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(picasso_afrr_mw ORDER BY delivery_timestamp DESC))[1],
            (ARRAY_AGG(sum_exchange_european_platforms_mw ORDER BY delivery_timestamp DESC))[1]
        FROM interval_data
        WHERE (trade_date, TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI')) IN %s
        GROUP BY trade_date, interval_start
        ON CONFLICT (trade_date, time_interval) DO UPDATE SET
            imbalance_netting_mean_mw = EXCLUDED.imbalance_netting_mean_mw,
            mari_mfrr_mean_mw = EXCLUDED.mari_mfrr_mean_mw,
            picasso_afrr_mean_mw = EXCLUDED.picasso_afrr_mean_mw,
            sum_exchange_mean_mw = EXCLUDED.sum_exchange_mean_mw,
            imbalance_netting_median_mw = EXCLUDED.imbalance_netting_median_mw,
            mari_mfrr_median_mw = EXCLUDED.mari_mfrr_median_mw,
            picasso_afrr_median_mw = EXCLUDED.picasso_afrr_median_mw,
            sum_exchange_median_mw = EXCLUDED.sum_exchange_median_mw,
            imbalance_netting_last_at_interval_mw = EXCLUDED.imbalance_netting_last_at_interval_mw,
            mari_mfrr_last_at_interval_mw = EXCLUDED.mari_mfrr_last_at_interval_mw,
            picasso_afrr_last_at_interval_mw = EXCLUDED.picasso_afrr_last_at_interval_mw,
            sum_exchange_last_at_interval_mw = EXCLUDED.sum_exchange_last_at_interval_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(query, (tuple(affected_intervals),))
        rows = cur.rowcount
        conn.commit()

    return rows


def upsert_imbalance_data(records: List[Dict], conn, logger) -> int:
    """Upload System Imbalance data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    # Get affected intervals before insert
    affected_intervals = get_affected_intervals(records)

    # Prepare data for bulk insert
    values = [(r['delivery_timestamp'], r['load_mw']) for r in records]

    query_1min = """
        INSERT INTO finance.ceps_actual_imbalance_1min (delivery_timestamp, load_mw)
        VALUES %s
        ON CONFLICT (delivery_timestamp)
        DO UPDATE SET load_mw = EXCLUDED.load_mw, created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query_1min, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_actual_imbalance_1min")

    # Aggregate affected intervals
    agg_count = aggregate_imbalance_15min(affected_intervals, conn, logger)
    logger.info(f"  ✓ Aggregated {agg_count:,} intervals to ceps_actual_imbalance_15min")

    feat_count = aggregate_1min_features(affected_intervals, conn, logger)
    if feat_count:
        logger.info(f"  ✓ Computed {feat_count:,} feature intervals to ceps_1min_features_15min")

    derived_count = aggregate_derived_features(affected_intervals, conn, logger)
    if derived_count:
        logger.info(f"  ✓ Computed {derived_count:,} derived feature intervals")

    return len(records)


def upsert_re_price_data(records: List[Dict], conn, logger) -> int:
    """Upload RE Price data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    values = [
        (r['delivery_timestamp'], r['price_afrr_plus_eur_mwh'], r['price_afrr_minus_eur_mwh'],
         r['price_mfrr_plus_eur_mwh'], r['price_mfrr_minus_eur_mwh'], r['price_mfrr_5_eur_mwh'])
        for r in records
    ]

    query_1min = """
        INSERT INTO finance.ceps_actual_re_price_1min
            (delivery_timestamp, price_afrr_plus_eur_mwh, price_afrr_minus_eur_mwh,
             price_mfrr_plus_eur_mwh, price_mfrr_minus_eur_mwh, price_mfrr_5_eur_mwh)
        VALUES %s
        ON CONFLICT (delivery_timestamp)
        DO UPDATE SET
            price_afrr_plus_eur_mwh = EXCLUDED.price_afrr_plus_eur_mwh,
            price_afrr_minus_eur_mwh = EXCLUDED.price_afrr_minus_eur_mwh,
            price_mfrr_plus_eur_mwh = EXCLUDED.price_mfrr_plus_eur_mwh,
            price_mfrr_minus_eur_mwh = EXCLUDED.price_mfrr_minus_eur_mwh,
            price_mfrr_5_eur_mwh = EXCLUDED.price_mfrr_5_eur_mwh,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query_1min, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_actual_re_price_1min")

    agg_count = aggregate_re_price_15min(affected_intervals, conn, logger)
    logger.info(f"  ✓ Aggregated {agg_count:,} intervals to ceps_actual_re_price_15min")

    feat_count = aggregate_1min_features(affected_intervals, conn, logger)
    if feat_count:
        logger.info(f"  ✓ Computed {feat_count:,} feature intervals to ceps_1min_features_15min")

    return len(records)


def upsert_svr_activation_data(records: List[Dict], conn, logger) -> int:
    """Upload SVR Activation data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    values = [
        (r['delivery_timestamp'], r['afrr_plus_mw'], r['afrr_minus_mw'],
         r['mfrr_plus_mw'], r['mfrr_minus_mw'], r['mfrr_5_mw'])
        for r in records
    ]

    query_1min = """
        INSERT INTO finance.ceps_svr_activation_1min
            (delivery_timestamp, afrr_plus_mw, afrr_minus_mw, mfrr_plus_mw, mfrr_minus_mw, mfrr_5_mw)
        VALUES %s
        ON CONFLICT (delivery_timestamp)
        DO UPDATE SET
            afrr_plus_mw = EXCLUDED.afrr_plus_mw,
            afrr_minus_mw = EXCLUDED.afrr_minus_mw,
            mfrr_plus_mw = EXCLUDED.mfrr_plus_mw,
            mfrr_minus_mw = EXCLUDED.mfrr_minus_mw,
            mfrr_5_mw = EXCLUDED.mfrr_5_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query_1min, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_svr_activation_1min")

    agg_count = aggregate_svr_activation_15min(affected_intervals, conn, logger)
    logger.info(f"  ✓ Aggregated {agg_count:,} intervals to ceps_svr_activation_15min")

    return len(records)


def upsert_export_import_svr_data(records: List[Dict], conn, logger) -> int:
    """Upload Export/Import SVR data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    values = [
        (r['delivery_timestamp'], r['imbalance_netting_mw'], r['mari_mfrr_mw'],
         r['picasso_afrr_mw'], r['sum_exchange_european_platforms_mw'])
        for r in records
    ]

    query_1min = """
        INSERT INTO finance.ceps_export_import_svr_1min
            (delivery_timestamp, imbalance_netting_mw, mari_mfrr_mw,
             picasso_afrr_mw, sum_exchange_european_platforms_mw)
        VALUES %s
        ON CONFLICT (delivery_timestamp)
        DO UPDATE SET
            imbalance_netting_mw = EXCLUDED.imbalance_netting_mw,
            mari_mfrr_mw = EXCLUDED.mari_mfrr_mw,
            picasso_afrr_mw = EXCLUDED.picasso_afrr_mw,
            sum_exchange_european_platforms_mw = EXCLUDED.sum_exchange_european_platforms_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query_1min, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_export_import_svr_1min")

    agg_count = aggregate_export_import_svr_15min(affected_intervals, conn, logger)
    logger.info(f"  ✓ Aggregated {agg_count:,} intervals to ceps_export_import_svr_15min")

    feat_count = aggregate_1min_features(affected_intervals, conn, logger)
    if feat_count:
        logger.info(f"  ✓ Computed {feat_count:,} feature intervals to ceps_1min_features_15min")

    return len(records)


def aggregate_generation_res_15min(affected_intervals: Set[tuple], conn, logger) -> int:
    """Aggregate Generation RES data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        WITH interval_data AS (
            SELECT
                delivery_timestamp::date AS trade_date,
                DATE_TRUNC('hour', delivery_timestamp) +
                    INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS interval_start,
                delivery_timestamp,
                wind_mw, solar_mw
            FROM finance.ceps_generation_res_1min
        )
        INSERT INTO finance.ceps_generation_res_15min
            (trade_date, time_interval,
             wind_mean_mw, wind_median_mw, wind_last_at_interval_mw,
             solar_mean_mw, solar_median_mw, solar_last_at_interval_mw)
        SELECT
            trade_date,
            TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI') AS time_interval,
            AVG(wind_mw), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wind_mw),
            (ARRAY_AGG(wind_mw ORDER BY delivery_timestamp DESC))[1],
            AVG(solar_mw), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY solar_mw),
            (ARRAY_AGG(solar_mw ORDER BY delivery_timestamp DESC))[1]
        FROM interval_data
        WHERE (trade_date, TO_CHAR(interval_start, 'HH24:MI') || '-' || TO_CHAR(interval_start + INTERVAL '15 minutes', 'HH24:MI')) IN %s
        GROUP BY trade_date, interval_start
        ON CONFLICT (trade_date, time_interval) DO UPDATE SET
            wind_mean_mw = EXCLUDED.wind_mean_mw,
            wind_median_mw = EXCLUDED.wind_median_mw,
            wind_last_at_interval_mw = EXCLUDED.wind_last_at_interval_mw,
            solar_mean_mw = EXCLUDED.solar_mean_mw,
            solar_median_mw = EXCLUDED.solar_median_mw,
            solar_last_at_interval_mw = EXCLUDED.solar_last_at_interval_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(query, (tuple(affected_intervals),))
        rows = cur.rowcount
        conn.commit()

    return rows


def upsert_generation_res_data(records: List[Dict], conn, logger) -> int:
    """Upload Generation RES data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    values = [
        (r['delivery_timestamp'], r['wind_mw'], r['solar_mw'])
        for r in records
    ]

    query_1min = """
        INSERT INTO finance.ceps_generation_res_1min
            (delivery_timestamp, wind_mw, solar_mw)
        VALUES %s
        ON CONFLICT (delivery_timestamp)
        DO UPDATE SET
            wind_mw = EXCLUDED.wind_mw,
            solar_mw = EXCLUDED.solar_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query_1min, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_generation_res_1min")

    agg_count = aggregate_generation_res_15min(affected_intervals, conn, logger)
    logger.info(f"  ✓ Aggregated {agg_count:,} intervals to ceps_generation_res_15min")

    derived_count = aggregate_derived_features(affected_intervals, conn, logger)
    if derived_count:
        logger.info(f"  ✓ Computed {derived_count:,} derived feature intervals")

    return len(records)


def get_15min_interval(ts: datetime) -> tuple:
    """
    Convert timestamp to (trade_date, time_interval) for native 15-min data.
    Time interval format: "HH:MM-HH:MM" (e.g., "14:00-14:15")
    """
    trade_date = ts.strftime('%Y-%m-%d')
    interval_minute = (ts.minute // 15) * 15
    next_minute = (interval_minute + 15) % 60
    next_hour = ts.hour + (1 if interval_minute == 45 else 0)
    if next_hour == 24:
        next_hour = 0
    time_interval = f"{ts.hour:02d}:{interval_minute:02d}-{next_hour:02d}:{next_minute:02d}"
    return (trade_date, time_interval)


def upsert_generation_data(records: List[Dict], conn, logger) -> int:
    """
    Upload actual Generation data (by plant type) with UPSERT logic.
    Native 15-min data - no aggregation needed.
    """
    if not records:
        return 0

    values = []
    for r in records:
        trade_date, time_interval = get_15min_interval(r['delivery_timestamp'])
        values.append((
            trade_date, time_interval,
            r['tpp_mw'], r['ccgt_mw'], r['npp_mw'], r['hpp_mw'],
            r['pspp_mw'], r['altpp_mw'], r['appp_mw'], r['wpp_mw'], r['pvpp_mw']
        ))

    query = """
        INSERT INTO finance.ceps_generation_15min
            (trade_date, time_interval, tpp_mw, ccgt_mw, npp_mw, hpp_mw,
             pspp_mw, altpp_mw, appp_mw, wpp_mw, pvpp_mw)
        VALUES %s
        ON CONFLICT (trade_date, time_interval)
        DO UPDATE SET
            tpp_mw = EXCLUDED.tpp_mw,
            ccgt_mw = EXCLUDED.ccgt_mw,
            npp_mw = EXCLUDED.npp_mw,
            hpp_mw = EXCLUDED.hpp_mw,
            pspp_mw = EXCLUDED.pspp_mw,
            altpp_mw = EXCLUDED.altpp_mw,
            appp_mw = EXCLUDED.appp_mw,
            wpp_mw = EXCLUDED.wpp_mw,
            pvpp_mw = EXCLUDED.pvpp_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_generation_15min")

    affected_intervals = set(get_15min_interval(r['delivery_timestamp']) for r in records)
    derived_count = aggregate_derived_features(affected_intervals, conn, logger)
    if derived_count:
        logger.info(f"  ✓ Computed {derived_count:,} derived feature intervals")

    return len(records)


def upsert_generation_plan_data(records: List[Dict], conn, logger) -> int:
    """
    Upload Generation Plan data (total planned) with UPSERT logic.
    Native 15-min data - no aggregation needed.
    """
    if not records:
        return 0

    values = []
    for r in records:
        trade_date, time_interval = get_15min_interval(r['delivery_timestamp'])
        values.append((trade_date, time_interval, r['total_mw']))

    query = """
        INSERT INTO finance.ceps_generation_plan_15min
            (trade_date, time_interval, total_mw)
        VALUES %s
        ON CONFLICT (trade_date, time_interval)
        DO UPDATE SET
            total_mw = EXCLUDED.total_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_generation_plan_15min")

    affected_intervals = set(get_15min_interval(r['delivery_timestamp']) for r in records)
    derived_count = aggregate_derived_features(affected_intervals, conn, logger)
    if derived_count:
        logger.info(f"  ✓ Computed {derived_count:,} derived feature intervals")

    return len(records)


def upsert_estimated_imbalance_price_data(records: List[Dict], conn, logger) -> int:
    """
    Upload Estimated Imbalance Price data (OdhadovanaCenaOdchylky) with UPSERT logic.
    Native 15-min data - no aggregation needed.

    This dataset uses trade_date and time_interval directly from the parser
    (unique structure compared to other datasets).
    """
    if not records:
        return 0

    values = []
    for r in records:
        # This dataset already has trade_date and time_interval from the parser
        values.append((r['trade_date'], r['time_interval'], r['estimated_price_czk_mwh']))

    query = """
        INSERT INTO finance.ceps_estimated_imbalance_price_15min
            (trade_date, time_interval, estimated_price_czk_mwh)
        VALUES %s
        ON CONFLICT (trade_date, time_interval)
        DO UPDATE SET
            estimated_price_czk_mwh = EXCLUDED.estimated_price_czk_mwh,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_estimated_imbalance_price_15min")
    return len(records)


def upsert_data(dataset: str, records: List[Dict], conn, logger) -> int:
    """
    Upload data for any dataset with UPSERT logic.
    Automatically aggregates to 15-minute intervals where applicable.
    """
    if dataset == 'imbalance':
        return upsert_imbalance_data(records, conn, logger)
    elif dataset == 're_price':
        return upsert_re_price_data(records, conn, logger)
    elif dataset == 'svr_activation':
        return upsert_svr_activation_data(records, conn, logger)
    elif dataset == 'export_import_svr':
        return upsert_export_import_svr_data(records, conn, logger)
    elif dataset == 'generation_res':
        return upsert_generation_res_data(records, conn, logger)
    elif dataset == 'generation':
        return upsert_generation_data(records, conn, logger)
    elif dataset == 'generation_plan':
        return upsert_generation_plan_data(records, conn, logger)
    elif dataset == 'estimated_imbalance_price':
        return upsert_estimated_imbalance_price_data(records, conn, logger)
    else:
        raise ValueError(f"Unknown dataset: {dataset}")
