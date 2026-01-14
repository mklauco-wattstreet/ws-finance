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


def get_affected_intervals(records: List[Dict]) -> Set[str]:
    """
    Get unique (trade_date, time_interval) pairs from records.
    Used to determine which 15min intervals need re-aggregation.
    """
    intervals = set()
    for r in records:
        ts = r['delivery_timestamp']
        trade_date = ts.strftime('%Y-%m-%d')
        interval_minute = (ts.minute // 15) * 15
        time_interval = f"{ts.hour:02d}:{interval_minute:02d}"
        intervals.add((trade_date, time_interval))
    return intervals


def aggregate_imbalance_15min(affected_intervals: Set[str], conn, logger) -> int:
    """Aggregate imbalance data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        INSERT INTO finance.ceps_actual_imbalance_15min
            (trade_date, time_interval, load_mean_mw, load_median_mw, last_load_at_interval_mw)
        SELECT
            delivery_timestamp::date AS trade_date,
            to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0') AS time_interval,
            avg(load_mw),
            percentile_cont(0.5) within group (order by load_mw),
            (array_agg(load_mw order by delivery_timestamp desc))[1]
        FROM finance.ceps_actual_imbalance_1min
        WHERE (delivery_timestamp::date, to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0'))
              IN %s
        GROUP BY trade_date, time_interval
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


def aggregate_re_price_15min(affected_intervals: Set[str], conn, logger) -> int:
    """Aggregate RE price data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        INSERT INTO finance.ceps_actual_re_price_15min
            (trade_date, time_interval,
             price_afrr_plus_mean_eur_mwh, price_afrr_minus_mean_eur_mwh,
             price_mfrr_plus_mean_eur_mwh, price_mfrr_minus_mean_eur_mwh, price_mfrr_5_mean_eur_mwh,
             price_afrr_plus_median_eur_mwh, price_afrr_minus_median_eur_mwh,
             price_mfrr_plus_median_eur_mwh, price_mfrr_minus_median_eur_mwh, price_mfrr_5_median_eur_mwh,
             price_afrr_plus_last_at_interval_eur_mwh, price_afrr_minus_last_at_interval_eur_mwh,
             price_mfrr_plus_last_at_interval_eur_mwh, price_mfrr_minus_last_at_interval_eur_mwh, price_mfrr_5_last_at_interval_eur_mwh)
        SELECT
            delivery_timestamp::date AS trade_date,
            to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0') AS time_interval,
            avg(price_afrr_plus_eur_mwh), avg(price_afrr_minus_eur_mwh),
            avg(price_mfrr_plus_eur_mwh), avg(price_mfrr_minus_eur_mwh), avg(price_mfrr_5_eur_mwh),
            percentile_cont(0.5) within group (order by price_afrr_plus_eur_mwh),
            percentile_cont(0.5) within group (order by price_afrr_minus_eur_mwh),
            percentile_cont(0.5) within group (order by price_mfrr_plus_eur_mwh),
            percentile_cont(0.5) within group (order by price_mfrr_minus_eur_mwh),
            percentile_cont(0.5) within group (order by price_mfrr_5_eur_mwh),
            (array_agg(price_afrr_plus_eur_mwh order by delivery_timestamp desc))[1],
            (array_agg(price_afrr_minus_eur_mwh order by delivery_timestamp desc))[1],
            (array_agg(price_mfrr_plus_eur_mwh order by delivery_timestamp desc))[1],
            (array_agg(price_mfrr_minus_eur_mwh order by delivery_timestamp desc))[1],
            (array_agg(price_mfrr_5_eur_mwh order by delivery_timestamp desc))[1]
        FROM finance.ceps_actual_re_price_1min
        WHERE (delivery_timestamp::date, to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0'))
              IN %s
        GROUP BY trade_date, time_interval
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


def aggregate_svr_activation_15min(affected_intervals: Set[str], conn, logger) -> int:
    """Aggregate SVR activation data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        INSERT INTO finance.ceps_svr_activation_15min
            (trade_date, time_interval,
             afrr_plus_mean_mw, afrr_minus_mean_mw, mfrr_plus_mean_mw, mfrr_minus_mean_mw, mfrr_5_mean_mw,
             afrr_plus_median_mw, afrr_minus_median_mw, mfrr_plus_median_mw, mfrr_minus_median_mw, mfrr_5_median_mw,
             afrr_plus_last_at_interval_mw, afrr_minus_last_at_interval_mw, mfrr_plus_last_at_interval_mw, mfrr_minus_last_at_interval_mw, mfrr_5_last_at_interval_mw)
        SELECT
            delivery_timestamp::date AS trade_date,
            to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0') AS time_interval,
            avg(afrr_plus_mw), avg(afrr_minus_mw), avg(mfrr_plus_mw), avg(mfrr_minus_mw), avg(mfrr_5_mw),
            percentile_cont(0.5) within group (order by afrr_plus_mw),
            percentile_cont(0.5) within group (order by afrr_minus_mw),
            percentile_cont(0.5) within group (order by mfrr_plus_mw),
            percentile_cont(0.5) within group (order by mfrr_minus_mw),
            percentile_cont(0.5) within group (order by mfrr_5_mw),
            (array_agg(afrr_plus_mw order by delivery_timestamp desc))[1],
            (array_agg(afrr_minus_mw order by delivery_timestamp desc))[1],
            (array_agg(mfrr_plus_mw order by delivery_timestamp desc))[1],
            (array_agg(mfrr_minus_mw order by delivery_timestamp desc))[1],
            (array_agg(mfrr_5_mw order by delivery_timestamp desc))[1]
        FROM finance.ceps_svr_activation_1min
        WHERE (delivery_timestamp::date, to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0'))
              IN %s
        GROUP BY trade_date, time_interval
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


def aggregate_export_import_svr_15min(affected_intervals: Set[str], conn, logger) -> int:
    """Aggregate Export/Import SVR data for affected 15-minute intervals."""
    if not affected_intervals:
        return 0

    query = """
        INSERT INTO finance.ceps_export_import_svr_15min
            (trade_date, time_interval,
             imbalance_netting_mean_mw, mari_mfrr_mean_mw, picasso_afrr_mean_mw, sum_exchange_mean_mw,
             imbalance_netting_median_mw, mari_mfrr_median_mw, picasso_afrr_median_mw, sum_exchange_median_mw,
             imbalance_netting_last_at_interval_mw, mari_mfrr_last_at_interval_mw, picasso_afrr_last_at_interval_mw, sum_exchange_last_at_interval_mw)
        SELECT
            delivery_timestamp::date AS trade_date,
            to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0') AS time_interval,
            avg(imbalance_netting_mw), avg(mari_mfrr_mw), avg(picasso_afrr_mw), avg(sum_exchange_european_platforms_mw),
            percentile_cont(0.5) within group (order by imbalance_netting_mw),
            percentile_cont(0.5) within group (order by mari_mfrr_mw),
            percentile_cont(0.5) within group (order by picasso_afrr_mw),
            percentile_cont(0.5) within group (order by sum_exchange_european_platforms_mw),
            (array_agg(imbalance_netting_mw order by delivery_timestamp desc))[1],
            (array_agg(mari_mfrr_mw order by delivery_timestamp desc))[1],
            (array_agg(picasso_afrr_mw order by delivery_timestamp desc))[1],
            (array_agg(sum_exchange_european_platforms_mw order by delivery_timestamp desc))[1]
        FROM finance.ceps_export_import_svr_1min
        WHERE (delivery_timestamp::date, to_char(delivery_timestamp, 'HH24:') || lpad(((extract(minute from delivery_timestamp)::int / 15) * 15)::text, 2, '0'))
              IN %s
        GROUP BY trade_date, time_interval
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

    return len(records)


def upsert_data(dataset: str, records: List[Dict], conn, logger) -> int:
    """
    Upload data for any dataset with UPSERT logic.
    Automatically aggregates to 15-minute intervals.
    """
    if dataset == 'imbalance':
        return upsert_imbalance_data(records, conn, logger)
    elif dataset == 're_price':
        return upsert_re_price_data(records, conn, logger)
    elif dataset == 'svr_activation':
        return upsert_svr_activation_data(records, conn, logger)
    elif dataset == 'export_import_svr':
        return upsert_export_import_svr_data(records, conn, logger)
    else:
        raise ValueError(f"Unknown dataset: {dataset}")
