#!/usr/bin/env python3
"""
CEPS SOAP Data Uploader

Uploads parsed XML data to PostgreSQL with UPSERT logic.
Uses INSERT ... ON CONFLICT DO UPDATE for safe backfills.
Aggregates 1-minute data to 15-minute intervals.

Every table is keyed on delivery_datetime (UTC instant); trade_date,
time_interval and the 1-min delivery_timestamp are local descriptive values
(see ceps/ceps_time.py). Affected intervals are passed around as sets of UTC
quarter-hour starts.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Sequence, Set, Tuple
import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
from ceps.ceps_time import (
    affected_quarters, bucket_start_sql, instant_window, local_date_sql,
    local_dates, local_label_sql, local_window,
)
from ceps.preprocess_ceps_data import aggregate_1min_features, aggregate_derived_features
from dst_calendar import PRAGUE_TZ, local_label

# (1-min source column, 15-min mean column, 15-min median column, 15-min last column)
AggColumns = Sequence[Tuple[str, str, str, str]]

IMBALANCE_COLUMNS: AggColumns = [
    ('system_imbalance_mw', 'system_imbalance_mean_mw', 'system_imbalance_median_mw',
     'system_imbalance_last_mw'),
]
RE_PRICE_COLUMNS: AggColumns = [
    (f'price_{p}_eur_mwh', f'price_{p}_mean_eur_mwh', f'price_{p}_median_eur_mwh',
     f'price_{p}_last_at_interval_eur_mwh')
    for p in ('afrr_plus', 'afrr_minus', 'mfrr_plus', 'mfrr_minus', 'mfrr_5')
]
SVR_ACTIVATION_COLUMNS: AggColumns = [
    (f'{p}_mw', f'{p}_mean_mw', f'{p}_median_mw', f'{p}_last_at_interval_mw')
    for p in ('afrr_plus', 'afrr_minus', 'mfrr_plus', 'mfrr_minus', 'mfrr_5')
]
EXPORT_IMPORT_SVR_COLUMNS: AggColumns = [
    (f'{p}_mw', f'{p}_mean_mw', f'{p}_median_mw', f'{p}_last_at_interval_mw')
    for p in ('imbalance_netting', 'mari_mfrr', 'picasso_afrr')
] + [
    ('sum_exchange_european_platforms_mw', 'sum_exchange_mean_mw', 'sum_exchange_median_mw',
     'sum_exchange_last_at_interval_mw'),
]
GENERATION_RES_COLUMNS: AggColumns = [
    (f'{p}_mw', f'{p}_mean_mw', f'{p}_median_mw', f'{p}_last_at_interval_mw')
    for p in ('wind', 'solar')
]


def get_affected_intervals(records: List[Dict]) -> Set[datetime]:
    """UTC starts of the 15-min intervals that need re-aggregation."""
    return affected_quarters(records)


def _aggregate_1min_to_15min(table_1min: str, table_15min: str, columns: AggColumns,
                             affected_intervals: Set[datetime], conn,
                             last_order: str = 'delivery_datetime') -> int:
    """Aggregate a 1-min table into its 15-min table for the affected intervals.

    Buckets on the UTC instant, so the two passes of the fall-back hour stay
    separate. mean / median / last value per source column; the last value is
    taken by `last_order` DESC.
    """
    if not affected_intervals:
        return 0

    lo_utc, hi_utc = instant_window(affected_intervals)
    lo_local, hi_local = local_window(lo_utc, hi_utc)

    targets = [c for _src, *aggs in columns for c in aggs]
    selects = []
    for src, mean_col, median_col, last_col in columns:
        selects += [
            f"AVG({src})",
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {src})",
            f"(ARRAY_AGG({src} ORDER BY {last_order} DESC))[1]",
        ]
    updates = ",\n            ".join(f"{c} = EXCLUDED.{c}" for c in targets)
    changed = "\n           OR ".join(
        f"finance.{table_15min}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in targets + ['time_interval']
    )

    query = f"""
        WITH interval_data AS (
            SELECT
                {bucket_start_sql()} AS interval_start,
                delivery_datetime,
                {", ".join(src for src, *_ in columns)}
            FROM finance.{table_1min}
            WHERE delivery_timestamp >= %s AND delivery_timestamp < %s
              AND delivery_datetime >= %s AND delivery_datetime < %s
        )
        INSERT INTO finance.{table_15min}
            (trade_date, time_interval, delivery_datetime, {", ".join(targets)})
        SELECT
            {local_date_sql('interval_start')},
            {local_label_sql('interval_start')},
            interval_start,
            {", ".join(selects)}
        FROM interval_data
        WHERE interval_start = ANY(%s)
        GROUP BY interval_start
        ON CONFLICT (delivery_datetime, trade_date) DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            {updates},
            updated_at = CURRENT_TIMESTAMP
        WHERE {changed}
    """

    with conn.cursor() as cur:
        cur.execute(query, (lo_local, hi_local, lo_utc, hi_utc, list(affected_intervals)))
        rows = cur.rowcount
        conn.commit()

    return rows


def aggregate_imbalance_15min(affected_intervals: Set[datetime], conn, logger) -> int:
    """Aggregate imbalance data for affected 15-minute intervals.

    system_imbalance_last_mw keeps its historical definition: ordered by the
    interval start, which is constant within the group, so in practice it is
    the first minute of the interval, not the last.
    """
    return _aggregate_1min_to_15min('ceps_actual_imbalance_1min', 'ceps_actual_imbalance_15min',
                                    IMBALANCE_COLUMNS, affected_intervals, conn,
                                    last_order='interval_start')


def aggregate_re_price_15min(affected_intervals: Set[datetime], conn, logger) -> int:
    """Aggregate RE price data for affected 15-minute intervals."""
    return _aggregate_1min_to_15min('ceps_actual_re_price_1min', 'ceps_actual_re_price_15min',
                                    RE_PRICE_COLUMNS, affected_intervals, conn)


def aggregate_svr_activation_15min(affected_intervals: Set[datetime], conn, logger) -> int:
    """Aggregate SVR activation data for affected 15-minute intervals."""
    return _aggregate_1min_to_15min('ceps_svr_activation_1min', 'ceps_svr_activation_15min',
                                    SVR_ACTIVATION_COLUMNS, affected_intervals, conn)


def aggregate_export_import_svr_15min(affected_intervals: Set[datetime], conn, logger) -> int:
    """Aggregate Export/Import SVR data for affected 15-minute intervals."""
    return _aggregate_1min_to_15min('ceps_export_import_svr_1min', 'ceps_export_import_svr_15min',
                                    EXPORT_IMPORT_SVR_COLUMNS, affected_intervals, conn)


def aggregate_generation_res_15min(affected_intervals: Set[datetime], conn, logger) -> int:
    """Aggregate Generation RES data for affected 15-minute intervals."""
    return _aggregate_1min_to_15min('ceps_generation_res_1min', 'ceps_generation_res_15min',
                                    GENERATION_RES_COLUMNS, affected_intervals, conn)


def _upsert_1min(table: str, value_cols: Sequence[str], records: List[Dict], conn) -> None:
    """Bulk upsert 1-min records keyed on (delivery_datetime, delivery_timestamp)."""
    values = [
        (r['delivery_datetime'], r['delivery_timestamp'], *(r[c] for c in value_cols))
        for r in records
    ]
    updates = ",\n            ".join(f"{c} = EXCLUDED.{c}" for c in value_cols)
    changed = "\n           OR ".join(
        f"finance.{table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in value_cols
    )
    query = f"""
        INSERT INTO finance.{table} (delivery_datetime, delivery_timestamp, {", ".join(value_cols)})
        VALUES %s
        ON CONFLICT (delivery_datetime, delivery_timestamp)
        DO UPDATE SET
            {updates},
            updated_at = CURRENT_TIMESTAMP
        WHERE {changed}
    """
    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()


def upsert_imbalance_data(records: List[Dict], conn, logger) -> int:
    """Upload System Imbalance data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    # Get affected intervals before insert
    affected_intervals = get_affected_intervals(records)

    _upsert_1min('ceps_actual_imbalance_1min', ['system_imbalance_mw'], records, conn)
    logger.debug(f"  Upserted {len(records):,} to ceps_actual_imbalance_1min")

    # Aggregate affected intervals
    agg_count = aggregate_imbalance_15min(affected_intervals, conn, logger)
    logger.debug(f"  Aggregated {agg_count:,} to ceps_actual_imbalance_15min")

    feat_count = aggregate_1min_features(affected_intervals, conn, logger)
    if feat_count:
        logger.debug(f" Computed {feat_count:,} feature intervals to ceps_1min_features_15min")

    derived_count = aggregate_derived_features(affected_intervals, conn, logger)
    if derived_count:
        logger.debug(f" Computed {derived_count:,} derived feature intervals")

    return len(records)


def upsert_re_price_data(records: List[Dict], conn, logger) -> int:
    """Upload RE Price data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    _upsert_1min('ceps_actual_re_price_1min', [src for src, *_ in RE_PRICE_COLUMNS], records, conn)
    logger.debug(f" Upserted {len(records):,} records to ceps_actual_re_price_1min")

    agg_count = aggregate_re_price_15min(affected_intervals, conn, logger)
    logger.debug(f" Aggregated {agg_count:,} intervals to ceps_actual_re_price_15min")

    feat_count = aggregate_1min_features(affected_intervals, conn, logger)
    if feat_count:
        logger.debug(f" Computed {feat_count:,} feature intervals to ceps_1min_features_15min")

    return len(records)


def upsert_svr_activation_data(records: List[Dict], conn, logger) -> int:
    """Upload SVR Activation data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    _upsert_1min('ceps_svr_activation_1min', [src for src, *_ in SVR_ACTIVATION_COLUMNS], records, conn)
    logger.debug(f" Upserted {len(records):,} records to ceps_svr_activation_1min")

    agg_count = aggregate_svr_activation_15min(affected_intervals, conn, logger)
    logger.debug(f" Aggregated {agg_count:,} intervals to ceps_svr_activation_15min")

    feat_count = aggregate_1min_features(affected_intervals, conn, logger)
    if feat_count:
        logger.debug(f" Computed {feat_count:,} feature intervals to ceps_1min_features_15min")

    return len(records)


def upsert_export_import_svr_data(records: List[Dict], conn, logger) -> int:
    """Upload Export/Import SVR data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    _upsert_1min('ceps_export_import_svr_1min', [src for src, *_ in EXPORT_IMPORT_SVR_COLUMNS],
                 records, conn)
    logger.debug(f" Upserted {len(records):,} records to ceps_export_import_svr_1min")

    agg_count = aggregate_export_import_svr_15min(affected_intervals, conn, logger)
    logger.debug(f" Aggregated {agg_count:,} intervals to ceps_export_import_svr_15min")

    feat_count = aggregate_1min_features(affected_intervals, conn, logger)
    if feat_count:
        logger.debug(f" Computed {feat_count:,} feature intervals to ceps_1min_features_15min")

    return len(records)


def upsert_generation_res_data(records: List[Dict], conn, logger) -> int:
    """Upload Generation RES data with UPSERT logic and aggregate to 15min."""
    if not records:
        return 0

    affected_intervals = get_affected_intervals(records)

    _upsert_1min('ceps_generation_res_1min', [src for src, *_ in GENERATION_RES_COLUMNS], records, conn)
    logger.debug(f" Upserted {len(records):,} records to ceps_generation_res_1min")

    agg_count = aggregate_generation_res_15min(affected_intervals, conn, logger)
    logger.debug(f" Aggregated {agg_count:,} intervals to ceps_generation_res_15min")

    derived_count = aggregate_derived_features(affected_intervals, conn, logger)
    if derived_count:
        logger.debug(f" Computed {derived_count:,} derived feature intervals")

    return len(records)


def get_15min_interval(ts: datetime) -> tuple:
    """
    Convert an aware timestamp to (trade_date, time_interval) for native 15-min data.
    Both are local Prague values; time_interval is "HH:MM-HH:MM" (e.g., "14:00-14:15").
    """
    start = ts.astimezone(PRAGUE_TZ)
    return (start.strftime('%Y-%m-%d'), local_label(ts, 15, midnight='00:00'))


def _upsert_native_15min(table: str, value_cols: Sequence[str], records: List[Dict], conn) -> None:
    """Bulk upsert native 15-min records keyed on (delivery_datetime, trade_date)."""
    values = []
    for r in records:
        trade_date, time_interval = get_15min_interval(r['delivery_datetime'])
        values.append((trade_date, time_interval, r['delivery_datetime'], *(r[c] for c in value_cols)))
    updates = ",\n            ".join(f"{c} = EXCLUDED.{c}" for c in value_cols)
    changed = "\n           OR ".join(
        f"finance.{table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in list(value_cols) + ['time_interval']
    )
    query = f"""
        INSERT INTO finance.{table}
            (trade_date, time_interval, delivery_datetime, {", ".join(value_cols)})
        VALUES %s
        ON CONFLICT (delivery_datetime, trade_date)
        DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            {updates},
            updated_at = CURRENT_TIMESTAMP
        WHERE {changed}
    """
    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()


def upsert_generation_data(records: List[Dict], conn, logger) -> int:
    """
    Upload actual Generation data (by plant type) with UPSERT logic.
    Native 15-min data - no aggregation needed.
    """
    if not records:
        return 0

    _upsert_native_15min(
        'ceps_generation_15min',
        ['tpp_mw', 'ccgt_mw', 'npp_mw', 'hpp_mw', 'pspp_mw', 'altpp_mw', 'appp_mw', 'wpp_mw', 'pvpp_mw'],
        records, conn,
    )
    logger.debug(f" Upserted {len(records):,} records to ceps_generation_15min")

    derived_count = aggregate_derived_features(get_affected_intervals(records), conn, logger)
    if derived_count:
        logger.debug(f" Computed {derived_count:,} derived feature intervals")

    return len(records)


def upsert_generation_plan_data(records: List[Dict], conn, logger) -> int:
    """
    Upload Generation Plan data (total planned) with UPSERT logic.
    Native 15-min data - no aggregation needed.
    """
    if not records:
        return 0

    _upsert_native_15min('ceps_generation_plan_15min', ['total_mw'], records, conn)
    logger.debug(f" Upserted {len(records):,} records to ceps_generation_plan_15min")

    derived_count = aggregate_derived_features(get_affected_intervals(records), conn, logger)
    if derived_count:
        logger.debug(f" Computed {derived_count:,} derived feature intervals")

    return len(records)


def upsert_estimated_imbalance_price_data(records: List[Dict], conn, logger) -> int:
    """
    Upload Estimated Imbalance Price data (OdhadovanaCenaOdchylky) with UPSERT logic.
    Native 15-min data - no aggregation needed.

    This dataset uses trade_date and time_interval directly from the parser
    (unique structure compared to other datasets); the parser resolves each
    label to its delivery_datetime.

    CEPS publishes the last period's price as 0.00 as a temporary placeholder.
    This disrupts statistics, so we replace the trailing 0.00 per day with NULL.
    The real value will overwrite NULL on the next fetch cycle.
    Two consecutive zeros at the end: only the very last is temporary.
    """
    if not records:
        return 0

    values = []
    for r in records:
        values.append([r['trade_date'], r['time_interval'], r['delivery_datetime'],
                       r['estimated_price_czk_mwh']])

    # Null-out the last period's 0.00 per trade_date (temporary placeholder from CEPS)
    last_by_date = {}
    for v in values:
        if v[0] not in last_by_date or v[2] > last_by_date[v[0]][2]:
            last_by_date[v[0]] = v

    nullified = 0
    for v in last_by_date.values():
        if v[3] == 0.0:
            v[3] = None
            nullified += 1

    if nullified:
        logger.debug(f" Nullified trailing 0.00 placeholder for {nullified} day(s)")

    query = """
        INSERT INTO finance.ceps_estimated_imbalance_price_15min
            (trade_date, time_interval, delivery_datetime, estimated_price_czk_mwh)
        VALUES %s
        ON CONFLICT (delivery_datetime, trade_date)
        DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            estimated_price_czk_mwh = EXCLUDED.estimated_price_czk_mwh,
            updated_at = CURRENT_TIMESTAMP
        WHERE finance.ceps_estimated_imbalance_price_15min.estimated_price_czk_mwh
              IS DISTINCT FROM EXCLUDED.estimated_price_czk_mwh
           OR finance.ceps_estimated_imbalance_price_15min.time_interval
              IS DISTINCT FROM EXCLUDED.time_interval
    """

    with conn.cursor() as cur:
        execute_values(cur, query, [tuple(v) for v in values])
        conn.commit()

    logger.debug(f" Upserted {len(records):,} records to ceps_estimated_imbalance_price_15min")

    # Compute 60-min averages for affected hours
    affected_hours = {
        v[2].replace(minute=0, second=0, microsecond=0) for v in values
    }
    if affected_hours:
        updated = _update_estimated_price_60min(affected_hours, conn, logger)
        if updated:
            logger.debug(f" Updated {updated} intervals with 60min avg price")

    return len(records)


def _update_estimated_price_60min(affected_hours: Set[datetime], conn, logger) -> int:
    """
    Compute estimated_price_czk_mwh_60min as average of available 15-min
    prices within each affected hour and update the rows.

    `affected_hours` are UTC hour starts; each hour is its 4 intervals by
    instant, so the two passes of the fall-back hour are averaged separately.
    If the hour is incomplete, averages from existing non-NULL data points.
    """
    dates = local_dates(affected_hours)
    dates = sorted(set(dates) | {d - timedelta(days=1) for d in dates} | {d + timedelta(days=1) for d in dates})
    with conn.cursor() as cur:
        cur.execute("""
            WITH hours AS (
                SELECT h, AVG(p.estimated_price_czk_mwh) AS avg_price
                FROM unnest(%s::timestamptz[]) AS h
                JOIN finance.ceps_estimated_imbalance_price_15min p
                  ON p.delivery_datetime >= h AND p.delivery_datetime < h + interval '1 hour'
                 AND p.trade_date = ANY(%s)
                WHERE p.estimated_price_czk_mwh IS NOT NULL
                GROUP BY h
            )
            UPDATE finance.ceps_estimated_imbalance_price_15min t
            SET estimated_price_czk_mwh_60min = hours.avg_price
            FROM hours
            WHERE t.trade_date = ANY(%s)
              AND t.delivery_datetime >= hours.h AND t.delivery_datetime < hours.h + interval '1 hour'
        """, (list(affected_hours), dates, dates))
        updated = cur.rowcount
        conn.commit()
    return updated


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
