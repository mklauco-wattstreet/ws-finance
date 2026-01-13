#!/usr/bin/env python3
"""
CEPS SOAP Data Uploader

Uploads parsed XML data to PostgreSQL with UPSERT logic.
Uses INSERT ... ON CONFLICT DO UPDATE for safe backfills.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT


def upsert_imbalance_data(records: List[Dict], conn, logger) -> int:
    """
    Upload System Imbalance data with UPSERT logic.

    Args:
        records: List of parsed records
        conn: Database connection
        logger: Logger instance

    Returns:
        Number of records upserted
    """
    if not records:
        return 0

    # Prepare data for bulk insert
    values = [
        (r['delivery_timestamp'], r['load_mw'])
        for r in records
    ]

    # UPSERT query for 1min table
    query_1min = """
        INSERT INTO finance.ceps_actual_imbalance_1min (delivery_timestamp, load_mw)
        VALUES %s
        ON CONFLICT (delivery_timestamp)
        DO UPDATE SET
            load_mw = EXCLUDED.load_mw,
            created_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        execute_values(cur, query_1min, values)
        conn.commit()

    logger.info(f"  ✓ Upserted {len(records):,} records to ceps_actual_imbalance_1min")
    return len(records)


def upsert_re_price_data(records: List[Dict], conn, logger) -> int:
    """
    Upload RE Price data with UPSERT logic.

    Args:
        records: List of parsed records
        conn: Database connection
        logger: Logger instance

    Returns:
        Number of records upserted
    """
    if not records:
        return 0

    # Prepare data for bulk insert
    values = [
        (
            r['delivery_timestamp'],
            r['price_afrr_plus_eur_mwh'],
            r['price_afrr_minus_eur_mwh'],
            r['price_mfrr_plus_eur_mwh'],
            r['price_mfrr_minus_eur_mwh'],
            r['price_mfrr_5_eur_mwh']
        )
        for r in records
    ]

    # UPSERT query for 1min table
    query_1min = """
        INSERT INTO finance.ceps_actual_re_price_1min (
            delivery_timestamp,
            price_afrr_plus_eur_mwh,
            price_afrr_minus_eur_mwh,
            price_mfrr_plus_eur_mwh,
            price_mfrr_minus_eur_mwh,
            price_mfrr_5_eur_mwh
        )
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
    return len(records)


def upsert_svr_activation_data(records: List[Dict], conn, logger) -> int:
    """
    Upload SVR Activation data with UPSERT logic.

    Args:
        records: List of parsed records
        conn: Database connection
        logger: Logger instance

    Returns:
        Number of records upserted
    """
    if not records:
        return 0

    # Prepare data for bulk insert
    values = [
        (
            r['delivery_timestamp'],
            r['afrr_plus_mw'],
            r['afrr_minus_mw'],
            r['mfrr_plus_mw'],
            r['mfrr_minus_mw'],
            r['mfrr_5_mw']
        )
        for r in records
    ]

    # UPSERT query for 1min table
    query_1min = """
        INSERT INTO finance.ceps_svr_activation_1min (
            delivery_timestamp,
            afrr_plus_mw,
            afrr_minus_mw,
            mfrr_plus_mw,
            mfrr_minus_mw,
            mfrr_5_mw
        )
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
    return len(records)


def upsert_export_import_svr_data(records: List[Dict], conn, logger) -> int:
    """
    Upload Export/Import SVR data with UPSERT logic.

    Args:
        records: List of parsed records
        conn: Database connection
        logger: Logger instance

    Returns:
        Number of records upserted
    """
    if not records:
        return 0

    # Prepare data for bulk insert
    values = [
        (
            r['delivery_timestamp'],
            r['imbalance_netting_mw'],
            r['mari_mfrr_mw'],
            r['picasso_afrr_mw'],
            r['sum_exchange_european_platforms_mw']
        )
        for r in records
    ]

    # UPSERT query for 1min table
    query_1min = """
        INSERT INTO finance.ceps_export_import_svr_1min (
            delivery_timestamp, imbalance_netting_mw, mari_mfrr_mw,
            picasso_afrr_mw, sum_exchange_european_platforms_mw
        )
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
    return len(records)


def upsert_data(dataset: str, records: List[Dict], conn, logger) -> int:
    """
    Upload data for any dataset with UPSERT logic.

    Args:
        dataset: Dataset key
        records: List of parsed records
        conn: Database connection
        logger: Logger instance

    Returns:
        Number of records upserted
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


def aggregate_to_15min(dataset: str, start_date: datetime, end_date: datetime, conn, logger):
    """
    Aggregate 1-minute data to 15-minute intervals.

    Args:
        dataset: Dataset key
        start_date: Start datetime for aggregation
        end_date: End datetime for aggregation
        conn: Database connection
        logger: Logger instance
    """
    # Table names
    table_mapping = {
        'imbalance': {
            'source': 'ceps_actual_imbalance_1min',
            'target': 'ceps_actual_imbalance_15min'
        },
        're_price': {
            'source': 'ceps_actual_re_price_1min',
            'target': 'ceps_actual_re_price_15min',
            'value_cols': ['price_afrr_plus_eur_mwh', 'price_afrr_minus_eur_mwh',
                          'price_mfrr_plus_eur_mwh', 'price_mfrr_minus_eur_mwh', 'price_mfrr_5_eur_mwh']
        },
        'svr_activation': {
            'source': 'ceps_svr_activation_1min',
            'target': 'ceps_svr_activation_15min',
            'value_cols': ['afrr_plus_mw', 'afrr_minus_mw', 'mfrr_plus_mw',
                          'mfrr_minus_mw', 'mfrr_5_mw']
        },
        'export_import_svr': {
            'source': 'ceps_export_import_svr_1min',
            'target': 'ceps_export_import_svr_15min',
            'value_cols': ['imbalance_netting_mw', 'mari_mfrr_mw',
                          'picasso_afrr_mw', 'sum_exchange_european_platforms_mw']
        }
    }

    if dataset not in table_mapping:
        raise ValueError(f"Unknown dataset: {dataset}")

    config = table_mapping[dataset]
    source_table = config['source']
    target_table = config['target']

    # Build aggregation query based on dataset
    if dataset == 'imbalance':
        # Single value column - use 'load_mw' for imbalance
        value_col = 'load_mw'
        agg_cols = f"""
            AVG({value_col}) AS {value_col}_mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {value_col}) AS {value_col}_median,
            (array_agg({value_col} ORDER BY delivery_timestamp DESC))[1] AS {value_col}_last_at_interval
        """
        insert_cols = f"{value_col}_mean, {value_col}_median, {value_col}_last_at_interval"
        update_cols = f"""
            {value_col}_mean = EXCLUDED.{value_col}_mean,
            {value_col}_median = EXCLUDED.{value_col}_median,
            {value_col}_last_at_interval = EXCLUDED.{value_col}_last_at_interval
        """
    else:
        # Multiple value columns
        value_cols = config['value_cols']
        agg_parts = []
        insert_parts = []
        update_parts = []

        for col in value_cols:
            agg_parts.append(f"""
                AVG({col}) AS {col}_mean,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col}) AS {col}_median,
                (array_agg({col} ORDER BY delivery_timestamp DESC))[1] AS {col}_last_at_interval
            """)
            insert_parts.extend([f"{col}_mean", f"{col}_median", f"{col}_last_at_interval"])
            update_parts.extend([
                f"{col}_mean = EXCLUDED.{col}_mean",
                f"{col}_median = EXCLUDED.{col}_median",
                f"{col}_last_at_interval = EXCLUDED.{col}_last_at_interval"
            ])

        agg_cols = ",\n            ".join(agg_parts)
        insert_cols = ", ".join(insert_parts)
        update_cols = ",\n            ".join(update_parts)

    query = f"""
        INSERT INTO finance.{target_table} (delivery_timestamp, {insert_cols})
        SELECT
            date_trunc('hour', delivery_timestamp) +
                INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15) AS delivery_timestamp,
            {agg_cols}
        FROM finance.{source_table}
        WHERE delivery_timestamp >= %s
          AND delivery_timestamp <= %s
        GROUP BY date_trunc('hour', delivery_timestamp) +
                 INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM delivery_timestamp) / 15)
        ON CONFLICT (delivery_timestamp)
        DO UPDATE SET
            {update_cols},
            last_updated = CURRENT_TIMESTAMP;
    """

    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date))
        rows_affected = cur.rowcount
        conn.commit()

    logger.info(f"  ✓ Aggregated to 15min: {rows_affected:,} intervals")
