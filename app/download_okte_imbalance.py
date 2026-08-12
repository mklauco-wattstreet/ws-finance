#!/usr/bin/env python3
"""
Download Slovak OKTE imbalance settlement data (regulardaily) into PostgreSQL.

Source (public, no authentication):
    GET https://iszo.okte.sk/api/v1/SystemImbalance
        ?dateFrom=YYYY-MM-DD&dateTo=YYYY-MM-DD&evaluationType=regulardaily

Unlike the IDM feed this returns plain JSON (no zip), so fetch and upload live
in one script.

Usage:
    python3 download_okte_imbalance.py [START_DATE END_DATE] [--days N] [--debug]

Examples:
    # Auto mode - trailing 7 days (regulardaily lands at D+2, so the window
    # must reach well past it or rows would never arrive)
    python3 download_okte_imbalance.py

    # Manual mode - backfill a range
    python3 download_okte_imbalance.py 2026-01-01 2026-08-11

Notes:
    - Only regulardaily is stored. OKTE republishes the same day as
      preliminarydaily/decadal/monthly/final with DIFFERENT values; mixing
      vintages in one table would corrupt it.
    - A day not yet evaluated returns [] - that is normal, not an error.
    - Data is only loaded from 2026-01-01 onward (MIN_DATE).
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("okte")
import sys
import time
from datetime import datetime, timedelta, date

import requests
import psycopg2
from psycopg2 import extras

from common import setup_logging, parse_date, validate_date_range, print_banner
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

BASE_URL = "https://iszo.okte.sk/api/v1/SystemImbalance"
EVALUATION_TYPE = "regulardaily"

# regulardaily is published at D+2; a shorter window would silently never fill.
AUTO_DAYS_DEFAULT = 7

# Nothing before 2026 is loaded.
MIN_DATE = date(2026, 1, 1)

# The API caps nothing explicitly, but wide ranges are slow - chunk them.
CHUNK_DAYS = 31

# (db_column, json_key) for the period object.
#
# This is a deliberate subset of the ~36 fields the API returns - only the
# imbalance price/volume signal, matching the scope of the CZ table. Settlement
# bookkeeping (sip/oip/balance/psrec), contracted+metered energy (tcc/tcs/tmc*/
# tms*), the balanced-from-top/bottom aggregates (*_sbb/*_sbt), the day-level
# coefficients (srec/kzpo) and the discontinued fields (tacre/pckre/pczre/
# mppre/mpnre) are all skipped. Re-fetching is cheap if any are wanted later.
#
# NOTE: `srec` here is the period-level EUR/MWh cost. The day-level `srec` is a
# different quantity (a coefficient) and is intentionally not read.
PERIOD_FIELDS = [
    ('emergency', 'emergency'),
    ('settlement_price_imbalance_eur_mwh', 'isp'),
    ('system_imbalance_price_eur_mwh', 'sipr'),
    ('shared_regulation_electricity_cost_eur_mwh', 'srec'),
    ('shared_regulation_electricity_price_eur_mwh', 'srecp'),
    ('system_imbalance_mwh', 'si'),
    ('positive_imbalance_mwh', 'pi'),
    ('negative_imbalance_mwh', 'ni'),
    ('cost_of_imbalance_eur', 'psi'),
    ('positive_regulation_electricity_mwh', 'pre'),
    ('negative_regulation_electricity_mwh', 'nre'),
    ('payment_positive_regulation_electricity_eur', 'pspre'),
    ('payment_negative_regulation_electricity_eur', 'psnre'),
    ('total_regulation_electricity_mwh', 'tre'),
    ('total_cost_regulation_electricity_eur', 'tcre'),
]

COLUMNS = ['trade_date', 'period', 'evaluation_date'] + [c for c, _ in PERIOD_FIELDS]


def chunk_ranges(start, end, chunk_days=CHUNK_DAYS):
    """Split an inclusive date range into windows of at most chunk_days."""
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def fetch_range(start, end, logger):
    """Fetch one date window. Returns the parsed JSON list ([] if unevaluated)."""
    params = {
        'dateFrom': start.strftime('%Y-%m-%d'),
        'dateTo': end.strftime('%Y-%m-%d'),
        'evaluationType': EVALUATION_TYPE,
    }
    logger.debug(f"GET {BASE_URL} {params}")
    response = requests.get(BASE_URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def build_records(payload, logger):
    """Flatten the day/periods structure into one row per (date, period)."""
    records = []
    for day in payload:
        trade_date = datetime.strptime(day['date'], '%Y-%m-%d').date()
        evaluation_date = (
            datetime.strptime(day['evaluationDate'], '%Y-%m-%d').date()
            if day.get('evaluationDate') else None
        )

        if trade_date < MIN_DATE:
            logger.debug(f"  skipping {trade_date} (before {MIN_DATE})")
            continue

        for period in day.get('periods', []):
            records.append(
                [trade_date, period['period'], evaluation_date]
                + [period.get(key) for _, key in PERIOD_FIELDS]
            )
    return records


def upsert(records, conn):
    """Bulk upsert on (trade_date, period). Returns row count."""
    if not records:
        return 0

    update_columns = [c for c in COLUMNS if c not in ('trade_date', 'period')]
    query = f"""
        INSERT INTO okte_prices_imbalance ({', '.join(COLUMNS)})
        VALUES %s
        ON CONFLICT (trade_date, period) DO UPDATE SET
            {', '.join(f'{c} = EXCLUDED.{c}' for c in update_columns)},
            updated_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        extras.execute_values(cur, query, [tuple(r) for r in records])
    conn.commit()
    return len(records)


def connect():
    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        dbname=DB_NAME, port=DB_PORT, connect_timeout=10,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {DB_SCHEMA}")
    conn.autocommit = False
    return conn


def main():
    debug_mode = '--debug' in sys.argv

    auto_days = AUTO_DAYS_DEFAULT
    argv = sys.argv[1:]
    if '--days' in argv:
        idx = argv.index('--days')
        try:
            auto_days = int(argv[idx + 1])
            if auto_days < 1:
                raise ValueError
        except (IndexError, ValueError):
            print("Error: --days requires a positive integer")
            sys.exit(1)
        del argv[idx:idx + 2]

    args = [a for a in argv if a != '--debug']
    auto_mode = len(args) == 0
    manual_mode = len(args) == 2

    if not auto_mode and not manual_mode:
        print("Usage: python3 download_okte_imbalance.py [START_DATE END_DATE] [--days N] [--debug]")
        sys.exit(1)

    logger = setup_logging(debug=debug_mode)

    if auto_mode:
        print_banner("OKTE Imbalance Settlement (AUTO)", debug_mode)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=auto_days - 1)
        logger.info(
            f"OKTE imbalance AUTO {start_date.strftime('%Y-%m-%d')}"
            f"..{end_date.strftime('%Y-%m-%d')} (trailing {auto_days} days)"
        )
    else:
        print_banner("OKTE Imbalance Settlement (MANUAL)", debug_mode)
        start_date = parse_date(args[0])
        end_date = parse_date(args[1])
        validate_date_range(start_date, end_date)
        logger.info(
            f"OKTE imbalance MANUAL {start_date.strftime('%Y-%m-%d')}"
            f"..{end_date.strftime('%Y-%m-%d')}"
        )

    if start_date.date() < MIN_DATE:
        logger.info(f"Clamping start to {MIN_DATE} (nothing before 2026 is loaded)")
        start_date = datetime.combine(MIN_DATE, datetime.min.time())

    if end_date < start_date:
        logger.info("Nothing to do after clamping")
        return

    chunks = list(chunk_ranges(start_date, end_date))
    logger.info(
        f"Range spans {(end_date - start_date).days + 1} day(s) "
        f"in {len(chunks)} chunk(s) of <= {CHUNK_DAYS} days"
    )

    started_at = time.time()
    total_rows = 0
    total_days = 0
    empty_days = 0
    failed_chunks = 0

    conn = connect()
    try:
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            logger.info(
                f"[chunk {i}/{len(chunks)}] "
                f"{chunk_start.strftime('%Y-%m-%d')}..{chunk_end.strftime('%Y-%m-%d')}"
            )
            try:
                payload = fetch_range(chunk_start, chunk_end, logger)
            except Exception as e:
                failed_chunks += 1
                logger.error(f"    FETCH FAILED: {e}")
                continue

            if not payload:
                empty_days += (chunk_end - chunk_start).days + 1
                logger.info("    no regulardaily evaluation yet for this window")
                continue

            records = build_records(payload, logger)
            try:
                rows = upsert(records, conn)
            except Exception as e:
                conn.rollback()
                failed_chunks += 1
                logger.error(f"    UPSERT FAILED: {e}")
                continue

            total_rows += rows
            total_days += len(payload)
            logger.info(f"    {len(payload)} day(s), {rows} rows upserted")

            if i < len(chunks):
                time.sleep(1)

        logger.info("-" * 60)
        logger.info(
            f"OKTE imbalance done: {len(chunks) - failed_chunks}/{len(chunks)} chunks OK"
            + (f", {failed_chunks} FAILED" if failed_chunks else "")
        )
        logger.info(f"  days evaluated: {total_days}")
        logger.info(f"  rows upserted:  {total_rows:,}")
        if empty_days:
            logger.info(f"  not yet evaluated: {empty_days} day(s)")
        logger.info(f"  elapsed:        {time.time() - started_at:.1f}s")

        if failed_chunks:
            sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
