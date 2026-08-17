#!/usr/bin/env python3
"""
Download Slovak OKTE Intraday Auction (IDA1/IDA2/IDA3) prices into PostgreSQL.

Source (public, no authentication):
    GET https://isot.okte.sk/api/v1/ida/results
        ?deliveryDayFrom=YYYY-MM-DD&deliveryDayTo=YYYY-MM-DD
    Header: Accept: application/json (without it the API returns a schema
    stub instead of data)

Returns a flat JSON array, one element per (auction, period). Only rows with
publicationStatus == "final" are kept - IDA results can be republished before
finalizing, and mixing vintages would corrupt the table.

Usage:
    python3 download_okte_ida.py [START_DATE END_DATE] [--days N] [--debug] [--dry-run]

Examples:
    # Auto mode - trailing 3 days (today plus the 2 before it), re-fetched
    python3 download_okte_ida.py
    python3 download_okte_ida.py --days 7        # wider self-heal window

    # Manual mode - specify date range (backfill), chunked into <=7-day requests
    python3 download_okte_ida.py 2026-08-01 2026-08-10
    python3 download_okte_ida.py 2026-08-01 2026-08-10 --dry-run --debug

Notes:
    - Like the IDM feed this is plain JSON (no zip), so fetch and upload live
      in one script - no file artifacts.
    - deliveryDay/deliveryStart/deliveryEnd in the payload are UTC; trade_date
      and time_interval are instead derived from `period` (1..96) directly,
      matching OTE's own IDA convention (Europe/Prague local wall-clock time).
"""

import sentry_init  # noqa: F401 - must be first to capture errors
sentry_init.set_module("okte")
import sys
import time
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2 import extras

from common import setup_logging, parse_date, validate_date_range, print_banner
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

BASE_URL = "https://isot.okte.sk/api/v1/ida/results"

# Auto mode fetches a trailing window, not just today: this makes a container
# outage self-heal (nothing else ever revisits a past day) and re-fetching is
# idempotent via upsert.
AUTO_DAYS_DEFAULT = 3

# The API caps nothing explicitly, but chunk wide backfill ranges anyway.
CHUNK_DAYS = 7

# Only these auctions are recognized; anything else is a malformed/unknown row.
IDA_AUCTIONS = {"IDA1": 1, "IDA2": 2, "IDA3": 3}

COLUMNS = [
    'trade_date', 'period', 'ida_idx', 'time_interval',
    'price_eur_mwh', 'volume_mwh', 'saldo_dm_mwh', 'export_mwh', 'import_mwh',
    'flow_sk_cz', 'flow_cz_sk', 'flow_sk_hu', 'flow_hu_sk', 'flow_sk_pl', 'flow_pl_sk',
]


def period_to_time_interval(period):
    """Convert a 1..96 period index to 'HH:MM-HH:MM' Europe/Prague wall-clock.

    Mirrors OTE's own IDA time_interval convention (see upload_ida_prices.py):
    period 1 = '00:00-00:15', period 96 = '23:45-00:00'.
    """
    start_minutes = (period - 1) * 15
    end_minutes = (start_minutes + 15) % (24 * 60)
    start = f"{start_minutes // 60:02d}:{start_minutes % 60:02d}"
    end = f"{end_minutes // 60:02d}:{end_minutes % 60:02d}"
    return f"{start}-{end}"


def build_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def chunk_ranges(start, end, chunk_days=CHUNK_DAYS):
    """Split an inclusive date range into windows of at most chunk_days."""
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def fetch_range(session, start, end, logger):
    """Fetch one date window. Returns the parsed JSON list ([] if none)."""
    params = {
        'deliveryDayFrom': start.strftime('%Y-%m-%d'),
        'deliveryDayTo': end.strftime('%Y-%m-%d'),
    }
    headers = {'Accept': 'application/json'}
    logger.debug(f"GET {BASE_URL} {params}")
    response = session.get(BASE_URL, params=params, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def build_records(payload, logger):
    """Filter to final rows and map JSON fields to DB columns."""
    records = []
    skipped_non_final = 0
    skipped_unknown_auction = 0

    for row in payload:
        if row.get('publicationStatus') != 'final':
            skipped_non_final += 1
            continue

        auction = row.get('auction')
        ida_idx = IDA_AUCTIONS.get(auction)
        if ida_idx is None:
            skipped_unknown_auction += 1
            continue

        try:
            period = int(row['period'])
        except (KeyError, TypeError, ValueError):
            continue

        trade_date = datetime.strptime(row['deliveryDay'], '%Y-%m-%d').date()
        time_interval = period_to_time_interval(period)

        export_mwh = (row.get('flowSkCz') or 0) + (row.get('flowSkHu') or 0) + (row.get('flowSkPl') or 0)
        import_mwh = (row.get('flowCzSk') or 0) + (row.get('flowHuSk') or 0) + (row.get('flowPlSk') or 0)
        saldo_dm_mwh = import_mwh - export_mwh

        records.append((
            trade_date,
            period,
            ida_idx,
            time_interval,
            row.get('price'),
            row.get('saleSuccessfulVolume'),
            saldo_dm_mwh,
            export_mwh,
            import_mwh,
            row.get('flowSkCz'),
            row.get('flowCzSk'),
            row.get('flowSkHu'),
            row.get('flowHuSk'),
            row.get('flowSkPl'),
            row.get('flowPlSk'),
        ))

    if skipped_non_final:
        logger.debug(f"    skipped {skipped_non_final} non-final row(s)")
    if skipped_unknown_auction:
        logger.warning(f"    skipped {skipped_unknown_auction} row(s) with unrecognized auction")

    return records


def upsert(records, conn):
    """Bulk upsert on (trade_date, period, ida_idx). Returns row count."""
    if not records:
        return 0

    update_columns = [c for c in COLUMNS if c not in ('trade_date', 'period', 'ida_idx')]
    query = f"""
        INSERT INTO okte_prices_ida ({', '.join(COLUMNS)})
        VALUES %s
        ON CONFLICT (trade_date, period, ida_idx) DO UPDATE SET
            {', '.join(f'{c} = EXCLUDED.{c}' for c in update_columns)},
            updated_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        extras.execute_values(cur, query, records)
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
    dry_run = '--dry-run' in sys.argv

    auto_days = AUTO_DAYS_DEFAULT
    argv = [a for a in sys.argv[1:] if a not in ('--debug', '--dry-run')]
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

    auto_mode = len(argv) == 0
    manual_mode = len(argv) == 2

    if not auto_mode and not manual_mode:
        print("Usage: python3 download_okte_ida.py [START_DATE END_DATE] [--days N] [--debug] [--dry-run]")
        sys.exit(1)

    logger = setup_logging(debug=debug_mode)

    if auto_mode:
        print_banner("OKTE Intraday Auction (AUTO)", debug_mode)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=auto_days - 1)
        logger.info(
            f"OKTE IDA AUTO {start_date.strftime('%Y-%m-%d')}"
            f"..{end_date.strftime('%Y-%m-%d')} (trailing {auto_days} days)"
        )
    else:
        print_banner("OKTE Intraday Auction (MANUAL)", debug_mode)
        start_date = parse_date(argv[0])
        end_date = parse_date(argv[1])
        validate_date_range(start_date, end_date)
        logger.info(
            f"OKTE IDA MANUAL {start_date.strftime('%Y-%m-%d')}"
            f"..{end_date.strftime('%Y-%m-%d')}"
        )

    if dry_run:
        logger.info("DRY-RUN: will fetch and parse but skip DB upload")

    chunks = list(chunk_ranges(start_date, end_date))
    logger.info(
        f"Range spans {(end_date - start_date).days + 1} day(s) "
        f"in {len(chunks)} chunk(s) of <= {CHUNK_DAYS} days"
    )

    session = build_session()
    started_at = time.time()
    total_rows = 0
    total_raw_rows = 0
    failed_chunks = 0

    conn = None if dry_run else connect()
    try:
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            logger.info(
                f"[chunk {i}/{len(chunks)}] "
                f"{chunk_start.strftime('%Y-%m-%d')}..{chunk_end.strftime('%Y-%m-%d')}"
            )
            try:
                payload = fetch_range(session, chunk_start, chunk_end, logger)
            except Exception as e:
                failed_chunks += 1
                logger.error(f"    FETCH FAILED: {e}")
                continue

            total_raw_rows += len(payload)
            records = build_records(payload, logger)

            if dry_run:
                logger.info(f"    {len(payload)} raw row(s), {len(records)} final row(s) parsed")
                total_rows += len(records)
                continue

            try:
                rows = upsert(records, conn)
            except Exception as e:
                conn.rollback()
                failed_chunks += 1
                logger.error(f"    UPSERT FAILED: {e}")
                continue

            total_rows += rows
            logger.info(f"    {len(payload)} raw row(s), {rows} rows upserted")

            if i < len(chunks):
                time.sleep(1)

        elapsed = time.time() - started_at
        logger.info("-" * 60)
        logger.info(
            f"OKTE IDA done: {len(chunks) - failed_chunks}/{len(chunks)} chunks OK"
            + (f", {failed_chunks} FAILED" if failed_chunks else "")
        )
        logger.info(f"  raw rows fetched: {total_raw_rows}")
        logger.info(f"  rows {'parsed (dry-run)' if dry_run else 'upserted'}: {total_rows:,}")
        logger.info(f"  elapsed: {elapsed:.1f}s")

        if failed_chunks:
            sys.exit(1)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    main()
