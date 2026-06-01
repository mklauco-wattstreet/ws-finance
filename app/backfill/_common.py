"""Shared helpers for 60-min backfill scripts.

Every script in this package follows the same shape:
    python3 -m backfill.backfill_<source> YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]

Aggregations are SQL-only (INSERT … SELECT … GROUP BY hour),
day-by-day with one commit per day, ON CONFLICT DO UPDATE for idempotency.
"""

import argparse
import logging
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA


# SQL fragment that derives the 60-min time_interval from a 15-min one.
# Example: '09:15-09:30' -> '09:00-10:00', '23:45-00:00' -> '23:00-00:00'.
# NOTE: psycopg2 treats `%` as parameter placeholder when execute() gets a
# params tuple, so the literal modulo operator must be escaped as `%%`.
HOUR_INTERVAL_SQL = (
    "SUBSTRING(time_interval, 1, 2) || ':00-' || "
    "LPAD(((SUBSTRING(time_interval, 1, 2)::INT + 1) %% 24)::TEXT, 2, '0') || ':00'"
)

# GROUP BY expression that buckets 15-min rows into hours.
HOUR_GROUP_SQL = "SUBSTRING(time_interval, 1, 2)"


def setup_logging(name: str, debug: bool = False) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    return logging.getLogger(name)


@contextmanager
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA}")
        yield conn
    finally:
        conn.close()


def print_banner(title: str) -> None:
    bar = "=" * 60
    print(bar)
    print(title)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(bar)


def parse_args(label: str) -> argparse.Namespace:
    """Standard CLI for every backfill script.

    Positional args (REQUIRED — no defaults, per architect decision):
        start  YYYY-MM-DD
        end    YYYY-MM-DD (inclusive)
    Optional: --debug, --dry-run
    """
    parser = argparse.ArgumentParser(description=f"60-min backfill: {label}")
    parser.add_argument('start', help='Start date YYYY-MM-DD (inclusive)')
    parser.add_argument('end', help='End date YYYY-MM-DD (inclusive)')
    parser.add_argument('--debug', action='store_true', help='Verbose logging')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report source row counts without writing')
    args = parser.parse_args()
    try:
        args.start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
        args.end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
    except ValueError as e:
        parser.error(f"Invalid date format: {e}")
    if args.end_date < args.start_date:
        parser.error("end must be >= start")
    return args


def daterange(start: date, end: date):
    """Inclusive day-by-day iterator."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def run_backfill(
    label: str,
    queries: list,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> None:
    """Standard day-by-day driver.

    Args:
        label: human-readable name (for the banner).
        queries: list of (target_table, sql) tuples. Each SQL takes
                 a single '%s' parameter — the day being processed.
        args: parsed CLI args (with start_date, end_date, dry_run, debug).
        logger: configured logger.
    """
    print_banner(f"{label} backfill {args.start_date} to {args.end_date}")
    total_days = (args.end_date - args.start_date).days + 1
    totals = {t: 0 for t, _ in queries}

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for i, day in enumerate(daterange(args.start_date, args.end_date), 1):
                if args.dry_run:
                    logger.info(f"  DRY-RUN day {day}: would run {len(queries)} INSERTs")
                else:
                    try:
                        for target, sql in queries:
                            cur.execute(sql, (day,))
                            totals[target] += cur.rowcount
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"  Day {day} failed: {e}")
                        raise

                if i % 30 == 0:
                    logger.info(f"  Progress: {i}/{total_days} days")

    for target, count in totals.items():
        logger.info(f"  {target}: {count} rows upserted")
    logger.info(f"{label} backfill complete")
