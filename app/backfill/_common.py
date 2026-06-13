"""Shared helpers for 60-min backfill / aggregator scripts.

Two CLI modes:
    python3 -m backfill.backfill_<source> YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
    python3 -m backfill.backfill_<source> --auto                 [--debug] [--dry-run]

`--auto` processes the trailing 6 hours in Europe/Prague — equivalent to
running `(NOW - 6h).date()` through `NOW.date()` (1-2 days). The HAVING
gate (HOUR_COMPLETE_HAVING) makes reprocessing whole days safe; partial
hours produce no row.

Aggregations are SQL-only (INSERT … SELECT … GROUP BY hour),
day-by-day with one commit per day, ON CONFLICT DO UPDATE for idempotency.
"""

import argparse
import logging
import sys
import zoneinfo
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA, DB_SESSION_OPTIONS


PRAGUE_TZ = zoneinfo.ZoneInfo("Europe/Prague")

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

# HAVING fragment enforcing "all four quarters of the hour present".
# Combined with GROUP BY hour, COUNT(DISTINCT time_interval) = 4 can only
# mean the exact 4 expected quarters — partial hours are filtered out.
HOUR_COMPLETE_HAVING = "HAVING COUNT(DISTINCT time_interval) = 4"


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
        options=DB_SESSION_OPTIONS,  # pin session tz; do not inherit server ambient
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
    """Standard CLI for every backfill / aggregator script.

    Positional args (optional iff `--auto` is given):
        start  YYYY-MM-DD
        end    YYYY-MM-DD (inclusive)

    Flags:
        --auto      process trailing 6 hours in Europe/Prague
                    (start_date = (NOW-6h).date(), end_date = NOW.date())
        --debug     verbose logging
        --dry-run   report without writing
    """
    parser = argparse.ArgumentParser(description=f"60-min backfill: {label}")
    parser.add_argument('start', nargs='?',
                        help='Start date YYYY-MM-DD (required unless --auto)')
    parser.add_argument('end', nargs='?',
                        help='End date YYYY-MM-DD (required unless --auto)')
    parser.add_argument('--auto', action='store_true',
                        help='Process trailing 6 hours (replaces start/end)')
    parser.add_argument('--auto-days', type=int, default=None, metavar='N',
                        help='With --auto, look back N days instead of 6 hours. '
                             'Use for sources published with a multi-day lag '
                             '(e.g. OTE settlement data) so late-arriving days '
                             'still get aggregated. Reprocessing is idempotent.')
    parser.add_argument('--debug', action='store_true', help='Verbose logging')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report source row counts without writing')
    args = parser.parse_args()

    if args.auto:
        if args.start or args.end:
            parser.error('--auto cannot be combined with positional start/end')
        now = datetime.now(PRAGUE_TZ)
        if args.auto_days is not None:
            args.start_date = (now - timedelta(days=args.auto_days)).date()
        else:
            args.start_date = (now - timedelta(hours=6)).date()
        args.end_date = now.date()
    else:
        if not (args.start and args.end):
            parser.error('start and end are required unless --auto is given')
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
