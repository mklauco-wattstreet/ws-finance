#!/usr/bin/env python3
"""
OTE SOAP - settlement exchange rate (SFVOTREQ 425 -> SFVOTEXCHRATE 426, "Vypořádací
kurz OTE") into finance.ote_exchange_rate.

One run: request the rates for an interval, wait for the answer in the OTE queue,
parse it (exchange_rate_parser.py, XSD-validated) into ONE row per valid date, upsert
with one execute_values statement, write one finance.fetch_log row (source "ote",
dataset "soap_exchange_rate", data_from/data_to = the requested dates). Scheduled once
a day in the crontab.

Interval: default yesterday..today (Europe/Prague), because OTE does not document when
the day's rate is published. Nothing before FIRST_DATE is requested: the default
`from` is raised to it and an explicit --from or --to before it is refused (no
historical data wanted yet).

The upsert keeps a rate that this message does not carry (COALESCE), so a message with
only one kind of rate never erases another kind already stored. Before writing, the
stored rows for the requested dates are read and every rate that is already stored with
a DIFFERENT value is logged as a WARNING (date, column, old -> new); the new value is
still written.

The answer arrives asynchronously in the queue (see ote_soap/report.py): every
picked-up message is saved to $OTE_SOAP_OUT_DIR, and every request and every answer is
one finance.ote_soap_message row linked to this run's fetch_log row (an unrelated
message gets level warning). Our answer is the SFVOTEXCHRATE whose Reference@id is our
request id. The run holds the queue lock /tmp/ote_soap_queue.lock (shared with
ote_soap.limits and ote_soap.fetch --collect) and waits up to 60 s for it; the
fetch_log row is written before the lock is taken.

Usage (inside the container, cwd /app/scripts):
    python3 -m ote_soap.exchange_rate [--dry-run] [--debug] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
                                      [--timeout 60] [--poll 2]
        --dry-run   the request IS sent and the queue IS read (files are saved, the
                    messages are logged); ote_exchange_rate is not read or written,
                    the rows are printed
        --from/--to first / last valid date asked for (default yesterday / today)
        --timeout   seconds to wait for our answer after the request was accepted
        --poll      seconds between queue pickups while the queue is empty

Exit codes: 0 ok, 1 failed (rejected, timeout, no data, no rates, format error,
database error), 2 queue lock still held by another process after 60 s (fetch_log
row "failed" is written) or a refused argument (argparse usage error).
"""

import sentry_init  # noqa: F401  (must be first)

sentry_init.set_module("ote")

import argparse
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2 import extras

import fetch_log
from common import setup_logging
from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_SCHEMA, DB_USER
from ote_soap import message_log
from ote_soap.client import BASE_URL, SIGN_ALG, OteSoapClient, OteSoapError
from ote_soap.exchange_rate_parser import (
    COLUMNS, ExchangeRateFormatError, is_exchange_rate_message, parse_exchange_rate,
)
from ote_soap.messages import PRAGUE
from ote_soap.report import LOCK_WAIT_S, ReportRunError, ask_and_wait
from ote_soap.shared import OUT_ROOT, QueueLockHeld, load_credentials, queue_lock

REPORT_CODE = "425"
FETCH_SOURCE = "ote"
FETCH_DATASET = "soap_exchange_rate"
TABLE = "ote_exchange_rate"
FIRST_DATE = date(2026, 11, 10)  # owner's rule: no data before this date is requested or stored

EXIT_OK, EXIT_FAILED, EXIT_LOCK_HELD = 0, 1, 2

RATE_COLUMNS = ("ote_final_rate", "ote_final_vdt_rate", "ote_final_ida_rate")
CONFLICT_COLUMN = "valid_date"
UPSERT_SQL = (
    f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s "
    f"ON CONFLICT ({CONFLICT_COLUMN}) DO UPDATE SET "
    + ", ".join(f"{c} = COALESCE(EXCLUDED.{c}, {TABLE}.{c})" for c in RATE_COLUMNS)
    + ", message_id = EXCLUDED.message_id, request_id = EXCLUDED.request_id"
    ", message_code = EXCLUDED.message_code, message_at = EXCLUDED.message_at"
    ", updated_at = CURRENT_TIMESTAMP RETURNING (xmax = 0) AS inserted"
)
SELECT_STORED_SQL = f"SELECT {CONFLICT_COLUMN}, {', '.join(RATE_COLUMNS)} FROM {TABLE} WHERE {CONFLICT_COLUMN} = ANY(%s)"


class ExchangeRateRunError(ReportRunError):
    """The run failed; the message is the reason (goes to the log and to fetch_log.error)."""


@dataclass
class RunResult:
    rows: int  # one per valid date
    inserted: int | None  # None in a dry run
    updated: int | None


# --- interval ---------------------------------------------------------------

def resolve_interval(date_from: date | None, date_to: date | None, today: date) -> tuple[date, date]:
    """(from, to) to request. Raises ValueError with the reason when the arguments are refused."""
    d_to = date_to or today
    if date_from is None:
        d_from = max(FIRST_DATE, min(today - timedelta(days=1), d_to))
    else:
        d_from = date_from
    if d_from < FIRST_DATE:
        raise ValueError(f"--from {d_from} is before {FIRST_DATE}: no data before {FIRST_DATE} is requested")
    if d_to < FIRST_DATE:
        raise ValueError(f"--to {d_to} is before {FIRST_DATE}: no data before {FIRST_DATE} is requested")
    if d_to < d_from:
        raise ValueError(f"--to {d_to} is before --from {d_from}")
    return d_from, d_to


# --- rows -------------------------------------------------------------------

def parse_rows(element) -> list[dict]:
    """Validate + parse the SFVOTEXCHRATE element; zero rows is a failure."""
    try:
        rows = parse_exchange_rate(element, validate=True)
    except ExchangeRateFormatError as exc:
        raise ExchangeRateRunError(f"SFVOTEXCHRATE format error: {exc}") from exc
    if not rows:
        raise ExchangeRateRunError("no rates in the message")
    return rows


def print_rows(rows: list[dict]) -> None:
    """A small table, one line per valid date."""
    cols = ("valid_date", "ote_final_rate", "ote_final_vdt_rate", "ote_final_ida_rate", "message_at")
    cells = [[("NULL" if r[c] is None else str(r[c])) for c in cols] for r in rows]
    widths = [max([len(c)] + [len(row[i]) for row in cells]) for i, c in enumerate(cols)]

    def fmt(values: list[str]) -> str:
        return "  ".join(
            v.rjust(widths[i]) if 1 <= i <= 3 else v.ljust(widths[i]) for i, v in enumerate(values)
        ).rstrip()

    print(fmt(list(cols)))
    print("  ".join("-" * w for w in widths))
    for row in cells:
        print(fmt(row))
    print(f"message_id={rows[0]['message_id']} request_id={rows[0]['request_id']} rows={len(rows)}")


# --- database ---------------------------------------------------------------

def warn_changed_rates(rows: list[dict], stored: dict[date, dict], logger) -> int:
    """WARNING for every rate stored with a different value than the one now received."""
    changed = 0
    for row in rows:
        old = stored.get(row["valid_date"])
        if old is None:
            continue
        for column in RATE_COLUMNS:
            if old[column] is not None and row[column] is not None and old[column] != row[column]:
                changed += 1
                logger.warning("OTE exchange rate changed: valid_date %s %s stored %s -> received %s",
                               row["valid_date"], column, old[column], row[column])
    return changed


def upsert_rows(rows: list[dict], logger) -> tuple[int, int]:
    """Read the stored rows of these dates (warn on changed rates), then one execute_values upsert.

    One connection, one transaction. Returns (inserted, updated).
    """
    values = [tuple(r[c] for c in COLUMNS) for r in rows]
    dates = [r["valid_date"] for r in rows]
    try:
        with closing(psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, port=DB_PORT, connect_timeout=10,
        )) as conn:
            with conn:  # one transaction: commit on success, rollback on error
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {DB_SCHEMA}")
                    cur.execute(SELECT_STORED_SQL, (dates,))
                    stored = {rec[0]: dict(zip(RATE_COLUMNS, rec[1:])) for rec in cur.fetchall()}
                    warn_changed_rates(rows, stored, logger)
                    result = extras.execute_values(cur, UPSERT_SQL, values, page_size=1000, fetch=True)
    except psycopg2.Error as exc:
        raise ExchangeRateRunError(f"database error: {type(exc).__name__}: {exc}") from exc
    inserted = sum(1 for (flag,) in result if flag)
    return inserted, len(result) - inserted


# --- run --------------------------------------------------------------------

def execute(args: argparse.Namespace, logger) -> RunResult:
    try:
        creds = load_credentials(logger)
    except SystemExit as exc:
        raise ExchangeRateRunError(f"credentials: {exc.code}") from exc
    client = OteSoapClient(creds)

    element = ask_and_wait(client, REPORT_CODE, args.date_from, args.date_to, is_exchange_rate_message,
                           "SFVOTEXCHRATE", args.timeout, args.poll, logger)
    rows = parse_rows(element)

    if args.dry_run:
        print_rows(rows)
        print("dry run: nothing written to the database")
        return RunResult(len(rows), None, None)
    inserted, updated = upsert_rows(rows, logger)
    return RunResult(len(rows), inserted, updated)


def run_logged(args: argparse.Namespace, logger) -> int:
    """One fetch_log row around the locked execute(); returns the exit code."""
    run = fetch_log.start(FETCH_SOURCE, FETCH_DATASET, dry_run=args.dry_run,
                          data_from=args.date_from, data_to=args.date_to)
    message_log.set_run(run)
    try:
        with queue_lock(wait_s=LOCK_WAIT_S):
            result = execute(args, logger)
    except QueueLockHeld:
        reason = f"queue lock held by another process for {LOCK_WAIT_S:g} s"
        logger.error("OTE exchange rate not run: %s", reason)
        fetch_log.finish(run, "failed", error=reason)
        return EXIT_LOCK_HELD
    except (ReportRunError, OteSoapError) as exc:
        reason = str(exc)
    except Exception as exc:  # unexpected: still leave a failed row
        logger.exception("Unexpected error")
        reason = f"{type(exc).__name__}: {exc}"
    else:
        what = f"{args.date_from}..{args.date_to}, {result.rows} {'row' if result.rows == 1 else 'rows'}"
        if args.dry_run:
            logger.info("OTE exchange rate OK (dry run, nothing written): %s", what)
        else:
            logger.info("OTE exchange rate OK: %s, %d inserted, %d updated", what, result.inserted, result.updated)
        fetch_log.finish(run, "ok", rows_inserted=result.inserted, rows_updated=result.updated)
        return EXIT_OK
    logger.error("OTE exchange rate FAILED: %s", reason)
    fetch_log.finish(run, "failed", error=reason)
    return EXIT_FAILED


def parse_args(argv: list[str] | None = None, today: date | None = None) -> argparse.Namespace:
    """Parse the CLI; args.date_from / args.date_to are the resolved interval."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="send the request and read the queue, skip the database")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat,
                        help=f"first valid date YYYY-MM-DD, default yesterday, never before {FIRST_DATE}")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat,
                        help="last valid date YYYY-MM-DD, default today")
    parser.add_argument("--timeout", type=float, default=60, help="seconds to wait for our answer (default 60)")
    parser.add_argument("--poll", type=float, default=2, help="seconds between queue pickups (default 2)")
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.poll <= 0:
        parser.error("--timeout and --poll must be positive")
    try:
        args.date_from, args.date_to = resolve_interval(
            args.date_from, args.date_to, today or datetime.now(PRAGUE).date())
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.debug)
    logger.info("OTE SOAP endpoint %s, signing %s, output %s%s", BASE_URL, SIGN_ALG, OUT_ROOT,
                " (dry run)" if args.dry_run else "")
    return run_logged(args, logger)


if __name__ == "__main__":
    sys.exit(main())
