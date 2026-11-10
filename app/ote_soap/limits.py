#!/usr/bin/env python3
"""
OTE SOAP - financial limit snapshot (SFVOTREQ 463 -> SFVOTLIMITS 464) into
finance.ote_limit_snapshot.

One run: request the snapshot, wait for the answer in the OTE queue, parse it,
bulk-upsert it, write one finance.fetch_log row (source "ote", dataset
"soap_limit_snapshot").

The answer arrives asynchronously in the queue that is emptied one message at a
time with COMMONREQ 921 (D1.4.4 section 8.1). OTE removes a message from the
queue once it hands it over, so EVERY picked-up message is saved to
$OTE_SOAP_OUT_DIR (same names as ote_soap.fetch --collect) even when it is not
ours. Our answer is the SFVOTLIMITS whose Reference@id is our request id. The
whole run holds the queue lock /tmp/ote_soap_queue.lock, shared with
ote_soap.fetch --collect.

Usage (inside the container, cwd /app/scripts):
    python3 -m ote_soap.limits [--dry-run] [--debug] [--timeout 60] [--poll 2]
        --dry-run   the request IS sent and the queue IS read (files are saved);
                    only the database INSERT is skipped and the rows are printed
        --timeout   seconds to wait for our answer after the request was accepted
        --poll      seconds between queue pickups while the queue is empty

Exit codes: 0 ok, 1 failed (rejected, timeout, no data, format error, database
error), 2 queue lock held by another process.
"""

import sentry_init  # noqa: F401  (must be first)

sentry_init.set_module("ote")

import argparse
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import psycopg2
from lxml import etree
from psycopg2 import extras

import fetch_log
from common import setup_logging
from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_SCHEMA, DB_USER
from ote_soap.client import BASE_URL, SIGN_ALG, OteResponse, OteSoapClient, OteSoapError
from ote_soap.limits_parser import COLUMNS, LimitsFormatError, is_limits_message, parse_limits
from ote_soap.messages import PRAGUE
from ote_soap.shared import (
    OUT_ROOT, QueueLockHeld, describe, load_credentials, pickup, queue_lock, send_report,
)

REPORT_CODE = "463"
FETCH_SOURCE = "ote"
FETCH_DATASET = "soap_limit_snapshot"
TABLE = "ote_limit_snapshot"

EXIT_OK, EXIT_FAILED, EXIT_LOCK_HELD = 0, 1, 2

CONFLICT_COLUMNS = ("message_id", "participant_ean", "commodity", "data_group", "value_type", "vat_period")
UPDATE_COLUMNS = (
    "value", "currency", "snapshot_at", "message_at", "request_id", "message_code", "report_title",
    "participant_name", "participant_id", "participant_dic", "participant_eic", "participant_ic",
)
UPSERT_SQL = (
    f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s "
    f"ON CONFLICT ({', '.join(CONFLICT_COLUMNS)}) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS)
    + ", updated_at = CURRENT_TIMESTAMP RETURNING (xmax = 0) AS inserted"
)

# Verdicts of classify()
OURS, FAILED, WAIT, UNRELATED = "ours", "failed", "wait", "unrelated"


class LimitsRunError(RuntimeError):
    """The run failed; the message is the reason (goes to the log and to fetch_log.error)."""


@dataclass
class RunResult:
    snapshot_at: Any
    rows: int
    inserted: int | None  # None in a dry run
    updated: int | None


# --- queue matching ---------------------------------------------------------

def reference_id(element: etree._Element) -> str | None:
    """Reference@id of an OTE message element (the id of the request it answers)."""
    for child in element:
        if isinstance(child.tag, str) and etree.QName(child).localname == "Reference":
            return child.get("id")
    return None


def classify(resp: OteResponse, request_id: str) -> tuple[str, etree._Element | None]:
    """Decide what a queue pickup is. Returns (verdict, SFVOTLIMITS element for OURS).

    OURS       an SFVOTLIMITS answering our request
    FAILED     a queue item answers our request but carries no SFVOTLIMITS (no data, error)
    WAIT       plain "queue empty" (RESPONSE 922, no payload)
    UNRELATED  any other message
    """
    for element in resp.payload:
        if is_limits_message(element) and reference_id(element) == request_id:
            return OURS, element
    if resp.reference_id == request_id or any(reference_id(p) == request_id for p in resp.payload):
        return FAILED, None
    if not resp.payload and resp.response_code == "922":
        return WAIT, None
    return UNRELATED, None


def wait_for_limits(
    client: OteSoapClient, request_id: str, timeout_s: float, poll_s: float, logger,
    sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
) -> etree._Element:
    """Poll the queue until our SFVOTLIMITS arrives. Every handed-over message is saved."""
    deadline = clock() + timeout_s
    last_empty: OteResponse | None = None
    n = 0
    while True:
        if n and clock() + poll_s > deadline:
            if last_empty is not None:
                logger.info("Last queue answer before the timeout: %s", describe(last_empty))
            raise LimitsRunError(f"timeout: no SFVOTLIMITS for request {request_id} within {timeout_s:g} s")
        sleep(poll_s)
        n += 1
        resp, path = pickup(client, n, logger, save_empty_queue=False)
        verdict, element = classify(resp, request_id)
        if verdict == OURS and element is not None:
            logger.info("Got our SFVOTLIMITS in pickup %d (%s)", n, path.name if path else "-")
            return element
        if verdict == FAILED:
            raise LimitsRunError(f"OTE answered request {request_id} without limits data: {describe(resp)}")
        if verdict == UNRELATED:
            logger.warning("picked up unrelated message (%s) saved to %s", describe(resp), path)
        else:
            last_empty = resp


# --- rows -------------------------------------------------------------------

def parse_rows(element: etree._Element) -> list[dict]:
    try:
        rows = parse_limits(element, validate=True)
    except LimitsFormatError as exc:
        raise LimitsRunError(f"SFVOTLIMITS format error: {exc}") from exc
    if not rows:
        raise LimitsRunError("SFVOTLIMITS parsed to zero rows")
    seen: set[tuple] = set()
    for row in rows:
        key = tuple(row[c] for c in CONFLICT_COLUMNS)
        if key in seen:
            raise LimitsRunError(f"duplicate key in SFVOTLIMITS rows (one INSERT cannot hold it twice): {key}")
        seen.add(key)
    return rows


def print_rows(rows: list[dict]) -> None:
    cols = ("participant_ean", "commodity", "data_group", "value_type", "vat_period", "value", "currency")
    table = [[str(r[c]) if r[c] is not None else "-" for c in cols] for r in rows]
    widths = [max(len(c), *(len(t[i]) for t in table)) for i, c in enumerate(cols)]
    print(f"snapshot_at={rows[0]['snapshot_at']}  message_id={rows[0]['message_id']}  "
          f"request_id={rows[0]['request_id']}  {len(rows)} rows")
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for t in table:
        print("  ".join(v.rjust(w) if i == 5 else v.ljust(w) for i, (v, w) in enumerate(zip(t, widths))))


# --- database ---------------------------------------------------------------

def upsert_rows(rows: list[dict]) -> tuple[int, int]:
    """One execute_values upsert; returns (inserted, updated)."""
    values = [tuple(r[c] for c in COLUMNS) for r in rows]
    try:
        with closing(psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, port=DB_PORT, connect_timeout=10,
        )) as conn:
            with conn:  # one transaction: commit on success, rollback on error
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {DB_SCHEMA}")
                    result = extras.execute_values(cur, UPSERT_SQL, values, page_size=1000, fetch=True)
    except psycopg2.Error as exc:
        raise LimitsRunError(f"database error: {type(exc).__name__}: {exc}") from exc
    inserted = sum(1 for (flag,) in result if flag)
    return inserted, len(result) - inserted


# --- run --------------------------------------------------------------------

def execute(args: argparse.Namespace, logger) -> RunResult:
    try:
        creds = load_credentials(logger)
    except SystemExit as exc:
        raise LimitsRunError(f"credentials: {exc.code}") from exc
    client = OteSoapClient(creds)

    today = datetime.now(PRAGUE).date()
    request_id, receipt = send_report(client, REPORT_CODE, today, today, logger)
    if receipt.return_code != "0":
        raise LimitsRunError(f"request {request_id} not accepted: {describe(receipt)}")

    element = wait_for_limits(client, request_id, args.timeout, args.poll, logger)
    rows = parse_rows(element)
    snapshot_at = rows[0]["snapshot_at"]

    if args.dry_run:
        print_rows(rows)
        print("dry run: nothing written to the database")
        return RunResult(snapshot_at, len(rows), None, None)
    inserted, updated = upsert_rows(rows)
    return RunResult(snapshot_at, len(rows), inserted, updated)


def run_logged(args: argparse.Namespace, logger) -> int:
    """One fetch_log row around execute(); returns the exit code."""
    now = datetime.now(PRAGUE)  # the snapshot is taken seconds after this
    run = fetch_log.start(FETCH_SOURCE, FETCH_DATASET, dry_run=args.dry_run, data_from=now, data_to=now)
    try:
        result = execute(args, logger)
    except (LimitsRunError, OteSoapError) as exc:
        reason = str(exc)
    except Exception as exc:  # unexpected: still leave a failed row
        logger.exception("Unexpected error")
        reason = f"{type(exc).__name__}: {exc}"
    else:
        if args.dry_run:
            logger.info("OTE limit snapshot OK (dry run, nothing written): snapshot %s, %d rows parsed",
                        result.snapshot_at, result.rows)
        else:
            logger.info("OTE limit snapshot OK: snapshot %s, %d rows, %d inserted, %d updated",
                        result.snapshot_at, result.rows, result.inserted, result.updated)
        fetch_log.finish(run, "ok", rows_inserted=result.inserted, rows_updated=result.updated)
        return EXIT_OK
    logger.error("OTE limit snapshot FAILED: %s", reason)
    fetch_log.finish(run, "failed", error=reason)
    return EXIT_FAILED


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="send the request and read the queue, skip the INSERT")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--timeout", type=float, default=60, help="seconds to wait for our answer (default 60)")
    parser.add_argument("--poll", type=float, default=2, help="seconds between queue pickups (default 2)")
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.poll <= 0:
        parser.error("--timeout and --poll must be positive")
    return args


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.debug)
    logger.info("OTE SOAP endpoint %s, signing %s, output %s%s", BASE_URL, SIGN_ALG, OUT_ROOT,
                " (dry run)" if args.dry_run else "")
    try:
        with queue_lock():
            return run_logged(args, logger)
    except QueueLockHeld as exc:
        logger.error("OTE limit snapshot not run: %s", exc)
        return EXIT_LOCK_HELD


if __name__ == "__main__":
    sys.exit(main())
