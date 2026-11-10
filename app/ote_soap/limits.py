#!/usr/bin/env python3
"""
OTE SOAP - financial limit snapshot (SFVOTREQ 463 -> SFVOTLIMITS 464) into
finance.ote_limit_snapshot.

One run: request the snapshot, wait for the answer in the OTE queue, parse it,
pivot it into ONE row per snapshot (per participant and commodity, one column per
number OTE sends), upsert it with one execute_values statement, write one
finance.fetch_log row (source "ote", dataset "soap_limit_snapshot"). Scheduled
every 5 minutes in the crontab.

The parser (limits_parser.py) returns one long row per <Data> value; pivot_rows()
maps each (data_group, value_type) to its column through VALUE_COLUMNS. Everything
without a column of its own (bonityCoef, bonityUtilization, other utilization
market codes, every VatData row) and every value whose currency is not exactly
CZK goes into the other_values JSON array, so nothing OTE sends is dropped.

The answer arrives asynchronously in the queue that is emptied one message at a
time with COMMONREQ 921 (D1.4.4 section 8.1). OTE removes a message from the
queue once it hands it over, so EVERY picked-up message is saved to
$OTE_SOAP_OUT_DIR (same names as ote_soap.fetch --collect) even when it is not
ours; an unrelated one is also registered in finance.ote_soap_message (a database
problem there is only a warning). Our answer is the SFVOTLIMITS whose Reference@id
is our request id. The request/queue/match logic is shared with ote_soap.exchange_rate
(ote_soap/report.py). The whole run holds the queue lock /tmp/ote_soap_queue.lock,
shared with ote_soap.fetch --collect and ote_soap.exchange_rate; it waits up to
60 s for it. The fetch_log row is written before the lock is taken.

Usage (inside the container, cwd /app/scripts):
    python3 -m ote_soap.limits [--dry-run] [--debug] [--timeout 60] [--poll 2]
        --dry-run   the request IS sent and the queue IS read (files are saved);
                    only the database INSERT is skipped and the wide row is printed
        --timeout   seconds to wait for our answer after the request was accepted
        --poll      seconds between queue pickups while the queue is empty

Exit codes: 0 ok, 1 failed (rejected, timeout, no data, format error, database
error), 2 queue lock still held by another process after 60 s (fetch_log row
"failed" is written).
"""

import sentry_init  # noqa: F401  (must be first)

sentry_init.set_module("ote")

import argparse
import json
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg2
from lxml import etree
from psycopg2 import extras

import fetch_log
from common import setup_logging
from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_SCHEMA, DB_USER
from ote_soap.client import BASE_URL, SIGN_ALG, OteSoapClient, OteSoapError
from ote_soap.limits_parser import LimitsFormatError, is_limits_message, parse_limits
from ote_soap.messages import PRAGUE
from ote_soap.report import LOCK_WAIT_S, ReportRunError, ask_and_wait
from ote_soap.shared import OUT_ROOT, QueueLockHeld, load_credentials, queue_lock

REPORT_CODE = "463"
FETCH_SOURCE = "ote"
FETCH_DATASET = "soap_limit_snapshot"
TABLE = "ote_limit_snapshot"

EXIT_OK, EXIT_FAILED, EXIT_LOCK_HELD = 0, 1, 2

# Columns taken from the header of a snapshot (identical on every long row of a group).
HEADER_COLUMNS = (
    "snapshot_at", "message_at", "message_id", "request_id", "message_code", "report_title",
    "participant_ean", "participant_name", "participant_id",
    "participant_dic", "participant_eic", "participant_ic",
    "commodity",
)
# (data_group, value_type) as sent by OTE -> column. THE ONE mapping; order = table order.
VALUE_COLUMNS: dict[tuple[str, str], str] = {
    ("totals", "limitIM"): "im_limit_czk",
    ("totals", "utilizationIM"): "im_utilization_czk",
    ("totals", "freeResourcesIM"): "im_free_resources_czk",
    ("totals", "limit"): "total_limit_czk",
    ("totals", "utilization"): "total_utilization_czk",
    ("totals", "freeResources"): "free_resources_czk",
    ("limits", "fsLimit"): "fs_limit_czk",
    ("limits", "dynamicLimit"): "dynamic_limit_czk",
    ("utilization", "VDT"): "utilization_vdt_czk",
    ("utilization", "IDA"): "utilization_ida_czk",
    ("utilization", "DT"): "utilization_dt_czk",
    ("utilization", "DEV"): "utilization_dev_czk",
    ("utilization", "TDD"): "utilization_tdd_czk",
    ("utilization", "POP"): "utilization_pop_czk",
    ("utilization", "ACT"): "utilization_act_czk",
    ("utilization", "RE"): "utilization_re_czk",
    ("utilization", "REVF"): "utilization_revf_czk",
    ("utilization", "LIM_VDT"): "utilization_lim_vdt_czk",
    ("additional", "warningLimit"): "warning_limit_czk",
}
VALUE_CZK = "CZK"  # a *_czk column only ever holds a value OTE sent with this currency
VALUE_COLS = tuple(VALUE_COLUMNS.values())
WIDE_COLUMNS = HEADER_COLUMNS + VALUE_COLS + ("other_values",)  # = table columns without id, created_at, updated_at

CONFLICT_COLUMNS = ("message_id", "participant_ean", "commodity")
UPDATE_COLUMNS = tuple(c for c in WIDE_COLUMNS if c not in CONFLICT_COLUMNS)
UPSERT_SQL = (
    f"INSERT INTO {TABLE} ({', '.join(WIDE_COLUMNS)}) VALUES %s "
    f"ON CONFLICT ({', '.join(CONFLICT_COLUMNS)}) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS)
    + ", updated_at = CURRENT_TIMESTAMP RETURNING (xmax = 0) AS inserted"
)


class LimitsRunError(ReportRunError):
    """The run failed; the message is the reason (goes to the log and to fetch_log.error)."""


@dataclass
class RunResult:
    snapshot_at: Any
    rows: int  # snapshot rows (normally 1)
    values: int  # <Data> values OTE sent, in columns and in other_values together
    other: int  # of those, the number in other_values
    inserted: int | None  # None in a dry run
    updated: int | None


# --- rows -------------------------------------------------------------------

def _json_default(obj: Any) -> str:
    if isinstance(obj, Decimal):
        return str(obj)  # exactly as OTE sent it (scale kept)
    raise TypeError(f"not JSON serializable: {type(obj).__name__}")


def other_values_json(items: list[dict]) -> str:
    """other_values as JSON text; Decimal values become strings."""
    return json.dumps(items, default=_json_default, ensure_ascii=False)


def pivot_rows(long_rows: list[dict]) -> list[dict]:
    """Long parser rows (one per <Data>) -> wide rows (one per message_id, participant_ean, commodity).

    Keys of a wide row: WIDE_COLUMNS. A value goes to its column when (data_group, value_type) is
    in VALUE_COLUMNS and its currency is exactly "CZK"; anything else is appended to other_values
    as {data_group, value_type, vat_period, value (Decimal), currency}. The same data_group +
    value_type + vat_period twice in one group is an error.
    """
    groups: dict[tuple, dict] = {}
    seen: set[tuple] = set()
    for row in long_rows:
        key = tuple(row[c] for c in CONFLICT_COLUMNS)
        wide = groups.get(key)
        if wide is None:
            wide = {c: row[c] for c in HEADER_COLUMNS}
            wide.update({c: None for c in VALUE_COLS})
            wide["other_values"] = []
            groups[key] = wide
        item_key = key + (row["data_group"], row["value_type"], row["vat_period"])
        if item_key in seen:
            raise LimitsRunError(f"duplicate value in SFVOTLIMITS (one row cannot hold it twice): {item_key}")
        seen.add(item_key)
        column = VALUE_COLUMNS.get((row["data_group"], row["value_type"]))
        if column is not None and row["currency"] == VALUE_CZK:
            wide[column] = row["value"]
        else:
            wide["other_values"].append({
                "data_group": row["data_group"], "value_type": row["value_type"],
                "vat_period": row["vat_period"], "value": row["value"], "currency": row["currency"],
            })
    return list(groups.values())


def parse_rows(element: etree._Element) -> list[dict]:
    """Validate + parse the SFVOTLIMITS element and pivot it into wide rows."""
    try:
        long_rows = parse_limits(element, validate=True)
    except LimitsFormatError as exc:
        raise LimitsRunError(f"SFVOTLIMITS format error: {exc}") from exc
    if not long_rows:
        raise LimitsRunError("SFVOTLIMITS parsed to zero rows")
    return pivot_rows(long_rows)


def count_values(rows: list[dict]) -> tuple[int, int]:
    """(all <Data> values, of those in other_values) over the wide rows."""
    other = sum(len(r["other_values"]) for r in rows)
    return other + sum(1 for r in rows for c in VALUE_COLS if r[c] is not None), other


def print_rows(rows: list[dict]) -> None:
    """Each wide row vertically, `column : value`, in table order, other_values last."""
    width = max(len(c) for c in WIDE_COLUMNS)
    for i, row in enumerate(rows, 1):
        print(f"--- snapshot row {i} of {len(rows)}")
        for c in WIDE_COLUMNS:
            if c == "other_values":
                text = other_values_json(row[c])
            else:
                text = "NULL" if row[c] is None else str(row[c])
            print(f"{c.ljust(width)} : {text}")


# --- database ---------------------------------------------------------------

def upsert_rows(rows: list[dict]) -> tuple[int, int]:
    """One execute_values upsert; returns (inserted, updated)."""
    values = [
        tuple(extras.Json(r[c], dumps=other_values_json) if c == "other_values" else r[c] for c in WIDE_COLUMNS)
        for r in rows
    ]
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
    element = ask_and_wait(client, REPORT_CODE, today, today, is_limits_message, "SFVOTLIMITS",
                           args.timeout, args.poll, logger)
    rows = parse_rows(element)
    snapshot_at = rows[0]["snapshot_at"]
    values, other = count_values(rows)

    if args.dry_run:
        print_rows(rows)
        print("dry run: nothing written to the database")
        return RunResult(snapshot_at, len(rows), values, other, None, None)
    inserted, updated = upsert_rows(rows)
    return RunResult(snapshot_at, len(rows), values, other, inserted, updated)


def run_logged(args: argparse.Namespace, logger) -> int:
    """One fetch_log row around the locked execute(); returns the exit code."""
    now = datetime.now(PRAGUE)  # the snapshot is taken seconds after this
    run = fetch_log.start(FETCH_SOURCE, FETCH_DATASET, dry_run=args.dry_run, data_from=now, data_to=now)
    try:
        with queue_lock(wait_s=LOCK_WAIT_S):
            result = execute(args, logger)
    except QueueLockHeld:
        reason = f"queue lock held by another process for {LOCK_WAIT_S:g} s"
        logger.error("OTE limit snapshot not run: %s", reason)
        fetch_log.finish(run, "failed", error=reason)
        return EXIT_LOCK_HELD
    except (ReportRunError, OteSoapError) as exc:
        reason = str(exc)
    except Exception as exc:  # unexpected: still leave a failed row
        logger.exception("Unexpected error")
        reason = f"{type(exc).__name__}: {exc}"
    else:
        what = (f"snapshot {result.snapshot_at}, {result.rows} {'row' if result.rows == 1 else 'rows'} "
                f"({result.values} values, {result.other} in other_values)")
        if args.dry_run:
            logger.info("OTE limit snapshot OK (dry run, nothing written): %s", what)
        else:
            logger.info("OTE limit snapshot OK: %s, %d inserted, %d updated", what, result.inserted, result.updated)
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
    return run_logged(args, logger)


if __name__ == "__main__":
    sys.exit(main())
