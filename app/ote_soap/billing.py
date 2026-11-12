#!/usr/bin/env python3
"""
OTE SOAP - monthly billing data (SFVOTREQ 400 -> SFVOTBILLING 401, SFVOTREQ 403 ->
SFVOTBILLING 404, "Podklady pro fakturaci") into finance.ote_monthly_settlement_data
(one row per <Data> line) and finance.ote_monthly_settlement_summary (one row per
<Summary>, the period totals per trade type).

    400 -> 401  Billing@ote = 0   Watt Street invoices OTE   (invoice_from_ote = 0)
    403 -> 404  Billing@ote = 1   OTE invoices Watt Street   (invoice_from_ote = 1)

The line table is also filled by the web upload of the same XML files (trader-web
ImportOteSettlementXml), which stores only the first thirteen columns. A line this run
finds in the table (same trade_date, variable_symbol, trade_type, version,
invoice_from_ote; NULL variable_symbol equals NULL) is UPDATED, not skipped: the seven
"old" value columns are set to OTE's values (every difference is logged as a WARNING
first), the columns the upload does not know (currency, original_amount_czk/_eur,
original_etax_czk/_eur, payment_czk, value_date, message_id, request_id, source) are
filled, and the keys Reference / Billing / Rut are added to the stored header JSON (the
stored root / SenderIdentification / ReceiverIdentification stay as they are).
created_at and id never change. A line that is not in the table is inserted with
source 'soap'. A stored line OTE does not send is left alone and reported as a WARNING.

Month: default the previous calendar month (Europe/Prague). Nothing before FIRST_MONTH
is requested, and a month that has not ended yet is refused.

Already loaded: unless --force, the run first looks in finance.fetch_log for a finished,
non-dry-run, status 'ok' row (source "ote", dataset "soap_billing") of that month. If
there is one it logs a line and exits 0 WITHOUT contacting OTE, taking the lock or
writing a fetch_log row. A failure of this check is only a warning.

No data yet: before OTE has settled a month it sends nothing for an accepted request.
A timeout, or an answer with reason code 9001, therefore means "not yet":
    neither report delivered   fetch_log "skipped", exit 0 (the next scheduled run asks again)
    only one delivered         that one is loaded, fetch_log "failed", exit 1
    both delivered             loaded, fetch_log "ok", exit 0
Any other problem (request not accepted, XSD / format error, a message for another
period, a 401 with ote != 0 or 404 with ote != 1, a value that does not fit its column
without rounding, a duplicate key, a database error) fails the run before anything of it
is written; the writing of both reports is ONE transaction.

The answers arrive asynchronously in the OTE queue (ote_soap/report.py); the run holds the
queue lock /tmp/ote_soap_queue.lock (shared with the other OTE SOAP runners) and waits up
to 60 s for it. The fetch_log row (source "ote", dataset "soap_billing", data_from /
data_to = first / last day of the month) is written before the lock is taken. Every
request and every answer is one finance.ote_soap_message row linked to that fetch_log row.

Usage (inside the container, cwd /app/scripts):
    python3 -m ote_soap.billing [--month YYYY-MM] [--dry-run] [--debug] [--force]
                                [--timeout 40] [--poll 2]
        --month     month to load (default the previous month, never before FIRST_MONTH)
        --dry-run   the requests ARE sent and the queue IS read (files are saved, the messages
                    are logged); the line table is only read (to compare), nothing is written
                    to either table
        --force     load even though fetch_log says the month is already loaded
        --timeout   seconds to wait for EACH answer after its request was accepted (default 40:
                    OTE answers within seconds, and the cron run at 18:00 holds the queue lock
                    for at most ~85 s when nothing is settled yet)
        --poll      seconds between queue pickups while the queue is empty

Exit codes: 0 ok / skipped / already loaded, 1 failed, 2 queue lock still held by another
process after 60 s (fetch_log row "failed" is written) or a refused argument.
"""

import sentry_init  # noqa: F401  (must be first)

sentry_init.set_module("ote")

import argparse
import calendar
import sys
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import psycopg2
from lxml import etree
from psycopg2 import extras

import fetch_log
from common import setup_logging
from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_SCHEMA, DB_USER
from ote_soap.billing_parser import (
    DATA_NUMBERS, SUMMARY_NUMBERS, BillingFormatError, is_billing_message, parse_billing,
)
from ote_soap import message_log
from ote_soap.client import BASE_URL, SIGN_ALG, OteSoapClient, OteSoapError
from ote_soap.messages import PRAGUE
from ote_soap.report import (
    LOCK_WAIT_S, ReportNoData, ReportRunError, ReportTimeout, ask_and_wait,
)
from ote_soap.shared import OUT_ROOT, QueueLockHeld, load_credentials, queue_lock

FETCH_SOURCE = "ote"
FETCH_DATASET = "soap_billing"
TABLE = "ote_monthly_settlement_data"
SUMMARY_TABLE = "ote_monthly_settlement_summary"
SOURCE_VALUE = "soap"  # the `source` column value of rows written here
FIRST_MONTH = date(2026, 9, 1)  # owner's rule: no month before this one is requested or stored

EXIT_OK, EXIT_FAILED, EXIT_LOCK_HELD = 0, 1, 2
LIST_CAP = 20  # keys / differences printed or logged individually


@dataclass(frozen=True)
class ReportSpec:
    request_code: str  # SFVOTREQ message-code
    data_code: str  # SFVOTBILLING message-code that answers it
    invoice_from_ote: int  # expected Billing@ote
    meaning: str


REPORTS: tuple[ReportSpec, ...] = (
    ReportSpec("400", "401", 0, "Watt Street invoices OTE"),
    ReportSpec("403", "404", 1, "OTE invoices Watt Street"),
)

# --- columns ----------------------------------------------------------------

KEY_COLUMNS = ("trade_date", "variable_symbol", "trade_type", "version", "invoice_from_ote")
# The seven value columns the web upload fills ("old"), in the parser's key names.
OLD_VALUE_COLUMNS = (
    "volume_mwh", "exchange_rate_czk_eur", "original_amount_net_czk", "original_amount_net_eur",
    "original_vat_czk", "original_vat_eur", "payment_eur",
)
NEW_NUMERIC_COLUMNS = (
    "original_amount_czk", "original_amount_eur", "original_etax_czk", "original_etax_eur", "payment_czk",
)
# Columns filled only by this runner (everything an UPDATE adds besides header keys).
NEW_COLUMNS = ("currency", *NEW_NUMERIC_COLUMNS, "value_date", "message_id", "request_id", "source")

# (precision, scale) of the typed numeric columns of the line table.
NUMERIC_LIMITS: dict[str, tuple[int, int]] = {
    "volume_mwh": (12, 4),
    "exchange_rate_czk_eur": (10, 6),
    **{c: (15, 3) for c in OLD_VALUE_COLUMNS[2:] + NEW_NUMERIC_COLUMNS},
}

READ_OLD_COLUMNS = ("id", *KEY_COLUMNS, *OLD_VALUE_COLUMNS)
READ_ALL_COLUMNS = (
    "id", "header", *KEY_COLUMNS, *OLD_VALUE_COLUMNS, *NEW_COLUMNS, "created_at", "updated_at",
)

INSERT_COLUMNS = (
    "header", *KEY_COLUMNS, *OLD_VALUE_COLUMNS, *NEW_COLUMNS, "created_at", "updated_at",
)
# UPDATE ... FROM (VALUES ...): the value columns copied from the VALUES row, then header keys + time.
UPDATE_VALUE_COLUMNS = (*OLD_VALUE_COLUMNS, *NEW_COLUMNS)
UPDATE_VALUES_COLUMNS = ("id", *UPDATE_VALUE_COLUMNS, "header_extra", "updated_at")
HEADER_EXTRA_KEYS = ("Reference", "Billing", "Rut")  # the only header keys an UPDATE touches

_TEXT_COLUMNS = {"currency", "message_id", "request_id", "source"}


def _cast(column: str) -> str:
    """SQL type of a VALUES-row column, so a NULL parameter has the right type."""
    if column == "id":
        return "bigint"
    if column == "header_extra":
        return "jsonb"
    if column == "updated_at":
        return "timestamp"
    if column == "value_date":
        return "date"
    if column in _TEXT_COLUMNS:
        return "text"
    return "numeric"


INSERT_SQL = f"INSERT INTO {TABLE} ({', '.join(INSERT_COLUMNS)}) VALUES %s"
UPDATE_TEMPLATE = "(" + ", ".join(f"%s::{_cast(c)}" for c in UPDATE_VALUES_COLUMNS) + ")"
UPDATE_SQL = (
    f"UPDATE {TABLE} AS t SET "
    + ", ".join(f"{c} = v.{c}" for c in UPDATE_VALUE_COLUMNS)
    + ", header = t.header || v.header_extra, updated_at = v.updated_at "
    f"FROM (VALUES %s) AS v({', '.join(UPDATE_VALUES_COLUMNS)}) "
    "WHERE t.id = v.id RETURNING t.id"
)

SUMMARY_VALUE_KEYS = tuple(SUMMARY_NUMBERS.values())  # volume_mwh + the ten amounts, parser key names
SUMMARY_CONFLICT = ("date_from", "date_to", "invoice_from_ote", "trade_type", "version")
SUMMARY_COLUMNS = (
    *SUMMARY_CONFLICT, "message_code", "currency", *SUMMARY_VALUE_KEYS, "message_id", "request_id", "message_at",
)
SUMMARY_SQL = (
    f"INSERT INTO {SUMMARY_TABLE} ({', '.join(SUMMARY_COLUMNS)}) VALUES %s "
    f"ON CONFLICT ({', '.join(SUMMARY_CONFLICT)}) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in SUMMARY_COLUMNS if c not in SUMMARY_CONFLICT)
    + ", updated_at = CURRENT_TIMESTAMP"
)

Key = tuple  # (trade_date, variable_symbol, trade_type, version, invoice_from_ote)


class BillingRunError(ReportRunError):
    """The run failed; the message is the reason (goes to the log and to fetch_log.error)."""


# --- month ------------------------------------------------------------------

def month_bounds(first: date) -> tuple[date, date]:
    """(first day, last day) of the month that starts on `first`."""
    return first, first.replace(day=calendar.monthrange(first.year, first.month)[1])


def parse_month(text: str) -> date:
    """'YYYY-MM' -> first day of that month (argparse type)."""
    try:
        parsed = datetime.strptime(text.strip(), "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{text!r} is not a month, expected YYYY-MM") from exc
    return date(parsed.year, parsed.month, 1)


def previous_month(today: date) -> date:
    """First day of the calendar month before today's."""
    return (today.replace(day=1) - timedelta(days=1)).replace(day=1)


def resolve_month(month: date | None, today: date) -> tuple[date, date]:
    """(first, last) day of the month to load. Raises ValueError with the reason when it is refused."""
    first = month or previous_month(today)
    first, last = month_bounds(first)
    label = first.strftime("%Y-%m")
    if first < FIRST_MONTH:
        raise ValueError(f"month {label} is before {FIRST_MONTH:%Y-%m}: no billing data before "
                         f"{FIRST_MONTH:%Y-%m} is requested")
    if today <= last:
        raise ValueError(f"month {label} has not ended yet (it ends on {last}, today is {today})")
    return first, last


# --- database ---------------------------------------------------------------

def _connect():
    return closing(psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT, connect_timeout=10,
    ))


def find_loaded(first: date) -> datetime | None:
    """finished_at of the newest real (not dry-run) fetch_log row with status 'ok' for the month, or None.

    fetch_log stores data_from of a date as Prague midnight (timestamptz), so that exact instant is compared.
    """
    data_from = datetime.combine(first, time(), tzinfo=PRAGUE)
    with closing(psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT, connect_timeout=5,
    )) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA}")
            cur.execute(
                "SELECT finished_at FROM fetch_log WHERE source = %s AND dataset = %s AND dry_run = false "
                "AND status = 'ok' AND data_from = %s ORDER BY finished_at DESC NULLS LAST LIMIT 1",
                (FETCH_SOURCE, FETCH_DATASET, data_from),
            )
            row = cur.fetchone()
    return row[0] if row else None


def read_existing(cur, lo: date, hi: date, allow_fallback: bool) -> tuple[list[dict], bool]:
    """All stored lines with trade_date in lo..hi. Returns (rows as dicts, True if every column was read).

    allow_fallback: when the full SELECT fails (e.g. the new columns do not exist yet), read only
    READ_OLD_COLUMNS (through a savepoint, so the transaction stays usable) and return full=False.
    """
    def select(columns: tuple[str, ...]) -> list[dict]:
        cur.execute(f"SELECT {', '.join(columns)} FROM {TABLE} WHERE trade_date BETWEEN %s AND %s", (lo, hi))
        return [dict(zip(columns, record)) for record in cur.fetchall()]

    if not allow_fallback:
        return select(READ_ALL_COLUMNS), True
    cur.execute("SAVEPOINT read_existing")
    try:
        rows, full = select(READ_ALL_COLUMNS), True
    except psycopg2.Error:
        cur.execute("ROLLBACK TO SAVEPOINT read_existing")
        rows, full = select(READ_OLD_COLUMNS), False
    cur.execute("RELEASE SAVEPOINT read_existing")
    return rows, full


def insert_lines(cur, plan: "ReportPlan", now: datetime) -> None:
    """One bulk INSERT of the plan's new lines."""
    if not plan.inserts:
        return
    header = extras.Json(plan.message["header"])
    values = [
        (header, *(line[c] for c in (*KEY_COLUMNS, *OLD_VALUE_COLUMNS)), line["currency"],
         *(line[c] for c in NEW_NUMERIC_COLUMNS), line["value_date"],
         plan.message["message_id"], plan.message["request_id"], SOURCE_VALUE, now, now)
        for line in plan.inserts
    ]
    extras.execute_values(cur, INSERT_SQL, values, page_size=1000)


def update_lines(cur, plan: "ReportPlan", now: datetime) -> None:
    """One bulk UPDATE ... FROM (VALUES ...) of the plan's matched lines; every row must be hit."""
    if not plan.updates:
        return
    header_extra = extras.Json({k: v for k, v in plan.message["header"].items() if k in HEADER_EXTRA_KEYS})
    values = [
        (row_id, *(line[c] for c in OLD_VALUE_COLUMNS), line["currency"],
         *(line[c] for c in NEW_NUMERIC_COLUMNS), line["value_date"],
         plan.message["message_id"], plan.message["request_id"], SOURCE_VALUE, header_extra, now)
        for row_id, line in plan.updates
    ]
    hit = extras.execute_values(cur, UPDATE_SQL, values, template=UPDATE_TEMPLATE, page_size=1000, fetch=True)
    if len(hit) != len(values):
        raise BillingRunError(f"UPDATE of {TABLE} hit {len(hit)} rows, expected {len(values)}; nothing written")


def upsert_summaries(cur, plan: "ReportPlan") -> None:
    """One bulk upsert of the plan's summaries."""
    if not plan.summaries:
        return
    msg = plan.message
    values = [
        (msg["date_from"], msg["date_to"], msg["invoice_from_ote"], s["trade_type"], s["version"],
         msg["message_code"], s["currency"], *(s[k] for k in SUMMARY_VALUE_KEYS),
         msg["message_id"], msg["request_id"], msg["message_at"])
        for s in plan.summaries
    ]
    # SUMMARY_COLUMNS order: conflict columns first, then message_code, currency, values, ids, message_at
    extras.execute_values(cur, SUMMARY_SQL, values, page_size=1000)


# --- checking and planning (pure) --------------------------------------------

def line_key(line: dict) -> Key:
    return tuple(line[c] for c in KEY_COLUMNS)


def key_text(key: Key) -> str:
    trade_date, variable_symbol, trade_type, version, invoice_from_ote = key
    return (f"{trade_date} {trade_type} v{version} ote={invoice_from_ote} "
            f"VS {'NULL' if variable_symbol is None else variable_symbol}")


def _show(value: Any) -> str:
    return "NULL" if value is None else str(value)


def fits_numeric(value: Decimal, precision: int, scale: int) -> bool:
    """True if value fits NUMERIC(precision, scale) without rounding (trailing zeros do not count)."""
    if value == 0:
        return True
    _, digits, exponent = value.normalize().as_tuple()
    assert isinstance(exponent, int)
    return max(0, -exponent) <= scale and max(0, len(digits) + exponent) <= precision - scale


def check_parsed(spec: ReportSpec, parsed: dict, first: date, last: date) -> None:
    """Everything that must hold before a message is used. Raises BillingRunError.

    The right report (ote flag), exactly the requested period, at least one line, no duplicate
    line / summary key, and every Decimal fits its typed column without rounding.
    """
    msg, lines, summaries = parsed["message"], parsed["lines"], parsed["summaries"]
    tag = f"SFVOTBILLING {spec.data_code}"
    if msg["message_code"] != spec.data_code:
        raise BillingRunError(f"{tag}: message-code is {msg['message_code']}")
    if msg["invoice_from_ote"] != spec.invoice_from_ote:
        raise BillingRunError(f"{tag}: Billing@ote is {msg['invoice_from_ote']}, expected {spec.invoice_from_ote}")
    if (msg["date_from"], msg["date_to"]) != (first, last):
        raise BillingRunError(f"{tag}: Billing period {msg['date_from']}..{msg['date_to']} is not the "
                              f"requested month {first}..{last}")
    if not lines:
        raise BillingRunError(f"{tag}: no <Data> lines in the message")
    seen: set[Key] = set()
    for line in lines:
        key = line_key(line)
        if key in seen:
            raise BillingRunError(f"{tag}: the same line twice (one row cannot hold it twice): {key_text(key)}")
        seen.add(key)
        for column, (precision, scale) in NUMERIC_LIMITS.items():
            value = line[column]
            if value is not None and not fits_numeric(value, precision, scale):
                raise BillingRunError(
                    f"{tag}: line {key_text(key)}: {column} = {value} does not fit NUMERIC({precision},{scale}) "
                    "without rounding; nothing written")
    seen_summary: set[tuple] = set()
    for summary in summaries:
        skey = (summary["trade_type"], summary["version"])
        if skey in seen_summary:
            raise BillingRunError(f"{tag}: the same <Summary> twice: {skey}")
        seen_summary.add(skey)


@dataclass
class ReportPlan:
    spec: ReportSpec
    message: dict
    lines: list[dict]
    summaries: list[dict]
    existing: int | None = None  # message lines already in the table; None = table not readable
    inserts: list[dict] = field(default_factory=list)
    updates: list[tuple[int, dict]] = field(default_factory=list)  # (row id, line)
    changes: list[tuple[Key, str, Any, Any]] = field(default_factory=list)  # (key, column, stored, OTE)
    not_in_message: list[Key] = field(default_factory=list)  # stored lines of the period OTE did not send
    unreadable: str | None = None  # why the table could not be read (dry run only)


def plan_report(spec: ReportSpec, parsed: dict, rows: list[dict] | None) -> ReportPlan:
    """Match the message lines against the stored rows (a list of dicts with READ_OLD_COLUMNS keys).

    rows None = the table could not be read: no insert / update is planned. A stored row with the
    same key twice is an error. Only stored rows of this report (same invoice_from_ote) are compared;
    stored rows of the message period that OTE did not send are listed in not_in_message.
    """
    msg = parsed["message"]
    plan = ReportPlan(spec, msg, parsed["lines"], parsed["summaries"])
    if rows is None:
        return plan
    stored: dict[Key, dict] = {}
    for row in rows:
        if row["invoice_from_ote"] != spec.invoice_from_ote:
            continue
        key = line_key(row)
        if key in stored:
            raise BillingRunError(f"{TABLE} holds the same key twice: {key_text(key)}")
        stored[key] = row
    sent: set[Key] = set()
    for line in plan.lines:
        key = line_key(line)
        sent.add(key)
        row = stored.get(key)
        if row is None:
            plan.inserts.append(line)
            continue
        plan.updates.append((row["id"], line))
        for column in OLD_VALUE_COLUMNS:
            old, new = row[column], line[column]
            if old != new:  # Decimal comparison is numeric; None only equals None
                plan.changes.append((key, column, old, new))
    plan.existing = len(plan.updates)
    plan.not_in_message = sorted(
        (k for k, r in stored.items() if k not in sent and msg["date_from"] <= k[0] <= msg["date_to"]),
        key=lambda k: (k[0], k[2], k[3], str(k[1])),
    )
    return plan


def log_warnings(plan: ReportPlan, logger) -> None:
    """WARNING for every changed stored value and for stored lines OTE did not send."""
    for key, column, old, new in plan.changes:
        logger.warning("billing value changed: %s %s stored %s -> OTE %s", key_text(key), column, _show(old), _show(new))
    if plan.not_in_message:
        shown = "; ".join(key_text(k) for k in plan.not_in_message[:LIST_CAP])
        more = f" (and {len(plan.not_in_message) - LIST_CAP} more)" if len(plan.not_in_message) > LIST_CAP else ""
        logger.warning("%s: %d stored line(s) of the period are not in OTE's message and were left alone: %s%s",
                       plan.spec.data_code, len(plan.not_in_message), shown, more)


def plan_all(delivered: list[tuple[ReportSpec, dict]], rows: list[dict] | None, logger) -> list[ReportPlan]:
    plans = [plan_report(spec, parsed, rows) for spec, parsed in delivered]
    for plan in plans:
        log_warnings(plan, logger)
    return plans


def date_range(delivered: list[tuple[ReportSpec, dict]], first: date, last: date) -> tuple[date, date]:
    """Stored rows to read: the month, widened to every trade_date OTE sent (so no key can collide unseen)."""
    dates = [line["trade_date"] for _, parsed in delivered for line in parsed["lines"]]
    return min([first, *dates]), max([last, *dates])


# --- database runs --------------------------------------------------------------

def store(delivered: list[tuple[ReportSpec, dict]], first: date, last: date, logger) -> list[ReportPlan]:
    """Read, plan and write both reports in ONE transaction. Returns the plans (all written)."""
    lo, hi = date_range(delivered, first, last)
    # Eloquent (config/app.php timezone Europe/Prague) writes created_at / updated_at as naive
    # Prague wall-clock timestamps; the same is stored here so both writers look alike.
    now = datetime.now(PRAGUE).replace(tzinfo=None)
    try:
        with _connect() as conn:
            with conn:  # one transaction: commit on success, rollback on error
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {DB_SCHEMA}")
                    rows, _ = read_existing(cur, lo, hi, allow_fallback=False)
                    plans = plan_all(delivered, rows, logger)
                    for plan in plans:
                        insert_lines(cur, plan, now)
                        update_lines(cur, plan, now)
                        upsert_summaries(cur, plan)
    except psycopg2.Error as exc:
        raise BillingRunError(f"database error: {type(exc).__name__}: {exc}") from exc
    return plans


def compare(delivered: list[tuple[ReportSpec, dict]], first: date, last: date, logger) -> tuple[list[ReportPlan], bool]:
    """Dry run: read the stored lines in a read-only transaction and plan; writes nothing.

    Returns (plans, full). full is False when only the old columns could be read. When the table cannot
    be read at all the plans carry `unreadable` and no insert / update.
    """
    lo, hi = date_range(delivered, first, last)
    full = True
    try:
        with _connect() as conn:
            conn.set_session(readonly=True)
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {DB_SCHEMA}")
                    rows, full = read_existing(cur, lo, hi, allow_fallback=True)
    except psycopg2.Error as exc:
        reason = f"{type(exc).__name__}: {exc}".strip()
        logger.warning("billing: %s not readable for the comparison: %s", TABLE, reason)
        plans = plan_all(delivered, None, logger)
        for plan in plans:
            plan.unreadable = reason
        return plans, False
    return plan_all(delivered, rows, logger), full


# --- output -----------------------------------------------------------------

def print_plan(plan: ReportPlan) -> None:
    """The dry-run block of one report."""
    msg = plan.message
    rows = [
        ("message id", msg["message_id"]),
        ("period", f"{msg['date_from']} .. {msg['date_to']}  (invoice_from_ote={msg['invoice_from_ote']}, "
                   f"{plan.spec.meaning})"),
        ("rows in the message", str(len(plan.lines))),
    ]
    if plan.existing is None:
        rows.append(("rows in the table", f"table not readable ({plan.unreadable})"))
    else:
        rows += [
            ("rows already in the table", str(plan.existing)),
            ("rows that would be inserted", str(len(plan.inserts))),
            ("rows that would be updated", str(len(plan.updates))),
            ("differing stored values", str(len(plan.changes))),
        ]
    rows.append(("summaries", str(len(plan.summaries))))
    print(f"--- {plan.spec.data_code} (request {plan.spec.request_code})")
    width = max(len(name) for name, _ in rows)
    for name, value in rows:
        print(f"{name.ljust(width)} : {value}")
    for key, column, old, new in plan.changes[:LIST_CAP]:
        print(f"  differs: {key_text(key)} {column} stored {_show(old)} -> OTE {_show(new)}")
    if len(plan.changes) > LIST_CAP:
        print(f"  ... and {len(plan.changes) - LIST_CAP} more")
    if plan.not_in_message:
        print(f"  stored lines of the period not in OTE's message: {len(plan.not_in_message)}")
        for key in plan.not_in_message[:LIST_CAP]:
            print(f"    {key_text(key)}")


# --- run --------------------------------------------------------------------

@dataclass
class Outcome:
    status: str  # ok | skipped | failed
    text: str  # final log line body / fetch_log.error for skipped and failed
    inserted: int | None = None
    updated: int | None = None


def parse_message(element: etree._Element, spec: ReportSpec, first: date, last: date) -> dict:
    """Validate + parse one SFVOTBILLING element and check it is the report asked for."""
    try:
        parsed = parse_billing(element, validate=True)
    except BillingFormatError as exc:
        raise BillingRunError(f"SFVOTBILLING {spec.data_code} format error: {exc}") from exc
    check_parsed(spec, parsed, first, last)
    return parsed


def describe_plans(plans: list[ReportPlan], dry_run: bool) -> str:
    parts = []
    for plan in plans:
        n = len(plan.lines)
        code = plan.spec.data_code
        if not dry_run:
            parts.append(f"{code}: {n} rows ({len(plan.inserts)} inserted, {len(plan.updates)} updated)")
        elif plan.existing is None:
            parts.append(f"{code}: {n} rows (table not readable)")
        else:
            parts.append(f"{code}: {n} rows ({plan.existing} in the table, {len(plan.inserts)} to insert, "
                         f"{len(plan.updates)} to update)")
    return ", ".join(parts) + f", {sum(len(p.summaries) for p in plans)} summaries"


def fetch_reports(client: OteSoapClient, args: argparse.Namespace, logger) -> tuple[list[tuple[ReportSpec, dict]], list[tuple[ReportSpec, str]]]:
    """Ask for both reports. Returns (delivered with parsed data, missing with reason).

    Only a timeout / "no data" answer counts as missing; any other error propagates.
    """
    delivered: list[tuple[ReportSpec, dict]] = []
    missing: list[tuple[ReportSpec, str]] = []
    for spec in REPORTS:
        def wanted(element: etree._Element, code: str = spec.data_code) -> bool:
            return is_billing_message(element) and element.get("message-code") == code

        try:
            element = ask_and_wait(client, spec.request_code, args.first, args.last, wanted,
                                   f"SFVOTBILLING {spec.data_code}", args.timeout, args.poll, logger)
        except (ReportTimeout, ReportNoData) as exc:
            logger.warning("SFVOTBILLING %s (request %s) not delivered: %s", spec.data_code, spec.request_code, exc)
            missing.append((spec, str(exc)))
            continue
        delivered.append((spec, parse_message(element, spec, args.first, args.last)))
    return delivered, missing


def execute(args: argparse.Namespace, logger) -> Outcome:
    label = args.first.strftime("%Y-%m")
    try:
        creds = load_credentials(logger)
    except SystemExit as exc:
        raise BillingRunError(f"credentials: {exc.code}") from exc
    client = OteSoapClient(creds)

    delivered, missing = fetch_reports(client, args, logger)
    if not delivered:
        return Outcome("skipped", f"no billing data from OTE for {label} yet")

    if args.dry_run:
        plans, full = compare(delivered, args.first, args.last, logger)
        for plan in plans:
            print_plan(plan)
        if not full and plans and plans[0].unreadable is None:
            print("note: only the old columns of the table could be read (new columns missing?)")
        print("dry run: nothing written to the database")
        inserted = updated = None
    else:
        plans = store(delivered, args.first, args.last, logger)
        inserted = sum(len(p.inserts) for p in plans)
        updated = sum(len(p.updates) for p in plans)
    summary = describe_plans(plans, args.dry_run)

    if missing:
        absent = "; ".join(f"{s.data_code} (request {s.request_code}) missing: {why}" for s, why in missing)
        return Outcome("failed", f"{absent}; loaded {summary}" if not args.dry_run else f"{absent}; compared {summary}",
                       inserted, updated)
    return Outcome("ok", summary, inserted, updated)


def run_logged(args: argparse.Namespace, logger) -> int:
    """Already-loaded check, then one fetch_log row around the locked execute(); returns the exit code."""
    label = args.first.strftime("%Y-%m")
    if not args.force:
        try:
            loaded_at = find_loaded(args.first)
        except Exception as exc:  # the check must never stop the run
            logger.warning("billing: could not check fetch_log for an earlier load of %s: %s: %s",
                           label, type(exc).__name__, exc)
            loaded_at = None
        if loaded_at is not None:
            when = loaded_at.astimezone(PRAGUE).strftime("%Y-%m-%d %H:%M") if loaded_at.tzinfo else str(loaded_at)
            logger.info("billing data for %s already loaded on %s, nothing to do (use --force to reload)", label, when)
            return EXIT_OK

    run = fetch_log.start(FETCH_SOURCE, FETCH_DATASET, dry_run=args.dry_run, data_from=args.first, data_to=args.last)
    message_log.set_run(run)
    try:
        with queue_lock(wait_s=LOCK_WAIT_S):
            outcome = execute(args, logger)
    except QueueLockHeld:
        reason = f"queue lock held by another process for {LOCK_WAIT_S:g} s"
        logger.error("OTE billing not run: %s", reason)
        fetch_log.finish(run, "failed", error=reason)
        return EXIT_LOCK_HELD
    except (ReportRunError, OteSoapError) as exc:
        reason = str(exc)
    except Exception as exc:  # unexpected: still leave a failed row
        logger.exception("Unexpected error")
        reason = f"{type(exc).__name__}: {exc}"
    else:
        if outcome.status == "ok":
            tail = " (dry run, nothing written)" if args.dry_run else ""
            logger.info("OTE billing OK%s: %s, %s", tail, label, outcome.text)
            fetch_log.finish(run, "ok", rows_inserted=outcome.inserted, rows_updated=outcome.updated)
            return EXIT_OK
        if outcome.status == "skipped":
            logger.info("OTE billing SKIPPED: %s", outcome.text)
            fetch_log.finish(run, "skipped", error=outcome.text)
            return EXIT_OK
        logger.error("OTE billing FAILED: %s", outcome.text)
        fetch_log.finish(run, "failed", error=outcome.text, rows_inserted=outcome.inserted, rows_updated=outcome.updated)
        return EXIT_FAILED
    logger.error("OTE billing FAILED: %s", reason)
    fetch_log.finish(run, "failed", error=reason)
    return EXIT_FAILED


def parse_args(argv: list[str] | None = None, today: date | None = None) -> argparse.Namespace:
    """Parse the CLI; args.first / args.last are the first / last day of the month to load."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", type=parse_month,
                        help=f"month to load YYYY-MM, default the previous month, never before {FIRST_MONTH:%Y-%m}")
    parser.add_argument("--dry-run", action="store_true",
                        help="send the requests and read the queue; only read the table, write nothing")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="load even though fetch_log says the month is already loaded")
    # Default 40 s: OTE generated both messages ~3 s after the requests (observed 2026-11-10).
    # Cron runs at 18:00 and the live limit snapshot at 18:02 waits only 60 s (LOCK_WAIT_S) for the
    # queue lock, so before OTE has settled the month (nothing is ever sent) the two waits together
    # (~80-85 s) must end before 18:02. A longer timeout would make the 18:02 / 18:07 limit runs fail.
    parser.add_argument("--timeout", type=float, default=40, help="seconds to wait for each answer (default 40)")
    parser.add_argument("--poll", type=float, default=2, help="seconds between queue pickups (default 2)")
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.poll <= 0:
        parser.error("--timeout and --poll must be positive")
    try:
        args.first, args.last = resolve_month(args.month, today or datetime.now(PRAGUE).date())
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
