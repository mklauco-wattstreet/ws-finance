"""
Message log - one finance.ote_soap_message row per OTE SOAP message, sent or
received (migrations 087 and 089). This module is the only writer of that table.

    direction 'out'  a request we send: report request SFVOTREQ (reportService) or
                     queue pickup COMMONREQ 921 (commonService). Written BEFORE the
                     HTTP call, so a call that hangs or dies still leaves a row.
    direction 'in'   OTE's answer to that call: one row per payload element, one row
                     when there is no payload (also a plain "queue empty" 922).
                     reply_to_id = id of the 'out' row of the same call. A failed call
                     that still returned something (HTTP error, SOAP Fault, non-XML) is
                     one 'in' row with the error text; a call with no answer at all
                     marks its 'out' row 'error' instead.
    level            ok / warning / error. answer_level() sets it on an 'in' row;
                     report.py and fetch.py raise it later for a message nobody asked for.
    outcome          only on an 'out' report request a runner waited for: delivered,
                     no_data, timeout or failed (set by report.py through set_outcome()).
    fetch_log_id     the run given to set_run() by a runner; NULL for ote_soap.fetch.

The XML itself is not stored: file_name is the file in downloads/ote_soap/<YYYY-MM-DD>/.

Logging never breaks or slows a fetch: nothing here raises. The first failed connect or
statement is logged as ONE warning and switches the log off for the rest of the process,
so there are no repeated connect waits (connect timeout 3 s). One lazily opened
autocommit connection per process, reused; the table name is schema-qualified.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

import psycopg2
from lxml import etree
from psycopg2 import extras

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_SCHEMA, DB_USER
from ote_soap.client import OteResponse, reference_id

logger = logging.getLogger("ote_soap.message_log")

TABLE = f"{DB_SCHEMA}.ote_soap_message"
CONNECT_TIMEOUT_S = 3
ERROR_MAX_CHARS = 2000

REPORT_SERVICE, COMMON_SERVICE = "reportService", "commonService"
OK, WARNING, ERROR = "ok", "warning", "error"
DELIVERED, NO_DATA, TIMEOUT, FAILED = "delivered", "no_data", "timeout", "failed"  # outcome values

NO_DATA_REASON_CODE = "9001"  # Reason@code of "no data for the requested interval"
LIMIT_WARNING_CODE = "424"  # RESPONSE@message-code of the financial limit warning
REJECTED_REASON_TYPES = ("A01", "A02")  # Reason@type: syntax error, rejection by application
_LEVEL_RANK = {OK: 0, WARNING: 1, ERROR: 2}

COLUMNS = (
    "direction", "service", "message_type", "message_code", "message_id", "request_id",
    "response_code", "reason_code", "reason_text", "reason_type", "result_code",
    "return_code", "http_status", "file_name", "reply_to_id", "fetch_log_id", "level", "error",
)
# VARCHAR widths: a longer value is cut, so one odd value from OTE cannot switch the log off.
_WIDTHS = {
    "direction": 3, "service": 20, "message_type": 40, "message_code": 3, "message_id": 35,
    "request_id": 35, "response_code": 3, "reason_code": 10, "reason_type": 3,
    "result_code": 10, "return_code": 10, "level": 7,
}
INSERT_SQL = f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s RETURNING id"

_conn: Any = None
_enabled = True
_run_id: int | None = None


# --- switches ---------------------------------------------------------------

def set_run(fetch_log_id: int | None) -> None:
    """Link every row written from now on to this finance.fetch_log run (None = no run)."""
    global _run_id
    _run_id = fetch_log_id


def switch_off(reason: str) -> None:
    """Write nothing more in this process (e.g. ote_soap.fetch --no-db)."""
    global _enabled
    if _enabled:
        logger.info("ote_soap_message: message log switched off (%s)", reason)
    _enabled = False
    _close()


# --- rows (pure) ------------------------------------------------------------

def answer_rows(resp: OteResponse | None) -> list[dict]:
    """Column values of the 'in' row(s) of one answer, without direction / level / file and call fields.

    One row per payload element (message_type = local name, message_code = @message-code,
    message_id = @id, request_id = the element's Reference@id, else the RESPONSE's). Without a
    payload one row with message_type NULL and request_id = RESPONSE Reference@id. The response
    and reason fields (first Reason) are the same on every row. resp None (nothing parseable)
    gives one empty row.
    """
    if resp is None:
        return [{}]
    reason = resp.reasons[0] if resp.reasons else {}
    common = {
        "response_code": resp.response_code,
        "reason_code": reason.get("code") or None,
        "reason_text": reason.get("text") or None,
        "reason_type": reason.get("type") or None,
        "result_code": reason.get("result-code") or None,
        "return_code": resp.return_code or None,
    }
    if not resp.payload:
        return [{**common, "request_id": resp.reference_id}]
    return [
        {
            **common,
            "message_type": etree.QName(element).localname,
            "message_code": element.get("message-code"),
            "message_id": element.get("id"),
            "request_id": reference_id(element) or resp.reference_id,
        }
        for element in resp.payload
    ]


def answer_level(resp: OteResponse | None, http_status: int | None, error: str | None) -> str:
    """Level of an 'in' row; the first rule that matches wins.

    error    an error text; HTTP status >= 400; no parsed answer; RETURN_CODE missing or not 0
    warning  any Reason code 9001 (no data)
    error    any Reason type A01 or A02 (syntax error, rejected)
    warning  RESPONSE 424 (financial limit warning)
    ok       everything else (a plain "queue empty" 922 is ok)
    """
    if error or (http_status is not None and http_status >= 400) or resp is None or resp.return_code != "0":
        return ERROR
    if any(r.get("code") == NO_DATA_REASON_CODE for r in resp.reasons):
        return WARNING
    if any(r.get("type") in REJECTED_REASON_TYPES for r in resp.reasons):
        return ERROR
    if resp.response_code == LIMIT_WARNING_CODE:
        return WARNING
    return OK


def _clip_error(error: str | None) -> str | None:
    if error and len(error) > ERROR_MAX_CHARS:
        return error[:ERROR_MAX_CHARS] + "..."
    return error or None


def _values(row: dict) -> tuple:
    """One INSERT tuple in COLUMNS order; fetch_log_id is the current run."""
    row = {**row, "fetch_log_id": _run_id}
    out = []
    for column in COLUMNS:
        value = row.get(column)
        width = _WIDTHS.get(column)
        if width is not None and isinstance(value, str):
            value = value[:width]
        out.append(value)
    return tuple(out)


# --- writes -----------------------------------------------------------------

def log_request(service: str, message_type: str, message_code: str, message_id: str) -> int | None:
    """Write the 'out' row of a request about to be sent; returns its id (None when not written)."""
    ids = _insert([{
        "direction": "out", "service": service, "message_type": message_type,
        "message_code": message_code, "message_id": message_id, "level": OK,
    }])
    return ids[0] if ids else None


def complete_request(row_id: int | None, *, file_name: str | None = None,
                     level: str | None = None, error: str | None = None) -> None:
    """Fill in an 'out' row after the call: request file, level, error text (None = leave as is)."""
    error = _clip_error(error)
    if row_id is None or (file_name is None and level is None and error is None):
        return
    _write(
        f"UPDATE {TABLE} SET file_name = COALESCE(%s, file_name), level = COALESCE(%s, level), "
        "error = COALESCE(%s, error) WHERE id = %s",
        (file_name, level, error, row_id),
    )


def log_answer(reply_to_id: int | None, service: str, resp: OteResponse | None, *,
               http_status: int | None, file_name: str | None, error: str | None = None) -> list[int]:
    """Write the 'in' row(s) of one answer; returns their ids ([] when not written).

    resp None + error: a call that raised after OTE returned something (HTTP error, SOAP Fault,
    non-XML); one row with the HTTP status and the error text.
    """
    error = _clip_error(error)
    common = {
        "direction": "in", "service": service, "reply_to_id": reply_to_id, "http_status": http_status,
        "file_name": file_name, "level": answer_level(resp, http_status, error), "error": error,
    }
    return _insert([{**row, **common} for row in answer_rows(resp)])


def set_outcome(message_id: str, outcome: str, level: str | None = None) -> None:
    """Outcome (and level, None = unchanged) of the 'out' report request with this message id."""
    _write(
        f"UPDATE {TABLE} SET outcome = %s, level = COALESCE(%s, level) "
        "WHERE direction = 'out' AND message_id = %s",
        (outcome, level, message_id),
    )


def raise_level(row_ids: Sequence[int], level: str) -> None:
    """Set `level` on these rows where their level is lower (an 'error' row never becomes 'warning')."""
    lower = [name for name, rank in _LEVEL_RANK.items() if rank < _LEVEL_RANK[level]]
    if not row_ids or not lower:
        return
    _write(
        f"UPDATE {TABLE} SET level = %s WHERE id = ANY(%s) AND level = ANY(%s)",
        (level, list(row_ids), lower),
    )


# --- database ---------------------------------------------------------------

def _insert(rows: list[dict]) -> list[int]:
    result = _write(INSERT_SQL, [_values(row) for row in rows], many=True)
    return [record[0] for record in result] if result else []


def _write(sql: str, params: Sequence, many: bool = False) -> list[tuple] | None:
    """Run one statement on the shared connection; never raises.

    many=True: execute_values with RETURNING (params = list of row tuples). Returns the
    returned rows ([] for a plain statement), or None when the log is off or the write failed.
    """
    global _conn
    if not _enabled:
        return None
    try:
        if _conn is None:
            _conn = psycopg2.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
                database=DB_NAME, port=DB_PORT, connect_timeout=CONNECT_TIMEOUT_S,
            )
            _conn.autocommit = True
        with _conn.cursor() as cur:
            if many:
                return extras.execute_values(cur, sql, params, page_size=1000, fetch=True)
            cur.execute(sql, params)
            return []
    except Exception as exc:  # never fatal: the files on disk are the record of last resort
        _give_up(exc)
        return None


def _give_up(exc: Exception) -> None:
    """One warning, then the log stays off for the rest of the process."""
    global _enabled
    _enabled = False
    logger.warning("ote_soap_message: database write failed, message log switched off for this run: %s: %s",
                   type(exc).__name__, str(exc).strip())
    _close()


def _close() -> None:
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
    _conn = None
