"""
"Ask and wait" for the OTE SOAP report runners (limits, exchange_rate), and the
register of queue messages that no table keeps.

The FZ scenario is asynchronous (D1.4.4 section 8.1): ReportService only
acknowledges a request; the answer is queued and handed over one message at a
time by COMMONREQ 921. OTE removes a message from the queue once it hands it
over, so EVERY picked-up message is saved to disk even when it is not ours.

    ask_and_wait()       request -> receipt check -> poll the queue until the
                         message answering our request arrives
    classify()           what one queue pickup is: ours, failed, wait, unrelated
    register_messages()  one finance.ote_soap_message row per payload element of
                         a pickup that has no table of its own (file saved first;
                         a database error is only a warning)
    ReportRunError       the run failed; the text goes to the log and fetch_log.error
    LOCK_WAIT_S          how long a runner waits for the queue lock

A runner passes a predicate "is this payload element the kind of message I wait
for" (e.g. is_limits_message); the Reference@id == request id match is done here.
"""

from __future__ import annotations

import time
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Callable

import psycopg2
from lxml import etree
from psycopg2 import extras

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_SCHEMA, DB_USER
from ote_soap.client import OteResponse, OteSoapClient
from ote_soap.shared import describe, pickup, send_report

LOCK_WAIT_S = 60  # a runner waits this long for the queue lock before it gives up

REGISTER_TABLE = "ote_soap_message"
REGISTER_COLUMNS = (
    "message_type", "message_code", "message_id", "request_id",
    "response_code", "reason_code", "reason_text", "file_name",
)
REGISTER_SQL = f"INSERT INTO {REGISTER_TABLE} ({', '.join(REGISTER_COLUMNS)}) VALUES %s"
QUEUE_EMPTY_CODE = "922"  # RESPONSE@message-code of "the queue holds nothing"

# Verdicts of classify()
OURS, FAILED, WAIT, UNRELATED = "ours", "failed", "wait", "unrelated"


class ReportRunError(RuntimeError):
    """The run failed; the message is the reason (goes to the log and to fetch_log.error)."""


# --- queue matching ---------------------------------------------------------

def reference_id(element: etree._Element) -> str | None:
    """Reference@id of an OTE message element (the id of the request it answers)."""
    for child in element:
        if isinstance(child.tag, str) and etree.QName(child).localname == "Reference":
            return child.get("id")
    return None


def is_queue_empty(resp: OteResponse) -> bool:
    """Plain "queue empty" answer: RESPONSE 922 and no payload."""
    return not resp.payload and resp.response_code == QUEUE_EMPTY_CODE


def classify(
    resp: OteResponse, request_id: str, is_wanted: Callable[[etree._Element], bool],
) -> tuple[str, etree._Element | None]:
    """Decide what a queue pickup is. Returns (verdict, wanted element for OURS).

    OURS       a payload element for which is_wanted() is true, answering our request
    FAILED     a queue item answers our request but carries no such element (no data, error)
    WAIT       plain "queue empty" (RESPONSE 922, no payload)
    UNRELATED  any other message
    """
    for element in resp.payload:
        if is_wanted(element) and reference_id(element) == request_id:
            return OURS, element
    if resp.reference_id == request_id or any(reference_id(p) == request_id for p in resp.payload):
        return FAILED, None
    if is_queue_empty(resp):
        return WAIT, None
    return UNRELATED, None


def wait_for_report(
    client: OteSoapClient, request_id: str, is_wanted: Callable[[etree._Element], bool], label: str,
    timeout_s: float, poll_s: float, logger,
    sleep: Callable[[float], None] | None = None, clock: Callable[[], float] | None = None,
) -> etree._Element:
    """Poll the queue until the message answering request_id arrives; label names it in logs.

    Every handed-over message is saved; an unrelated one is also registered in
    finance.ote_soap_message.
    """
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    deadline = clock() + timeout_s
    last_empty: OteResponse | None = None
    n = 0
    while True:
        if n and clock() + poll_s > deadline:
            if last_empty is not None:
                logger.info("Last queue answer before the timeout: %s", describe(last_empty))
            raise ReportRunError(f"timeout: no {label} for request {request_id} within {timeout_s:g} s")
        sleep(poll_s)
        n += 1
        resp, path = pickup(client, n, logger, save_empty_queue=False)
        verdict, element = classify(resp, request_id, is_wanted)
        if verdict == OURS and element is not None:
            logger.info("Got our %s in pickup %d (%s)", label, n, path.name if path else "-")
            return element
        if verdict == FAILED:
            raise ReportRunError(f"OTE answered request {request_id} without {label} data: {describe(resp)}")
        if verdict == UNRELATED:
            logger.warning("picked up unrelated message (%s) saved to %s", describe(resp), path)
            register_messages(resp, path, logger)
        else:
            last_empty = resp


def ask_and_wait(
    client: OteSoapClient, code: str, d_from: date, d_to: date,
    is_wanted: Callable[[etree._Element], bool], label: str,
    timeout_s: float, poll_s: float, logger,
    sleep: Callable[[float], None] | None = None, clock: Callable[[], float] | None = None,
) -> etree._Element:
    """Send report request `code` for d_from..d_to, check the receipt, wait for the answer element."""
    request_id, receipt = send_report(client, code, d_from, d_to, logger)
    if receipt.return_code != "0":
        raise ReportRunError(f"request {request_id} not accepted: {describe(receipt)}")
    return wait_for_report(client, request_id, is_wanted, label, timeout_s, poll_s, logger, sleep, clock)


# --- register of messages without a table ------------------------------------

def register_rows(resp: OteResponse, path: Path) -> list[tuple]:
    """Rows (order REGISTER_COLUMNS) for one pickup: one per payload element, else one with message_type None."""
    reason = resp.reasons[0] if resp.reasons else {}
    reason_code = reason.get("code") or None
    reason_text = reason.get("text") or None
    common = (resp.response_code, reason_code, reason_text, path.name)
    if not resp.payload:
        return [(None, None, None, resp.reference_id) + common]
    return [
        (
            etree.QName(element).localname, element.get("message-code"), element.get("id"),
            reference_id(element) or resp.reference_id,
        ) + common
        for element in resp.payload
    ]


def register_messages(resp: OteResponse, path: Path | None, logger) -> int:
    """Insert the pickup into finance.ote_soap_message; returns the number of rows written.

    Call it AFTER the raw file is saved (path is its location). Never raises: a
    database problem (e.g. the table does not exist yet) is logged as a WARNING and
    0 is returned. Own short autocommit connection, like fetch_log.
    """
    if path is None:
        logger.warning("ote_soap_message: pickup was not saved to a file, not registered")
        return 0
    rows = register_rows(resp, path)
    try:
        with closing(psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, port=DB_PORT, connect_timeout=3,
        )) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {DB_SCHEMA}")
                extras.execute_values(cur, REGISTER_SQL, rows, page_size=1000)
    except Exception as exc:  # never fatal: the file on disk is the record of last resort
        logger.warning("ote_soap_message: could not register %s: %s: %s", path.name, type(exc).__name__, exc)
        return 0
    logger.info("Registered %d message(s) of %s in %s", len(rows), path.name, REGISTER_TABLE)
    return len(rows)
