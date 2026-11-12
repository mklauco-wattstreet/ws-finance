"""
"Ask and wait" for the OTE SOAP report runners (limits, exchange_rate, billing).

The FZ scenario is asynchronous (D1.4.4 section 8.1): ReportService only
acknowledges a request; the answer is queued and handed over one message at a
time by COMMONREQ 921. OTE removes a message from the queue once it hands it
over, so EVERY picked-up message is saved to disk even when it is not ours.
Every request and every answer is also one finance.ote_soap_message row
(shared.py writes them through message_log.py); here the report request gets its
outcome and an unrelated message its level.

    ask_and_wait()       request -> receipt check -> poll the queue until the
                         message answering our request arrives
    classify()           what one queue pickup is: ours, failed, wait, unrelated
    ReportRunError       the run failed; the text goes to the log and fetch_log.error
      ReportTimeout      ... because nothing arrived within the timeout
      ReportNoData       ... because OTE answered our request with reason code 9001 and no data
    LOCK_WAIT_S          how long a runner waits for the queue lock

Outcome of the 'out' report request row: delivered (level unchanged), no_data
(level warning), timeout (error), failed (error: receipt not accepted, an answer
without our data, or any other error while asking or waiting). An unrelated
message's 'in' rows get level warning (an 'error' stays).

A runner passes a predicate "is this payload element the kind of message I wait
for" (e.g. is_limits_message); the Reference@id == request id match is done here.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Callable

from lxml import etree

from ote_soap import message_log
from ote_soap.client import OteResponse, OteSoapClient, reference_id
from ote_soap.message_log import NO_DATA_REASON_CODE
from ote_soap.messages import new_message_id
from ote_soap.shared import describe, pickup, send_report

LOCK_WAIT_S = 60  # a runner waits this long for the queue lock before it gives up

QUEUE_EMPTY_CODE = "922"  # RESPONSE@message-code of "the queue holds nothing"

# Verdicts of classify()
OURS, FAILED, WAIT, UNRELATED = "ours", "failed", "wait", "unrelated"


class ReportRunError(RuntimeError):
    """The run failed; the message is the reason (goes to the log and to fetch_log.error)."""


class ReportTimeout(ReportRunError):
    """No answer to our request arrived within the timeout."""


class ReportNoData(ReportRunError):
    """OTE answered our request without the wanted element and with reason code 9001 (no data)."""


# --- queue matching ---------------------------------------------------------

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

    Every handed-over message is saved; the outcome of request_id and the level of an
    unrelated message are set in finance.ote_soap_message.
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
            message_log.set_outcome(request_id, message_log.TIMEOUT, message_log.ERROR)
            raise ReportTimeout(f"timeout: no {label} for request {request_id} within {timeout_s:g} s")
        sleep(poll_s)
        n += 1
        try:
            resp, path, row_ids = pickup(client, n, logger, save_empty_queue=False)
        except BaseException:
            message_log.set_outcome(request_id, message_log.FAILED, message_log.ERROR)
            raise
        verdict, element = classify(resp, request_id, is_wanted)
        if verdict == OURS and element is not None:
            message_log.set_outcome(request_id, message_log.DELIVERED)
            logger.info("Got our %s in pickup %d (%s)", label, n, path.name if path else "-")
            return element
        if verdict == FAILED:
            error = f"OTE answered request {request_id} without {label} data: {describe(resp)}"
            if any(r.get("code") == NO_DATA_REASON_CODE for r in resp.reasons):
                message_log.set_outcome(request_id, message_log.NO_DATA, message_log.WARNING)
                raise ReportNoData(error)
            message_log.set_outcome(request_id, message_log.FAILED, message_log.ERROR)
            raise ReportRunError(error)
        if verdict == UNRELATED:
            logger.warning("picked up unrelated message (%s) saved to %s", describe(resp), path)
            message_log.raise_level(row_ids, message_log.WARNING)
        else:
            last_empty = resp


def ask_and_wait(
    client: OteSoapClient, code: str, d_from: date, d_to: date,
    is_wanted: Callable[[etree._Element], bool], label: str,
    timeout_s: float, poll_s: float, logger,
    sleep: Callable[[float], None] | None = None, clock: Callable[[], float] | None = None,
) -> etree._Element:
    """Send report request `code` for d_from..d_to, check the receipt, wait for the answer element."""
    request_id = new_message_id()
    try:
        _, receipt = send_report(client, code, d_from, d_to, logger, msg_id=request_id)
    except BaseException:
        message_log.set_outcome(request_id, message_log.FAILED, message_log.ERROR)
        raise
    if receipt.return_code != "0":
        message_log.set_outcome(request_id, message_log.FAILED, message_log.ERROR)
        raise ReportRunError(f"request {request_id} not accepted: {describe(receipt)}")
    return wait_for_report(client, request_id, is_wanted, label, timeout_s, poll_s, logger, sleep, clock)
