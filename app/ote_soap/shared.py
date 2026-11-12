"""
Helpers shared by the OTE SOAP command-line entry points (fetch, limits, exchange_rate).

    - raw-XML files on disk (every request, receipt, queue message, rejected answer)
    - one-line description of an OTE answer
    - sending a report request / picking up one queue message, with the raw
      answer saved even when OTE rejects the call, and every request and answer
      written to finance.ote_soap_message (message_log.py; never raises)
    - the queue lock: only one process at a time may empty the OTE queue,
      because OTE removes a message from the queue as soon as it hands it over
      (a runner waits for it, ote_soap.fetch --collect does not)
"""

from __future__ import annotations

import fcntl
import os
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from lxml import etree

from config import OTE_CERT_PASSWORD, OTE_CERT_PATH
from ote_soap import message_log
from ote_soap.client import OteResponse, OteSoapClient, OteSoapError
from ote_soap.message_log import COMMON_SERVICE, REPORT_SERVICE
from ote_soap.messages import PRAGUE, SFVOTREQ_CODES, new_message_id, queue_pickup_request, report_request
from ote_soap.wssec import Credentials

OUT_ROOT = Path(os.getenv("OTE_SOAP_OUT_DIR", "/app/downloads/ote_soap"))
QUEUE_LOCK_PATH = "/tmp/ote_soap_queue.lock"


class QueueLockHeld(RuntimeError):
    """Another process is emptying the OTE queue right now."""


@contextmanager
def queue_lock(path: str = QUEUE_LOCK_PATH, wait_s: float = 0) -> Iterator[None]:
    """Exclusive lock for the whole time the queue is read.

    wait_s = 0 (default): fail at once when another process holds it.
    wait_s > 0: retry the non-blocking flock about once per second until wait_s
    has passed. Raises QueueLockHeld when the lock could not be taken. The lock
    is released when the block ends or the process dies (flock is per open file).
    """
    with open(path, "a") as handle:
        deadline = time.monotonic() + wait_s
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    waited = f" for {wait_s:g} s" if wait_s > 0 else ""
                    raise QueueLockHeld(f"OTE queue lock {path} is held by another process{waited}") from exc
                time.sleep(min(1.0, remaining))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def out_dir() -> Path:
    path = OUT_ROOT / datetime.now(PRAGUE).strftime("%Y-%m-%d")
    path.mkdir(parents=True, exist_ok=True)
    return path


def save(name: str, data: bytes) -> Path:
    path = out_dir() / f"{datetime.now(PRAGUE).strftime('%H%M%S')}_{name}.xml"
    path.write_bytes(data)
    return path


def describe(resp: OteResponse) -> str:
    reasons = "; ".join(
        f"{r.get('code', '?')} {r.get('text', '')}".strip() for r in resp.reasons
    ) or "-"
    payload = ", ".join(
        f"{etree.QName(p).localname}[{p.get('message-code', '?')}]" for p in resp.payload
    ) or "none"
    return (
        f"RETURN_CODE={resp.return_code} RESPONSE={resp.response_code or '-'} "
        f"reasons=[{reasons}] reference={resp.reference_id or '-'} payload={payload}"
    )


def load_credentials(logger) -> Credentials:
    if not OTE_CERT_PATH:
        raise SystemExit("OTE_CERT_PATH is not set")
    creds = Credentials.from_pkcs12(OTE_CERT_PATH, OTE_CERT_PASSWORD)
    logger.info("Certificate: %s (valid until %s), password [CONFIGURED]", creds.subject, creds.not_valid_after.date())
    if creds.not_valid_after < datetime.now(creds.not_valid_after.tzinfo):
        raise SystemExit(f"Certificate expired on {creds.not_valid_after.date()}")
    return creds


def save_rejected(client: OteSoapClient, name: str, logger) -> Path | None:
    """Keep whatever OTE returned for a call that raised, and log its HTTP status. Returns the file or None."""
    if client.last_response:
        path = save(name, client.last_response)
        logger.error("OTE answered HTTP %s, raw answer (%d bytes) saved to %s",
                     client.last_status, len(client.last_response), path)
        return path
    logger.error("No HTTP answer from OTE to keep (status %s)", client.last_status)
    return None


def _file_name(path: Path | None) -> str | None:
    return path.name if path is not None else None


def _log_failure(out_id: int | None, service: str, client: OteSoapClient, exc: BaseException,
                 rejected: Path | None, request_file: Path | None = None) -> None:
    """Message log of a call that raised (any exception).

    OTE returned something (status or body): an 'in' row with the HTTP status, the error text,
    level 'error' and the rejected file. Nothing came back: the 'out' row becomes 'error' with
    the error text. request_file (if any) goes on the 'out' row either way.
    """
    error = str(exc) if isinstance(exc, OteSoapError) else f"{type(exc).__name__}: {exc}"
    if client.last_response or client.last_status is not None:
        message_log.complete_request(out_id, file_name=_file_name(request_file))
        message_log.log_answer(out_id, service, None, http_status=client.last_status,
                               file_name=_file_name(rejected), error=error)
    else:
        message_log.complete_request(out_id, file_name=_file_name(request_file),
                                     level=message_log.ERROR, error=error)


def send_report(client: OteSoapClient, code: str, d_from: date, d_to: date, logger,
                msg_id: str | None = None) -> tuple[str, OteResponse]:
    """Send one SFVOTREQ to ReportService; save request and receipt.

    Returns (request id, receipt). msg_id: the request id to use (default a new one). A call
    OTE rejects saves the raw answer as rejected_<code>_<id> and re-raises OteSoapError.
    The request ('out' row, written before the call) and the receipt or the failure ('in'
    row) go to finance.ote_soap_message.
    """
    msg_id = msg_id or new_message_id()
    payload = report_request(code, d_from, d_to, msg_id)
    logger.info("Requesting report %s (%s) for %s..%s, request id %s",
                code, SFVOTREQ_CODES[code], d_from, d_to, msg_id)
    out_id = message_log.log_request(REPORT_SERVICE, "SFVOTREQ", code, msg_id)
    request_file: Path | None = None
    rejected: Path | None = None
    try:
        try:
            resp = client.send(REPORT_SERVICE, payload)
        except OteSoapError:
            rejected = save_rejected(client, f"rejected_{code}_{msg_id}", logger)
            raise
        finally:
            if client.last_request:
                request_file = save(f"request_{code}_{msg_id}", client.last_request)
    except BaseException as exc:
        _log_failure(out_id, REPORT_SERVICE, client, exc, rejected, request_file)
        raise
    path = save(f"response_{code}_{msg_id}", resp.raw)
    logger.info("ReportService answered: %s", describe(resp))
    logger.info("Response saved to %s", path)
    message_log.complete_request(out_id, file_name=_file_name(request_file))
    message_log.log_answer(out_id, REPORT_SERVICE, resp, http_status=client.last_status, file_name=path.name)
    return msg_id, resp


def pickup(client: OteSoapClient, n: int, logger,
           save_empty_queue: bool = True) -> tuple[OteResponse, Path | None, list[int]]:
    """One COMMONREQ 921 on CommonService; the raw answer is saved as collect_<n>_<payload>.

    Returns (answer, saved file or None, ids of its finance.ote_soap_message 'in' rows; [] when
    the log is off). save_empty_queue=False skips the file for a plain "queue empty" (922)
    answer (it still gets a row, with file_name NULL).
    A rejected call saves the raw answer as collect_<n>_error and re-raises.
    """
    msg_id = new_message_id()
    out_id = message_log.log_request(COMMON_SERVICE, "COMMONREQ", "921", msg_id)
    rejected: Path | None = None
    try:
        try:
            resp = client.send(COMMON_SERVICE, queue_pickup_request(msg_id))
        except OteSoapError:
            rejected = save_rejected(client, f"collect_{n:03d}_error", logger)
            raise
    except BaseException as exc:
        _log_failure(out_id, COMMON_SERVICE, client, exc, rejected)
        raise
    names = "_".join(f"{etree.QName(p).localname}-{p.get('message-code', 'x')}" for p in resp.payload) or "empty"
    if not save_empty_queue and not resp.payload and resp.response_code == "922":
        logger.debug("Pickup %d: queue empty (not saved)", n)
        path = None
    else:
        path = save(f"collect_{n:03d}_{names}", resp.raw)
        logger.info("Pickup %d: %s -> %s", n, describe(resp), path.name)
    row_ids = message_log.log_answer(out_id, COMMON_SERVICE, resp, http_status=client.last_status,
                                     file_name=_file_name(path))
    return resp, path, row_ids
