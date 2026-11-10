"""
Helpers shared by the OTE SOAP command-line entry points (fetch, limits).

    - raw-XML files on disk (every request, receipt, queue message, rejected answer)
    - one-line description of an OTE answer
    - sending a report request / picking up one queue message, with the raw
      answer saved even when OTE rejects the call
    - the queue lock: only one process at a time may empty the OTE queue,
      because OTE removes a message from the queue as soon as it hands it over
"""

from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from lxml import etree

from config import OTE_CERT_PASSWORD, OTE_CERT_PATH
from ote_soap.client import OteResponse, OteSoapClient, OteSoapError
from ote_soap.messages import PRAGUE, SFVOTREQ_CODES, new_message_id, queue_pickup_request, report_request
from ote_soap.wssec import Credentials

OUT_ROOT = Path(os.getenv("OTE_SOAP_OUT_DIR", "/app/downloads/ote_soap"))
QUEUE_LOCK_PATH = "/tmp/ote_soap_queue.lock"


class QueueLockHeld(RuntimeError):
    """Another process is emptying the OTE queue right now."""


@contextmanager
def queue_lock(path: str = QUEUE_LOCK_PATH) -> Iterator[None]:
    """Exclusive, non-blocking lock for the whole time the queue is read.

    Raises QueueLockHeld at once if another process holds it. The lock is
    released when the block ends or the process dies (flock is per open file).
    """
    with open(path, "a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise QueueLockHeld(f"OTE queue lock {path} is held by another process") from exc
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


def save_rejected(client: OteSoapClient, name: str, logger) -> None:
    """Keep whatever OTE returned for a call that raised, and log its HTTP status."""
    if client.last_response:
        path = save(name, client.last_response)
        logger.error("OTE answered HTTP %s, raw answer (%d bytes) saved to %s",
                     client.last_status, len(client.last_response), path)
    else:
        logger.error("No HTTP answer from OTE to keep (status %s)", client.last_status)


def send_report(client: OteSoapClient, code: str, d_from: date, d_to: date, logger) -> tuple[str, OteResponse]:
    """Send one SFVOTREQ to ReportService; save request and receipt.

    Returns (request id, receipt). A call OTE rejects saves the raw answer as
    rejected_<code>_<id> and re-raises OteSoapError.
    """
    msg_id = new_message_id()
    payload = report_request(code, d_from, d_to, msg_id)
    logger.info("Requesting report %s (%s) for %s..%s, request id %s",
                code, SFVOTREQ_CODES[code], d_from, d_to, msg_id)
    try:
        resp = client.send("reportService", payload)
    except OteSoapError:
        save_rejected(client, f"rejected_{code}_{msg_id}", logger)
        raise
    finally:
        if client.last_request:
            save(f"request_{code}_{msg_id}", client.last_request)
    path = save(f"response_{code}_{msg_id}", resp.raw)
    logger.info("ReportService answered: %s", describe(resp))
    logger.info("Response saved to %s", path)
    return msg_id, resp


def pickup(client: OteSoapClient, n: int, logger, save_empty_queue: bool = True) -> tuple[OteResponse, Path | None]:
    """One COMMONREQ 921 on CommonService; the raw answer is saved as collect_<n>_<payload>.

    Returns (answer, saved file or None). save_empty_queue=False skips the file
    for a plain "queue empty" (922) answer.
    A rejected call saves the raw answer as collect_<n>_error and re-raises.
    """
    msg_id = new_message_id()
    try:
        resp = client.send("commonService", queue_pickup_request(msg_id))
    except OteSoapError:
        save_rejected(client, f"collect_{n:03d}_error", logger)
        raise
    names = "_".join(f"{etree.QName(p).localname}-{p.get('message-code', 'x')}" for p in resp.payload) or "empty"
    if not save_empty_queue and not resp.payload and resp.response_code == "922":
        logger.debug("Pickup %d: queue empty (not saved)", n)
        return resp, None
    path = save(f"collect_{n:03d}_{names}", resp.raw)
    logger.info("Pickup %d: %s -> %s", n, describe(resp), path.name)
    return resp, path
