#!/usr/bin/env python3
"""
OTE SOAP - financial reports (FZ), exploration stage.

Writes every request and every response as raw XML files; touches NO database.
Files go to $OTE_SOAP_OUT_DIR (default /app/downloads/ote_soap/<YYYY-MM-DD>/).

The FZ scenario is asynchronous (D1.4.4 section 8.1): ReportService only
acknowledges a request; the report itself is queued and picked up with
COMMONREQ 921 on CommonService, one message per call.

Usage (inside the container, cwd /app/scripts):
    python3 -m ote_soap.fetch --dry-run [--code 463]
        Build and sign a request, self-check the signature, print it. No network.
    python3 -m ote_soap.fetch --request [--code 463] [--from YYYY-MM-DD --to YYYY-MM-DD]
        Send one SFVOTREQ to ReportService. Dates default to today.
    python3 -m ote_soap.fetch --collect [--max 20]
        Pick up queued messages (COMMONREQ 921) until the queue is empty or --max.
    --debug   verbose logging
"""

import sentry_init  # noqa: F401  (must be first)

import argparse
import os
import sys
from datetime import date, datetime
from pathlib import Path

from lxml import etree

from common import setup_logging
from config import OTE_CERT_PASSWORD, OTE_CERT_PATH
from ote_soap.client import BASE_URL, SIGN_ALG, OteResponse, OteSoapClient, OteSoapError
from ote_soap.messages import PRAGUE, SFVOTREQ_CODES, new_message_id, queue_pickup_request, report_request
from ote_soap.wssec import Credentials, verify_own_envelope

OUT_ROOT = Path(os.getenv("OTE_SOAP_OUT_DIR", "/app/downloads/ote_soap"))


def _out_dir() -> Path:
    path = OUT_ROOT / datetime.now(PRAGUE).strftime("%Y-%m-%d")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _save(name: str, data: bytes) -> Path:
    path = _out_dir() / f"{datetime.now(PRAGUE).strftime('%H%M%S')}_{name}.xml"
    path.write_bytes(data)
    return path


def _describe(resp: OteResponse) -> str:
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


def _load_credentials(logger) -> Credentials:
    if not OTE_CERT_PATH:
        raise SystemExit("OTE_CERT_PATH is not set")
    creds = Credentials.from_pkcs12(OTE_CERT_PATH, OTE_CERT_PASSWORD)
    logger.info("Certificate: %s (valid until %s), password [CONFIGURED]", creds.subject, creds.not_valid_after.date())
    if creds.not_valid_after < datetime.now(creds.not_valid_after.tzinfo):
        raise SystemExit(f"Certificate expired on {creds.not_valid_after.date()}")
    return creds


def do_dry_run(client: OteSoapClient, code: str, d_from: date, d_to: date, logger) -> int:
    msg_id = new_message_id()
    envelope = client.build(report_request(code, d_from, d_to, msg_id))
    verify_own_envelope(envelope, client.creds)
    logger.info("Signature self-check OK (%s). Request id %s, %d bytes.", client.sign_alg, msg_id, len(envelope))
    path = _save(f"dryrun_request_{code}_{msg_id}", envelope)
    logger.info("Signed request written to %s (not sent)", path)
    print(etree.tostring(etree.fromstring(envelope), pretty_print=True).decode())
    return 0


def do_request(client: OteSoapClient, code: str, d_from: date, d_to: date, logger) -> int:
    msg_id = new_message_id()
    payload = report_request(code, d_from, d_to, msg_id)
    logger.info("Requesting report %s (%s) for %s..%s, request id %s",
                code, SFVOTREQ_CODES[code], d_from, d_to, msg_id)
    try:
        resp = client.send("reportService", payload)
    finally:
        if client.last_request:
            _save(f"request_{code}_{msg_id}", client.last_request)
    path = _save(f"response_{code}_{msg_id}", resp.raw)
    logger.info("ReportService answered: %s", _describe(resp))
    logger.info("Response saved to %s", path)
    return 0 if resp.return_code == "0" else 1


def do_collect(client: OteSoapClient, max_messages: int, logger) -> int:
    received = 0
    for n in range(1, max_messages + 1):
        msg_id = new_message_id()
        resp = client.send("commonService", queue_pickup_request(msg_id))
        names = "_".join(f"{etree.QName(p).localname}-{p.get('message-code', 'x')}" for p in resp.payload) or "empty"
        path = _save(f"collect_{n:03d}_{names}", resp.raw)
        logger.info("Pickup %d: %s -> %s", n, _describe(resp), path.name)
        if not resp.payload:
            break
        received += 1
    else:
        logger.warning("Stopped after --max %d pickups; the queue may hold more", max_messages)
    logger.info("Collected %d message(s) into %s", received, _out_dir())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="build + sign + self-check, no network")
    mode.add_argument("--request", action="store_true", help="send one SFVOTREQ to ReportService")
    mode.add_argument("--collect", action="store_true", help="pick up queued messages (COMMONREQ 921)")
    parser.add_argument("--code", default="463", choices=sorted(SFVOTREQ_CODES), help="SFVOTREQ report code (default 463)")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, help="YYYY-MM-DD, default today")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, help="YYYY-MM-DD, default today")
    parser.add_argument("--max", type=int, default=20, help="max pickups for --collect (default 20)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.debug)
    today = datetime.now(PRAGUE).date()
    d_from = args.date_from or today
    d_to = args.date_to or d_from

    logger.info("OTE SOAP endpoint %s, signing %s, output %s", BASE_URL, SIGN_ALG, OUT_ROOT)
    client = OteSoapClient(_load_credentials(logger))
    try:
        if args.dry_run:
            return do_dry_run(client, args.code, d_from, d_to, logger)
        if args.request:
            return do_request(client, args.code, d_from, d_to, logger)
        return do_collect(client, args.max, logger)
    except OteSoapError as exc:
        logger.error("%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
