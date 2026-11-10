#!/usr/bin/env python3
"""
OTE SOAP - financial reports (FZ), exploration stage.

Writes every request and every response as raw XML files; touches NO database.
An answer OTE rejects (SOAP Fault, HTTP error, non-XML) is kept too, as
rejected_<code>_<id> / collect_<n>_error.
Files go to $OTE_SOAP_OUT_DIR (default /app/downloads/ote_soap/<YYYY-MM-DD>/).

The FZ scenario is asynchronous (D1.4.4 section 8.1): ReportService only
acknowledges a request; the report itself is queued and picked up with
COMMONREQ 921 on CommonService, one message per call.

Usage (inside the container, cwd /app/scripts):
    python3 -m ote_soap.fetch --dry-run [--code 463]
        Build and sign a request, self-check the signature, print it. No network.
    python3 -m ote_soap.fetch --request [--code 463|400,403,406|all] [--from YYYY-MM-DD --to YYYY-MM-DD]
        Send SFVOTREQ to ReportService for one code, a comma list, or all
        financial report codes (SFVOTREQ.xsd), 2 s apart. Dates default to today.
    python3 -m ote_soap.fetch --collect [--max 100]
        Pick up queued messages (COMMONREQ 921) until the queue is empty or --max.
        Takes the queue lock (/tmp/ote_soap_queue.lock, shared with ote_soap.limits);
        exit 3 if another process holds it.
    --debug   verbose logging
"""

import sentry_init  # noqa: F401  (must be first)

import argparse
import sys
import time
from datetime import date, datetime

from lxml import etree

from common import setup_logging
from ote_soap.client import BASE_URL, SIGN_ALG, OteSoapClient, OteSoapError
from ote_soap.messages import PRAGUE, SFVOTREQ_CODES, new_message_id, report_request
from ote_soap.shared import (
    OUT_ROOT, QueueLockHeld, load_credentials, out_dir, pickup, queue_lock, save, send_report,
)
from ote_soap.wssec import verify_own_envelope

REQUEST_GAP_S = 2  # pause between requests in --code all (OTE's per-code minimum delay is unpublished)
EXIT_LOCK_HELD = 3  # exit 2 is already "OTE call failed"


def _parse_codes(value: str) -> list[str]:
    """'463' | '400,403,406' | 'all' -> list of valid SFVOTREQ codes (order kept, duplicates dropped)."""
    if value.strip().lower() == "all":
        return sorted(SFVOTREQ_CODES)
    codes = list(dict.fromkeys(c.strip() for c in value.split(",") if c.strip()))
    unknown = [c for c in codes if c not in SFVOTREQ_CODES]
    if not codes or unknown:
        raise argparse.ArgumentTypeError(
            f"unknown report code(s) {', '.join(unknown) or value!r}; valid: {', '.join(sorted(SFVOTREQ_CODES))} or all"
        )
    return codes


def do_dry_run(client: OteSoapClient, codes: list[str], d_from: date, d_to: date, logger) -> int:
    for code in codes:
        msg_id = new_message_id()
        envelope = client.build(report_request(code, d_from, d_to, msg_id))
        verify_own_envelope(envelope, client.creds)
        path = save(f"dryrun_request_{code}_{msg_id}", envelope)
        logger.info("Report %s (%s): signature self-check OK (%s), %d bytes, written to %s (not sent)",
                    code, SFVOTREQ_CODES[code], client.sign_alg, len(envelope), path.name)
        if len(codes) == 1:
            print(etree.tostring(etree.fromstring(envelope), pretty_print=True).decode())
    return 0


def do_request(client: OteSoapClient, code: str, d_from: date, d_to: date, logger) -> int:
    _, resp = send_report(client, code, d_from, d_to, logger)
    return 0 if resp.return_code == "0" else 1


def do_request_many(client: OteSoapClient, codes: list[str], d_from: date, d_to: date, logger) -> int:
    """Request each code in turn; one failure does not stop the others."""
    failed = []
    for i, code in enumerate(codes):
        if i:
            time.sleep(REQUEST_GAP_S)
        try:
            if do_request(client, code, d_from, d_to, logger) != 0:
                failed.append(code)
        except OteSoapError as exc:
            logger.error("Report %s: %s", code, exc)
            failed.append(code)
    logger.info("Requested %d report(s), %d not accepted: %s",
                len(codes), len(failed), ", ".join(failed) or "none")
    return 1 if failed else 0


def do_collect(client: OteSoapClient, max_messages: int, logger) -> int:
    received = 0
    for n in range(1, max_messages + 1):
        resp, _ = pickup(client, n, logger)
        if not resp.payload:
            break
        received += 1
    else:
        logger.warning("Stopped after --max %d pickups; the queue may hold more", max_messages)
    logger.info("Collected %d message(s) into %s", received, out_dir())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="build + sign + self-check, no network")
    mode.add_argument("--request", action="store_true", help="send one SFVOTREQ to ReportService")
    mode.add_argument("--collect", action="store_true", help="pick up queued messages (COMMONREQ 921)")
    parser.add_argument("--code", default="463", type=_parse_codes,
                        help="report code(s): one code, a comma list (400,403,406) or 'all' (default 463)")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, help="YYYY-MM-DD, default today")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, help="YYYY-MM-DD, default today")
    parser.add_argument("--max", type=int, default=100, help="max pickups for --collect (default 100)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.debug)
    today = datetime.now(PRAGUE).date()
    d_from = args.date_from or today
    d_to = args.date_to or d_from

    logger.info("OTE SOAP endpoint %s, signing %s, output %s", BASE_URL, SIGN_ALG, OUT_ROOT)
    codes = args.code
    client = OteSoapClient(load_credentials(logger))
    try:
        if args.dry_run:
            return do_dry_run(client, codes, d_from, d_to, logger)
        if args.request:
            return do_request_many(client, codes, d_from, d_to, logger)
        with queue_lock():
            return do_collect(client, args.max, logger)
    except QueueLockHeld as exc:
        logger.error("%s; not collecting", exc)
        return EXIT_LOCK_HELD
    except OteSoapError as exc:
        logger.error("%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
