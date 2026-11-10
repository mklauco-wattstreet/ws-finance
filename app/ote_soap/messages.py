"""
Request message builders for the FZ (financial) scenario.

Element names, attributes and allowed message codes are taken from the OTE
XSD bundle (D1.4.2_XSD_sablony_2026_10_26):
    XML/SFVOTREQ/SFVOTREQ.xsd    report request, sent to ReportService
    XML/COMMONREQ/COMMONREQ.xsd  queue pickup (921), sent to CommonService
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from zoneinfo import ZoneInfo

from lxml import etree

PRAGUE = ZoneInfo("Europe/Prague")

# Sender = Watt Street, s.r.o. (EAN from OTE's participant register, ID RUT 36808).
# Receiver = OTE, a.s. (EAN from every OTE message and the D1.4.3 examples).
PARTICIPANT_EAN = os.getenv("OTE_PARTICIPANT_EAN", "8591824680834")
OTE_EAN = "8591824000007"
CODING_SCHEME_EAN = "14"

NS_SVC_REPORT = "http://www.ote-cr.cz/schema/service/report"
NS_SVC_COMMON = "http://www.ote-cr.cz/schema/service/common"
NS_SFVOTREQ = "http://www.ote-cr.cz/schema/sfvot/request"
NS_COMMONREQ = "http://www.ote-cr.cz/schema/common/request"

# SFVOTREQ@message-code enumeration (SFVOTREQ.xsd). 481 is NOT here: it is the
# SFVOTSETTINGS code that CHANGES the intraday limit and is never sent by us.
SFVOTREQ_CODES = {
    "400": "Podklady pro fakturaci",
    "403": "Podklady pro fakturaci OTE",
    "406": "Přehled plateb a reklamací",
    "409": "Přehled plateb a reklamací OTE",
    "418": "Vypořádání PXE-OTE",
    "421": "Vypořádání OTE-PXE",
    "425": "Vypořádací kurz OTE",
    "428": "Pohledávky a závazky",
    "431": "Konfirmace denních plateb",
    "434": "Podklady pro fakturaci - souhrn MV",
    "437": "Podklady pro fakturaci OTE - souhrn MV",
    "440": "Přehled reklamací - souhrn ZMV",
    "443": "Přehled reklamací OTE - souhrn ZMV",
    "460": "Netting TDD",
    "463": "Stav finančního limitu",
    "466": "Definitivní netting TDD",
    "469": "Definitivní zúčtování rozdílů z TDD",
    "472": "Definitivní zúčtování rozdílů z TDD OTE",
    "475": "Podklady pro fakturaci RE - ČEPS",
    "478": "Přehled plateb a reklamací RE - ČEPS",
    "487": "Stav nouze ČR",
    "490": "Stav nouze - vypořádání exportu/importu z DT",
    "493": "Stav nouze - vypořádání exportu/importu z VDT",
    "496": "Stav nouze verze 1 - netting - debet SZ",
    "499": "Stav nouze verze 2 - netting - debet SZ",
    "502": "Stav nouze - uhrazené závazky OTE",
    "505": "Stav nouze - vypořádání exportu/importu z IDA",
    "514": "Stav nouze verze 1 - netting - kredit SZ",
    "517": "Stav nouze verze 2 - netting - kredit SZ",
}


def new_message_id() -> str:
    """msg-id: string, 1..35 chars (ote_globals.xsd)."""
    return uuid.uuid4().hex


def _now_local() -> str:
    return datetime.now(PRAGUE).strftime("%Y-%m-%dT%H:%M:%S")


def _identification(parent: etree._Element, ns: str) -> None:
    sender = etree.SubElement(parent, f"{{{ns}}}SenderIdentification")
    sender.set("coding-scheme", CODING_SCHEME_EAN)
    sender.set("id", PARTICIPANT_EAN)
    receiver = etree.SubElement(parent, f"{{{ns}}}ReceiverIdentification")
    receiver.set("coding-scheme", CODING_SCHEME_EAN)
    receiver.set("id", OTE_EAN)


def report_request(code: str, date_from: date, date_to: date, msg_id: str) -> etree._Element:
    """<SendRequest> for ReportService carrying one SFVOTREQ."""
    if code not in SFVOTREQ_CODES:
        raise ValueError(f"Report code {code} is not a valid SFVOTREQ code")
    if date_to < date_from:
        raise ValueError(f"date_to {date_to} is before date_from {date_from}")

    send = etree.Element(f"{{{NS_SVC_REPORT}}}SendRequest", nsmap={None: NS_SVC_REPORT})
    req = etree.SubElement(send, f"{{{NS_SFVOTREQ}}}SFVOTREQ", nsmap={None: NS_SFVOTREQ})
    req.set("id", msg_id)
    req.set("message-code", code)
    req.set("date-time", _now_local())
    req.set("dtd-version", "1")
    req.set("dtd-release", "1")
    _identification(req, NS_SFVOTREQ)
    interval = etree.SubElement(req, f"{{{NS_SFVOTREQ}}}Interval")
    interval.set("date-from", date_from.isoformat())
    interval.set("date-to", date_to.isoformat())
    return send


def queue_pickup_request(msg_id: str) -> etree._Element:
    """<SendRequest> for CommonService: COMMONREQ 921 = hand over one queued message."""
    send = etree.Element(f"{{{NS_SVC_COMMON}}}SendRequest", nsmap={None: NS_SVC_COMMON})
    req = etree.SubElement(send, f"{{{NS_COMMONREQ}}}COMMONREQ", nsmap={None: NS_COMMONREQ})
    req.set("id", msg_id)
    req.set("message-code", "921")
    req.set("date-time", _now_local())
    req.set("dtd-version", "1")
    req.set("dtd-release", "1")
    _identification(req, NS_COMMONREQ)
    return send
