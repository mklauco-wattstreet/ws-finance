"""
OTE SOAP - parser for SFVOTBILLING (data codes 401 and 404, "Podklady pro fakturaci").

Pure and offline: turns one <SFVOTBILLING> element into the message data, one dict per
<Data> line (-> finance.ote_monthly_settlement_data) and one dict per <Summary>
(-> finance.ote_monthly_settlement_summary). No network, no database.

    401  request 400  Billing@ote = 0   Watt Street invoices OTE
    404  request 403  Billing@ote = 1   OTE invoices Watt Street

The message is validated against the OTE XSD copy in ote_soap/xsd/ (copied byte for
byte from XML/SFVOTBILLING/ in the bundle D1.4.2_XSD_sablony_2026_10_26; its imports
ote_globals.xsd and xmldsig-core-schema.xsd are the copies already used for the other
messages). The ds:Signature subtree is part of the schema, nothing is stripped.

Structure: SFVOTBILLING > Billing > Rut* > TradeType+ > Data+ , Summary
Every attribute the XSD allows has a place in the result:
    SFVOTBILLING   id, message-code, date-time, answer-required, dtd-version, dtd-release
                   -> message ids / message_at / header["root"]
    Reference@id   -> request_id / header["Reference"]
    Billing        date, dateFrom, dateTo, title, ote -> date_from / date_to / invoice_from_ote
                   / header["Billing"]
    Rut            dic, ean, ico, id, name -> header["Rut"] (one dict per Rut)
    TradeType      curr, tradeType, version -> currency / trade_type / version of its lines
                   and of its summary
    Data           tradeDate, valueDate, varSymb and the twelve numbers of DATA_NUMBERS
    Summary        the eleven numbers of SUMMARY_NUMBERS
An element or attribute outside this list is a BillingFormatError.

"header" is the JSON-serialisable dict stored next to every line: the first three keys
exactly as the web upload (trader-web ImportOteSettlementXml) builds them, then
"Reference" (absent without a Reference element), "Billing" and "Rut" (a list). All
attribute values stay the original strings.

Usage (offline check, cwd = repo root):
    PYTHONPATH=app python3 -m ote_soap.billing_parser FILE [FILE...]
FILE is a full SOAP envelope (as saved by ote_soap.fetch) or a bare SFVOTBILLING
document. Prints message info, the line count per (trade_type, version, currency) and
the first lines; exit code 1 on a BillingFormatError.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

from lxml import etree

NS_BILLING = "http://www.ote-cr.cz/schema/sfvot/billing"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_DS = "http://www.w3.org/2000/09/xmldsig#"

PRAGUE = ZoneInfo("Europe/Prague")
XSD_PATH = Path(__file__).resolve().parent / "xsd" / "SFVOTBILLING.xsd"

# <Data> attribute -> line key, for the twelve numbers (the three others are tradeDate,
# valueDate and varSymb). The first seven are the "old" columns the web upload fills.
DATA_NUMBERS: dict[str, str] = {
    "volume": "volume_mwh",
    "exchRate": "exchange_rate_czk_eur",
    "origAmountNetCzk": "original_amount_net_czk",
    "origAmountNetEur": "original_amount_net_eur",
    "origVatCzk": "original_vat_czk",
    "origVatEur": "original_vat_eur",
    "paymentEur": "payment_eur",
    "origAmountCzk": "original_amount_czk",
    "origAmountEur": "original_amount_eur",
    "origEtaxCzk": "original_etax_czk",
    "origEtaxEur": "original_etax_eur",
    "paymentCzk": "payment_czk",
}
# <Summary> attribute -> summary key (no exchRate in a summary).
SUMMARY_NUMBERS: dict[str, str] = {a: c for a, c in DATA_NUMBERS.items() if a != "exchRate"}

LINE_KEYS: tuple[str, ...] = (
    "trade_date", "variable_symbol", "trade_type", "version", "invoice_from_ote", "currency",
    *DATA_NUMBERS.values(), "value_date",
)
SUMMARY_KEYS: tuple[str, ...] = ("trade_type", "version", "currency", *SUMMARY_NUMBERS.values())

_ROOT_ATTRS = {"answer-required", "date-time", "dtd-release", "dtd-version", "id", "message-code"}
_BILLING_ATTRS = {"date", "dateFrom", "dateTo", "title", "ote"}
_RUT_ATTRS = {"dic", "ean", "ico", "id", "name"}
_TRADETYPE_ATTRS = {"curr", "tradeType", "version"}
_DATA_ATTRS = {"tradeDate", "valueDate", "varSymb"} | set(DATA_NUMBERS)
_SUMMARY_ATTRS = set(SUMMARY_NUMBERS)

_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})(Z|[+-]\d{2}:\d{2})?$")  # xs:date, optional timezone


class BillingFormatError(Exception):
    """The message is not a valid SFVOTBILLING document."""


_schema: etree.XMLSchema | None = None


def _q(name: str) -> str:
    return f"{{{NS_BILLING}}}{name}"


def _local(element: etree._Element) -> str:
    return etree.QName(element).localname


def is_billing_message(element) -> bool:
    """True if the lxml element is {NS_BILLING}SFVOTBILLING."""
    return isinstance(getattr(element, "tag", None), str) and element.tag == _q("SFVOTBILLING")


def _get_schema() -> etree.XMLSchema:
    """Load the XSD once; the imports resolve relative to the xsd/ folder."""
    global _schema
    if _schema is None:
        try:
            _schema = etree.XMLSchema(etree.parse(str(XSD_PATH)))
        except (OSError, etree.XMLSyntaxError, etree.XMLSchemaParseError) as exc:
            raise BillingFormatError(f"Cannot load {XSD_PATH}: {exc}") from exc
    return _schema


def validate_billing(element) -> None:
    """Validate against app/ote_soap/xsd/SFVOTBILLING.xsd; raise BillingFormatError with the XSD error text."""
    if not is_billing_message(element):
        raise BillingFormatError(f"Not an {{{NS_BILLING}}}SFVOTBILLING element: {getattr(element, 'tag', element)!r}")
    schema = _get_schema()
    if not schema.validate(element):
        errors = "; ".join(
            f"line {e.line}: {e.message}" for e in schema.error_log  # type: ignore[attr-defined]
        )
        raise BillingFormatError(f"SFVOTBILLING fails XSD validation: {errors}")


# --- value helpers ----------------------------------------------------------

def _parse_dt(value: str, what: str) -> datetime:
    """ISO-8601 timestamp; a value without offset is Europe/Prague wall-clock."""
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise BillingFormatError(f"{what}: bad timestamp {value!r}") from exc
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=PRAGUE)


def _parse_date(value: str, what: str) -> date:
    """xs:date (YYYY-MM-DD, an optional timezone suffix is dropped)."""
    match = _DATE_RE.match(value.strip())
    if match is None:
        raise BillingFormatError(f"{what}: bad date {value!r}")
    try:
        return date.fromisoformat(match.group(1))
    except ValueError as exc:
        raise BillingFormatError(f"{what}: bad date {value!r}") from exc


def _parse_bool(value: str, what: str) -> int:
    """xs:boolean -> 1 / 0."""
    text = value.strip()
    if text in ("1", "true"):
        return 1
    if text in ("0", "false"):
        return 0
    raise BillingFormatError(f"{what}: bad boolean {value!r}")


def _parse_int(value: str, what: str) -> int:
    try:
        return int(value.strip())
    except ValueError as exc:
        raise BillingFormatError(f"{what}: bad integer {value!r}") from exc


def _decimal(value: str | None, what: str) -> Decimal | None:
    """A missing or empty attribute is None (as in the web upload); anything else must be a finite number."""
    if value is None or value.strip() == "":
        return None
    try:
        number = Decimal(value.strip())
    except InvalidOperation as exc:
        raise BillingFormatError(f"{what}: bad decimal {value!r}") from exc
    if not number.is_finite():
        raise BillingFormatError(f"{what}: bad decimal {value!r}")
    return number


def _req(element: etree._Element, attr: str) -> str:
    value = element.get(attr)
    if value is None:
        raise BillingFormatError(f"<{_local(element)}> is missing attribute {attr!r}")
    return value


def _attr_name(name: str) -> str:
    """lxml attribute key -> the name the web upload stores (xsi:... keeps its prefix)."""
    if name.startswith(f"{{{NS_XSI}}}"):
        return "xsi:" + name[len(NS_XSI) + 2:]
    return name


def _attrs(element: etree._Element) -> dict[str, str]:
    """All attributes with original string values (like DOMNode attributes in the web upload)."""
    return {_attr_name(k): v for k, v in element.attrib.items()}


def _check_attrs(element: etree._Element, allowed: set[str], allow_xsi: bool = False) -> None:
    unknown = {
        _attr_name(k) for k in element.attrib
        if k not in allowed and not (allow_xsi and k.startswith(f"{{{NS_XSI}}}"))
    }
    if unknown:
        raise BillingFormatError(f"<{_local(element)}> has unmapped attribute(s): {sorted(unknown)}")


def _children(element: etree._Element, allowed: tuple[str, ...], signature_ok: bool = False) -> list[etree._Element]:
    """Element children (comments skipped); an element that is not {NS_BILLING}<one of allowed> is an error.

    signature_ok: a ds:Signature child is skipped (only the message root carries one).
    """
    out = []
    for child in element:
        if not isinstance(child.tag, str):
            continue
        if signature_ok and child.tag == f"{{{NS_DS}}}Signature":
            continue
        if child.tag not in {_q(n) for n in allowed}:
            raise BillingFormatError(f"Unknown element <{child.tag}> under <{_local(element)}>")
        out.append(child)
    return out


# --- parse ------------------------------------------------------------------

def _parse_data(data: etree._Element, trade_type: str, version: int | None, currency: str,
                invoice_from_ote: int) -> dict:
    _check_attrs(data, _DATA_ATTRS)
    trade_date = _parse_date(_req(data, "tradeDate"), "Data@tradeDate")
    var_symb = data.get("varSymb")
    value_date = data.get("valueDate")
    line: dict = {
        "trade_date": trade_date,
        "variable_symbol": var_symb if var_symb else None,  # same rule as the web upload
        "trade_type": trade_type,
        "version": version,
        "invoice_from_ote": invoice_from_ote,
        "currency": currency,
    }
    for attr, key in DATA_NUMBERS.items():
        line[key] = _decimal(data.get(attr), f"Data@{attr} {trade_date} {trade_type}")
    line["value_date"] = _parse_date(value_date, "Data@valueDate") if value_date else None
    return line


def _parse_summary(summary: etree._Element, trade_type: str, version: int | None, currency: str) -> dict:
    _check_attrs(summary, _SUMMARY_ATTRS)
    out: dict = {"trade_type": trade_type, "version": version, "currency": currency}
    for attr, key in SUMMARY_NUMBERS.items():
        out[key] = _decimal(summary.get(attr), f"Summary@{attr} {trade_type}")
    return out


def parse_billing(element, validate: bool = True) -> dict:
    """{"message": {...}, "lines": [...], "summaries": [...]}; see the module docstring.

    message keys: message_id, request_id, message_code, message_at (aware), invoice_from_ote,
    date_from, date_to, header. Lines keep document order. Raises BillingFormatError on
    anything unexpected (XSD error, unknown element or attribute, bad number or date).
    """
    if validate:
        validate_billing(element)
    elif not is_billing_message(element):
        raise BillingFormatError(f"Not an {{{NS_BILLING}}}SFVOTBILLING element: {getattr(element, 'tag', element)!r}")
    _check_attrs(element, _ROOT_ATTRS, allow_xsi=True)

    message_at = _parse_dt(_req(element, "date-time"), "SFVOTBILLING@date-time")
    message_id = _req(element, "id")
    message_code = _req(element, "message-code")

    sender = receiver = reference = billing = None
    for child in _children(
            element, ("SenderIdentification", "ReceiverIdentification", "Reference", "Billing"), signature_ok=True):
        name = _local(child)
        if name == "SenderIdentification":
            sender = child
        elif name == "ReceiverIdentification":
            receiver = child
        elif name == "Reference":
            reference = child
        else:
            billing = child
    for what, found in (("SenderIdentification", sender), ("ReceiverIdentification", receiver), ("Billing", billing)):
        if found is None:
            raise BillingFormatError(f"SFVOTBILLING has no <{what}> element")
    assert sender is not None and receiver is not None and billing is not None
    _check_attrs(sender, {"coding-scheme", "id"})
    _check_attrs(receiver, {"coding-scheme", "id"})
    if reference is not None:
        _check_attrs(reference, {"id"})
    request_id = reference.get("id") if reference is not None else None

    _check_attrs(billing, _BILLING_ATTRS)
    invoice_from_ote = _parse_bool(_req(billing, "ote"), "Billing@ote")
    date_from = _parse_dt(_req(billing, "dateFrom"), "Billing@dateFrom").date()
    date_to = _parse_dt(_req(billing, "dateTo"), "Billing@dateTo").date()

    header: dict = {
        "root": _attrs(element),
        "SenderIdentification": _attrs(sender),
        "ReceiverIdentification": _attrs(receiver),
    }
    if reference is not None:
        header["Reference"] = _attrs(reference)
    header["Billing"] = _attrs(billing)
    header["Rut"] = []

    lines: list[dict] = []
    summaries: list[dict] = []
    for rut in _children(billing, ("Rut",)):
        _check_attrs(rut, _RUT_ATTRS)
        header["Rut"].append(_attrs(rut))
        for trade in _children(rut, ("TradeType",)):
            _check_attrs(trade, _TRADETYPE_ATTRS)
            trade_type = _req(trade, "tradeType")
            currency = _req(trade, "curr")
            version = _parse_int(_req(trade, "version"), "TradeType@version")
            for child in _children(trade, ("Data", "Summary")):
                if _local(child) == "Data":
                    lines.append(_parse_data(child, trade_type, version, currency, invoice_from_ote))
                else:
                    summaries.append(_parse_summary(child, trade_type, version, currency))

    return {
        "message": {
            "message_id": message_id,
            "request_id": request_id,
            "message_code": message_code,
            "message_at": message_at,
            "invoice_from_ote": invoice_from_ote,
            "date_from": date_from,
            "date_to": date_to,
            "header": header,
        },
        "lines": lines,
        "summaries": summaries,
    }


# --- CLI ---------------------------------------------------------------------

def _find_billing_element(root: etree._Element) -> etree._Element:
    if is_billing_message(root):
        return root
    found = root.findall(f".//{_q('SFVOTBILLING')}")
    if len(found) != 1:
        raise BillingFormatError(f"Expected exactly one SFVOTBILLING element, found {len(found)}")
    return found[0]


def _print_result(result: dict, first: int = 5) -> None:
    msg, lines, summaries = result["message"], result["lines"], result["summaries"]
    print(f"message_id={msg['message_id']} request_id={msg['request_id']} message_code={msg['message_code']} "
          f"message_at={msg['message_at'].isoformat()}")
    print(f"period {msg['date_from']}..{msg['date_to']}  invoice_from_ote={msg['invoice_from_ote']}  "
          f"title={msg['header']['Billing'].get('title')!r}  rut={len(msg['header']['Rut'])}")
    print(f"lines={len(lines)} summaries={len(summaries)}")
    counts = Counter((ln["trade_type"], ln["version"], ln["currency"]) for ln in lines)
    for (trade_type, version, currency), n in sorted(counts.items()):
        print(f"  {trade_type}/{version}/{currency}: {n}")
    for ln in lines[:first]:
        print("  line:", {k: (None if v is None else str(v)) for k, v in ln.items() if v is not None})


def main(argv: list[str] | None = None) -> int:
    paths = (sys.argv[1:] if argv is None else argv)
    if paths and paths[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    if not paths:
        print("Usage: python3 -m ote_soap.billing_parser FILE [FILE...]", file=sys.stderr)
        return 2
    status = 0
    for path in paths:
        print(f"== {path}")
        try:
            try:
                # Disable entity resolution / network access when reading files.
                parser = etree.XMLParser(resolve_entities=False, no_network=True)
                root = etree.parse(path, parser).getroot()
            except (OSError, etree.XMLSyntaxError) as exc:
                raise BillingFormatError(f"Cannot read XML: {exc}") from exc
            element = _find_billing_element(root)
            validate_billing(element)
            result = parse_billing(element, validate=False)
        except BillingFormatError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            status = 1
            continue
        _print_result(result)
    return status


if __name__ == "__main__":
    sys.exit(main())
