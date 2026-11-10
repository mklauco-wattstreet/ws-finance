"""
OTE SOAP - parser for SFVOTEXCHRATE (data code 426, "Vypořádací kurz OTE").

Pure and offline: turns one <SFVOTEXCHRATE> element into rows for
finance.ote_exchange_rate (ONE row per valid date, one column per kind of rate).
No network, no database.

The message is validated against the OTE XSD copy in ote_soap/xsd/ (copied byte
for byte from the bundle D1.4.2_XSD_sablony_2026_10_26: SFVOTEXCHRATE.xsd from
XML/SFVOTEXCHRATE/; its imports ote_globals.xsd and xmldsig-core-schema.xsd are
the copies already used for SFVOTLIMITS). The real message of 2026-11-10
validates as it is, including its ds:Signature subtree, so nothing is stripped
before validation.

ExchRates holds up to three kinds of rate, each element carrying @validDate and
the rate as its text:
    OteFinalRate     -> ote_final_rate       (OTE settlement rate for the day)
    OteFinalVdtRate  -> ote_final_vdt_rate   (for the intraday market, VDT)
    OteFinalIdaRate  -> ote_final_ida_rate   (for the IDA market)
A kind that is absent for a date stays None. The same kind twice for one date,
or any other element under ExchRates, is a format error. A message without any
rate element parses to an empty list.

Usage (offline check, cwd = repo root):
    PYTHONPATH=app python3 -m ote_soap.exchange_rate_parser FILE [FILE...]
FILE is a full SOAP envelope (as saved by ote_soap.fetch) or a bare
SFVOTEXCHRATE document. Prints the rows as a table and a one-line summary; exit
code 1 on an ExchangeRateFormatError.
"""

from __future__ import annotations

import re
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

from lxml import etree

NS_EXCHRATE = "http://www.ote-cr.cz/schema/sfvot/exchrate"

PRAGUE = ZoneInfo("Europe/Prague")
XSD_PATH = Path(__file__).resolve().parent / "xsd" / "SFVOTEXCHRATE.xsd"

COLUMNS: tuple[str, ...] = (
    "valid_date", "ote_final_rate", "ote_final_vdt_rate", "ote_final_ida_rate",
    "message_id", "request_id", "message_code", "message_at",
)

# Element under ExchRates -> column.
RATE_ELEMENTS: dict[str, str] = {
    "OteFinalRate": "ote_final_rate",
    "OteFinalVdtRate": "ote_final_vdt_rate",
    "OteFinalIdaRate": "ote_final_ida_rate",
}

_RATE_ATTRS = {"validDate"}
_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})(Z|[+-]\d{2}:\d{2})?$")  # xs:date, optional timezone


class ExchangeRateFormatError(Exception):
    """The message is not a valid SFVOTEXCHRATE document."""


_schema: etree.XMLSchema | None = None


def _q(name: str) -> str:
    return f"{{{NS_EXCHRATE}}}{name}"


def _local(element: etree._Element) -> str:
    return etree.QName(element).localname


def is_exchange_rate_message(element) -> bool:
    """True if the lxml element is {NS_EXCHRATE}SFVOTEXCHRATE."""
    return isinstance(getattr(element, "tag", None), str) and element.tag == _q("SFVOTEXCHRATE")


def _get_schema() -> etree.XMLSchema:
    """Load the XSD once; the imports resolve relative to the xsd/ folder."""
    global _schema
    if _schema is None:
        try:
            _schema = etree.XMLSchema(etree.parse(str(XSD_PATH)))
        except (OSError, etree.XMLSyntaxError, etree.XMLSchemaParseError) as exc:
            raise ExchangeRateFormatError(f"Cannot load {XSD_PATH}: {exc}") from exc
    return _schema


def validate_exchange_rate(element) -> None:
    """Validate against app/ote_soap/xsd/SFVOTEXCHRATE.xsd; raise ExchangeRateFormatError with the XSD error text."""
    if not is_exchange_rate_message(element):
        raise ExchangeRateFormatError(
            f"Not an {{{NS_EXCHRATE}}}SFVOTEXCHRATE element: {getattr(element, 'tag', element)!r}")
    schema = _get_schema()
    if not schema.validate(element):
        errors = "; ".join(
            f"line {e.line}: {e.message}" for e in schema.error_log  # type: ignore[attr-defined]
        )
        raise ExchangeRateFormatError(f"SFVOTEXCHRATE fails XSD validation: {errors}")


def _parse_dt(value: str, what: str) -> datetime:
    """ISO-8601 timestamp; a value without offset is Europe/Prague wall-clock."""
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise ExchangeRateFormatError(f"{what}: bad timestamp {value!r}") from exc
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=PRAGUE)


def _parse_date(value: str, what: str) -> date:
    """xs:date (YYYY-MM-DD, an optional timezone suffix is dropped)."""
    match = _DATE_RE.match(value.strip())
    if match is None:
        raise ExchangeRateFormatError(f"{what}: bad date {value!r}")
    try:
        return date.fromisoformat(match.group(1))
    except ValueError as exc:
        raise ExchangeRateFormatError(f"{what}: bad date {value!r}") from exc


def _req(element: etree._Element, attr: str) -> str:
    value = element.get(attr)
    if value is None:
        raise ExchangeRateFormatError(f"<{_local(element)}> is missing attribute {attr!r}")
    return value


def _check_attrs(element: etree._Element, allowed: set[str]) -> None:
    unknown = set(element.attrib) - allowed
    if unknown:
        raise ExchangeRateFormatError(f"<{_local(element)}> has unmapped attribute(s): {sorted(unknown)}")


def _decimal(value: str, what: str) -> Decimal:
    try:
        number = Decimal(value.strip())
    except InvalidOperation as exc:
        raise ExchangeRateFormatError(f"{what}: bad decimal {value!r}") from exc
    if not number.is_finite():
        raise ExchangeRateFormatError(f"{what}: bad decimal {value!r}")
    return number


def parse_exchange_rate(element, validate: bool = True) -> list[dict]:
    """One dict per valid date, keys exactly COLUMNS, sorted by valid_date.

    Raises ExchangeRateFormatError on anything unexpected. A message without any
    rate element returns [].
    """
    if validate:
        validate_exchange_rate(element)
    elif not is_exchange_rate_message(element):
        raise ExchangeRateFormatError(
            f"Not an {{{NS_EXCHRATE}}}SFVOTEXCHRATE element: {getattr(element, 'tag', element)!r}")

    message_at = _parse_dt(_req(element, "date-time"), "SFVOTEXCHRATE@date-time")
    message_id = _req(element, "id")
    message_code = _req(element, "message-code")

    reference = element.find(_q("Reference"))
    request_id = reference.get("id") if reference is not None else None

    rates = element.find(_q("ExchRates"))
    if rates is None:
        raise ExchangeRateFormatError("SFVOTEXCHRATE has no <ExchRates> element")
    _check_attrs(rates, set())

    by_date: dict[date, dict] = {}
    for rate in rates:
        if not isinstance(rate.tag, str):
            continue  # comment / processing instruction
        name = _local(rate) if rate.tag.startswith(f"{{{NS_EXCHRATE}}}") else rate.tag
        column = RATE_ELEMENTS.get(name)
        if column is None:
            raise ExchangeRateFormatError(f"Unknown element under <ExchRates>: {rate.tag}")
        _check_attrs(rate, _RATE_ATTRS)
        valid_date = _parse_date(_req(rate, "validDate"), f"{name}@validDate")
        value = _decimal(rate.text or "", f"{name} {valid_date}")
        row = by_date.get(valid_date)
        if row is None:
            row = {c: None for c in RATE_ELEMENTS.values()}
            by_date[valid_date] = row
        if row[column] is not None:
            raise ExchangeRateFormatError(f"<{name}> appears twice for valid date {valid_date}")
        row[column] = value

    return [
        {
            "valid_date": valid_date,
            "ote_final_rate": by_date[valid_date]["ote_final_rate"],
            "ote_final_vdt_rate": by_date[valid_date]["ote_final_vdt_rate"],
            "ote_final_ida_rate": by_date[valid_date]["ote_final_ida_rate"],
            "message_id": message_id,
            "request_id": request_id,
            "message_code": message_code,
            "message_at": message_at,
        }
        for valid_date in sorted(by_date)
    ]


# --- CLI ---------------------------------------------------------------------

def _find_exchange_rate_element(root: etree._Element) -> etree._Element:
    if is_exchange_rate_message(root):
        return root
    found = root.findall(f".//{_q('SFVOTEXCHRATE')}")
    if len(found) != 1:
        raise ExchangeRateFormatError(f"Expected exactly one SFVOTEXCHRATE element, found {len(found)}")
    return found[0]


def _format_table(rows: list[dict]) -> str:
    cols = ("valid_date", "ote_final_rate", "ote_final_vdt_rate", "ote_final_ida_rate", "message_at")
    cells = [[("" if r[c] is None else str(r[c])) for c in cols] for r in rows]
    widths = [max([len(c)] + [len(row[i]) for row in cells]) for i, c in enumerate(cols)]
    numeric = {1, 2, 3}

    def fmt(values: list[str]) -> str:
        return "  ".join(
            v.rjust(widths[i]) if i in numeric else v.ljust(widths[i]) for i, v in enumerate(values)
        ).rstrip()

    lines = [fmt(list(cols)), "  ".join("-" * w for w in widths)]
    lines.extend(fmt(row) for row in cells)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    paths = (sys.argv[1:] if argv is None else argv)
    if not paths:
        print("Usage: python3 -m ote_soap.exchange_rate_parser FILE [FILE...]", file=sys.stderr)
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
                raise ExchangeRateFormatError(f"Cannot read XML: {exc}") from exc
            element = _find_exchange_rate_element(root)
            validate_exchange_rate(element)
            rows = parse_exchange_rate(element, validate=False)
        except ExchangeRateFormatError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            status = 1
            continue
        if rows:
            print(_format_table(rows))
        message_id = rows[0]["message_id"] if rows else element.get("id")
        print(f"message_id={message_id} rows={len(rows)}")
    return status


if __name__ == "__main__":
    sys.exit(main())
