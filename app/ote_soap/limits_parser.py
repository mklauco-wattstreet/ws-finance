"""
OTE SOAP - parser for SFVOTLIMITS (data code 464, "Stav finančního limitu").

Pure and offline: turns one <SFVOTLIMITS> element into flat rows for
finance.ote_limit_snapshot (one row per <Data> element). No network, no database.

The message is validated against the OTE XSD copies in ote_soap/xsd/ (copied
byte for byte from the bundle D1.4.2_XSD_sablony_2026_10_26: SFVOTLIMITS.xsd from
XML/SFVOTLIMITS/, ote_globals.xsd and xmldsig-core-schema.xsd from XML/Globals/).
The two real messages of 2026-11-10 validate as they are, including their
ds:Signature subtree, so nothing is stripped before validation.

Usage (offline check, cwd = repo root):
    PYTHONPATH=app python3 -m ote_soap.limits_parser FILE [FILE...]
FILE is a full SOAP envelope (as saved by ote_soap.fetch) or a bare SFVOTLIMITS
document. Prints the rows as a table and a one-line summary; exit code 1 on a
LimitsFormatError.
"""

from __future__ import annotations

import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

from lxml import etree

NS_LIMITS = "http://www.ote-cr.cz/schema/sfvot/limits"

PRAGUE = ZoneInfo("Europe/Prague")
XSD_PATH = Path(__file__).resolve().parent / "xsd" / "SFVOTLIMITS.xsd"

COLUMNS: tuple[str, ...] = (
    "snapshot_at", "message_at", "message_id", "request_id", "message_code", "report_title",
    "participant_ean", "participant_name", "participant_id",
    "participant_dic", "participant_eic", "participant_ic",
    "commodity", "data_group", "value_type", "vat_period", "value", "currency",
)

# Wrapper element under DataGroups -> value of the data_group column.
DATA_GROUPS: dict[str, str] = {
    "AdditionalData": "additional",
    "UtilizationData": "utilization",
    "LimitsData": "limits",
    "VatData": "vat",
    "TotalsData": "totals",
}

_PARTICIPANT_ATTRS = {"ean", "name", "dic", "eic", "ic", "particId"}
_DATA_ATTRS = {"type", "value", "currency", "period"}
_LIMITS_ATTRS = {"date", "title"}


class LimitsFormatError(Exception):
    """The message is not a valid SFVOTLIMITS document."""


_schema: etree.XMLSchema | None = None


def _q(name: str) -> str:
    return f"{{{NS_LIMITS}}}{name}"


def _local(element: etree._Element) -> str:
    return etree.QName(element).localname


def is_limits_message(element) -> bool:
    """True if the lxml element is {NS_LIMITS}SFVOTLIMITS."""
    return isinstance(getattr(element, "tag", None), str) and element.tag == _q("SFVOTLIMITS")


def _get_schema() -> etree.XMLSchema:
    """Load the XSD once; the imports resolve relative to the xsd/ folder."""
    global _schema
    if _schema is None:
        try:
            _schema = etree.XMLSchema(etree.parse(str(XSD_PATH)))
        except (OSError, etree.XMLSyntaxError, etree.XMLSchemaParseError) as exc:
            raise LimitsFormatError(f"Cannot load {XSD_PATH}: {exc}") from exc
    return _schema


def validate_limits(element) -> None:
    """Validate against app/ote_soap/xsd/SFVOTLIMITS.xsd; raise LimitsFormatError with the XSD error text."""
    if not is_limits_message(element):
        raise LimitsFormatError(f"Not an {{{NS_LIMITS}}}SFVOTLIMITS element: {getattr(element, 'tag', element)!r}")
    schema = _get_schema()
    if not schema.validate(element):
        errors = "; ".join(
            f"line {e.line}: {e.message}" for e in schema.error_log  # type: ignore[attr-defined]
        )
        raise LimitsFormatError(f"SFVOTLIMITS fails XSD validation: {errors}")


def _parse_dt(value: str, what: str) -> datetime:
    """ISO-8601 timestamp; a value without offset is Europe/Prague wall-clock."""
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise LimitsFormatError(f"{what}: bad timestamp {value!r}") from exc
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=PRAGUE)


def _req(element: etree._Element, attr: str) -> str:
    value = element.get(attr)
    if value is None:
        raise LimitsFormatError(f"<{_local(element)}> is missing attribute {attr!r}")
    return value


def _check_attrs(element: etree._Element, allowed: set[str]) -> None:
    unknown = set(element.attrib) - allowed
    if unknown:
        raise LimitsFormatError(f"<{_local(element)}> has unmapped attribute(s): {sorted(unknown)}")


def _decimal(value: str, what: str) -> Decimal:
    try:
        return Decimal(value.strip())
    except InvalidOperation as exc:
        raise LimitsFormatError(f"{what}: bad decimal {value!r}") from exc


def parse_limits(element, validate: bool = True) -> list[dict]:
    """One dict per <Data> element, keys exactly COLUMNS. Raises LimitsFormatError on anything unexpected."""
    if validate:
        validate_limits(element)
    elif not is_limits_message(element):
        raise LimitsFormatError(f"Not an {{{NS_LIMITS}}}SFVOTLIMITS element: {getattr(element, 'tag', element)!r}")

    message_at = _parse_dt(_req(element, "date-time"), "SFVOTLIMITS@date-time")
    message_id = _req(element, "id")
    message_code = _req(element, "message-code")

    reference = element.find(_q("Reference"))
    request_id = reference.get("id") if reference is not None else None

    limits = element.find(_q("Limits"))
    if limits is None:
        raise LimitsFormatError("SFVOTLIMITS has no <Limits> element")
    _check_attrs(limits, _LIMITS_ATTRS)
    snapshot_at = _parse_dt(_req(limits, "date"), "Limits@date")
    report_title = _req(limits, "title")

    rows: list[dict] = []
    for participant in limits:
        if not isinstance(participant.tag, str):
            continue  # comment / processing instruction
        if participant.tag != _q("Participant"):
            raise LimitsFormatError(f"Unexpected element under <Limits>: {participant.tag}")
        _check_attrs(participant, _PARTICIPANT_ATTRS)
        raw_id = participant.get("particId")
        try:
            participant_id = int(raw_id) if raw_id is not None else None
        except ValueError as exc:
            raise LimitsFormatError(f"Participant@particId is not an integer: {raw_id!r}") from exc
        base = {
            "snapshot_at": snapshot_at,
            "message_at": message_at,
            "message_id": message_id,
            "request_id": request_id,
            "message_code": message_code,
            "report_title": report_title,
            "participant_ean": participant.get("ean"),
            "participant_name": participant.get("name"),
            "participant_id": participant_id,
            "participant_dic": participant.get("dic"),
            "participant_eic": participant.get("eic"),
            "participant_ic": participant.get("ic"),
        }

        for groups in participant:
            if not isinstance(groups.tag, str):
                continue
            if groups.tag != _q("DataGroups"):
                raise LimitsFormatError(f"Unexpected element under <Participant>: {groups.tag}")
            _check_attrs(groups, {"commodity"})
            commodity = _req(groups, "commodity")

            for wrapper in groups:
                if not isinstance(wrapper.tag, str):
                    continue
                name = _local(wrapper) if wrapper.tag.startswith(f"{{{NS_LIMITS}}}") else wrapper.tag
                data_group = DATA_GROUPS.get(name)
                if data_group is None:
                    raise LimitsFormatError(f"Unknown element under <DataGroups>: {wrapper.tag}")
                _check_attrs(wrapper, set())

                for data in wrapper:
                    if not isinstance(data.tag, str):
                        continue
                    if data.tag != _q("Data"):
                        raise LimitsFormatError(f"Unexpected element under <{name}>: {data.tag}")
                    _check_attrs(data, _DATA_ATTRS)
                    value_type = _req(data, "type")
                    rows.append({
                        **base,
                        "commodity": commodity,
                        "data_group": data_group,
                        "value_type": value_type,
                        "vat_period": data.get("period") if data_group == "vat" else None,
                        "value": _decimal(_req(data, "value"), f"{name}/{value_type}@value"),
                        "currency": data.get("currency"),
                    })
                    if data_group != "vat" and data.get("period") is not None:
                        raise LimitsFormatError(f"<{name}> Data {value_type!r} has an unexpected period attribute")
    return rows


# --- CLI ---------------------------------------------------------------------

def _find_limits_element(root: etree._Element) -> etree._Element:
    if is_limits_message(root):
        return root
    found = root.findall(f".//{_q('SFVOTLIMITS')}")
    if len(found) != 1:
        raise LimitsFormatError(f"Expected exactly one SFVOTLIMITS element, found {len(found)}")
    return found[0]


def _format_table(rows: list[dict]) -> str:
    cols = ("participant_name", "commodity", "data_group", "value_type", "vat_period", "value", "currency")
    cells = [[("" if r[c] is None else str(r[c])) for c in cols] for r in rows]
    widths = [max([len(c)] + [len(row[i]) for row in cells]) for i, c in enumerate(cols)]
    numeric = {cols.index("value")}

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
        print("Usage: python3 -m ote_soap.limits_parser FILE [FILE...]", file=sys.stderr)
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
                raise LimitsFormatError(f"Cannot read XML: {exc}") from exc
            element = _find_limits_element(root)
            validate_limits(element)
            rows = parse_limits(element, validate=False)
        except LimitsFormatError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            status = 1
            continue
        if rows:
            print(_format_table(rows))
        snapshot = rows[0]["snapshot_at"].isoformat() if rows else "-"
        message_id = rows[0]["message_id"] if rows else element.get("id")
        print(f"snapshot={snapshot} message_id={message_id} rows={len(rows)}")
    return status


if __name__ == "__main__":
    sys.exit(main())
