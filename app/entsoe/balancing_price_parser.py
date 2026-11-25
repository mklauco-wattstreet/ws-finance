"""Parsers for ENTSO-E activated balancing energy prices (documentType A84).

``BalancingPriceParser``  15-minute prices of processType A16 (realised) or A68
                          (aFRR local selection), one wide row per slot.
``AfrrPrice4sParser``     4-second aFRR prices of processType A67.

Business types: A95 FCR, A96 aFRR, A97 mFRR, A98 RR. A97 may hold two series per
direction, the standard and a specific product (``original_MarketProduct`` A02);
they go into separate columns.

Curve expansion: for curveType A01 only the published points count, otherwise a
missing point repeats the previous one. A point without a price is an explicit
"no price" and ends the repetition.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from entsoe.border_balancing_parser import resolution_seconds
from entsoe.parsers import BaseParser

SLOT_SECONDS = 900

PRODUCTS = ("fcr", "afrr", "mfrr", "rr", "mfrr_specific", "afrr_local")
PRICE_COLUMNS = [f"{p}_{d}_price_eur" for p in PRODUCTS for d in ("up", "down")]

REALISED_PRODUCTS = {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"}
DIRECTIONS = {"A01": "up", "A02": "down"}

logger = logging.getLogger(__name__)


def _text(element: ET.Element, name: str) -> Optional[str]:
    found = element.find(f"{{*}}{name}")
    return found.text.strip() if found is not None and found.text else None


def price_column(series: ET.Element, process_type: str) -> Optional[str]:
    """Target column of an A84 TimeSeries, None when it has no column."""
    business_type = _text(series, "businessType")
    direction = DIRECTIONS.get(_text(series, "flowDirection.direction"))
    if direction is None:
        return None
    specific = _text(series, "original_MarketProduct.marketProductType") is not None
    if process_type == "A68":
        product = "afrr_local" if business_type == "A96" and not specific else None
    elif process_type == "A16":
        product = REALISED_PRODUCTS.get(business_type)
        if specific:
            product = "mfrr_specific" if product == "mfrr" else None
    else:
        raise ValueError(f"Unsupported processType {process_type!r}")
    return f"{product}_{direction}_price_eur" if product else None


class A84ParserBase(BaseParser):
    """Shared curve expansion."""

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        with open(xml_file_path, "r", encoding="utf-8") as f:
            return self.parse_xml_content(f.read())

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    def expand(self, series: ET.Element) -> Iterator[Tuple[datetime, int, Optional[Decimal]]]:
        """Yield (start, resolution in seconds, price or None) for every covered interval."""
        forward_fill = _text(series, "curveType") != "A01"
        for period in series.findall("{*}Period"):
            interval = period.find("{*}timeInterval")
            start = self.parse_timestamp(interval.find("{*}start").text)
            end = self.parse_timestamp(interval.find("{*}end").text)
            seconds = resolution_seconds(_text(period, "resolution") or "PT15M")

            points: Dict[int, Optional[Decimal]] = {}
            for point in period.findall("{*}Point"):
                amount = _text(point, "activation_Price.amount")
                points[int(_text(point, "position"))] = Decimal(amount) if amount is not None else None

            started = False
            last: Optional[Decimal] = None
            for idx in range(int((end - start).total_seconds()) // seconds):
                if idx + 1 in points:
                    last = points[idx + 1]
                    started = True
                elif not (forward_fill and started):
                    continue
                yield start + timedelta(seconds=idx * seconds), seconds, last


class BalancingPriceParser(A84ParserBase):
    """15-minute prices. Each row carries only the columns its document covers (key 'prices')."""

    def __init__(self, area_id: int, country_code: str, process_type: str) -> None:
        super().__init__()
        self.area_id = area_id
        self.country_code = country_code
        self.process_type = process_type

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        slots: Dict[datetime, Dict[str, Optional[Decimal]]] = {}
        skipped: Set[Tuple[Optional[str], Optional[str]]] = set()
        for series in ET.fromstring(xml_string).findall("{*}TimeSeries"):
            column = price_column(series, self.process_type)
            if column is None:
                skipped.add((_text(series, "businessType"), _text(series, "flowDirection.direction")))
                continue
            for start, seconds, price in self.expand(series):
                if seconds % SLOT_SECONDS:
                    raise ValueError(f"Unsupported resolution of {seconds}s")
                for sub in range(seconds // SLOT_SECONDS):
                    slots.setdefault(start + timedelta(seconds=sub * SLOT_SECONDS), {})[column] = price
        if skipped:
            logger.warning("%s %s: TimeSeries without a column skipped: %s",
                           self.country_code, self.process_type, sorted(skipped, key=str))
        return [
            {
                "delivery_datetime": start,
                "trade_date": self.convert_to_local_time(start).date(),
                "period": self.calculate_period_number(start),
                "time_interval": self.format_time_interval(start, 15),
                "area_id": self.area_id,
                "country_code": self.country_code,
                "prices": prices,
            }
            for start, prices in sorted(slots.items())
        ]


class AfrrPrice4sParser(A84ParserBase):
    """4-second aFRR prices (processType A67)."""

    def __init__(self, area_id: int, country_code: str) -> None:
        super().__init__()
        self.area_id = area_id
        self.country_code = country_code

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        rows: Dict[datetime, Dict[str, Optional[Decimal]]] = {}
        for series in ET.fromstring(xml_string).findall("{*}TimeSeries"):
            direction = DIRECTIONS.get(_text(series, "flowDirection.direction"))
            if _text(series, "businessType") != "A96" or direction is None:
                continue
            for start, seconds, price in self.expand(series):
                if seconds != 4:
                    raise ValueError(f"Expected 4-second prices, got {seconds}s")
                rows.setdefault(start, {})[direction] = price
        return [
            {
                "delivery_datetime": start,
                "area_id": self.area_id,
                "country_code": self.country_code,
                "trade_date": self.convert_to_local_time(start).date(),
                "afrr_up_price_eur": prices.get("up"),
                "afrr_down_price_eur": prices.get("down"),
            }
            for start, prices in sorted(rows.items())
        ]
