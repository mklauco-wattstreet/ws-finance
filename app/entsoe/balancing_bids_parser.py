"""Parser for ENTSO-E individual balancing energy bids (documentType A37, businessType B74)."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from decimal import Decimal
from typing import Any, Dict, List, Optional

from entsoe.parsers import BaseParser

DIRECTIONS = {"A01": "up", "A02": "down"}


def _text(element: Optional[ET.Element], path: str) -> Optional[str]:
    found = element.find(path) if element is not None else None
    return found.text.strip() if found is not None and found.text else None


class BalancingBidsParser(BaseParser):
    """One row per Bid_TimeSeries."""

    def __init__(self, area_id: int, country_code: str, process: str) -> None:
        super().__init__()
        self.area_id = area_id
        self.country_code = country_code
        self.process = process

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        with open(xml_file_path, "r", encoding="utf-8") as f:
            return self.parse_xml_content(f.read())

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        return [self._row(bid) for bid in ET.fromstring(xml_string).findall("{*}Bid_TimeSeries")]

    def _row(self, bid: ET.Element) -> Dict[str, Any]:
        mrid = _text(bid, "{*}mRID")
        periods = bid.findall("{*}Period")
        points = bid.findall("{*}Period/{*}Point")
        if len(periods) != 1 or len(points) != 1:
            raise ValueError(f"Bid {mrid}: expected one Period with one Point, got {len(periods)}/{len(points)}")
        start = self.parse_timestamp(_text(periods[0], "{*}timeInterval/{*}start"))
        end = self.parse_timestamp(_text(periods[0], "{*}timeInterval/{*}end"))
        quantity = _text(points[0], "{*}quantity.quantity")
        price = _text(points[0], "{*}energy_Price.amount")
        divisible = _text(bid, "{*}divisible")
        if _text(bid, "{*}original_MarketProduct.marketProductType") is not None:
            product = "specific"
        elif _text(bid, "{*}standard_MarketProduct.marketProductType") is not None:
            product = "standard"
        else:
            product = None
        return {
            "delivery_datetime": start,
            "area_id": self.area_id,
            "country_code": self.country_code,
            "process": self.process,
            "bid_mrid": mrid,
            "delivery_end": end,
            "trade_date": self.convert_to_local_time(start).date(),
            "period": self.calculate_period_number(start),
            "time_interval": self.format_time_interval(start, int((end - start).total_seconds() // 60)),
            "direction": DIRECTIONS[_text(bid, "{*}flowDirection.direction")],
            "product": product,
            "acquiring_eic": _text(bid, "{*}acquiring_Domain.mRID"),
            "quantity_mw": Decimal(quantity) if quantity is not None else None,
            "price_eur": Decimal(price) if price is not None else None,
            "divisible": None if divisible is None else divisible == "A01",
            "status": _text(bid, "{*}status/{*}value"),
        }
