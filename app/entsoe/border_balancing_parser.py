"""Parsers for ENTSO-E cross-border balancing documents.

Three document families, all expanded into 15-minute slots keyed on the UTC start
instant (``delivery_datetime``):

``BorderLimitParser``      Capacity_MarketDocument, documentType A31 / businessType A26
                           (balancing border capacity limitations, IFs 4.3 & 4.4).
                           One TimeSeries per direction, ``in_Domain`` /
                           ``out_Domain``, unit MAW (= MW). Power, compressed
                           curve (curveType A03).
``BorderVolumeParser``     Balancing_MarketDocument, documentType A30 / businessType
                           A45 (netted and exchanged volumes per border). One
                           TimeSeries, ``acquiring_Domain`` / ``connecting_Domain``,
                           unit MWH per interval, curveType A01 (every point).
``AreaVolumeParser``       Balancing_MarketDocument, documentType B17 / businessType
                           B09 (netted and exchanged volumes per area). Two
                           TimeSeries against the European platform: the one with
                           ``acquiring_Domain`` = area is the area's import, the one
                           with ``connecting_Domain`` = area its export. Unit MWH,
                           PT15M (netting, mFRR) or PT4S (aFRR).

Period expansion (shared, :class:`BalancingParserBase`):

* A ``Period`` is walked position by position from 1 to the length implied by its
  ``timeInterval`` and ``resolution``. Only for ``curveType`` A03 a missing Point
  repeats the previous value (forward fill up to the end of the Period); a
  genuinely absent leading Point stays absent. For A01 (and unknown curves) a
  missing Point is simply not reported - sparse documents are not filled with 0.
* Documents may hold many Periods per TimeSeries (the area and border volume
  documents send one Period per run of reported intervals, with gaps between).
* PT15M is the native slot. PT60M is split into four slots (a power value is
  repeated, an energy value is divided by four). PT4S points are aggregated into
  their 15-minute slot (energy: summed, power: averaged) and a slot is emitted
  only when all 225 points of it are present, so a partial running interval is
  never stored; the next run re-fetches and completes it.
* The unit decides power vs energy: MAW is power, MWH is energy. Any other unit
  raises ``ValueError``.

``period`` counts 15-minute slots from Prague local midnight (1..92/96/100) and
``time_interval`` is the descriptive local label, exactly as in
``CrossBorderFlowsParser`` / ``BorderFlowParser``; ``delivery_datetime`` is the
authoritative key and stays unique on the repeated fall-back hour.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from entsoe.parsers import BaseParser

NATIVE_INTERVAL_SECONDS = 900
NATIVE_INTERVAL_MINUTES = 15

POWER_UNITS = {"MAW"}
ENERGY_UNITS = {"MWH"}

ENERGY_QUANTUM = Decimal("0.0001")

logger = logging.getLogger(__name__)

_DURATION = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")


def resolution_seconds(resolution: str) -> int:
    """ISO 8601 time duration (PT4S, PT15M, PT60M, PT1H) in seconds."""
    match = _DURATION.match(resolution.strip())
    if not match or not any(match.groups()):
        raise ValueError(f"Unsupported resolution {resolution!r}")
    hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def _text(element: Optional[ET.Element]) -> Optional[str]:
    return element.text.strip() if element is not None and element.text else None


class BalancingParserBase(BaseParser):
    """Shared Period expansion and row construction."""

    def __init__(self) -> None:
        super().__init__()
        self.skipped_series = 0

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        with open(xml_file_path, "r", encoding="utf-8") as f:
            return self.parse_xml_content(f.read())

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    # ------------------------------------------------------------- helpers

    @staticmethod
    def _unit_kind(series: ET.Element) -> str:
        """'power' (MAW) or 'energy' (MWH) from the TimeSeries measurement unit."""
        unit = _text(series.find("{*}measurement_Unit.name"))
        if unit is None:
            unit = _text(series.find("{*}quantity_Measure_Unit.name"))
        if unit in POWER_UNITS:
            return "power"
        if unit in ENERGY_UNITS:
            return "energy"
        raise ValueError(f"Unexpected measurement unit {unit!r}")

    def _slot_fields(self, delivery_datetime: datetime) -> Dict[str, Any]:
        local_dt = self.convert_to_local_time(delivery_datetime)
        return {
            "delivery_datetime": delivery_datetime,
            "trade_date": local_dt.date(),
            "period": self.calculate_period_number(delivery_datetime),
            "time_interval": self.format_time_interval(delivery_datetime, NATIVE_INTERVAL_MINUTES),
        }

    def expand_series(self, series: ET.Element) -> Dict[datetime, Decimal]:
        """Expand every Period of a TimeSeries into ``{slot start (UTC): value}``.

        Power values stay as published (MW); energy values are MWh per 15-minute
        slot (4-second points summed, hourly points divided by four).
        """
        kind = self._unit_kind(series)
        curve_type = _text(series.find("{*}curveType"))
        slots: Dict[datetime, Tuple[Decimal, int]] = {}
        for period in series.findall("{*}Period"):
            self._expand_period(period, kind, curve_type == "A03", slots)
        return {start: value for start, (value, _res) in slots.items()}

    def _expand_period(
        self,
        period: ET.Element,
        kind: str,
        forward_fill: bool,
        slots: Dict[datetime, Tuple[Decimal, int]],
    ) -> None:
        interval = period.find("{*}timeInterval")
        period_start = self.parse_timestamp(interval.find("{*}start").text)
        period_end = self.parse_timestamp(interval.find("{*}end").text)
        resolution_el = period.find("{*}resolution")
        res_seconds = resolution_seconds(resolution_el.text if resolution_el is not None else "PT15M")

        total_seconds = int((period_end - period_start).total_seconds())
        if res_seconds <= 0 or total_seconds % res_seconds != 0:
            raise ValueError(
                f"Period {period_start:%Y-%m-%dT%H:%MZ}..{period_end:%Y-%m-%dT%H:%MZ} is not a whole "
                f"number of {res_seconds}s intervals"
            )
        num_points = total_seconds // res_seconds

        points: Dict[int, Optional[Decimal]] = {}
        for point in period.findall("{*}Point"):
            position = int(point.find("{*}position").text)
            quantity = _text(point.find("{*}quantity"))
            points[position] = Decimal(quantity) if quantity is not None else None

        # position -> value, forward-filled only for compressed (A03) curves.
        values: List[Tuple[int, Decimal]] = []
        last: Optional[Decimal] = None
        for idx in range(num_points):
            position = idx + 1
            if position in points:
                last = points[position]
                current = last
            elif forward_fill:
                current = last
            else:
                current = None
            if current is not None:
                values.append((idx, current))

        if res_seconds >= NATIVE_INTERVAL_SECONDS:
            if res_seconds % NATIVE_INTERVAL_SECONDS != 0:
                raise ValueError(f"Unsupported resolution of {res_seconds}s")
            parts = res_seconds // NATIVE_INTERVAL_SECONDS
            for idx, value in values:
                share = value / parts if (kind == "energy" and parts > 1) else value
                start = period_start + timedelta(seconds=idx * res_seconds)
                for sub in range(parts):
                    self._put(slots, start + timedelta(seconds=sub * NATIVE_INTERVAL_SECONDS),
                              share, res_seconds)
            return

        # Finer than 15 minutes: aggregate into complete 15-minute slots.
        if NATIVE_INTERVAL_SECONDS % res_seconds != 0:
            raise ValueError(f"Unsupported resolution of {res_seconds}s")
        per_slot = NATIVE_INTERVAL_SECONDS // res_seconds
        sums: Dict[int, Decimal] = {}
        counts: Dict[int, int] = {}
        for idx, value in values:
            slot_index = idx // per_slot
            sums[slot_index] = sums.get(slot_index, Decimal(0)) + value
            counts[slot_index] = counts.get(slot_index, 0) + 1
        for slot_index, total in sums.items():
            if counts[slot_index] != per_slot:
                continue
            aggregated = total if kind == "energy" else total / per_slot
            start = period_start + timedelta(seconds=slot_index * NATIVE_INTERVAL_SECONDS)
            self._put(slots, start, aggregated.quantize(ENERGY_QUANTUM), res_seconds)

    @staticmethod
    def _put(slots: Dict[datetime, Tuple[Decimal, int]], start: datetime, value: Decimal, res_seconds: int) -> None:
        """Store a slot; a finer (or equal, later) source resolution wins an overlap."""
        current = slots.get(start)
        if current is None or res_seconds <= current[1]:
            slots[start] = (value, res_seconds)


class BorderLimitParser(BalancingParserBase):
    """A31 balancing border capacity limit for one direction: ``out`` -> ``in``."""

    def __init__(self, from_area_id: int, from_eic: str, to_area_id: int, to_eic: str) -> None:
        super().__init__()
        self.from_area_id = from_area_id
        self.from_eic = from_eic
        self.to_area_id = to_area_id
        self.to_eic = to_eic

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        """One dict per slot: slot fields, ``from_area_id``, ``to_area_id``, ``value`` (MW)."""
        root = ET.fromstring(xml_string)
        merged: Dict[datetime, Decimal] = {}
        for series in root.findall(".//{*}TimeSeries"):
            in_eic = _text(series.find("{*}in_Domain.mRID"))
            out_eic = _text(series.find("{*}out_Domain.mRID"))
            if in_eic != self.to_eic or out_eic != self.from_eic:
                self.skipped_series += 1
                logger.debug("Skipping limit TimeSeries %s -> %s (expected %s -> %s)",
                             out_eic, in_eic, self.from_eic, self.to_eic)
                continue
            if self._unit_kind(series) != "power":
                raise ValueError("Balancing limit TimeSeries is not a power (MAW) series")
            merged.update(self.expand_series(series))
        return [
            {**self._slot_fields(slot), "from_area_id": self.from_area_id,
             "to_area_id": self.to_area_id, "value": merged[slot]}
            for slot in sorted(merged)
        ]


class BorderVolumeParser(BalancingParserBase):
    """A30 netted / exchanged energy for one direction: ``connecting`` (from) -> ``acquiring`` (to)."""

    def __init__(self, from_area_id: int, from_eic: str, to_area_id: int, to_eic: str) -> None:
        super().__init__()
        self.from_area_id = from_area_id
        self.from_eic = from_eic
        self.to_area_id = to_area_id
        self.to_eic = to_eic

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        """One dict per slot: slot fields, ``from_area_id``, ``to_area_id``, ``value`` (MWh)."""
        root = ET.fromstring(xml_string)
        merged: Dict[datetime, Decimal] = {}
        for series in root.findall(".//{*}TimeSeries"):
            acquiring = _text(series.find("{*}acquiring_Domain.mRID"))
            connecting = _text(series.find("{*}connecting_Domain.mRID"))
            if acquiring != self.to_eic or connecting != self.from_eic:
                self.skipped_series += 1
                logger.debug("Skipping volume TimeSeries connecting %s -> acquiring %s (expected %s -> %s)",
                             connecting, acquiring, self.from_eic, self.to_eic)
                continue
            if self._unit_kind(series) != "energy":
                raise ValueError("Balancing volume TimeSeries is not an energy (MWH) series")
            merged.update(self.expand_series(series))
        return [
            {**self._slot_fields(slot), "from_area_id": self.from_area_id,
             "to_area_id": self.to_area_id, "value": merged[slot]}
            for slot in sorted(merged)
        ]


class AreaVolumeParser(BalancingParserBase):
    """B17 netted / exchanged energy of one area against the European platform."""

    def __init__(self, area_id: int, area_eic: str, country_code: str) -> None:
        super().__init__()
        self.area_id = area_id
        self.area_eic = area_eic
        self.country_code = country_code

    def parse_xml_content(self, xml_string: str) -> List[Dict[str, Any]]:
        """One dict per slot: slot fields, ``area_id``, ``country_code``,
        ``import_mwh``, ``export_mwh`` (Decimal or None) and ``doc_status``."""
        root = ET.fromstring(xml_string)
        doc_status = _text(root.find("{*}docStatus/{*}value"))
        imports: Dict[datetime, Decimal] = {}
        exports: Dict[datetime, Decimal] = {}
        for series in root.findall(".//{*}TimeSeries"):
            acquiring = _text(series.find("{*}acquiring_Domain.mRID"))
            connecting = _text(series.find("{*}connecting_Domain.mRID"))
            if acquiring == self.area_eic and connecting != self.area_eic:
                target = imports
            elif connecting == self.area_eic and acquiring != self.area_eic:
                target = exports
            else:
                self.skipped_series += 1
                logger.debug("Skipping area TimeSeries acquiring %s / connecting %s (area %s)",
                             acquiring, connecting, self.area_eic)
                continue
            if self._unit_kind(series) != "energy":
                raise ValueError("Area balancing TimeSeries is not an energy (MWH) series")
            target.update(self.expand_series(series))
        rows = []
        for slot in sorted(set(imports) | set(exports)):
            rows.append({
                **self._slot_fields(slot),
                "area_id": self.area_id,
                "country_code": self.country_code,
                "import_mwh": imports.get(slot),
                "export_mwh": exports.get(slot),
                "doc_status": doc_status,
            })
        return rows
