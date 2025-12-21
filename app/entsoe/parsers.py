#!/usr/bin/env python3
"""
ENTSO-E XML Parsers for different document types.

Provides class-based parsers for:
- A85: Imbalance Prices
- A86: Imbalance Volumes
- A65: Actual/Forecast Load
- A75: Generation per Type

All parsers inherit from BaseParser which provides common utilities for
timestamp parsing, timezone conversion, and period calculation.
"""

import sys
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any
import zoneinfo


# Standardized data directory path
DATA_DIR = Path(__file__).parent / "data"


class BaseParser(ABC):
    """Base class for all ENTSO-E XML parsers.

    Provides common utilities for:
    - Timestamp parsing (ISO 8601 → datetime)
    - Timezone conversion (UTC → Prague)
    - Period number calculation (1-96)
    - Time interval formatting
    """

    def __init__(self):
        """Initialize parser with Prague timezone."""
        self.prague_tz = zoneinfo.ZoneInfo("Europe/Prague")
        self.data: List[Dict[str, Any]] = []

    def parse_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse ISO 8601 timestamp (always in UTC from ENTSO-E).

        Args:
            timestamp_str: ISO 8601 timestamp string

        Returns:
            Timezone-aware datetime in UTC
        """
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

    def convert_to_local_time(self, dt_utc: datetime) -> datetime:
        """
        Convert UTC datetime to Prague local time (CET/CEST).

        This is critical for alignment with OTE-CR data which uses Czech local time.
        Czech Republic uses CET (UTC+1) in winter and CEST (UTC+2) in summer.

        Args:
            dt_utc: UTC datetime (timezone-aware)

        Returns:
            datetime in Prague timezone
        """
        return dt_utc.astimezone(self.prague_tz)

    def calculate_period_number(self, dt: datetime) -> int:
        """
        Calculate period number (1-96) for a given datetime.

        Note: The datetime should be in Prague local time to align with OTE-CR periods.
        Period 1 = 00:00-00:15 CET/CEST
        Period 96 = 23:45-00:00 CET/CEST

        Args:
            dt: Datetime in local timezone

        Returns:
            Period number (1-96)
        """
        return (dt.hour * 4) + (dt.minute // 15) + 1

    def format_time_interval(self, start_dt: datetime, resolution_minutes: int) -> str:
        """
        Format time interval as HH:MM-HH:MM.

        Args:
            start_dt: Start datetime
            resolution_minutes: Interval resolution in minutes

        Returns:
            Formatted time interval string
        """
        end_dt = start_dt + timedelta(minutes=resolution_minutes)
        return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"

    def calculate_time_interval_from_period(self, period_num: int) -> str:
        """
        Calculate time interval string from period number.

        Args:
            period_num: Period number (1-96)

        Returns:
            Time interval string (HH:MM-HH:MM)
        """
        minutes_from_midnight = (period_num - 1) * 15
        hours = minutes_from_midnight // 60
        minutes = minutes_from_midnight % 60
        start_time = f"{hours:02d}:{minutes:02d}"

        end_minutes = minutes_from_midnight + 15
        end_hours = end_minutes // 60
        end_mins = end_minutes % 60
        end_time = f"{end_hours:02d}:{end_mins:02d}"

        return f"{start_time}-{end_time}"

    def get_resolution_minutes(self, resolution_str: str) -> int:
        """
        Parse resolution string to minutes.

        Args:
            resolution_str: ISO 8601 duration (e.g., PT15M, PT60M)

        Returns:
            Resolution in minutes
        """
        if 'M' in resolution_str:
            return int(resolution_str[2:-1])
        elif 'H' in resolution_str:
            return int(resolution_str[2:-1]) * 60
        return 60

    @abstractmethod
    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """
        Parse XML file and return structured data.

        Args:
            xml_file_path: Path to XML file

        Returns:
            List of parsed records
        """
        pass


class ImbalanceParser(BaseParser):
    """Parser for ENTSO-E Imbalance data combining A85 (prices) and A86 (volumes).

    Processes:
    - A85: Imbalance Prices with financial components (scarcity, incentive, neutrality)
    - A86: Imbalance Volumes with flow direction
    """

    def __init__(self):
        super().__init__()
        self.prices_data: Dict[tuple, Dict] = {}
        self.volumes_data: Dict[tuple, Dict] = {}
        self.combined_data: List[Dict] = []

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """Not used directly - use parse_prices_xml and parse_volumes_xml."""
        raise NotImplementedError(
            "Use parse_prices_xml() and parse_volumes_xml() separately"
        )

    def parse_prices_xml(self, xml_file_path: str) -> None:
        """
        Parse A85 (Imbalance Prices) XML file using interval-first approach.

        Args:
            xml_file_path: Path to A85 XML file
        """
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Get document status
        doc_status_elem = root.find('.//{*}docStatus/{*}value')
        doc_status = doc_status_elem.text if doc_status_elem is not None else 'A01'

        # Process each TimeSeries
        for timeseries in root.findall('.//{*}TimeSeries'):
            # Process ALL Period elements
            for period in timeseries.findall('{*}Period'):
                self._process_prices_period(period, doc_status)

    def _process_prices_period(self, period: ET.Element, doc_status: str) -> None:
        """Process a single Period element from A85 XML."""
        # Get time interval
        time_interval = period.find('{*}timeInterval')
        start_elem = time_interval.find('{*}start')
        end_elem = time_interval.find('{*}end')
        period_start = self.parse_timestamp(start_elem.text)
        period_end = self.parse_timestamp(end_elem.text)

        # Get resolution
        resolution_elem = period.find('{*}resolution')
        resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
        resolution_minutes = self.get_resolution_minutes(resolution)

        # Calculate intervals
        period_duration_minutes = int((period_end - period_start).total_seconds() / 60)
        num_intervals = period_duration_minutes // resolution_minutes

        # Build position -> Point mapping
        points_by_position = {}
        for point in period.findall('{*}Point'):
            position = int(point.find('{*}position').text)
            price_amount = float(point.find('{*}imbalance_Price.amount').text)
            category_elem = point.find('{*}imbalance_Price.category')
            category = category_elem.text if category_elem is not None else None

            # Get Financial_Price components
            financial_prices = {}
            for fp in point.findall('{*}Financial_Price'):
                amount = float(fp.find('{*}amount').text)
                price_type = fp.find('{*}priceDescriptor.type').text
                financial_prices[price_type] = amount

            points_by_position[position] = {
                'price_amount': price_amount,
                'category': category,
                'financial_prices': financial_prices
            }

        # Process intervals with forward-filling
        last_values = {
            'A04': {'price_amount': 0.0, 'financial_prices': {'A01': 0.0, 'A02': 0.0, 'A03': 0.0}},
            'A05': {'price_amount': 0.0, 'financial_prices': {'A01': 0.0, 'A02': 0.0, 'A03': 0.0}}
        }

        for interval_idx in range(num_intervals):
            point_time_utc = period_start + timedelta(minutes=interval_idx * resolution_minutes)
            point_time_local = self.convert_to_local_time(point_time_utc)
            trade_date = point_time_local.date()
            period_num = self.calculate_period_number(point_time_local)
            time_interval_str = self.format_time_interval(point_time_local, resolution_minutes)

            position = interval_idx + 1

            key = (trade_date, period_num)
            if key not in self.prices_data:
                self.prices_data[key] = {
                    'trade_date': trade_date,
                    'period': period_num,
                    'time_interval': time_interval_str,
                    'status': doc_status,
                    'pos_imb_price_czk_mwh': 0.0,
                    'pos_imb_scarcity_czk_mwh': 0.0,
                    'pos_imb_incentive_czk_mwh': 0.0,
                    'pos_imb_financial_neutrality_czk_mwh': 0.0,
                    'neg_imb_price_czk_mwh': 0.0,
                    'neg_imb_scarcity_czk_mwh': 0.0,
                    'neg_imb_incentive_czk_mwh': 0.0,
                    'neg_imb_financial_neutrality_czk_mwh': 0.0,
                }

            # Update last_values if Point exists
            if position in points_by_position:
                point_data = points_by_position[position]
                category = point_data['category']
                last_values[category] = {
                    'price_amount': point_data['price_amount'],
                    'financial_prices': point_data['financial_prices']
                }

            # Apply values
            if last_values['A04']['price_amount'] != 0.0:
                imb_price = last_values['A04']['price_amount']
                scarcity = last_values['A04']['financial_prices'].get('A01', 0.0)
                incentive = last_values['A04']['financial_prices'].get('A02', 0.0)
                neutrality = last_values['A04']['financial_prices'].get('A03', 0.0)
            elif last_values['A05']['price_amount'] != 0.0:
                imb_price = last_values['A05']['price_amount']
                scarcity = last_values['A05']['financial_prices'].get('A01', 0.0)
                incentive = last_values['A05']['financial_prices'].get('A02', 0.0)
                neutrality = last_values['A05']['financial_prices'].get('A03', 0.0)
            else:
                imb_price = scarcity = incentive = neutrality = 0.0

            self.prices_data[key]['pos_imb_price_czk_mwh'] = imb_price
            self.prices_data[key]['pos_imb_scarcity_czk_mwh'] = scarcity
            self.prices_data[key]['pos_imb_incentive_czk_mwh'] = incentive
            self.prices_data[key]['pos_imb_financial_neutrality_czk_mwh'] = neutrality
            self.prices_data[key]['neg_imb_price_czk_mwh'] = imb_price
            self.prices_data[key]['neg_imb_scarcity_czk_mwh'] = scarcity
            self.prices_data[key]['neg_imb_incentive_czk_mwh'] = incentive
            self.prices_data[key]['neg_imb_financial_neutrality_czk_mwh'] = neutrality

    def parse_volumes_xml(self, xml_file_path: str) -> None:
        """
        Parse A86 (Imbalance Volumes) XML file using interval-first approach.

        Args:
            xml_file_path: Path to A86 XML file
        """
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        for timeseries in root.findall('.//{*}TimeSeries'):
            flow_direction_elem = timeseries.find('{*}flowDirection.direction')
            flow_direction = flow_direction_elem.text if flow_direction_elem is not None else None

            situation_map = {'A01': 'surplus', 'A02': 'deficit', 'A03': 'balanced'}
            situation = situation_map.get(flow_direction, 'unknown')

            for period in timeseries.findall('{*}Period'):
                self._process_volumes_period(period, situation)

    def _process_volumes_period(self, period: ET.Element, situation: str) -> None:
        """Process a single Period element from A86 XML."""
        time_interval = period.find('{*}timeInterval')
        start_elem = time_interval.find('{*}start')
        end_elem = time_interval.find('{*}end')
        period_start = self.parse_timestamp(start_elem.text)
        period_end = self.parse_timestamp(end_elem.text)

        resolution_elem = period.find('{*}resolution')
        resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
        resolution_minutes = self.get_resolution_minutes(resolution)

        period_duration_minutes = int((period_end - period_start).total_seconds() / 60)
        num_intervals = period_duration_minutes // resolution_minutes

        points_by_position = {}
        for point in period.findall('{*}Point'):
            position = int(point.find('{*}position').text)
            quantity_elem = point.find('{*}quantity')
            quantity = float(quantity_elem.text) if quantity_elem is not None else 0.0
            secondary_elem = point.find('{*}secondaryQuantity')
            difference = float(secondary_elem.text) if secondary_elem is not None else None

            points_by_position[position] = {'quantity': quantity, 'difference': difference}

        last_quantity = 0.0
        last_difference = None

        for interval_idx in range(num_intervals):
            point_time_utc = period_start + timedelta(minutes=interval_idx * resolution_minutes)
            point_time_local = self.convert_to_local_time(point_time_utc)
            trade_date = point_time_local.date()
            period_num = self.calculate_period_number(point_time_local)

            position = interval_idx + 1
            if position in points_by_position:
                last_quantity = points_by_position[position]['quantity']
                last_difference = points_by_position[position]['difference']

            key = (trade_date, period_num)
            self.volumes_data[key] = {
                'imbalance_mwh': last_quantity,
                'difference_mwh': last_difference,
                'situation': situation
            }

    def combine_data(self) -> List[Dict]:
        """
        Combine prices and volumes data.

        Returns:
            List of combined records
        """
        all_keys = set(self.prices_data.keys()) | set(self.volumes_data.keys())

        for key in sorted(all_keys):
            trade_date, period_num = key

            if key in self.prices_data:
                record = self.prices_data[key].copy()
            else:
                record = {
                    'trade_date': trade_date,
                    'period': period_num,
                    'time_interval': self.calculate_time_interval_from_period(period_num),
                    'status': 'A01',
                    'pos_imb_price_czk_mwh': 0.0,
                    'pos_imb_scarcity_czk_mwh': 0.0,
                    'pos_imb_incentive_czk_mwh': 0.0,
                    'pos_imb_financial_neutrality_czk_mwh': 0.0,
                    'neg_imb_price_czk_mwh': 0.0,
                    'neg_imb_scarcity_czk_mwh': 0.0,
                    'neg_imb_incentive_czk_mwh': 0.0,
                    'neg_imb_financial_neutrality_czk_mwh': 0.0,
                }

            if key in self.volumes_data:
                record.update(self.volumes_data[key])
            else:
                record['imbalance_mwh'] = 0.0
                record['difference_mwh'] = None
                record['situation'] = 'surplus'

            self.combined_data.append(record)

        return self.combined_data


class LoadParser(BaseParser):
    """Parser for ENTSO-E Load data (A65).

    Processes both actual load and day-ahead forecast.
    """

    def __init__(self):
        super().__init__()
        self.actual_data: Dict[tuple, Dict] = {}
        self.forecast_data: Dict[tuple, Dict] = {}
        self.combined_data: List[Dict] = []

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """
        Parse A65 (Load) XML file.

        Args:
            xml_file_path: Path to A65 XML file

        Returns:
            List of load records
        """
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        for timeseries in root.findall('.//{*}TimeSeries'):
            # Determine if this is actual or forecast
            out_bz_elem = timeseries.find('{*}outBiddingZone_Domain.mRID')
            in_bz_elem = timeseries.find('{*}inBiddingZone_Domain.mRID')

            for period in timeseries.findall('{*}Period'):
                self._process_load_period(period)

        return self.data

    def _process_load_period(self, period: ET.Element) -> None:
        """Process a single Period element from A65 XML."""
        time_interval = period.find('{*}timeInterval')
        start_elem = time_interval.find('{*}start')
        end_elem = time_interval.find('{*}end')
        period_start = self.parse_timestamp(start_elem.text)
        period_end = self.parse_timestamp(end_elem.text)

        resolution_elem = period.find('{*}resolution')
        resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
        resolution_minutes = self.get_resolution_minutes(resolution)

        period_duration_minutes = int((period_end - period_start).total_seconds() / 60)
        num_intervals = period_duration_minutes // resolution_minutes

        for point in period.findall('{*}Point'):
            position = int(point.find('{*}position').text)
            quantity_elem = point.find('{*}quantity')
            quantity = float(quantity_elem.text) if quantity_elem is not None else None

            interval_idx = position - 1
            point_time_utc = period_start + timedelta(minutes=interval_idx * resolution_minutes)
            point_time_local = self.convert_to_local_time(point_time_utc)
            trade_date = point_time_local.date()
            period_num = self.calculate_period_number(point_time_local)
            time_interval_str = self.format_time_interval(point_time_local, resolution_minutes)

            self.data.append({
                'trade_date': trade_date,
                'period': period_num,
                'time_interval': time_interval_str,
                'load_mw': quantity
            })

    def parse_actual_load_xml(self, xml_file_path: str) -> None:
        """Parse actual load XML (A65 with processType=A16)."""
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        for timeseries in root.findall('.//{*}TimeSeries'):
            for period in timeseries.findall('{*}Period'):
                self._process_typed_load_period(period, is_forecast=False)

    def parse_forecast_load_xml(self, xml_file_path: str) -> None:
        """Parse forecast load XML (A65 with processType=A01)."""
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        for timeseries in root.findall('.//{*}TimeSeries'):
            for period in timeseries.findall('{*}Period'):
                self._process_typed_load_period(period, is_forecast=True)

    def _process_typed_load_period(self, period: ET.Element, is_forecast: bool) -> None:
        """Process load period distinguishing actual vs forecast."""
        time_interval = period.find('{*}timeInterval')
        start_elem = time_interval.find('{*}start')
        end_elem = time_interval.find('{*}end')
        period_start = self.parse_timestamp(start_elem.text)
        period_end = self.parse_timestamp(end_elem.text)

        resolution_elem = period.find('{*}resolution')
        resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
        resolution_minutes = self.get_resolution_minutes(resolution)

        for point in period.findall('{*}Point'):
            position = int(point.find('{*}position').text)
            quantity_elem = point.find('{*}quantity')
            quantity = float(quantity_elem.text) if quantity_elem is not None else None

            interval_idx = position - 1
            point_time_utc = period_start + timedelta(minutes=interval_idx * resolution_minutes)
            point_time_local = self.convert_to_local_time(point_time_utc)
            trade_date = point_time_local.date()
            period_num = self.calculate_period_number(point_time_local)
            time_interval_str = self.format_time_interval(point_time_local, resolution_minutes)

            key = (trade_date, period_num)
            target = self.forecast_data if is_forecast else self.actual_data

            if key not in target:
                target[key] = {
                    'trade_date': trade_date,
                    'period': period_num,
                    'time_interval': time_interval_str,
                    'load_mw': quantity
                }
            else:
                target[key]['load_mw'] = quantity

    def combine_data(self) -> List[Dict]:
        """Combine actual and forecast load data."""
        all_keys = set(self.actual_data.keys()) | set(self.forecast_data.keys())

        for key in sorted(all_keys):
            trade_date, period_num = key
            record = {
                'trade_date': trade_date,
                'period': period_num,
                'time_interval': self.calculate_time_interval_from_period(period_num),
                'actual_load_mw': None,
                'forecast_load_mw': None
            }

            if key in self.actual_data:
                record['actual_load_mw'] = self.actual_data[key].get('load_mw')
                record['time_interval'] = self.actual_data[key].get('time_interval', record['time_interval'])

            if key in self.forecast_data:
                record['forecast_load_mw'] = self.forecast_data[key].get('load_mw')

            self.combined_data.append(record)

        return self.combined_data


class GenerationParser(BaseParser):
    """Parser for ENTSO-E Generation per Type data (A75) - Wide Format.

    Aggregates PSR types into wide-format columns per timestamp.

    PSR Type to Column Mapping:
    - B14 → gen_nuclear_mw
    - B02 + B05 → gen_coal_mw (Brown coal/Lignite + Hard coal)
    - B04 → gen_gas_mw
    - B16 → gen_solar_mw
    - B19 → gen_wind_mw
    - B10 → gen_hydro_pumped_mw
    - B01 → gen_biomass_mw
    - B11 + B12 → gen_hydro_other_mw (Run-of-river + Reservoir)

    Resolution Priority: PT15M > PT60M (if both present, use 15-minute data)
    Missing Data: Defaults to 0.0
    """

    PSR_TYPES = {
        'B01': 'Biomass',
        'B02': 'Fossil Brown coal/Lignite',
        'B03': 'Fossil Coal-derived gas',
        'B04': 'Fossil Gas',
        'B05': 'Fossil Hard coal',
        'B06': 'Fossil Oil',
        'B09': 'Geothermal',
        'B10': 'Hydro Pumped Storage',
        'B11': 'Hydro Run-of-river and poundage',
        'B12': 'Hydro Water Reservoir',
        'B14': 'Nuclear',
        'B15': 'Other renewable',
        'B16': 'Solar',
        'B17': 'Waste',
        'B18': 'Wind Offshore',
        'B19': 'Wind Onshore',
        'B20': 'Other',
    }

    # PSR type to column mapping for wide format
    PSR_TO_COLUMN = {
        'B14': 'gen_nuclear_mw',
        'B02': 'gen_coal_mw',
        'B05': 'gen_coal_mw',
        'B04': 'gen_gas_mw',
        'B16': 'gen_solar_mw',
        'B19': 'gen_wind_mw',
        'B10': 'gen_hydro_pumped_mw',
        'B01': 'gen_biomass_mw',
        'B11': 'gen_hydro_other_mw',
        'B12': 'gen_hydro_other_mw',
    }

    # All wide-format columns
    WIDE_COLUMNS = [
        'gen_nuclear_mw', 'gen_coal_mw', 'gen_gas_mw', 'gen_solar_mw',
        'gen_wind_mw', 'gen_hydro_pumped_mw', 'gen_biomass_mw', 'gen_hydro_other_mw'
    ]

    def __init__(self):
        super().__init__()
        # Intermediate storage: key=(trade_date, period), value={column: (value, resolution)}
        self._wide_data: Dict[tuple, Dict[str, tuple]] = {}
        import logging
        self.logger = logging.getLogger(__name__)

    def parse_xml(self, xml_file_path: str) -> List[Dict[str, Any]]:
        """
        Parse A75 (Generation per Type) XML file into wide-format records.

        Args:
            xml_file_path: Path to A75 XML file

        Returns:
            List of wide-format generation records (one row per timestamp)
        """
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        for timeseries in root.findall('.//{*}TimeSeries'):
            # Get PSR type from MktPSRType
            psr_type_elem = timeseries.find('.//{*}MktPSRType/{*}psrType')
            psr_type = psr_type_elem.text if psr_type_elem is not None else 'B20'

            # Check unit symbol (should be MAW for MW)
            unit_elem = timeseries.find('.//{*}quantity_Measure_Unit.name')
            if unit_elem is not None and unit_elem.text != 'MAW':
                self.logger.warning(
                    f"Unexpected UnitSymbol '{unit_elem.text}' for PSR type {psr_type}. "
                    f"Expected 'MAW'. Data may need conversion."
                )

            for period in timeseries.findall('{*}Period'):
                self._process_generation_period(period, psr_type)

        # Convert intermediate data to final wide-format records
        return self._aggregate_to_wide_format()

    def _process_generation_period(self, period: ET.Element, psr_type: str) -> None:
        """Process a single Period element from A75 XML."""
        time_interval = period.find('{*}timeInterval')
        start_elem = time_interval.find('{*}start')
        end_elem = time_interval.find('{*}end')
        period_start = self.parse_timestamp(start_elem.text)
        period_end = self.parse_timestamp(end_elem.text)

        resolution_elem = period.find('{*}resolution')
        resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
        resolution_minutes = self.get_resolution_minutes(resolution)

        # Get target column for this PSR type
        column = self.PSR_TO_COLUMN.get(psr_type)
        if column is None:
            # Unmapped PSR type, skip
            return

        for point in period.findall('{*}Point'):
            position = int(point.find('{*}position').text)
            quantity_elem = point.find('{*}quantity')
            quantity = float(quantity_elem.text) if quantity_elem is not None else 0.0

            interval_idx = position - 1
            point_time_utc = period_start + timedelta(minutes=interval_idx * resolution_minutes)
            point_time_local = self.convert_to_local_time(point_time_utc)
            trade_date = point_time_local.date()
            period_num = self.calculate_period_number(point_time_local)
            time_interval_str = self.format_time_interval(point_time_local, resolution_minutes)

            key = (trade_date, period_num)

            # Initialize key if not exists
            if key not in self._wide_data:
                self._wide_data[key] = {
                    'time_interval': time_interval_str,
                    'columns': {}
                }

            # Resolution priority: PT15M (15) > PT60M (60)
            # Lower resolution_minutes = higher priority
            current = self._wide_data[key]['columns'].get(column)
            if current is None:
                # First value for this column
                self._wide_data[key]['columns'][column] = (quantity, resolution_minutes)
            else:
                existing_value, existing_res = current
                if resolution_minutes < existing_res:
                    # New data has finer resolution, replace
                    self._wide_data[key]['columns'][column] = (quantity, resolution_minutes)
                elif resolution_minutes == existing_res:
                    # Same resolution, aggregate (sum for columns that combine PSR types)
                    self._wide_data[key]['columns'][column] = (
                        existing_value + quantity, resolution_minutes
                    )
                # else: coarser resolution, ignore

    def _aggregate_to_wide_format(self) -> List[Dict[str, Any]]:
        """Convert intermediate data to final wide-format records."""
        result = []

        for (trade_date, period_num), data in sorted(self._wide_data.items()):
            record = {
                'trade_date': trade_date,
                'period': period_num,
                'time_interval': data['time_interval'],
            }

            # Add all wide columns, defaulting to 0.0 for missing
            for col in self.WIDE_COLUMNS:
                col_data = data['columns'].get(col)
                record[col] = col_data[0] if col_data else 0.0

            result.append(record)

        return result


# Backward compatibility alias
ImbalanceDataParser = ImbalanceParser


if __name__ == '__main__':
    """Test the parsers."""
    import argparse

    parser = argparse.ArgumentParser(description="Parse ENTSO-E XML data")
    parser.add_argument('--prices', type=str, help='Path to prices XML file')
    parser.add_argument('--volumes', type=str, help='Path to volumes XML file')
    parser.add_argument('--load', type=str, help='Path to load XML file')
    parser.add_argument('--generation', type=str, help='Path to generation XML file')

    args = parser.parse_args()

    if args.prices and args.volumes:
        print("Testing ImbalanceParser...")
        imb_parser = ImbalanceParser()
        imb_parser.parse_prices_xml(args.prices)
        imb_parser.parse_volumes_xml(args.volumes)
        combined = imb_parser.combine_data()
        print(f"Parsed {len(combined)} imbalance records")

    if args.load:
        print("Testing LoadParser...")
        load_parser = LoadParser()
        data = load_parser.parse_xml(args.load)
        print(f"Parsed {len(data)} load records")

    if args.generation:
        print("Testing GenerationParser...")
        gen_parser = GenerationParser()
        data = gen_parser.parse_xml(args.generation)
        print(f"Parsed {len(data)} generation records")
