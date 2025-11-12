#!/usr/bin/env python3
"""
Parse ENTSO-E Imbalance Prices (A85) and Volumes (A86) XML files.
Combines both datasets and formats them for database insertion.

This script parses XML files but does NOT connect to the database.
It generates SQL queries and displays data in markdown format.

Usage:
    python3 parse_imbalance_to_db.py <prices_xml> <volumes_xml>
    python3 parse_imbalance_to_db.py --period 202511100800 202511101200
"""

import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from collections import defaultdict


class ImbalanceDataParser:
    """Parser for ENTSO-E Imbalance data combining A85 (prices) and A86 (volumes)."""

    def __init__(self):
        self.prices_data = {}  # {(trade_date, period): {...}}
        self.volumes_data = {}  # {(trade_date, period): {...}}
        self.combined_data = []  # Final combined dataset

    def parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse ISO 8601 timestamp."""
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

    def calculate_period_number(self, dt: datetime) -> int:
        """Calculate period number (1-96) for a given datetime."""
        return (dt.hour * 4) + (dt.minute // 15) + 1

    def format_time_interval(self, start_dt: datetime, resolution_minutes: int) -> str:
        """Format time interval as HH:MM-HH:MM."""
        end_dt = start_dt + timedelta(minutes=resolution_minutes)
        return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"

    def parse_prices_xml(self, xml_file_path: str):
        """Parse A85 (Imbalance Prices) XML file."""
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Get document status
        doc_status_elem = root.find('.//{*}docStatus/{*}value')
        doc_status = doc_status_elem.text if doc_status_elem is not None else 'A01'

        # Process each TimeSeries
        for timeseries in root.findall('.//{*}TimeSeries'):
            # Process ALL Period elements (there can be multiple periods per TimeSeries)
            for period in timeseries.findall('{*}Period'):
                # Get time interval
                time_interval = period.find('{*}timeInterval')
                start_elem = time_interval.find('{*}start')
                end_elem = time_interval.find('{*}end')
                period_start = self.parse_timestamp(start_elem.text)
                period_end = self.parse_timestamp(end_elem.text)

                # Get resolution
                resolution_elem = period.find('{*}resolution')
                resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
                resolution_minutes = int(resolution[2:-1]) if 'M' in resolution else 60

                # Calculate how many intervals are in this Period
                period_duration_minutes = int((period_end - period_start).total_seconds() / 60)
                num_intervals = period_duration_minutes // resolution_minutes

                # Process each Point
                for point in period.findall('{*}Point'):
                    position = int(point.find('{*}position').text)

                    # Get price and category
                    price_amount = float(point.find('{*}imbalance_Price.amount').text)
                    category_elem = point.find('{*}imbalance_Price.category')
                    category = category_elem.text if category_elem is not None else None

                    # Get Financial_Price components
                    financial_prices = {}
                    for fp in point.findall('{*}Financial_Price'):
                        amount = float(fp.find('{*}amount').text)
                        price_type = fp.find('{*}priceDescriptor.type').text
                        financial_prices[price_type] = amount

                    # If there's only one Point but the Period spans multiple intervals,
                    # apply the same value to ALL intervals in the Period
                    if len(period.findall('{*}Point')) == 1 and num_intervals > 1:
                        # Apply to all intervals in this Period
                        for interval_idx in range(num_intervals):
                            point_time = period_start + timedelta(minutes=interval_idx * resolution_minutes)
                            trade_date = point_time.date()
                            period_num = self.calculate_period_number(point_time)
                            time_interval_str = self.format_time_interval(point_time, resolution_minutes)

                            # Initialize or update the key
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

                            # Store based on category
                            if category == 'A04':  # Excess balance (positive)
                                self.prices_data[key]['pos_imb_price_czk_mwh'] = price_amount
                                self.prices_data[key]['pos_imb_scarcity_czk_mwh'] = financial_prices.get('A01', 0.0)
                                self.prices_data[key]['pos_imb_incentive_czk_mwh'] = financial_prices.get('A02', 0.0)
                                self.prices_data[key]['pos_imb_financial_neutrality_czk_mwh'] = financial_prices.get('A03', 0.0)
                            elif category == 'A05':  # Insufficient balance (negative)
                                self.prices_data[key]['neg_imb_price_czk_mwh'] = price_amount
                                self.prices_data[key]['neg_imb_scarcity_czk_mwh'] = financial_prices.get('A01', 0.0)
                                self.prices_data[key]['neg_imb_incentive_czk_mwh'] = financial_prices.get('A02', 0.0)
                                self.prices_data[key]['neg_imb_financial_neutrality_czk_mwh'] = financial_prices.get('A03', 0.0)
                    else:
                        # Normal case: one Point per interval
                        point_time = period_start + timedelta(minutes=(position - 1) * resolution_minutes)
                        trade_date = point_time.date()
                        period_num = self.calculate_period_number(point_time)
                        time_interval_str = self.format_time_interval(point_time, resolution_minutes)

                        # Initialize or update the key
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

                        # Store based on category
                        if category == 'A04':  # Excess balance (positive)
                            self.prices_data[key]['pos_imb_price_czk_mwh'] = price_amount
                            self.prices_data[key]['pos_imb_scarcity_czk_mwh'] = financial_prices.get('A01', 0.0)
                            self.prices_data[key]['pos_imb_incentive_czk_mwh'] = financial_prices.get('A02', 0.0)
                            self.prices_data[key]['pos_imb_financial_neutrality_czk_mwh'] = financial_prices.get('A03', 0.0)
                        elif category == 'A05':  # Insufficient balance (negative)
                            self.prices_data[key]['neg_imb_price_czk_mwh'] = price_amount
                            self.prices_data[key]['neg_imb_scarcity_czk_mwh'] = financial_prices.get('A01', 0.0)
                            self.prices_data[key]['neg_imb_incentive_czk_mwh'] = financial_prices.get('A02', 0.0)
                            self.prices_data[key]['neg_imb_financial_neutrality_czk_mwh'] = financial_prices.get('A03', 0.0)


    def parse_volumes_xml(self, xml_file_path: str):
        """Parse A86 (Imbalance Volumes) XML file."""

        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Process each TimeSeries
        for timeseries in root.findall('.//{*}TimeSeries'):
            # Get flow direction
            flow_direction_elem = timeseries.find('{*}flowDirection.direction')
            flow_direction = flow_direction_elem.text if flow_direction_elem is not None else None

            # Map flow direction to situation
            situation_map = {
                'A01': 'surplus',
                'A02': 'deficit',
                'A03': 'balanced'
            }
            situation = situation_map.get(flow_direction, 'unknown')

            # Process ALL Period elements (there can be multiple periods per TimeSeries)
            for period in timeseries.findall('{*}Period'):
                # Get time interval
                time_interval = period.find('{*}timeInterval')
                start_elem = time_interval.find('{*}start')
                end_elem = time_interval.find('{*}end')
                period_start = self.parse_timestamp(start_elem.text)
                period_end = self.parse_timestamp(end_elem.text)

                # Get resolution
                resolution_elem = period.find('{*}resolution')
                resolution = resolution_elem.text if resolution_elem is not None else 'PT15M'
                resolution_minutes = int(resolution[2:-1]) if 'M' in resolution else 60

                # Calculate how many intervals are in this Period
                period_duration_minutes = int((period_end - period_start).total_seconds() / 60)
                num_intervals = period_duration_minutes // resolution_minutes

                # Process each Point
                for point in period.findall('{*}Point'):
                    position = int(point.find('{*}position').text)

                    # Get quantity
                    quantity_elem = point.find('{*}quantity')
                    quantity = float(quantity_elem.text) if quantity_elem is not None else 0.0

                    # Get secondary quantity (difference)
                    secondary_elem = point.find('{*}secondaryQuantity')
                    difference = float(secondary_elem.text) if secondary_elem is not None else None

                    # If there's only one Point but the Period spans multiple intervals,
                    # apply the same value to ALL intervals in the Period
                    if len(period.findall('{*}Point')) == 1 and num_intervals > 1:
                        # Apply to all intervals in this Period
                        for interval_idx in range(num_intervals):
                            point_time = period_start + timedelta(minutes=interval_idx * resolution_minutes)
                            trade_date = point_time.date()
                            period_num = self.calculate_period_number(point_time)

                            # Store volume data for each interval
                            key = (trade_date, period_num)
                            self.volumes_data[key] = {
                                'imbalance_mwh': quantity,
                                'difference_mwh': difference,
                                'situation': situation
                            }
                    else:
                        # Normal case: one Point per interval
                        point_time = period_start + timedelta(minutes=(position - 1) * resolution_minutes)
                        trade_date = point_time.date()
                        period_num = self.calculate_period_number(point_time)

                        # Store volume data
                        key = (trade_date, period_num)
                        self.volumes_data[key] = {
                            'imbalance_mwh': quantity,
                            'difference_mwh': difference,
                            'situation': situation
                        }


    def combine_data(self):
        """Combine prices and volumes data."""

        # Get all unique keys from both datasets
        all_keys = set(self.prices_data.keys()) | set(self.volumes_data.keys())

        for key in sorted(all_keys):
            trade_date, period_num = key

            # Get price data or create default
            if key in self.prices_data:
                record = self.prices_data[key].copy()
            else:
                # Missing price data - fill with zeros
                record = {
                    'trade_date': trade_date,
                    'period': period_num,
                    'time_interval': self._calculate_time_interval_from_period(period_num),
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

            # Add volume data
            if key in self.volumes_data:
                record.update(self.volumes_data[key])
            else:
                # Missing volume data
                if key in self.prices_data:
                    # Price exists but no volume - keep NULL
                    record['imbalance_mwh'] = None
                    record['difference_mwh'] = None
                    record['situation'] = None
                else:
                    # Both missing - fill with zero and surplus
                    record['imbalance_mwh'] = 0.0
                    record['difference_mwh'] = 0.0
                    record['situation'] = 'surplus'

            self.combined_data.append(record)


    def _calculate_time_interval_from_period(self, period_num: int) -> str:
        """Calculate time interval string from period number."""
        # period_num is 1-96
        minutes_from_midnight = (period_num - 1) * 15
        hours = minutes_from_midnight // 60
        minutes = minutes_from_midnight % 60
        start_time = f"{hours:02d}:{minutes:02d}"

        end_minutes = minutes_from_midnight + 15
        end_hours = end_minutes // 60
        end_mins = end_minutes % 60
        end_time = f"{end_hours:02d}:{end_mins:02d}"

        return f"{start_time}-{end_time}"

    def generate_sql_queries(self) -> List[str]:
        """Generate SQL INSERT queries with ON CONFLICT handling."""
        queries = []

        base_query = """
INSERT INTO entsoe_imbalance_prices (
    trade_date, period, time_interval,
    pos_imb_price_czk_mwh, pos_imb_scarcity_czk_mwh,
    pos_imb_incentive_czk_mwh, pos_imb_financial_neutrality_czk_mwh,
    neg_imb_price_czk_mwh, neg_imb_scarcity_czk_mwh,
    neg_imb_incentive_czk_mwh, neg_imb_financial_neutrality_czk_mwh,
    imbalance_mwh, difference_mwh, situation, status
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (trade_date, period)
DO UPDATE SET
    time_interval = EXCLUDED.time_interval,
    pos_imb_price_czk_mwh = EXCLUDED.pos_imb_price_czk_mwh,
    pos_imb_scarcity_czk_mwh = EXCLUDED.pos_imb_scarcity_czk_mwh,
    pos_imb_incentive_czk_mwh = EXCLUDED.pos_imb_incentive_czk_mwh,
    pos_imb_financial_neutrality_czk_mwh = EXCLUDED.pos_imb_financial_neutrality_czk_mwh,
    neg_imb_price_czk_mwh = EXCLUDED.neg_imb_price_czk_mwh,
    neg_imb_scarcity_czk_mwh = EXCLUDED.neg_imb_scarcity_czk_mwh,
    neg_imb_incentive_czk_mwh = EXCLUDED.neg_imb_incentive_czk_mwh,
    neg_imb_financial_neutrality_czk_mwh = EXCLUDED.neg_imb_financial_neutrality_czk_mwh,
    imbalance_mwh = EXCLUDED.imbalance_mwh,
    difference_mwh = EXCLUDED.difference_mwh,
    situation = EXCLUDED.situation,
    status = EXCLUDED.status;
"""

        for record in self.combined_data:
            values = (
                record['trade_date'],
                record['period'],
                record['time_interval'],
                record['pos_imb_price_czk_mwh'],
                record['pos_imb_scarcity_czk_mwh'],
                record['pos_imb_incentive_czk_mwh'],
                record['pos_imb_financial_neutrality_czk_mwh'],
                record['neg_imb_price_czk_mwh'],
                record['neg_imb_scarcity_czk_mwh'],
                record['neg_imb_incentive_czk_mwh'],
                record['neg_imb_financial_neutrality_czk_mwh'],
                record['imbalance_mwh'],
                record['difference_mwh'],
                record['situation'],
                record['status']
            )
            queries.append((base_query, values))

        return queries

    def display_as_markdown_table(self):
        """Display combined data as markdown table."""
        print("\n" + "=" * 120)
        print("COMBINED DATA - MARKDOWN TABLE")
        print("=" * 120 + "\n")

        # Header
        print("| trade_date | period | time_interval | pos_price | pos_scar | pos_incent | pos_neutral | neg_price | neg_scar | neg_incent | neg_neutral | imbalance | difference | situation | status |")
        print("|------------|--------|---------------|-----------|----------|------------|-------------|-----------|----------|------------|-------------|-----------|------------|-----------|--------|")

        # Rows
        for record in self.combined_data:
            # Format nullable numeric fields
            imb_str = f"{record['imbalance_mwh']:9.3f}" if record['imbalance_mwh'] is not None else "NULL     "
            diff_str = f"{record['difference_mwh']:10.3f}" if record['difference_mwh'] is not None else "NULL      "
            sit_str = f"{record['situation']:9s}" if record['situation'] else "NULL     "

            print(f"| {record['trade_date']} | "
                  f"{record['period']:2d} | "
                  f"{record['time_interval']:11s} | "
                  f"{record['pos_imb_price_czk_mwh']:9.3f} | "
                  f"{record['pos_imb_scarcity_czk_mwh']:8.3f} | "
                  f"{record['pos_imb_incentive_czk_mwh']:10.3f} | "
                  f"{record['pos_imb_financial_neutrality_czk_mwh']:11.3f} | "
                  f"{record['neg_imb_price_czk_mwh']:9.3f} | "
                  f"{record['neg_imb_scarcity_czk_mwh']:8.3f} | "
                  f"{record['neg_imb_incentive_czk_mwh']:10.3f} | "
                  f"{record['neg_imb_financial_neutrality_czk_mwh']:11.3f} | "
                  f"{imb_str} | "
                  f"{diff_str} | "
                  f"{sit_str} | "
                  f"{record['status']:6s} |")


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Parse ENTSO-E Imbalance data")
    parser.add_argument('--period', nargs=2, metavar=('START', 'END'),
                        help='Fetch data for period (format: YYYYMMDDHHmm)')
    parser.add_argument('--prices', type=str, help='Path to prices XML file')
    parser.add_argument('--volumes', type=str, help='Path to volumes XML file')

    args = parser.parse_args()

    if args.period:
        # Fetch data for the specified period
        start_str, end_str = args.period
        print(f"Fetching data for period: {start_str} to {end_str}")
        print("Note: This requires running the pipeline to fetch the data first")

        # Expected file paths
        start_dt = datetime.strptime(start_str, '%Y%m%d%H%M')
        prices_file = f"/app/scripts/entsoe/data/{start_dt.year}/{start_dt.month:02d}/entsoe_imbalance_prices_{start_str}_{end_str}.xml"
        volumes_file = f"/app/scripts/entsoe/data/{start_dt.year}/{start_dt.month:02d}/entsoe_imbalance_volumes_{start_str}_{end_str}.xml"

        print(f"\nExpected files:")
        print(f"  Prices:  {prices_file}")
        print(f"  Volumes: {volumes_file}")
    elif args.prices and args.volumes:
        prices_file = args.prices
        volumes_file = args.volumes
    else:
        parser.print_help()
        sys.exit(1)

    # Parse the data
    parser_obj = ImbalanceDataParser()

    if Path(prices_file).exists():
        parser_obj.parse_prices_xml(prices_file)
    else:
        print(f"ERROR: Prices file not found: {prices_file}")
        sys.exit(1)

    if Path(volumes_file).exists():
        parser_obj.parse_volumes_xml(volumes_file)
    else:
        print(f"ERROR: Volumes file not found: {volumes_file}")
        sys.exit(1)

    # Combine data
    parser_obj.combine_data()

    # Display as markdown table
    parser_obj.display_as_markdown_table()

    # Generate SQL queries
    print("\n" + "=" * 120)
    print("SQL QUERIES GENERATED")
    print("=" * 120)
    queries = parser_obj.generate_sql_queries()
    print(f"\nGenerated {len(queries)} SQL queries ready for database insertion")
    print("\nExample query (first record):")
    if queries:
        query, values = queries[0]
        print(query)
        print(f"\nValues: {values}")


if __name__ == '__main__':
    main()
