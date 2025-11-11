#!/usr/bin/env python3
"""
XML parser for ENTSO-E data.

This module provides functionality to parse XML data from ENTSO-E API
and extract imbalance prices and volumes.
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Optional


class EntsoeParser:
    """Parser for ENTSO-E XML responses."""

    # XML namespaces used by ENTSO-E
    NAMESPACES = {
        'ns': 'urn:iec62325.351:tc57wg16:451-6:balancingdocument:3:0'
    }

    @staticmethod
    def parse_timestamp(timestamp_str):
        """
        Parse ISO 8601 timestamp to datetime.

        Args:
            timestamp_str: ISO 8601 timestamp string

        Returns:
            datetime object
        """
        # Handle format: 2024-01-01T00:00Z
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'

        # Parse with timezone info
        try:
            # Try with timezone
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            # Fallback to basic ISO format
            return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')

    def parse_imbalance_prices(self, xml_content: str) -> List[Dict]:
        """
        Parse imbalance prices XML (documentType A85).

        Args:
            xml_content: XML content as string

        Returns:
            List of dictionaries containing parsed data
        """
        root = ET.fromstring(xml_content)

        records = []

        # Find all TimeSeries elements
        for timeseries in root.findall('.//ns:TimeSeries', self.NAMESPACES):
            # Get period information
            period = timeseries.find('.//ns:Period', self.NAMESPACES)
            if period is None:
                continue

            # Get time interval
            time_interval = period.find('ns:timeInterval', self.NAMESPACES)
            if time_interval is None:
                continue

            start_str = time_interval.find('ns:start', self.NAMESPACES).text
            end_str = time_interval.find('ns:end', self.NAMESPACES).text

            period_start = self.parse_timestamp(start_str)
            period_end = self.parse_timestamp(end_str)

            # Get resolution (e.g., PT15M for 15 minutes)
            resolution = period.find('ns:resolution', self.NAMESPACES).text
            interval_minutes = self._parse_resolution(resolution)

            # Get business type (to distinguish price types)
            business_type = timeseries.find('ns:businessType', self.NAMESPACES)
            business_type_value = business_type.text if business_type is not None else None

            # Parse all points in the period
            for point in period.findall('ns:Point', self.NAMESPACES):
                position = int(point.find('ns:position', self.NAMESPACES).text)
                price_amount = point.find('ns:imbalance_Price.amount', self.NAMESPACES)

                if price_amount is not None:
                    # Calculate timestamp for this point
                    # Position starts at 1
                    point_time = period_start + timedelta(minutes=(position - 1) * interval_minutes)

                    record = {
                        'timestamp': point_time,
                        'position': position,
                        'price': float(price_amount.text),
                        'business_type': business_type_value,
                        'resolution_minutes': interval_minutes
                    }

                    records.append(record)

        return records

    def parse_imbalance_volumes(self, xml_content: str) -> List[Dict]:
        """
        Parse total imbalance volumes XML (documentType A86).

        Args:
            xml_content: XML content as string

        Returns:
            List of dictionaries containing parsed data
        """
        root = ET.fromstring(xml_content)

        records = []

        # Find all TimeSeries elements
        for timeseries in root.findall('.//ns:TimeSeries', self.NAMESPACES):
            # Get period information
            period = timeseries.find('.//ns:Period', self.NAMESPACES)
            if period is None:
                continue

            # Get time interval
            time_interval = period.find('ns:timeInterval', self.NAMESPACES)
            if time_interval is None:
                continue

            start_str = time_interval.find('ns:start', self.NAMESPACES).text
            end_str = time_interval.find('ns:end', self.NAMESPACES).text

            period_start = self.parse_timestamp(start_str)
            period_end = self.parse_timestamp(end_str)

            # Get resolution (e.g., PT15M for 15 minutes)
            resolution = period.find('ns:resolution', self.NAMESPACES).text
            interval_minutes = self._parse_resolution(resolution)

            # Get business type
            business_type = timeseries.find('ns:businessType', self.NAMESPACES)
            business_type_value = business_type.text if business_type is not None else None

            # Parse all points in the period
            for point in period.findall('ns:Point', self.NAMESPACES):
                position = int(point.find('ns:position', self.NAMESPACES).text)
                quantity = point.find('ns:quantity', self.NAMESPACES)

                if quantity is not None:
                    # Calculate timestamp for this point
                    point_time = period_start + timedelta(minutes=(position - 1) * interval_minutes)

                    record = {
                        'timestamp': point_time,
                        'position': position,
                        'volume': float(quantity.text),
                        'business_type': business_type_value,
                        'resolution_minutes': interval_minutes
                    }

                    records.append(record)

        return records

    def _parse_resolution(self, resolution: str) -> int:
        """
        Parse resolution string to minutes.

        Args:
            resolution: Resolution string (e.g., PT15M, PT60M)

        Returns:
            int: Number of minutes
        """
        # Format is usually PT<number>M (e.g., PT15M for 15 minutes)
        if resolution.startswith('PT') and resolution.endswith('M'):
            return int(resolution[2:-1])
        elif resolution.startswith('PT') and resolution.endswith('H'):
            # Handle hours
            return int(resolution[2:-1]) * 60
        else:
            # Default to 15 minutes
            return 15

    def parse_generic(self, xml_content: str, document_type: str) -> List[Dict]:
        """
        Generic parser that routes to specific parser based on document type.

        Args:
            xml_content: XML content as string
            document_type: Document type (A85 or A86)

        Returns:
            List of dictionaries containing parsed data
        """
        if document_type == 'A85':
            return self.parse_imbalance_prices(xml_content)
        elif document_type == 'A86':
            return self.parse_imbalance_volumes(xml_content)
        else:
            raise ValueError(f"Unsupported document type: {document_type}")


if __name__ == '__main__':
    """Test the parser with sample XML."""
    print("Testing ENTSO-E Parser")
    print("=" * 60)

    # Sample XML for testing (minimal structure)
    sample_xml = """<?xml version="1.0" encoding="UTF-8"?>
<BalancingDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:3:0">
    <TimeSeries>
        <businessType>A04</businessType>
        <Period>
            <timeInterval>
                <start>2024-01-01T00:00Z</start>
                <end>2024-01-01T01:00Z</end>
            </timeInterval>
            <resolution>PT15M</resolution>
            <Point>
                <position>1</position>
                <imbalance_Price.amount>50.5</imbalance_Price.amount>
            </Point>
            <Point>
                <position>2</position>
                <imbalance_Price.amount>52.3</imbalance_Price.amount>
            </Point>
        </Period>
    </TimeSeries>
</BalancingDocument>"""

    parser = EntsoeParser()

    try:
        records = parser.parse_imbalance_prices(sample_xml)
        print(f"✓ Parsed {len(records)} records")

        for record in records:
            print(f"  {record['timestamp']} - Price: {record['price']} - Position: {record['position']}")

    except Exception as e:
        print(f"✗ Parsing failed: {e}")

    print("=" * 60)
