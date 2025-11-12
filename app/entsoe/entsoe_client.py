#!/usr/bin/env python3
"""
ENTSO-E API client for fetching electricity market data.

This module provides functionality to fetch data from the ENTSO-E Transparency Platform API.
"""

import sys
import io
import zipfile
from datetime import datetime, timedelta, timezone
import requests
import xml.etree.ElementTree as ET
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ENTSOE_SECURITY_TOKEN, ENTSOE_CONTROL_AREA_DOMAIN


class EntsoeClient:
    """Client for interacting with ENTSO-E Transparency Platform API."""

    # Updated to use temporary endpoint due to performance issues
    BASE_URL = "https://external-api.tp.entsoe.eu/api"
    # Fallback URL if needed: "https://web-api.tp.entsoe.eu/api"

    # Document types
    DOC_TYPE_IMBALANCE_PRICES = "A85"  # 17.1.G Imbalance prices
    DOC_TYPE_IMBALANCE_VOLUMES = "A86"  # 17.1.H Total Imbalance Volumes

    def __init__(self, security_token=None, control_area_domain=None):
        """
        Initialize ENTSO-E client.

        Args:
            security_token: API security token (defaults to env var)
            control_area_domain: Control area domain code (defaults to env var)
        """
        self.security_token = security_token or ENTSOE_SECURITY_TOKEN
        self.control_area_domain = control_area_domain or ENTSOE_CONTROL_AREA_DOMAIN

        if not self.security_token:
            raise ValueError("ENTSO-E security token not configured. Set ENTSOE_SECURITY_TOKEN in .env file")

        if not self.control_area_domain:
            raise ValueError("ENTSO-E control area domain not configured. Set ENTSOE_CONTROL_AREA_DOMAIN in .env file")

    def _format_timestamp(self, dt):
        """
        Format datetime to ENTSO-E API format (yyyyMMddHHmm).

        ENTSO-E API requires UTC timestamps. If datetime is timezone-aware,
        it will be converted to UTC. If naive, it's assumed to be UTC.

        Args:
            dt: datetime object (naive or timezone-aware)

        Returns:
            str: Formatted timestamp in UTC
        """
        # If timezone-aware, convert to UTC
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)

        # Remove timezone info for formatting (API expects naive format)
        dt = dt.replace(tzinfo=None)

        return dt.strftime("%Y%m%d%H%M")

    def _build_url(self, document_type, period_start, period_end):
        """
        Build API request URL.

        Args:
            document_type: Document type code (A85 or A86)
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: Complete API URL
        """
        params = {
            "securityToken": self.security_token,
            "documentType": document_type,
            "controlArea_Domain": self.control_area_domain,
            "periodStart": self._format_timestamp(period_start),
            "periodEnd": self._format_timestamp(period_end)
        }

        # Build query string
        query_string = "&".join([f"{key}={value}" for key, value in params.items()])
        return f"{self.BASE_URL}?{query_string}"

    def fetch_data(self, document_type, period_start, period_end, timeout=60):
        """
        Fetch data from ENTSO-E API.

        Args:
            document_type: Document type code (A85 or A86)
            period_start: Start datetime
            period_end: End datetime
            timeout: Request timeout in seconds

        Returns:
            str: XML content (unzipped if necessary)

        Raises:
            requests.RequestException: If API request fails
            ValueError: If response format is unexpected
        """
        url = self._build_url(document_type, period_start, period_end)

        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            # Check if response is zipped
            content_type = response.headers.get('Content-Type', '')

            # If content is zipped, unzip it
            if 'zip' in content_type or self._is_zip_content(response.content):
                return self._unzip_content(response.content)
            else:
                # Return XML as text
                return response.text

        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch data from ENTSO-E API: {e}")

    def _is_zip_content(self, content):
        """
        Check if content is a zip file by checking magic bytes.

        Args:
            content: Byte content

        Returns:
            bool: True if content is a zip file
        """
        # ZIP files start with PK (0x504B)
        return content[:2] == b'PK'

    def _unzip_content(self, content):
        """
        Unzip content and return XML.

        Args:
            content: Zipped byte content

        Returns:
            str: Unzipped XML content

        Raises:
            ValueError: If zip contains multiple files or no XML files
        """
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                # Get list of files in zip
                file_list = zf.namelist()

                # Find XML file
                xml_files = [f for f in file_list if f.lower().endswith('.xml')]

                if not xml_files:
                    raise ValueError("No XML file found in zip archive")

                if len(xml_files) > 1:
                    # If multiple XML files, use the first one
                    # (could be extended to handle multiple files if needed)
                    pass

                # Read and return the XML content
                xml_content = zf.read(xml_files[0])
                return xml_content.decode('utf-8')

        except zipfile.BadZipFile:
            raise ValueError("Invalid zip file received from API")

    def fetch_imbalance_prices(self, period_start, period_end):
        """
        Fetch imbalance prices (documentType=A85).

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(self.DOC_TYPE_IMBALANCE_PRICES, period_start, period_end)

    def fetch_imbalance_volumes(self, period_start, period_end):
        """
        Fetch total imbalance volumes (documentType=A86).

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(self.DOC_TYPE_IMBALANCE_VOLUMES, period_start, period_end)

    @staticmethod
    def get_preceding_hour_range(reference_time=None, lag_hours=3):
        """
        Get the time range for the preceding hour (for scheduled runs).

        Includes a lag to account for data availability delay.
        Default lag is 3 hours to ensure data is available.

        For example, if current time is 10:30 with 3-hour lag:
            period_start: 06:30
            period_end: 07:30

        Args:
            reference_time: Reference datetime (defaults to now)
            lag_hours: Hours to lag behind current time (default 3)

        Returns:
            tuple: (period_start, period_end) as datetime objects
        """
        if reference_time is None:
            reference_time = datetime.now()

        # Apply lag for data availability
        lagged_time = reference_time - timedelta(hours=lag_hours)

        # Round down to nearest 15 minutes
        minutes = (lagged_time.minute // 15) * 15
        period_end = lagged_time.replace(minute=minutes, second=0, microsecond=0)

        # One hour before
        period_start = period_end - timedelta(hours=1)

        return period_start, period_end


if __name__ == '__main__':
    """Test the ENTSO-E client."""
    print("Testing ENTSO-E Client")
    print("=" * 60)

    # Initialize client
    try:
        client = EntsoeClient()
        print(f"✓ Client initialized")
        print(f"  Security token: {client.security_token[:10]}...")
        print(f"  Control area: {client.control_area_domain}")
    except ValueError as e:
        print(f"✗ Failed to initialize client: {e}")
        sys.exit(1)

    # Test with a small time range
    period_start = datetime(2024, 1, 1, 0, 0)
    period_end = datetime(2024, 1, 1, 1, 0)

    print(f"\nTest period:")
    print(f"  Start: {period_start.strftime('%Y-%m-%d %H:%M')}")
    print(f"  End: {period_end.strftime('%Y-%m-%d %H:%M')}")

    # Test fetching imbalance prices
    print(f"\nFetching imbalance prices (A85)...")
    try:
        xml_data = client.fetch_imbalance_prices(period_start, period_end)
        print(f"✓ Received XML data ({len(xml_data)} bytes)")

        # Try to parse XML to verify it's valid
        root = ET.fromstring(xml_data)
        print(f"✓ XML is valid (root tag: {root.tag})")

    except Exception as e:
        print(f"✗ Failed to fetch data: {e}")

    print("=" * 60)
