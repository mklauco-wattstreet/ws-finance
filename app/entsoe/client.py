#!/usr/bin/env python3
"""
ENTSO-E API client for fetching electricity market data.

This module provides robust functionality to fetch data from the ENTSO-E
Transparency Platform API with retry logic and input validation.
"""

import sys
import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.etree.ElementTree as ET

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ENTSOE_BASE_URL, ENTSOE_SECURITY_TOKEN, ENTSOE_CONTROL_AREA_DOMAIN


class EntsoeClient:
    """Client for interacting with ENTSO-E Transparency Platform API.

    Features:
    - Automatic retry with exponential backoff
    - Input validation (date ranges)
    - Token masking in logs
    - Support for multiple document types
    """

    # Document types
    DOC_TYPE_IMBALANCE_PRICES = "A85"  # 17.1.G Imbalance prices
    DOC_TYPE_IMBALANCE_VOLUMES = "A86"  # 17.1.H Total Imbalance Volumes
    DOC_TYPE_ACTUAL_LOAD = "A65"  # 6.1.A Actual Total Load
    DOC_TYPE_LOAD_FORECAST = "A65"  # 6.1.B Day-Ahead Total Load Forecast
    DOC_TYPE_GENERATION_PER_TYPE = "A75"  # 14.1.C/D Actual Generation per Type
    DOC_TYPE_CROSS_BORDER_FLOWS = "A11"  # 12.1.F Physical Flows
    DOC_TYPE_GENERATION_FORECAST = "A69"  # 14.1.A Wind/Solar Generation Forecast
    DOC_TYPE_ACTIVATED_BALANCING = "A84"  # 17.1.E Activated Balancing Energy
    DOC_TYPE_SCHEDULED_GENERATION = "A71"  # 14.1.B Scheduled Generation
    DOC_TYPE_SCHEDULED_EXCHANGES = "A09"  # 12.1.D Scheduled Commercial Exchanges

    # Process types for A65 differentiation
    PROCESS_TYPE_REALISED = "A16"  # Actual load
    PROCESS_TYPE_DAY_AHEAD = "A01"  # Day-ahead forecast
    PROCESS_TYPE_INTRADAY_TOTAL = "A18"  # Intraday total

    # Maximum date range allowed by API
    MAX_DATE_RANGE_DAYS = 7

    def __init__(
        self,
        security_token: Optional[str] = None,
        control_area_domain: Optional[str] = None,
        base_url: Optional[str] = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0
    ):
        """
        Initialize ENTSO-E client with retry logic.

        Args:
            security_token: API security token (defaults to env var)
            control_area_domain: Control area domain code (defaults to env var)
            base_url: API base URL (defaults to env var)
            max_retries: Maximum number of retry attempts (default 3)
            backoff_factor: Backoff factor for exponential delay (default 1.0)
        """
        self.base_url = base_url or ENTSOE_BASE_URL
        self.security_token = security_token or ENTSOE_SECURITY_TOKEN
        self.control_area_domain = control_area_domain or ENTSOE_CONTROL_AREA_DOMAIN

        if not self.security_token:
            raise ValueError(
                "ENTSO-E security token not configured. "
                "Set ENTSOE_SECURITY_TOKEN in .env file"
            )

        if not self.control_area_domain:
            raise ValueError(
                "ENTSO-E control area domain not configured. "
                "Set ENTSOE_CONTROL_AREA_DOMAIN in .env file"
            )

        # Setup session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _format_timestamp(self, dt: datetime) -> str:
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

    def _validate_date_range(self, period_start: datetime, period_end: datetime) -> None:
        """
        Validate date range parameters.

        Args:
            period_start: Start datetime
            period_end: End datetime

        Raises:
            ValueError: If validation fails
        """
        if period_end <= period_start:
            raise ValueError(
                f"period_end ({period_end}) must be greater than "
                f"period_start ({period_start})"
            )

        date_range = period_end - period_start
        if date_range.days > self.MAX_DATE_RANGE_DAYS:
            raise ValueError(
                f"Date range ({date_range.days} days) exceeds maximum allowed "
                f"({self.MAX_DATE_RANGE_DAYS} days)"
            )

    def _build_url(
        self,
        document_type: str,
        period_start: datetime,
        period_end: datetime,
        process_type: Optional[str] = None,
        psr_type: Optional[str] = None,
        in_domain: Optional[str] = None,
        out_domain: Optional[str] = None
    ) -> str:
        """
        Build API request URL.

        Args:
            document_type: Document type code (A85, A86, A65, A75, A11)
            period_start: Start datetime
            period_end: End datetime
            process_type: Optional process type (A01, A16)
            psr_type: Optional PSR type for generation queries (B01-B20)
            in_domain: Optional in_Domain for A11 cross-border flows
            out_domain: Optional out_Domain for A11 cross-border flows

        Returns:
            str: Complete API URL (token masked in logs)
        """
        params = {
            "securityToken": self.security_token,
            "documentType": document_type,
            "controlArea_Domain": self.control_area_domain,
            "periodStart": self._format_timestamp(period_start),
            "periodEnd": self._format_timestamp(period_end)
        }

        if process_type:
            params["processType"] = process_type

        if psr_type:
            params["psrType"] = psr_type

        # For A65 (load), use outBiddingZone_Domain instead of controlArea_Domain
        if document_type == self.DOC_TYPE_ACTUAL_LOAD:
            params["outBiddingZone_Domain"] = params.pop("controlArea_Domain")

        # For A75 (generation), use in_Domain
        if document_type == self.DOC_TYPE_GENERATION_PER_TYPE:
            params["in_Domain"] = params.pop("controlArea_Domain")

        # For A11 (cross-border flows), use in_Domain and out_Domain
        if document_type == self.DOC_TYPE_CROSS_BORDER_FLOWS:
            params.pop("controlArea_Domain")
            if in_domain:
                params["in_Domain"] = in_domain
            if out_domain:
                params["out_Domain"] = out_domain

        # For A69 (generation forecast), use in_Domain
        if document_type == self.DOC_TYPE_GENERATION_FORECAST:
            params["in_Domain"] = params.pop("controlArea_Domain")

        # For A84 (activated balancing), use controlArea_Domain (default)
        # No change needed

        # For A71 (scheduled generation), use in_Domain
        if document_type == self.DOC_TYPE_SCHEDULED_GENERATION:
            params["in_Domain"] = params.pop("controlArea_Domain")

        # For A09 (scheduled exchanges), use in_Domain and out_Domain
        if document_type == self.DOC_TYPE_SCHEDULED_EXCHANGES:
            params.pop("controlArea_Domain")
            if in_domain:
                params["in_Domain"] = in_domain
            if out_domain:
                params["out_Domain"] = out_domain

        # Build query string
        query_string = "&".join([f"{key}={value}" for key, value in params.items()])
        return f"{self.base_url}?{query_string}"

    def fetch_data(
        self,
        document_type: str,
        period_start: datetime,
        period_end: datetime,
        process_type: Optional[str] = None,
        psr_type: Optional[str] = None,
        in_domain: Optional[str] = None,
        out_domain: Optional[str] = None,
        timeout: int = 60
    ) -> str:
        """
        Fetch data from ENTSO-E API with validation and retry.

        Args:
            document_type: Document type code (A85, A86, A65, A75, A11)
            period_start: Start datetime
            period_end: End datetime
            process_type: Optional process type
            psr_type: Optional PSR type for generation
            in_domain: Optional in_Domain for A11 cross-border flows
            out_domain: Optional out_Domain for A11 cross-border flows
            timeout: Request timeout in seconds

        Returns:
            str: XML content (unzipped if necessary)

        Raises:
            ValueError: If date range validation fails
            requests.RequestException: If API request fails after retries
        """
        # Validate date range
        self._validate_date_range(period_start, period_end)

        url = self._build_url(
            document_type, period_start, period_end, process_type, psr_type,
            in_domain, out_domain
        )

        try:
            response = self.session.get(url, timeout=timeout)
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
            raise requests.RequestException(
                f"Failed to fetch data from ENTSO-E API after retries: {e}"
            )

    def _is_zip_content(self, content: bytes) -> bool:
        """
        Check if content is a zip file by checking magic bytes.

        Args:
            content: Byte content

        Returns:
            bool: True if content is a zip file
        """
        # ZIP files start with PK (0x504B)
        return len(content) >= 2 and content[:2] == b'PK'

    def _unzip_content(self, content: bytes) -> str:
        """
        Unzip content and return XML.

        Args:
            content: Zipped byte content

        Returns:
            str: Unzipped XML content

        Raises:
            ValueError: If zip contains no XML files
        """
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                # Get list of files in zip
                file_list = zf.namelist()

                # Find XML file
                xml_files = [f for f in file_list if f.lower().endswith('.xml')]

                if not xml_files:
                    raise ValueError("No XML file found in zip archive")

                # Read and return the XML content
                xml_content = zf.read(xml_files[0])
                return xml_content.decode('utf-8')

        except zipfile.BadZipFile:
            raise ValueError("Invalid zip file received from API")

    def fetch_imbalance_prices(
        self, period_start: datetime, period_end: datetime
    ) -> str:
        """
        Fetch imbalance prices (documentType=A85).

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_IMBALANCE_PRICES, period_start, period_end
        )

    def fetch_imbalance_volumes(
        self, period_start: datetime, period_end: datetime
    ) -> str:
        """
        Fetch total imbalance volumes (documentType=A86).

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_IMBALANCE_VOLUMES, period_start, period_end
        )

    def fetch_actual_load(
        self, period_start: datetime, period_end: datetime
    ) -> str:
        """
        Fetch actual total load (documentType=A65, processType=A16).

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_ACTUAL_LOAD,
            period_start,
            period_end,
            process_type=self.PROCESS_TYPE_REALISED
        )

    def fetch_load_forecast(
        self, period_start: datetime, period_end: datetime
    ) -> str:
        """
        Fetch day-ahead load forecast (documentType=A65, processType=A01).

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_LOAD_FORECAST,
            period_start,
            period_end,
            process_type=self.PROCESS_TYPE_DAY_AHEAD
        )

    def fetch_generation_per_type(
        self,
        period_start: datetime,
        period_end: datetime,
        psr_type: Optional[str] = None
    ) -> str:
        """
        Fetch actual generation per type (documentType=A75).

        Args:
            period_start: Start datetime
            period_end: End datetime
            psr_type: Optional specific PSR type (B01-B20)

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_GENERATION_PER_TYPE,
            period_start,
            period_end,
            process_type=self.PROCESS_TYPE_REALISED,
            psr_type=psr_type
        )

    def fetch_cross_border_flows(
        self,
        period_start: datetime,
        period_end: datetime,
        in_domain: str,
        out_domain: str
    ) -> str:
        """
        Fetch cross-border physical flows (documentType=A11).

        Args:
            period_start: Start datetime
            period_end: End datetime
            in_domain: Importing domain EIC code
            out_domain: Exporting domain EIC code

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_CROSS_BORDER_FLOWS,
            period_start,
            period_end,
            in_domain=in_domain,
            out_domain=out_domain
        )

    def fetch_generation_forecast(
        self,
        period_start: datetime,
        period_end: datetime,
        psr_type: Optional[str] = None
    ) -> str:
        """
        Fetch day-ahead generation forecast (documentType=A69).

        For renewable forecasts (wind/solar). Use psr_type to filter:
        - B16: Solar
        - B18: Wind Offshore
        - B19: Wind Onshore

        Args:
            period_start: Start datetime
            period_end: End datetime
            psr_type: Optional specific PSR type (B16, B18, B19)

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_GENERATION_FORECAST,
            period_start,
            period_end,
            process_type=self.PROCESS_TYPE_DAY_AHEAD,
            psr_type=psr_type
        )

    def fetch_activated_balancing_energy(
        self,
        period_start: datetime,
        period_end: datetime
    ) -> str:
        """
        Fetch activated balancing energy (documentType=A84).

        Returns aFRR (A95) and mFRR (A96) activation volumes.

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_ACTIVATED_BALANCING,
            period_start,
            period_end
        )

    def fetch_scheduled_generation(
        self,
        period_start: datetime,
        period_end: datetime
    ) -> str:
        """
        Fetch scheduled generation (documentType=A71).

        Day-ahead scheduled total generation.

        Args:
            period_start: Start datetime
            period_end: End datetime

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_SCHEDULED_GENERATION,
            period_start,
            period_end,
            process_type=self.PROCESS_TYPE_DAY_AHEAD
        )

    def fetch_scheduled_exchanges(
        self,
        period_start: datetime,
        period_end: datetime,
        in_domain: str,
        out_domain: str
    ) -> str:
        """
        Fetch scheduled commercial exchanges (documentType=A09).

        Day-ahead scheduled cross-border exchanges.

        Args:
            period_start: Start datetime
            period_end: End datetime
            in_domain: Importing domain EIC code
            out_domain: Exporting domain EIC code

        Returns:
            str: XML content
        """
        return self.fetch_data(
            self.DOC_TYPE_SCHEDULED_EXCHANGES,
            period_start,
            period_end,
            process_type=self.PROCESS_TYPE_DAY_AHEAD,
            in_domain=in_domain,
            out_domain=out_domain
        )

    @staticmethod
    def get_preceding_hour_range(
        reference_time: Optional[datetime] = None,
        lag_hours: int = 3
    ) -> tuple:
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
        print(f"  Security token: [CONFIGURED]")
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
