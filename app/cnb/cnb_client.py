#!/usr/bin/env python3
"""
CNB (Czech National Bank) API client for fetching daily CZK/EUR exchange rates.

CNB publishes daily fixing rates at ~14:30 CET on business days.
API is free, no authentication required.
"""

import logging
from datetime import date, timedelta
from decimal import Decimal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class CnbClient:
    """Client for CNB daily exchange rate API."""

    BASE_URL = "https://api.cnb.cz/cnbapi/exrates/daily"

    def __init__(self, max_retries: int = 3, backoff_factor: float = 1.0):
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

    def fetch_rate(self, rate_date: date) -> dict:
        """Fetch EUR/CZK rate for a specific date.

        Returns:
            {'rate_date': date, 'czk_eur': Decimal}

        Raises:
            ValueError: If EUR entry not found in response
            requests.HTTPError: On non-200 response
        """
        params = {"date": rate_date.strftime("%Y-%m-%d"), "lang": "EN"}
        response = self.session.get(self.BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        for entry in data.get("rates", []):
            if entry.get("currencyCode") == "EUR" and entry.get("amount") == 1:
                return {
                    "rate_date": rate_date,
                    "czk_eur": Decimal(str(entry["rate"])),
                }

        raise ValueError(f"EUR entry not found in CNB response for {rate_date}")

    def fetch_rates_range(self, start_date: date, end_date: date) -> list:
        """Fetch rates for a date range, one request per business day.

        Skips weekends (Sat/Sun). Returns list of dicts.
        """
        results = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:  # Mon-Fri
                try:
                    result = self.fetch_rate(current)
                    results.append(result)
                except Exception as e:
                    logger.warning(f"Failed to fetch rate for {current}: {e}")
            current += timedelta(days=1)
        return results
