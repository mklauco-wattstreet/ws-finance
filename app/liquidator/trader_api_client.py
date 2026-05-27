#!/usr/bin/env python3
"""
Trader API HTTP client for position liquidation.

Provides authenticated GET (list expired positions) and POST (liquidate)
calls to the Trader API service with retry/backoff and token masking.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import TRADER_API_URL, TRADER_API_TOKEN


class TraderApiClient:
    """Authenticated HTTP client for the Trader API.

    Mirrors the Laravel-side TraderApi service: Bearer token in the
    Authorization header, short timeouts, transient-error retry.
    """

    # Endpoints relative to base URL.
    # LIQUIDATE_PATH is confirmed from the Laravel codebase
    # (POST /api/v1/ta/liquidate/{position_id}). LIST_EXPIRED_PATH is
    # proposed by symmetry — confirm with the Trader API team.
    LIST_EXPIRED_PATH = "/api/v1/ta/positions/expired"
    LIQUIDATE_PATH = "/api/v1/ta/liquidate/{position_id}"

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        connect_timeout: float = 5.0,
        read_timeout: float = 10.0,
    ):
        self.base_url = (base_url or TRADER_API_URL or "").rstrip("/")
        self.token = token or TRADER_API_TOKEN
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

        if not self.base_url:
            raise ValueError(
                "Trader API base URL not configured. "
                "Set TRADER_API_URL in .env file"
            )
        if not self.token:
            raise ValueError(
                "Trader API token not configured. "
                "Set TRADER_API_TOKEN in .env file"
            )

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

        retry_strategy = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _sanitize(self, text: str) -> str:
        """Strip the bearer token from any text before it reaches logs."""
        return text.replace(self.token, "***") if self.token else text

    def list_expired_positions(self) -> List[Dict[str, Any]]:
        """Fetch the current set of expired positions awaiting liquidation.

        Returns:
            List of position dicts. Expected fields per position:
            position_id, contract_id, side.
        """
        url = f"{self.base_url}{self.LIST_EXPIRED_PATH}"
        try:
            response = self.session.get(
                url, timeout=(self.connect_timeout, self.read_timeout)
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise requests.RequestException(
                f"Failed to list expired positions: {self._sanitize(str(e))}"
            )

        if isinstance(data, dict) and "positions" in data:
            return data["positions"]
        if isinstance(data, list):
            return data
        raise ValueError(
            f"Unexpected response shape from {self.LIST_EXPIRED_PATH}: "
            f"{type(data).__name__}"
        )

    def liquidate(
        self, position_id: str, payload: Dict[str, Any]
    ) -> requests.Response:
        """POST a liquidation request for a single position.

        Caller inspects status_code and json(); does not raise on non-2xx.
        """
        url = f"{self.base_url}{self.LIQUIDATE_PATH.format(position_id=position_id)}"
        try:
            return self.session.post(
                url,
                json=payload,
                timeout=(self.connect_timeout, self.read_timeout),
            )
        except requests.RequestException as e:
            raise requests.RequestException(
                f"Liquidate POST failed for {position_id}: {self._sanitize(str(e))}"
            )
