"""HTTP client for the JAO Publication Tool (Core region).

Two platforms are used, both under https://publicationtool.jao.eu:

* ``core``   - day-ahead results, ``/core/api/data/<endpoint>``
* ``coreID`` - intraday, ``/coreID/api/data/<prefix>_<endpoint>`` where the
  prefix is a capacity-calculation round (IDCCA..IDCCE) or an intraday
  auction (ID1..ID3)

Verified behaviour of the API (live calls, 2026-11-20):

* No token is needed.
* ``FromUtc`` / ``ToUtc`` are ISO-8601 UTC instants; ``ToUtc`` is exclusive.
  A request spanning more than 2 days is rejected with HTTP 400, so callers
  pass one delivery day (23, 24 or 25 hours) at a time.
* The response is ``{"data": [...], "rejected": bool, "messages": ...}`` and
  ``dateTimeUtc`` is the interval start. A window with no data gives an empty
  ``data`` array, not an error.
* ``Skip`` / ``Take`` are accepted but IGNORED by these endpoints (the whole
  window comes back). Paging is therefore off by default; it can be switched on
  with ``page_size`` and stops by itself if the server hands back the same page
  twice, so it can never loop on an endpoint that ignores the parameters.
* Rate limit is 100 requests/minute per IP. The client keeps a minimum gap
  between requests and retries 429/5xx with exponential backoff.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, FrozenSet, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://publicationtool.jao.eu"

# Endpoints of the day-ahead (Core) platform that this package knows how to parse.
CORE_ENDPOINTS: FrozenSet[str] = frozenset({
    "maxNetPos",
    "maxExchanges",
    "netPos",
    "scheduledExchanges",
    "priceSpread",
    "allocationConstraint",
})

# Intraday capacity calculation rounds (A..E) and their endpoints.
ID_CAPACITY_PREFIXES: FrozenSet[str] = frozenset(
    {"IDCCA", "IDCCB", "IDCCC", "IDCCD", "IDCCE"}
)
ID_CAPACITY_ENDPOINTS: FrozenSet[str] = frozenset({"intradayAtc", "intradayNtc"})

# Intraday auctions (IDA1..IDA3 in market terms) and their endpoints.
ID_AUCTION_PREFIXES: FrozenSet[str] = frozenset({"ID1", "ID2", "ID3"})
ID_AUCTION_ENDPOINTS: FrozenSet[str] = frozenset(
    {"netPos", "scheduledExchanges", "priceSpread"}
)

# The API refuses ranges longer than this.
MAX_RANGE = timedelta(days=2)


class JaoError(RuntimeError):
    """The JAO API answered with an error or a rejected request."""


def _iso_utc(value: datetime) -> str:
    """Format an aware datetime the way the API expects: ...T..:..:..Z (UTC)."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class JaoClient:
    """Typed, rate-limited client for the JAO Publication Tool data API."""

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        timeout: float = 60.0,
        connect_timeout: float = 10.0,
        min_interval: float = 0.7,
        max_retries: int = 5,
        backoff_factor: float = 1.5,
    ):
        """
        Args:
            session: Optional pre-built session (used as is, no retry mounted).
            timeout: Read timeout in seconds.
            connect_timeout: Connect timeout in seconds.
            min_interval: Minimum seconds between request starts. 0.7 s is at
                most ~85 requests/minute, under the 100/minute limit.
            max_retries: Retries for 429 and 5xx responses.
            backoff_factor: urllib3 exponential backoff factor.
        """
        if session is None:
            session = requests.Session()
            retry = Retry(
                total=max_retries,
                connect=max_retries,
                read=max_retries,
                status=max_retries,
                backoff_factor=backoff_factor,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET"}),
                respect_retry_after_header=True,
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        session.headers.update({"Accept": "application/json"})
        self.session = session
        self.timeout = (connect_timeout, timeout)
        self.min_interval = min_interval
        self._last_request = 0.0
        self.request_count = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_core(
        self,
        endpoint: str,
        start_utc: datetime,
        end_utc: datetime,
        page_size: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch a day-ahead Core endpoint for [start_utc, end_utc)."""
        if endpoint not in CORE_ENDPOINTS:
            raise ValueError(
                f"Unknown Core endpoint {endpoint!r}; known: {sorted(CORE_ENDPOINTS)}"
            )
        return self._fetch(f"core/api/data/{endpoint}", start_utc, end_utc, page_size)

    def fetch_core_id(
        self,
        prefix: str,
        endpoint: str,
        start_utc: datetime,
        end_utc: datetime,
        page_size: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch an intraday endpoint (``<prefix>_<endpoint>``) for [start_utc, end_utc)."""
        if prefix in ID_CAPACITY_PREFIXES:
            allowed = ID_CAPACITY_ENDPOINTS
        elif prefix in ID_AUCTION_PREFIXES:
            allowed = ID_AUCTION_ENDPOINTS
        else:
            raise ValueError(
                f"Unknown intraday prefix {prefix!r}; known: "
                f"{sorted(ID_CAPACITY_PREFIXES | ID_AUCTION_PREFIXES)}"
            )
        if endpoint not in allowed:
            raise ValueError(
                f"Endpoint {endpoint!r} is not published for {prefix}; known: {sorted(allowed)}"
            )
        return self._fetch(
            f"coreID/api/data/{prefix}_{endpoint}", start_utc, end_utc, page_size
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_range(start_utc: datetime, end_utc: datetime) -> None:
        if start_utc.tzinfo is None or end_utc.tzinfo is None:
            raise ValueError("start_utc and end_utc must be timezone-aware")
        if end_utc <= start_utc:
            raise ValueError(f"end_utc ({end_utc}) must be after start_utc ({start_utc})")
        if end_utc - start_utc > MAX_RANGE:
            raise ValueError(
                f"Range {start_utc} - {end_utc} exceeds the API limit of {MAX_RANGE.days} days"
            )

    def _pace(self) -> None:
        """Sleep so requests start at least min_interval apart."""
        wait = self.min_interval - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def _get_json(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self._pace()
        url = f"{BASE_URL}/{path}"
        self.request_count += 1
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            raise JaoError(f"GET {path} failed: {exc}") from exc
        if response.status_code != 200:
            raise JaoError(
                f"GET {path} returned HTTP {response.status_code}: {response.text[:200]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise JaoError(f"GET {path} returned invalid JSON: {exc}") from exc
        if not isinstance(payload, dict) or "data" not in payload:
            raise JaoError(f"GET {path} returned unexpected shape: {str(payload)[:200]}")
        if payload.get("rejected"):
            raise JaoError(f"GET {path} rejected by JAO: {payload.get('messages')}")
        return payload

    def _fetch(
        self,
        path: str,
        start_utc: datetime,
        end_utc: datetime,
        page_size: Optional[int],
    ) -> List[Dict[str, Any]]:
        self._validate_range(start_utc, end_utc)
        if page_size is not None and page_size <= 0:
            raise ValueError("page_size must be positive")

        params: Dict[str, Any] = {
            "FromUtc": _iso_utc(start_utc),
            "ToUtc": _iso_utc(end_utc),
        }
        logger.debug("JAO %s %s -> %s", path, params["FromUtc"], params["ToUtc"])

        if page_size is None:
            rows = self._get_json(path, params)["data"]
            return list(rows)

        rows: List[Dict[str, Any]] = []
        seen_first_ids = set()
        skip = 0
        while True:
            page = self._get_json(path, {**params, "Skip": skip, "Take": page_size})["data"]
            if not page:
                break
            first = (page[0].get("id"), page[0].get("dateTimeUtc"))
            if first in seen_first_ids:
                # Server ignored Skip and returned the same page again.
                logger.debug("JAO %s: Skip ignored by server, stopping paging", path)
                break
            seen_first_ids.add(first)
            rows.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        return rows
