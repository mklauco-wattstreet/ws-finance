"""Generic ENTSO-E document query.

``fetch_document`` sends any parameter set through an existing
:class:`EntsoeClient` (its session with retry/backoff, base URL, security token
and error classification) and returns the XML documents of the answer. It
exists so new cross-border runners do not each need a bespoke fetch method in
``client.py`` (which is over the file-size cap and only gets bug fixes).

Semantics:
    - ``None``         ENTSO-E answered "No matching data" (an
                       Acknowledgement_MarketDocument, reason 999, as HTTP 200
                       or HTTP 400). A normal empty result, never an error.
                       Other code-999 acknowledgements (bad parameter, invalid
                       document type) are real errors and raise.
    - ``list[str]``    one element for a plain XML response; one element per
                       XML file for a ZIP response (documentType A78 returns
                       ZIPs of up to 200 documents).
    - raises           ``requests.RequestException`` for every other failure,
                       with the security token masked as ``***`` and ENTSO-E's
                       reason text appended.

Paging (``offset``) is transparent: the caller puts ``"offset": "200"`` in
``params`` like any other parameter.
"""

import io
import logging
import re
import zipfile
from datetime import datetime, timezone
from typing import Optional

import requests

from entsoe.client import EntsoeClient

logger = logging.getLogger(__name__)


def fmt_period(dt: datetime) -> str:
    """Format a datetime as the ENTSO-E ``yyyyMMddHHmm`` UTC string.

    Timezone-aware values are converted to UTC; naive values are taken as UTC
    (same rule as ``EntsoeClient._format_timestamp``).
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M")


_REASON_TEXT = re.compile(r"<Reason>.*?<text>(.*?)</text>", re.DOTALL)


def _is_no_data(text: str) -> bool:
    """True when a body is an Acknowledgement_MarketDocument saying "No matching data".

    Deliberately stricter than ``EntsoeClient._is_no_data_acknowledgement``,
    which also accepts any ``<code>999</code>``: ENTSO-E uses code 999 for
    genuine errors too (unparsable parameter, invalid document type), and a
    generic query must not turn those into an empty result.
    """
    return "Acknowledgement_MarketDocument" in text and "No matching data" in text


def _error(client: EntsoeClient, error: requests.RequestException, shown: dict) -> requests.RequestException:
    """Build the raised exception: token masked, ENTSO-E's reason text appended.

    Not ``client._fetch_error``: that tags every code-999 400 as "No matching
    data", which BaseRunner then logs at INFO instead of as an error.
    """
    message = f"ENTSO-E request failed ({shown}): {client._sanitize_error(error)}"
    response = getattr(error, "response", None)
    if response is not None:
        try:
            match = _REASON_TEXT.search(response.text)
        except Exception:
            match = None
        if match:
            message += f" - reason: {match.group(1).strip()}"
    return requests.RequestException(message)


def _unzip_all(content: bytes) -> list[str]:
    """Return every XML file of a ZIP archive as text, in archive order."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            xml_files = [name for name in zf.namelist() if name.lower().endswith(".xml")]
            if not xml_files:
                raise ValueError("No XML file found in zip archive")
            return [zf.read(name).decode("utf-8") for name in xml_files]
    except zipfile.BadZipFile:
        raise ValueError("Invalid zip file received from API")


def fetch_document(
    client: EntsoeClient,
    params: dict[str, str],
    timeout: Optional[tuple] = None,
) -> Optional[list[str]]:
    """Query the ENTSO-E API with an arbitrary parameter set.

    Args:
        client: Existing client; its session, base URL, token and retry
            settings are reused. The token is added here (a ``securityToken``
            in ``params`` is ignored) and never logged.
        params: Query parameters exactly as the API names them, for example
            ``{"documentType": "A11", "in_Domain": ..., "out_Domain": ...,
            "periodStart": fmt_period(start), "periodEnd": fmt_period(end)}``.
            Include ``offset`` for paging.
        timeout: Optional ``(connect, read)`` override; defaults to
            ``client.REQUEST_TIMEOUT``.

    Returns:
        ``None`` on "No matching data", otherwise a list of XML strings (one
        per document / ZIP member).

    Raises:
        requests.RequestException: request failed after retries, or a genuine
            HTTP error. The message never contains the token.
        ValueError: the answer was a ZIP without XML files or a corrupt ZIP.
    """
    query = {k: v for k, v in params.items() if k != "securityToken"}
    query["securityToken"] = client.security_token
    shown = {k: v for k, v in query.items() if k != "securityToken"}

    try:
        response = client.session.get(
            client.base_url, params=query, timeout=timeout or client.REQUEST_TIMEOUT
        )
        response.raise_for_status()
    except requests.HTTPError as e:
        if (
            e.response is not None
            and e.response.status_code == 400
            and _is_no_data(e.response.text)
        ):
            logger.debug("No matching data for %s", shown)
            return None
        raise _error(client, e, shown)
    except requests.RequestException as e:
        raise _error(client, e, shown)

    content_type = response.headers.get("Content-Type", "")
    if "zip" in content_type or client._is_zip_content(response.content):
        return _unzip_all(response.content)

    text = response.text
    if _is_no_data(text):
        logger.debug("No matching data (HTTP 200 acknowledgement) for %s", shown)
        return None
    return [text]
