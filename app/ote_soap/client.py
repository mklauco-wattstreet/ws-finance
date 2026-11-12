"""
HTTPS transport for CS OTE web services and response parsing.

Production endpoints come from OTE's "Formulář pro získání údajů k nastavení
automatické komunikace" (Dodatek): https://csote.ote-cr.cz:443/KSX/<service>.
Override the base with OTE_SOAP_BASE_URL (e.g. for the Sandbox,
https://csote.sand.ote-cr.cz:1443/KSX).

Client-server mode: no TLS client certificate is needed (OTE's
"Použití certifikátů v automatické komunikaci": "certifikát použít nemusí");
identity comes from the WS-Security signature.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ote_soap.wssec import NS_DS, NS_SOAP, Credentials, build_signed_envelope

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("OTE_SOAP_BASE_URL", "https://csote.ote-cr.cz:443/KSX").rstrip("/")
SIGN_ALG = os.getenv("OTE_SOAP_SIGN_ALG", "sha1")
TIMEOUT_S = (15, 120)  # connect, read

NS_SVC_GLOBALS = "http://www.ote-cr.cz/schema/service/globals"
NS_RESPONSE = "http://www.ote-cr.cz/schema/response"


class OteSoapError(RuntimeError):
    """SOAP Fault, HTTP error or unparseable response."""


@dataclass
class OteResponse:
    return_code: str | None
    response_code: str | None = None          # RESPONSE@message-code
    reasons: list[dict] = field(default_factory=list)  # RESPONSE/Reason attributes + text
    reference_id: str | None = None           # RESPONSE/Reference@id (our request id)
    payload: list[etree._Element] = field(default_factory=list)  # data elements (SFVOTLIMITS, ...)
    raw: bytes = b""

    @property
    def payload_names(self) -> list[str]:
        return [etree.QName(p).localname for p in self.payload]


def _session() -> requests.Session:
    # Retry only when nothing reached OTE (connect errors) or OTE said it is
    # unavailable. No read retries: a queue pickup (921) whose answer was lost
    # must not be blindly repeated.
    retry = Retry(
        total=3, connect=3, read=0, status=3,
        backoff_factor=2,
        status_forcelist=(502, 503, 504),
        allowed_methods=frozenset({"POST"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def reference_id(element: etree._Element) -> str | None:
    """Reference@id of an OTE message element (the id of the request it answers)."""
    for child in element:
        if isinstance(child.tag, str) and etree.QName(child).localname == "Reference":
            return child.get("id")
    return None


def parse_response(raw: bytes) -> OteResponse:
    try:
        root = etree.fromstring(raw)
    except etree.XMLSyntaxError as exc:
        raise OteSoapError(f"Response is not XML: {raw[:300]!r}") from exc

    body = root.find(f"{{{NS_SOAP}}}Body")
    if body is None:
        raise OteSoapError("Response has no SOAP Body")
    fault = body.find(f"{{{NS_SOAP}}}Fault")
    if fault is not None:
        code = fault.findtext("faultcode")
        text = fault.findtext("faultstring")
        raise OteSoapError(f"SOAP Fault {code}: {text}")

    send_response = next(iter(body), None)
    if send_response is None:
        raise OteSoapError("SOAP Body is empty")

    result = OteResponse(return_code=None, raw=raw)
    for child in send_response:
        if not isinstance(child.tag, str):
            continue
        qname = etree.QName(child)
        if qname.localname == "RETURN_CODE":
            result.return_code = (child.text or "").strip()
        elif qname.namespace == NS_RESPONSE and qname.localname == "RESPONSE":
            result.response_code = child.get("message-code")
            ref = child.find(f"{{{NS_RESPONSE}}}Reference")
            result.reference_id = ref.get("id") if ref is not None else None
            for reason in child.findall(f"{{{NS_RESPONSE}}}Reason"):
                result.reasons.append({**reason.attrib, "text": (reason.text or "").strip()})
        elif qname.namespace == NS_DS:
            continue
        else:
            result.payload.append(child)
    return result


class OteSoapClient:
    def __init__(self, creds: Credentials, base_url: str = BASE_URL, sign_alg: str = SIGN_ALG):
        self.creds = creds
        self.base_url = base_url
        self.sign_alg = sign_alg
        self.session = _session()
        self.last_request: bytes = b""
        # Raw answer of the most recent send(), kept even when send() raises
        # (SOAP Fault, HTTP >= 400, non-XML, empty body) so callers can save it.
        self.last_response: bytes = b""
        self.last_status: int | None = None

    def build(self, payload: etree._Element) -> bytes:
        return build_signed_envelope(payload, self.creds, self.sign_alg)

    def send(self, service: str, payload: etree._Element) -> OteResponse:
        """Sign and POST. The exact bytes sent are kept in self.last_request;
        the raw HTTP answer and its status in self.last_response / self.last_status
        (also when this raises). Both answer fields are cleared at the start of each call."""
        self.last_response = b""
        self.last_status = None
        url = f"{self.base_url}/{service}"
        envelope = self.build(payload)
        self.last_request = envelope
        logger.debug("POST %s (%d bytes, sign=%s)", url, len(envelope), self.sign_alg)
        try:
            resp = self.session.post(
                url,
                data=envelope,
                headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": '""'},
                timeout=TIMEOUT_S,
            )
        except requests.RequestException as exc:
            raise OteSoapError(f"HTTP request to {url} failed: {exc}") from exc

        self.last_response = resp.content
        self.last_status = resp.status_code
        logger.debug("HTTP %s, %d bytes", resp.status_code, len(resp.content))
        if resp.status_code >= 400 and b"Fault" not in resp.content:
            raise OteSoapError(f"HTTP {resp.status_code} from {url}: {resp.text[:500]}")
        return parse_response(resp.content)
