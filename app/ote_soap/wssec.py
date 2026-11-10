"""
WS-Security signing for CS OTE web services.

Per D1.4.3 section 1.1.2.1:
  * Timestamp is mandatory.
  * Signature is mandatory and must cover Timestamp and Body.
  * The certificate is sent as a BinarySecurityToken outside the Signature
    element and referenced from KeyInfo by Direct Reference (no
    SubjectKeyIdentifier).
  * Exclusive C14N (xml-exc-c14n) for SignedInfo and both references.

The manual's example uses rsa-sha1 / sha1; that is the default. SHA-256 is
available via OTE_SOAP_SIGN_ALG=sha256 in case OTE rejects SHA-1.
"""

from __future__ import annotations

import base64
import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from lxml import etree

NS_SOAP = "http://schemas.xmlsoap.org/soap/envelope/"
NS_WSSE = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
NS_WSU = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
NS_DS = "http://www.w3.org/2000/09/xmldsig#"

EXC_C14N = "http://www.w3.org/2001/10/xml-exc-c14n#"
X509_TOKEN = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3"
BASE64_ENC = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary"

ALGORITHMS = {
    "sha1": {
        "signature": "http://www.w3.org/2000/09/xmldsig#rsa-sha1",
        "digest": "http://www.w3.org/2000/09/xmldsig#sha1",
        "hashlib": "sha1",
        "crypto": hashes.SHA1,
    },
    "sha256": {
        "signature": "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
        "digest": "http://www.w3.org/2001/04/xmlenc#sha256",
        "hashlib": "sha256",
        "crypto": hashes.SHA256,
    },
}

TIMESTAMP_TTL = timedelta(minutes=5)


@dataclass(frozen=True)
class Credentials:
    """Private key + certificate loaded from the participant's PKCS#12 file."""

    private_key: rsa.RSAPrivateKey
    cert_der: bytes
    subject: str
    not_valid_after: datetime

    @classmethod
    def from_pkcs12(cls, path: str | Path, password: str | None) -> "Credentials":
        data = Path(path).read_bytes()
        key, cert, _chain = pkcs12.load_key_and_certificates(
            data, password.encode() if password else None
        )
        if key is None or cert is None:
            raise ValueError(f"PKCS#12 file {path} has no private key or certificate")
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ValueError(f"Only RSA keys are supported, got {type(key).__name__}")
        return cls(
            private_key=key,
            cert_der=cert.public_bytes(serialization.Encoding.DER),
            subject=cert.subject.rfc4514_string(),
            not_valid_after=cert.not_valid_after_utc,
        )


def _q(ns: str, tag: str) -> str:
    return f"{{{ns}}}{tag}"


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _c14n(element: etree._Element) -> bytes:
    return etree.tostring(element, method="c14n", exclusive=True, with_comments=False)


def _utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_signed_envelope(
    payload: etree._Element, creds: Credentials, algorithm: str = "sha1"
) -> bytes:
    """
    Wrap `payload` (the <SendRequest> element of the target service) in a
    SOAP 1.1 envelope signed per D1.4.3. Returns the serialized request bytes.
    """
    if algorithm not in ALGORITHMS:
        raise ValueError(f"Unknown signing algorithm {algorithm!r}; use one of {sorted(ALGORITHMS)}")
    alg = ALGORITHMS[algorithm]

    nsmap = {"soapenv": NS_SOAP}
    envelope = etree.Element(_q(NS_SOAP, "Envelope"), nsmap=nsmap)
    header = etree.SubElement(envelope, _q(NS_SOAP, "Header"))
    body = etree.SubElement(
        envelope, _q(NS_SOAP, "Body"), nsmap={"wsu": NS_WSU}
    )
    body_id = _new_id("id")
    body.set(_q(NS_WSU, "Id"), body_id)
    body.append(payload)

    security = etree.SubElement(
        header, _q(NS_WSSE, "Security"), nsmap={"wsse": NS_WSSE, "wsu": NS_WSU}
    )
    security.set(_q(NS_SOAP, "mustUnderstand"), "1")

    token_id = _new_id("X509")
    token = etree.SubElement(security, _q(NS_WSSE, "BinarySecurityToken"))
    token.set("EncodingType", BASE64_ENC)
    token.set("ValueType", X509_TOKEN)
    token.set(_q(NS_WSU, "Id"), token_id)
    token.text = base64.b64encode(creds.cert_der).decode("ascii")

    ts_id = _new_id("TS")
    now = datetime.now(timezone.utc)
    timestamp = etree.SubElement(security, _q(NS_WSU, "Timestamp"))
    timestamp.set(_q(NS_WSU, "Id"), ts_id)
    etree.SubElement(timestamp, _q(NS_WSU, "Created")).text = _utc(now)
    etree.SubElement(timestamp, _q(NS_WSU, "Expires")).text = _utc(now + TIMESTAMP_TTL)

    signature = etree.SubElement(security, _q(NS_DS, "Signature"), nsmap={"ds": NS_DS})
    signature.set("Id", _new_id("SIG"))
    signed_info = etree.SubElement(signature, _q(NS_DS, "SignedInfo"))
    etree.SubElement(signed_info, _q(NS_DS, "CanonicalizationMethod")).set("Algorithm", EXC_C14N)
    etree.SubElement(signed_info, _q(NS_DS, "SignatureMethod")).set("Algorithm", alg["signature"])

    for ref_id, target in ((ts_id, timestamp), (body_id, body)):
        reference = etree.SubElement(signed_info, _q(NS_DS, "Reference"))
        reference.set("URI", f"#{ref_id}")
        transforms = etree.SubElement(reference, _q(NS_DS, "Transforms"))
        etree.SubElement(transforms, _q(NS_DS, "Transform")).set("Algorithm", EXC_C14N)
        etree.SubElement(reference, _q(NS_DS, "DigestMethod")).set("Algorithm", alg["digest"])
        digest = hashlib.new(alg["hashlib"], _c14n(target)).digest()
        etree.SubElement(reference, _q(NS_DS, "DigestValue")).text = base64.b64encode(digest).decode("ascii")

    signature_value = etree.SubElement(signature, _q(NS_DS, "SignatureValue"))
    signature_value.text = base64.b64encode(
        creds.private_key.sign(_c14n(signed_info), padding.PKCS1v15(), alg["crypto"]())
    ).decode("ascii")

    key_info = etree.SubElement(signature, _q(NS_DS, "KeyInfo"))
    token_ref = etree.SubElement(key_info, _q(NS_WSSE, "SecurityTokenReference"))
    direct_ref = etree.SubElement(token_ref, _q(NS_WSSE, "Reference"))
    direct_ref.set("URI", f"#{token_id}")
    direct_ref.set("ValueType", X509_TOKEN)

    return etree.tostring(envelope, xml_declaration=True, encoding="UTF-8")


def verify_own_envelope(envelope_bytes: bytes, creds: Credentials) -> None:
    """
    Self-check used by --dry-run: re-parse the serialized envelope, recompute
    both digests and verify the signature with our own public key. Raises
    ValueError on any mismatch. This proves the bytes on the wire are
    internally consistent; only OTE can confirm it accepts them.
    """
    root = etree.fromstring(envelope_bytes)
    ns = {"ds": NS_DS, "wsu": NS_WSU, "wsse": NS_WSSE, "soapenv": NS_SOAP}
    signed_info = root.find(".//ds:SignedInfo", ns)
    method = signed_info.find("ds:SignatureMethod", ns).get("Algorithm")
    alg = next(a for a in ALGORITHMS.values() if a["signature"] == method)

    for reference in signed_info.findall("ds:Reference", ns):
        ref_id = reference.get("URI")[1:]
        targets = root.xpath("//*[@wsu:Id=$i]", namespaces=ns, i=ref_id)
        if len(targets) != 1:
            raise ValueError(f"Reference #{ref_id} matches {len(targets)} elements")
        expected = reference.find("ds:DigestValue", ns).text
        actual = base64.b64encode(hashlib.new(alg["hashlib"], _c14n(targets[0])).digest()).decode()
        if expected != actual:
            raise ValueError(f"Digest mismatch for #{ref_id}")

    signature = base64.b64decode(root.find(".//ds:SignatureValue", ns).text)
    creds.private_key.public_key().verify(
        signature, _c14n(signed_info), padding.PKCS1v15(), alg["crypto"]()
    )
