"""
OTE CS OTE B2B web services (SOAP 1.1, client-server mode).

Spec: D1.4.3 "Externí rozhraní CS OTE - Webové služby" v2.3 and
D1.4.4 chapter 8 (FZ - financial reports). Message schemas live in the
OTE XSD bundle (D1.4.2_XSD_sablony_2026_10_26).

Modules:
    wssec     WS-Security envelope: Timestamp + BinarySecurityToken + XML Signature
    messages  SFVOTREQ (report request) and COMMONREQ 921 (queue pickup) builders
    client    HTTPS transport, response parsing
    fetch     CLI: request a report, collect queued messages, save raw XML to disk
    limits_parser  SFVOTLIMITS (464) -> one long row per value (offline, XSD-validated)
    limits    CLI/cron: request 463, wait for the 464, pivot to one row per snapshot,
              upsert into finance.ote_limit_snapshot
"""
