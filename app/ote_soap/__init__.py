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
    message_log  finance.ote_soap_message: one row per message sent or received, failed
              calls included (written by shared.send_report / shared.pickup; never raises)
    report    shared "ask and wait" (request -> poll the queue -> match the answer),
              sets the outcome of the report request
    limits_parser  SFVOTLIMITS (464) -> one long row per value (offline, XSD-validated)
    limits    CLI/cron: request 463, wait for the 464, pivot to one row per snapshot,
              upsert into finance.ote_limit_snapshot
    exchange_rate_parser  SFVOTEXCHRATE (426) -> one row per valid date (offline, XSD-validated)
    exchange_rate  CLI/cron: request 425, wait for the 426, upsert into
              finance.ote_exchange_rate
    billing_parser  SFVOTBILLING (401 / 404) -> message data, one line per <Data>, one summary per
              <Summary> (offline, XSD-validated)
    billing   CLI/cron: request 400 and 403 for a month, wait for the 401 and 404, update / insert
              finance.ote_monthly_settlement_data, upsert finance.ote_monthly_settlement_summary
"""
