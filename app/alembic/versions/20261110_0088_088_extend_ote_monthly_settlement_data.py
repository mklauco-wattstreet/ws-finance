"""Extend ote_monthly_settlement_data for the OTE SOAP loader; add ote_monthly_settlement_summary.

Revision ID: 088
Revises: 087
Create Date: 2026-11-10

ote_monthly_settlement_data (created outside Alembic, owner user_finance) holds OTE's
monthly billing data: message SFVOTBILLING code 401 ("Podklady pro fakturaci",
Billing@ote=0, Watt Street invoices OTE) and code 404 ("Podklady pro fakturaci OTE",
Billing@ote=1, OTE invoices Watt Street), one row per <Data> line. XSD:
D1.4.2_XSD_sablony_2026_10_26/XML/SFVOTBILLING/SFVOTBILLING.xsd. Until now it was filled
only by the trader web app's XML upload (ImportOteSettlementXml); from now on
app/ote_soap/billing.py also loads it from OTE SOAP (requests 400/403).

Additive changes to ote_monthly_settlement_data only (no column is dropped, renamed or
retyped, grants are untouched):
- ten new nullable columns: currency, original_amount_czk, original_amount_eur,
  original_etax_czk, original_etax_eur, payment_czk, value_date, message_id, request_id,
  source (the XSD attributes the table did not carry, plus SOAP bookkeeping);
- unique constraint ote_monthly_settlement_data_line_key on (trade_date, variable_symbol,
  trade_type, version, invoice_from_ote) with NULLS NOT DISTINCT (PostgreSQL 15+), because
  variable_symbol is NULL on many rows. It is the duplicate rule both loaders already
  follow; the SOAP loader upserts on it. The migration first counts duplicate keys and
  raises a RuntimeError naming the number if there are any.
- COMMENT ON TABLE and on every column.

ote_monthly_settlement_summary (new): one row per <Summary> element. The XSD requires
exactly one <Summary> in each <TradeType> (no minOccurs/maxOccurs, so 1..1), i.e. one per
trade type x version in a message. The period totals are kept out of the main table
because its readers sum all rows. Numeric columns are plain NUMERIC (no precision or
scale): stored exactly as OTE sent them. Unique key
ote_monthly_settlement_summary_key (date_from, date_to, invoice_from_ote, trade_type,
version) NULLS NOT DISTINCT; the loader upserts on it. Not partitioned.
"""

from alembic import op


revision = '088'
down_revision = '087'
branch_labels = None
depends_on = None

DATA_TABLE = 'ote_monthly_settlement_data'
SUMMARY_TABLE = 'ote_monthly_settlement_summary'
LINE_KEY = f'{DATA_TABLE}_line_key'
KEY_COLUMNS = 'trade_date, variable_symbol, trade_type, version, invoice_from_ote'

NEW_COLUMNS = [
    ('currency', 'VARCHAR(3)'),
    ('original_amount_czk', 'NUMERIC(15,3)'),
    ('original_amount_eur', 'NUMERIC(15,3)'),
    ('original_etax_czk', 'NUMERIC(15,3)'),
    ('original_etax_eur', 'NUMERIC(15,3)'),
    ('payment_czk', 'NUMERIC(15,3)'),
    ('value_date', 'DATE'),
    ('message_id', 'VARCHAR(35)'),
    ('request_id', 'VARCHAR(35)'),
    ('source', 'VARCHAR(10)'),
]

DATA_COMMENTS = {
    None: ('OTE monthly settlement data (Podklady pro fakturaci): one row per <Data> line (trade date) of an OTE '
           'SFVOTBILLING message, code 401 (Billing@ote=0, Watt Street invoices OTE) or 404 (Billing@ote=1, OTE invoices '
           'Watt Street). Loaded by two paths: the trader web app XML upload (ImportOteSettlementXml) and, from '
           'migration 088, OTE SOAP requests 400/403 via app/ote_soap/billing.py. Duplicate rule for both: one row per '
           '(trade_date, variable_symbol, trade_type, version, invoice_from_ote), NULLs equal; enforced by the unique '
           'constraint ote_monthly_settlement_data_line_key. Period totals (Summary elements) are in '
           'ote_monthly_settlement_summary, not here.'),
    'id': 'Surrogate primary key.',
    'header': ('Message header as JSON, from the message that inserted the row: keys root (SFVOTBILLING attributes), '
               'SenderIdentification, ReceiverIdentification; rows loaded from SOAP also carry Reference (our SFVOTREQ '
               'request), Billing (Billing attributes: date, dateFrom, dateTo, title, ote) and Rut (participant: id, '
               'name, ico, dic, ean).'),
    'trade_date': 'Data@tradeDate: trade date the line refers to.',
    'variable_symbol': 'Data@varSymb: variable symbol (payment reference number); NULL on lines without one.',
    'trade_type': ('TradeType@tradeType: OTE trade type code, e.g. DT day-ahead market, DE1 imbalance, VD+/VD- intraday '
                   'market supply/consumption, DA+/DA- intraday auction, PP1-PP9 fees (full list in the XSD).'),
    'version': 'TradeType@version: settlement type, 0 = daily (advance), 1 = monthly (payments).',
    'invoice_from_ote': ('Billing@ote: 1 = OTE invoices Watt Street (message 404), 0 = Watt Street invoices OTE '
                         '(message 401).'),
    'volume_mwh': 'Data@volume: volume in MWh.',
    'exchange_rate_czk_eur': 'Data@exchRate: exchange rate EUR (CZK per 1 EUR).',
    'original_amount_net_czk': 'Data@origAmountNetCzk: amount without VAT, CZK.',
    'original_amount_net_eur': 'Data@origAmountNetEur: amount without VAT, EUR.',
    'original_vat_czk': 'Data@origVatCzk: VAT, CZK.',
    'original_vat_eur': 'Data@origVatEur: VAT, EUR.',
    'payment_eur': 'Data@paymentEur: payment, EUR.',
    'created_at': 'When the row was inserted (naive timestamp, server local time).',
    'updated_at': 'When the row was last updated (naive timestamp, server local time).',
    'currency': 'TradeType@curr: settlement currency, CZK or EUR.',
    'original_amount_czk': 'Data@origAmountCzk: amount with tax, CZK. In the XSD; not sent in the 2026-09 messages, then NULL.',
    'original_amount_eur': 'Data@origAmountEur: amount with tax, EUR.',
    'original_etax_czk': 'Data@origEtaxCzk: electricity tax, CZK. In the XSD; not sent in the 2026-09 messages, then NULL.',
    'original_etax_eur': 'Data@origEtaxEur: electricity tax, EUR. In the XSD; not sent in the 2026-09 messages, then NULL.',
    'payment_czk': 'Data@paymentCzk: payment, CZK. In the XSD; not sent in the 2026-09 messages, then NULL.',
    'value_date': 'Data@valueDate: value (due) date. In the XSD; not sent in the 2026-09 messages, then NULL.',
    'message_id': 'SFVOTBILLING@id: id of the SOAP message that last wrote the row; NULL for rows from the web upload only.',
    'request_id': 'SFVOTBILLING/Reference@id: id of our SFVOTREQ request; NULL if not loaded from SOAP.',
    'source': "'soap' when the row was inserted or last updated by app/ote_soap/billing.py; NULL = trader web upload only.",
}

SUMMARY_COMMENTS = {
    None: ('OTE monthly settlement period totals: one row per <Summary> element of an SFVOTBILLING message (code 401 or '
           '404). The XSD requires exactly one Summary in every TradeType, so one row per trade type x version in a '
           'message. Kept apart from ote_monthly_settlement_data because that table is summed row by row. Loaded by '
           'app/ote_soap/billing.py (requests 400/403); upserted on ote_monthly_settlement_summary_key. Numbers stored '
           'exactly as OTE sent them.'),
    'id': 'Surrogate primary key.',
    'date_from': 'Billing@dateFrom (date part): start of the billed period.',
    'date_to': 'Billing@dateTo (date part): end of the billed period.',
    'invoice_from_ote': 'Billing@ote: 1 = OTE invoices Watt Street (message 404), 0 = Watt Street invoices OTE (message 401).',
    'message_code': 'SFVOTBILLING@message-code: 401 = Podklady pro fakturaci, 404 = Podklady pro fakturaci OTE.',
    'trade_type': 'TradeType@tradeType: OTE trade type code (same codes as ote_monthly_settlement_data.trade_type).',
    'version': 'TradeType@version: settlement type, 0 = daily (advance), 1 = monthly (payments).',
    'currency': 'TradeType@curr: settlement currency, CZK or EUR.',
    'volume_mwh': 'Summary@volume: total volume in MWh.',
    'original_amount_czk': 'Summary@origAmountCzk: total amount with tax, CZK.',
    'original_amount_eur': 'Summary@origAmountEur: total amount with tax, EUR.',
    'original_amount_net_czk': 'Summary@origAmountNetCzk: total amount without VAT, CZK.',
    'original_amount_net_eur': 'Summary@origAmountNetEur: total amount without VAT, EUR.',
    'original_etax_czk': 'Summary@origEtaxCzk: total electricity tax, CZK.',
    'original_etax_eur': 'Summary@origEtaxEur: total electricity tax, EUR.',
    'original_vat_czk': 'Summary@origVatCzk: total VAT, CZK.',
    'original_vat_eur': 'Summary@origVatEur: total VAT, EUR.',
    'payment_czk': 'Summary@paymentCzk: total payment, CZK.',
    'payment_eur': 'Summary@paymentEur: total payment, EUR.',
    'message_id': 'SFVOTBILLING@id: id of the message the row was last loaded from.',
    'request_id': 'SFVOTBILLING/Reference@id: id of our SFVOTREQ request; NULL if OTE sent the message unrequested.',
    'message_at': 'SFVOTBILLING@date-time: date and time of the message.',
    'created_at': 'When the row was inserted.',
    'updated_at': 'When the row was last updated.',
}


def _apply_comments(table: str, comments: dict) -> None:
    for column, text in comments.items():
        target = f"TABLE {table}" if column is None else f"COLUMN {table}.{column}"
        op.execute(f"COMMENT ON {target} IS '{text.replace(chr(39), chr(39) * 2)}';")


def _count_duplicate_keys() -> int:
    """Number of key groups on the five line-key columns with more than one row (NULLs equal)."""
    result = op.get_bind().exec_driver_sql(
        f"SELECT count(*) FROM (SELECT 1 FROM {DATA_TABLE} GROUP BY {KEY_COLUMNS} "
        f"HAVING count(*) > 1) AS duplicate_keys"
    )
    return int(result.scalar() or 0)


def upgrade() -> None:
    duplicates = _count_duplicate_keys()
    if duplicates:
        raise RuntimeError(
            f"Cannot add unique constraint {LINE_KEY} on {DATA_TABLE}: {duplicates} duplicate key(s) "
            f"on ({KEY_COLUMNS}) with NULLs treated as equal. Remove the duplicate rows first, "
            f"then rerun the migration."
        )
    for column, sql_type in NEW_COLUMNS:
        op.execute(f"ALTER TABLE {DATA_TABLE} ADD COLUMN IF NOT EXISTS {column} {sql_type};")
    op.execute(
        f"ALTER TABLE {DATA_TABLE} ADD CONSTRAINT {LINE_KEY} "
        f"UNIQUE NULLS NOT DISTINCT ({KEY_COLUMNS});"
    )
    _apply_comments(DATA_TABLE, DATA_COMMENTS)

    op.execute(f"""
        CREATE TABLE {SUMMARY_TABLE} (
            id BIGSERIAL,
            date_from DATE NOT NULL,
            date_to DATE NOT NULL,
            invoice_from_ote INTEGER NOT NULL,
            message_code VARCHAR(3) NOT NULL,
            trade_type VARCHAR(100) NOT NULL,
            version INTEGER,
            currency VARCHAR(3),
            volume_mwh NUMERIC,
            original_amount_czk NUMERIC,
            original_amount_eur NUMERIC,
            original_amount_net_czk NUMERIC,
            original_amount_net_eur NUMERIC,
            original_etax_czk NUMERIC,
            original_etax_eur NUMERIC,
            original_vat_czk NUMERIC,
            original_vat_eur NUMERIC,
            payment_czk NUMERIC,
            payment_eur NUMERIC,
            message_id VARCHAR(35) NOT NULL,
            request_id VARCHAR(35),
            message_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT {SUMMARY_TABLE}_pkey PRIMARY KEY (id),
            CONSTRAINT {SUMMARY_TABLE}_key UNIQUE NULLS NOT DISTINCT
                (date_from, date_to, invoice_from_ote, trade_type, version)
        );
    """)
    _apply_comments(SUMMARY_TABLE, SUMMARY_COMMENTS)
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {SUMMARY_TABLE} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {SUMMARY_TABLE}_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {SUMMARY_TABLE} CASCADE;")
    op.execute(f"ALTER TABLE {DATA_TABLE} DROP CONSTRAINT IF EXISTS {LINE_KEY};")
    for column, _ in NEW_COLUMNS:
        op.execute(f"ALTER TABLE {DATA_TABLE} DROP COLUMN IF EXISTS {column};")
