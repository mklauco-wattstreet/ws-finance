"""Add ote_financial_security_trend (OTE portal "Fin. security trend").

Revision ID: 081
Revises: 080
Create Date: 2026-09-18

Source: OTE portal, Risk Manag. > Financial security > report type
"Fin. security trend" (reportCode financial_security_trend_main), XLSX
export parsed by upload_financial_security_trend.py.

Ledger of the participant's overall financial security. One event
(Timestamp + Trade ID) is spread over several rows, one per utilization
bucket (Utilization type: RE, DYN, VDT, DEV, POP, LIM_VDT); each row carries
that bucket's change and a snapshot of every limit after it. Trade type is
what caused the event (VDT trade, BATCH_E1 settlement batch, BANK_PAIR bank
pairing, LIM_VDT limit change); Delivery day and Trade (price) are empty for
non-trade events.

Sibling of ote_intraday_limit_utilization, which holds the narrower
"Fin. security trend for limit IM" report (financial_security_trend_offline_main).

Timestamps are whole seconds, Europe/Prague wall-clock as printed by the
portal (naive TIMESTAMP, same convention as the other OTE tables).
(event_timestamp, utilization_type, trade_id) was unique across 33 745 rows
of Aug-Sep 2026 exports.
"""

from alembic import op


revision = '081'
down_revision = '080'
branch_labels = None
depends_on = None

TABLE = 'ote_financial_security_trend'

COMMENTS = {
    None: ('OTE portal report "Fin. security trend" (financial_security_trend_main): '
           'ledger of the participant\'s overall financial security, one row per '
           'event x utilization bucket. Timestamps are Europe/Prague wall-clock, '
           'whole seconds. Amounts in CZK.'),
    'event_timestamp': 'Timestamp / Datumova znacka (whole seconds).',
    'utilization_type': 'Utilization type / Typ utilizace: RE, DYN, VDT, DEV, POP, LIM_VDT.',
    'trade_type': 'Trade type / Typ pozadavku: VDT, BATCH_E1, BANK_PAIR, LIM_VDT.',
    'trade_id': 'Trade ID / Identifikace obchodu.',
    'delivery_date': 'Delivery day / Den dodavky; NULL for non-trade events.',
    'trade_price': 'Trade / Obchod: price part of e.g. "10.40 EUR"; NULL for non-trade events.',
    'trade_currency': 'Trade / Obchod: currency part of e.g. "10.40 EUR".',
    'utilization_czk': 'Utilization / Utilizace: this event\'s change in this bucket (CZK).',
    'total_utilization_by_type_czk': 'Total utiliz. by type / Celk.utilizace dle typu: running total of this bucket after the event (CZK).',
    'total_utilization_czk': 'Total utilization / Celkova utilizace: running total over all buckets after the event (CZK).',
    'static_limit_czk': 'Static limit / Staticky limit (CZK).',
    'dynamic_limit_czk': 'Dynamic limit / Dynamicky limit (CZK).',
    'total_limit_czk': 'Total limit / Celkovy limit = static + dynamic (CZK).',
    'free_resources_czk': 'Free resources / Volne prostredky = total limit + total utilization (CZK).',
}


def upgrade() -> None:
    op.execute(f"""
        CREATE TABLE {TABLE} (
            id SERIAL,
            event_timestamp TIMESTAMP NOT NULL,
            utilization_type VARCHAR(10) NOT NULL,
            trade_type VARCHAR(10) NOT NULL,
            trade_id BIGINT NOT NULL,
            delivery_date DATE,
            trade_price NUMERIC(12, 2),
            trade_currency VARCHAR(3),
            utilization_czk NUMERIC(15, 2),
            total_utilization_by_type_czk NUMERIC(15, 2),
            total_utilization_czk NUMERIC(15, 2),
            static_limit_czk NUMERIC(15, 2),
            dynamic_limit_czk NUMERIC(15, 2),
            total_limit_czk NUMERIC(15, 2),
            free_resources_czk NUMERIC(15, 2),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            CONSTRAINT {TABLE}_event_type_trade_key
                UNIQUE (event_timestamp, utilization_type, trade_id)
        );
    """)
    op.execute(
        f"CREATE INDEX ix_{TABLE}_delivery_date ON {TABLE} (delivery_date);"
    )
    for column, text in COMMENTS.items():
        target = f"TABLE {TABLE}" if column is None else f"COLUMN {TABLE}.{column}"
        op.execute(f"COMMENT ON {target} IS '{text.replace(chr(39), chr(39) * 2)}';")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {TABLE} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {TABLE}_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
