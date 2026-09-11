"""Add ote_intraday_limit_utilization (OTE portal "Fin. security trend for limit IM").

Revision ID: 079
Revises: 078
Create Date: 2026-09-11

Source: OTE portal, Risk Manag. > Financial security > report type
"Fin. security trend for limit IM" (reportCode financial_security_trend_offline_main),
exported as XLSX by ote_intraday_limit_downloader.py and parsed by
upload_intraday_limit.py.

One row per intraday-market (VDT/IM) order event as OTE books it against the
participant's financial-security limit. The report's period filter is on the
event timestamp, not the delivery day, so a delivery-day D order placed on D-1
appears in the D-1 export.

(event_timestamp, order_id) is the key: the XLSX carries millisecond
timestamps and the same order_id legitimately recurs (place / modify / cancel),
each with its own timestamp. The CSV export only has second precision and
collides on this key - never feed it to the uploader.

Timestamps are Europe/Prague wall-clock as printed by the portal (naive
TIMESTAMP, same convention as the other OTE tables).
"""

from alembic import op


revision = '079'
down_revision = '078'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ote_intraday_limit_utilization (
            id SERIAL,
            event_timestamp TIMESTAMP NOT NULL,
            order_type VARCHAR(10) NOT NULL,
            order_id BIGINT NOT NULL,
            delivery_date DATE NOT NULL,
            utilization_change_commodity NUMERIC(15, 4),
            utilization_change_imbalance NUMERIC(15, 4),
            utilization_change_total NUMERIC(15, 4),
            utilization_total NUMERIC(15, 2),
            limit_total NUMERIC(15, 2),
            limit_available NUMERIC(15, 2),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            CONSTRAINT ote_intraday_limit_utilization_event_order_key
                UNIQUE (event_timestamp, order_id)
        );
    """)
    op.execute(
        "CREATE INDEX ix_ote_intraday_limit_utilization_delivery_date "
        "ON ote_intraday_limit_utilization (delivery_date);"
    )
    op.execute(
        "COMMENT ON TABLE ote_intraday_limit_utilization IS "
        "'OTE portal report \"Fin. security trend for limit IM\" "
        "(financial_security_trend_offline_main): per-order utilization of the "
        "intraday-market financial-security limit. Timestamps are Europe/Prague "
        "wall-clock.';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.event_timestamp IS "
        "'Report column Timestamp / Datumova znacka (ms precision).';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.order_type IS "
        "'Report column Bid type / Typ pokynu (VDT).';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.order_id IS "
        "'Report column Bid ID / Identifikace pokynu.';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.utilization_change_commodity IS "
        "'Utilization change commodity / Zmena utilizace komodita (CZK).';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.utilization_change_imbalance IS "
        "'Utilization change deviation / Zmena utilizace odchylka (CZK).';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.utilization_change_total IS "
        "'Utilization change total / Zmena utilizace celkem (CZK).';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.utilization_total IS "
        "'Total utilization IM electricity / Celkem utilizace VDT elektrina (CZK, running).';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.limit_total IS "
        "'Total limit IM electricity / Celkem limit VDT elektrina (CZK).';"
    )
    op.execute(
        "COMMENT ON COLUMN ote_intraday_limit_utilization.limit_available IS "
        "'Free limit IM electricity / Volne prostredky limitu VDT elektrina (CZK).';"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ote_intraday_limit_utilization "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE ote_intraday_limit_utilization_id_seq "
        "TO user_finance;"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ote_intraday_limit_utilization CASCADE;")
