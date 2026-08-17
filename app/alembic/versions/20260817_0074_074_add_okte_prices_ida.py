"""Add okte_prices_ida table for OKTE (SK) Intraday Auction prices.

Revision ID: 074
Revises: 073
Create Date: 2026-08-17

Slovak counterpart to ote_prices_ida (migration 049). Source is the public
OKTE ISOT endpoint:

    GET https://isot.okte.sk/api/v1/ida/results
        ?deliveryDayFrom=YYYY-MM-DD&deliveryDayTo=YYYY-MM-DD
    Header: Accept: application/json

One row per (auction, period); auction in {IDA1, IDA2, IDA3}, period 1..96
(15-minute). Only rows with publicationStatus == "final" are stored.

Field mapping (see app/download_okte_ida.py):
    ida_idx        = int parsed from "IDA{n}"
    time_interval  = "HH:MM-HH:MM" in Europe/Prague local time, derived from
                     `period` the same way OTE's upload_ida_prices.py does
    price_eur_mwh  = price
    volume_mwh     = saleSuccessfulVolume
    export_mwh     = flowSkCz + flowSkHu + flowSkPl
    import_mwh     = flowCzSk + flowHuSk + flowPlSk
    saldo_dm_mwh   = import_mwh - export_mwh (matches OTE's sign convention:
                     a net importing period is a positive saldo)

Audit columns are TIMESTAMPTZ, matching the convention established for all
new OKTE tables since migration 071 (no naive-timestamp columns going forward).
"""

from alembic import op

revision = '074'
down_revision = '073'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE okte_prices_ida (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            ida_idx INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            price_eur_mwh NUMERIC(10, 2),
            volume_mwh NUMERIC(12, 3),
            saldo_dm_mwh NUMERIC(12, 3),
            export_mwh NUMERIC(12, 3),
            import_mwh NUMERIC(12, 3),
            flow_sk_cz NUMERIC(12, 3),
            flow_cz_sk NUMERIC(12, 3),
            flow_sk_hu NUMERIC(12, 3),
            flow_hu_sk NUMERIC(12, 3),
            flow_sk_pl NUMERIC(12, 3),
            flow_pl_sk NUMERIC(12, 3),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            CONSTRAINT okte_prices_ida_trade_date_period_ida_idx_key UNIQUE (trade_date, period, ida_idx)
        );
    """)
    op.execute(
        "CREATE INDEX ix_okte_prices_ida_trade_date ON okte_prices_ida (trade_date);"
    )
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE okte_prices_ida TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE okte_prices_ida_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS okte_prices_ida CASCADE;")
