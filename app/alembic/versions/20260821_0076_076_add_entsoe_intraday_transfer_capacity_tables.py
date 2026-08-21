"""Add ENTSO-E Intraday Offered Transfer Capacity [12.1.A/B] tables for CZ.

Revision ID: 076
Revises: 075
Create Date: 2026-08-21

Source: ENTSO-E Transparency Platform, documentType=A31 (Implicit Allocations
- Offered Capacity), contract_MarketAgreement.Type=A07 (Intraday). Four
products per border direction: idct (auction.Type=A08, continuous, rolling
revisions during the delivery day) and ida1/ida2/ida3 (auction.Type=A01 +
classificationSequence_AttributeInstanceComponent.Position=1|2|3, ID auction
results, no revisions). Eight border directions per product: CZ<->DE-LU,
CZ<->AT, CZ<->PL, CZ<->SK (in_Domain is the receiving/importing zone).
Capacity uses the DE-LU BIDDING ZONE (10Y1001A1001A82H), not the TenneT
control area used by the A11 cross-border flow runner. Native resolution
PT15M, curveType A03 (sparse points, forward-filled by the parser).

Two LIST-partitioned-by-country_code tables (CZ partition only, mirroring the
outages/procured-capacity convention for future multi-country expansion):
  - entsoe_intraday_transfer_capacity        (source-of-truth, 15-min)
  - entsoe_intraday_transfer_capacity_60min  (hourly aggregate)

(trade_date, time_interval) is the authoritative key; `period` is a
convenience column only (92/100 periods on DST switch days).
"""

from alembic import op


revision = '076'
down_revision = '075'
branch_labels = None
depends_on = None


_CAPACITY_COLUMNS = """
            cap_import_de_mw NUMERIC(12, 3),
            cap_export_de_mw NUMERIC(12, 3),
            cap_import_at_mw NUMERIC(12, 3),
            cap_export_at_mw NUMERIC(12, 3),
            cap_import_pl_mw NUMERIC(12, 3),
            cap_export_pl_mw NUMERIC(12, 3),
            cap_import_sk_mw NUMERIC(12, 3),
            cap_export_sk_mw NUMERIC(12, 3),
            cap_import_total_mw NUMERIC(12, 3),
            cap_export_total_mw NUMERIC(12, 3)
"""


def upgrade() -> None:
    # ------------------------------------------------------------------
    # entsoe_intraday_transfer_capacity — 15-min source of truth
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_intraday_transfer_capacity (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMP NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(2) NOT NULL,
            product VARCHAR(4) NOT NULL,
            {_CAPACITY_COLUMNS.strip().rstrip(',')},
            published_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code, product)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("""
        CREATE TABLE entsoe_intraday_transfer_capacity_cz
        PARTITION OF entsoe_intraday_transfer_capacity
        FOR VALUES IN ('CZ');
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_intraday_transfer_capacity_country_trade_date "
        "ON entsoe_intraday_transfer_capacity (country_code, trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_intraday_transfer_capacity "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE entsoe_intraday_transfer_capacity_id_seq "
        "TO user_finance;"
    )

    # ------------------------------------------------------------------
    # entsoe_intraday_transfer_capacity_60min — hourly aggregate
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_intraday_transfer_capacity_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMP NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(2) NOT NULL,
            product VARCHAR(4) NOT NULL,
            {_CAPACITY_COLUMNS.strip().rstrip(',')},
            published_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code, product)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("""
        CREATE TABLE entsoe_intraday_transfer_capacity_60min_cz
        PARTITION OF entsoe_intraday_transfer_capacity_60min
        FOR VALUES IN ('CZ');
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_intraday_transfer_capacity_60min_country_trade_date "
        "ON entsoe_intraday_transfer_capacity_60min (country_code, trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_intraday_transfer_capacity_60min "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE entsoe_intraday_transfer_capacity_60min_id_seq "
        "TO user_finance;"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS entsoe_intraday_transfer_capacity_60min CASCADE;")
    op.execute("DROP TABLE IF EXISTS entsoe_intraday_transfer_capacity CASCADE;")
