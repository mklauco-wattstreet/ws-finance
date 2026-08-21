"""Add ENTSO-E Congestion Income [12.1.E] tables for CZ.

Revision ID: 077
Revises: 076
Create Date: 2026-08-21

Source: ENTSO-E Transparency Platform, documentType=A25, businessType=B10,
contract_MarketAgreement.Type=A01. Queried per bidding zone with
in_Domain = out_Domain = 10YCZ-CEPS-----N (CZ is in the Core flow-based
region, so per-border queries return "no matching data"; auction.Type must
NOT be sent or the API demands per-border domains and errors). price.amount
is EUR per MTU. curveType A03, resolution PT15M from 2025-10-01 onwards,
PT60M before that (source_resolution records which applies per row).

Two LIST-partitioned-by-country_code tables (CZ partition only, mirroring the
outages/procured-capacity convention for future multi-country expansion):
  - entsoe_congestion_income        (source-of-truth, 15-min)
  - entsoe_congestion_income_60min  (hourly aggregate, SUM not AVG — this is
    a EUR amount per MTU, not a rate)

(trade_date, time_interval) is the authoritative key; `period` is a
convenience column only (92/100 periods on DST switch days).
"""

from alembic import op


revision = '077'
down_revision = '076'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # entsoe_congestion_income — 15-min (or 60-min pre-2025-10-01) source
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_congestion_income (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMP NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(2) NOT NULL,
            congestion_income_eur NUMERIC(14, 3),
            source_resolution VARCHAR(5),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("""
        CREATE TABLE entsoe_congestion_income_cz
        PARTITION OF entsoe_congestion_income
        FOR VALUES IN ('CZ');
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_congestion_income_country_trade_date "
        "ON entsoe_congestion_income (country_code, trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_congestion_income "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE entsoe_congestion_income_id_seq "
        "TO user_finance;"
    )

    # ------------------------------------------------------------------
    # entsoe_congestion_income_60min — hourly aggregate (SUM)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_congestion_income_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMP NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(2) NOT NULL,
            congestion_income_eur NUMERIC(14, 3),
            source_resolution VARCHAR(5),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("""
        CREATE TABLE entsoe_congestion_income_60min_cz
        PARTITION OF entsoe_congestion_income_60min
        FOR VALUES IN ('CZ');
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_congestion_income_60min_country_trade_date "
        "ON entsoe_congestion_income_60min (country_code, trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_congestion_income_60min "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE entsoe_congestion_income_60min_id_seq "
        "TO user_finance;"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS entsoe_congestion_income_60min CASCADE;")
    op.execute("DROP TABLE IF EXISTS entsoe_congestion_income CASCADE;")
