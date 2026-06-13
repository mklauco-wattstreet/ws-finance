"""Add ENTSO-E Procured Balancing Capacity [GL EB 12.3.F] tables for CZ.

Revision ID: 066
Revises: 065
Create Date: 2026-06-12

Source: ENTSO-E Transparency Platform, documentType=A15 (Procured Balancing
Capacity [12.3.F]), Area_Domain=10YCZ-CEPS-----N, type_MarketAgreement.Type=A01
(daily). CZ publishes aFRR (processType A51) and mFRR (processType A47); FCR and
RR are out of scope. Native resolution is hourly (PT60M), priced in 4-hour
blocks (curveType A03). Each TimeSeries is one awarded offer; the procured
volume per hour is the sum across offers and the marginal price is the highest
accepted price.

Three single (unpartitioned) CZ-only tables — CZ is the only domain ingested,
mirroring the entsoe_load_60min / entsoe_generation_forecast_60min convention:

- entsoe_procured_capacity_raw    : one row per awarded offer per hour (bid stack)
- entsoe_procured_capacity_60min  : aggregated wide summary (source-of-truth)
- entsoe_procured_capacity_15min  : 60min values forward-filled into 4 quarters

Prices are EUR/MW (capacity prices). All quantities are MW.
"""

from alembic import op


revision = '066'
down_revision = '065'
branch_labels = None
depends_on = None


# Wide summary columns shared by the 60min and 15min tables.
_SUMMARY_COLUMNS = """
            afrr_up_mw NUMERIC(12, 3),
            afrr_up_price_marginal_eur NUMERIC(12, 3),
            afrr_up_price_avg_eur NUMERIC(12, 3),
            afrr_down_mw NUMERIC(12, 3),
            afrr_down_price_marginal_eur NUMERIC(12, 3),
            afrr_down_price_avg_eur NUMERIC(12, 3),
            mfrr_up_mw NUMERIC(12, 3),
            mfrr_up_price_marginal_eur NUMERIC(12, 3),
            mfrr_up_price_avg_eur NUMERIC(12, 3),
            mfrr_down_mw NUMERIC(12, 3),
            mfrr_down_price_marginal_eur NUMERIC(12, 3),
            mfrr_down_price_avg_eur NUMERIC(12, 3)
"""


def _create_summary_table(name: str) -> None:
    op.execute(f"""
        CREATE TABLE {name} (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            {_SUMMARY_COLUMNS.strip().rstrip(',')},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, time_interval)
        );
    """)
    op.execute(f"CREATE INDEX ix_{name}_trade_date ON {name} (trade_date);")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {name} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {name}_id_seq TO user_finance;")


def upgrade() -> None:
    # ------------------------------------------------------------------
    # entsoe_procured_capacity_raw — per-offer bid stack (hourly expanded)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_procured_capacity_raw (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            reserve_type VARCHAR(4) NOT NULL,        -- 'afrr' | 'mfrr'
            direction VARCHAR(4) NOT NULL,           -- 'up' | 'down'
            offer_seq INTEGER NOT NULL,              -- TimeSeries mRID within the day's document
            quantity_mw NUMERIC(12, 3),
            price_eur NUMERIC(12, 3),                -- EUR/MW capacity price
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, time_interval, reserve_type, direction, offer_seq)
        );
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_procured_capacity_raw_trade_date "
        "ON entsoe_procured_capacity_raw (trade_date);"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_procured_capacity_raw "
        "TO user_finance;"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE entsoe_procured_capacity_raw_id_seq "
        "TO user_finance;"
    )

    # ------------------------------------------------------------------
    # Aggregated wide summaries (source-of-truth + forward-filled mirror)
    # ------------------------------------------------------------------
    _create_summary_table("entsoe_procured_capacity_60min")
    _create_summary_table("entsoe_procured_capacity_15min")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS entsoe_procured_capacity_15min;")
    op.execute("DROP TABLE IF EXISTS entsoe_procured_capacity_60min;")
    op.execute("DROP TABLE IF EXISTS entsoe_procured_capacity_raw;")
