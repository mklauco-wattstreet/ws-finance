"""Add intraday and current generation forecast tables.

Revision ID: 045
Revises: 044
Create Date: 2026-03-12

Creates two new partitioned tables for A69 generation forecast:
- entsoe_generation_forecast_intraday (A40 process type)
- entsoe_generation_forecast_current (A18 process type)

Same schema as entsoe_generation_forecast, partitioned by country_code.
"""

from alembic import op

revision = '045'
down_revision = '044'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # === Intraday forecast table (A40) ===
    op.execute("""
        CREATE TABLE entsoe_generation_forecast_intraday (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            forecast_solar_mw NUMERIC(12,3),
            forecast_wind_mw NUMERIC(12,3),
            forecast_wind_offshore_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    for cc in ('cz', 'de', 'at', 'pl', 'sk'):
        op.execute(f"""
            CREATE TABLE entsoe_generation_forecast_intraday_{cc}
            PARTITION OF entsoe_generation_forecast_intraday
            FOR VALUES IN ('{cc.upper()}');
        """)

    op.execute("""
        CREATE INDEX ix_entsoe_generation_forecast_intraday_trade_date
        ON entsoe_generation_forecast_intraday (trade_date);
    """)

    # === Current forecast table (A18) ===
    op.execute("""
        CREATE TABLE entsoe_generation_forecast_current (
            id SERIAL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            forecast_solar_mw NUMERIC(12,3),
            forecast_wind_mw NUMERIC(12,3),
            forecast_wind_offshore_mw NUMERIC(12,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)

    for cc in ('cz', 'de', 'at', 'pl', 'sk'):
        op.execute(f"""
            CREATE TABLE entsoe_generation_forecast_current_{cc}
            PARTITION OF entsoe_generation_forecast_current
            FOR VALUES IN ('{cc.upper()}');
        """)

    op.execute("""
        CREATE INDEX ix_entsoe_generation_forecast_current_trade_date
        ON entsoe_generation_forecast_current (trade_date);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS entsoe_generation_forecast_intraday CASCADE;")
    op.execute("DROP TABLE IF EXISTS entsoe_generation_forecast_current CASCADE;")
