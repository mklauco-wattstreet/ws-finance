"""Re-partition entsoe_load_60min and entsoe_generation_forecast_60min by country_code.

Revision ID: 062
Revises: 061
Create Date: 2026-06-01

Migration 061 created these two tables as single-series (no country_code).
That was wrong — the 15-min sources (entsoe_load, entsoe_generation_forecast)
both have country_code/area_id and store one row per country per quarter.
The single-series 60-min rows ended up being a mean across all countries
× all quarters — meaningless garbage that didn't reconcile to any country.

This migration drops and recreates both tables LIST-partitioned by
country_code (CZ, DE, AT, PL, SK) — same shape as the other partitioned
60-min ENTSO-E tables (generation_actual, day_ahead_prices, imbalance_prices).

Data is dropped because it was wrong; re-backfill is run after this lands.
"""

from alembic import op


revision = '062'
down_revision = '061'
branch_labels = None
depends_on = None


COUNTRIES = ('CZ', 'DE', 'AT', 'PL', 'SK')


def upgrade() -> None:
    # ------------------------------------------------------------------
    # Drop the broken single-series tables
    # ------------------------------------------------------------------
    op.execute("DROP TABLE IF EXISTS entsoe_load_60min CASCADE;")
    op.execute("DROP TABLE IF EXISTS entsoe_generation_forecast_60min CASCADE;")

    # ------------------------------------------------------------------
    # entsoe_load_60min — partitioned by country_code
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_load_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            actual_load_mw NUMERIC(12, 3),
            forecast_load_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    for cc in COUNTRIES:
        op.execute(f"""
            CREATE TABLE entsoe_load_60min_{cc.lower()}
            PARTITION OF entsoe_load_60min
            FOR VALUES IN ('{cc}');
        """)
    op.execute("CREATE INDEX ix_entsoe_load_60min_trade_date ON entsoe_load_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_load_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_load_60min_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_generation_forecast_60min — partitioned by country_code
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_generation_forecast_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            forecast_solar_mw NUMERIC(12, 3),
            forecast_wind_mw NUMERIC(12, 3),
            forecast_wind_offshore_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    for cc in COUNTRIES:
        op.execute(f"""
            CREATE TABLE entsoe_generation_forecast_60min_{cc.lower()}
            PARTITION OF entsoe_generation_forecast_60min
            FOR VALUES IN ('{cc}');
        """)
    op.execute("CREATE INDEX ix_entsoe_generation_forecast_60min_trade_date ON entsoe_generation_forecast_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_generation_forecast_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_generation_forecast_60min_id_seq TO user_finance;")


def downgrade() -> None:
    # Drop the partitioned tables and recreate as single-series (the broken 061 shape).
    op.execute("DROP TABLE IF EXISTS entsoe_generation_forecast_60min CASCADE;")
    op.execute("DROP TABLE IF EXISTS entsoe_load_60min CASCADE;")

    op.execute("""
        CREATE TABLE entsoe_load_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            actual_load_mw NUMERIC(12, 3),
            forecast_load_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, time_interval)
        );
    """)
    op.execute("CREATE INDEX ix_entsoe_load_60min_trade_date ON entsoe_load_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_load_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_load_60min_id_seq TO user_finance;")

    op.execute("""
        CREATE TABLE entsoe_generation_forecast_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            forecast_solar_mw NUMERIC(12, 3),
            forecast_wind_mw NUMERIC(12, 3),
            forecast_wind_offshore_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, time_interval)
        );
    """)
    op.execute("CREATE INDEX ix_entsoe_generation_forecast_60min_trade_date ON entsoe_generation_forecast_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_generation_forecast_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_generation_forecast_60min_id_seq TO user_finance;")
