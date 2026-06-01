"""Add ENTSO-E 60-min tables (7 tables, including imbalance prices).

Revision ID: 061
Revises: 060
Create Date: 2026-06-01

Materialized 60-minute aggregations for ENTSO-E sources. Seven tables:
  - entsoe_load_60min                            (non-partitioned)
  - entsoe_generation_forecast_60min             (non-partitioned)
  - entsoe_cross_border_flows_60min              (non-partitioned)
  - entsoe_scheduled_cross_border_flows_60min    (non-partitioned)
  - entsoe_generation_actual_60min               (LIST partition by country_code: CZ, DE, AT, PL, SK)
  - entsoe_day_ahead_prices_60min                (LIST partition by country_code: DE, AT, HU)
  - entsoe_imbalance_prices_60min                (LIST partition by country_code: CZ, DE, AT, PL, SK, HU)

See docs/60min_tables_plan.md §4.5.

`entsoe_imbalance_prices_60min` is included beyond the upstream
ta-feature-api spec — see plan §5 default #5.
"""

from alembic import op


revision = '061'
down_revision = '060'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # entsoe_load_60min (single CZ table, non-partitioned)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # entsoe_generation_forecast_60min (single CZ table)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # entsoe_cross_border_flows_60min (single CZ-centric table)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_cross_border_flows_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            delivery_datetime TIMESTAMP NOT NULL,
            area_id VARCHAR(20) NOT NULL,
            flow_de_mw NUMERIC(12, 3),
            flow_at_mw NUMERIC(12, 3),
            flow_pl_mw NUMERIC(12, 3),
            flow_sk_mw NUMERIC(12, 3),
            flow_total_net_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (delivery_datetime, area_id),
            UNIQUE (trade_date, time_interval, area_id)
        );
    """)
    op.execute("CREATE INDEX ix_entsoe_cross_border_flows_60min_trade_date ON entsoe_cross_border_flows_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_cross_border_flows_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_cross_border_flows_60min_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_scheduled_cross_border_flows_60min (single CZ-centric)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_scheduled_cross_border_flows_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            scheduled_de_mw NUMERIC(12, 3),
            scheduled_at_mw NUMERIC(12, 3),
            scheduled_pl_mw NUMERIC(12, 3),
            scheduled_sk_mw NUMERIC(12, 3),
            scheduled_total_net_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE (trade_date, time_interval)
        );
    """)
    op.execute("CREATE INDEX ix_entsoe_scheduled_cross_border_flows_60min_trade_date ON entsoe_scheduled_cross_border_flows_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_scheduled_cross_border_flows_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_scheduled_cross_border_flows_60min_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_generation_actual_60min — partitioned by country_code
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_generation_actual_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            gen_nuclear_mw NUMERIC(12, 3),
            gen_coal_mw NUMERIC(12, 3),
            gen_gas_mw NUMERIC(12, 3),
            gen_solar_mw NUMERIC(12, 3),
            gen_wind_mw NUMERIC(12, 3),
            gen_wind_offshore_mw NUMERIC(12, 3),
            gen_hydro_pumped_mw NUMERIC(12, 3),
            gen_biomass_mw NUMERIC(12, 3),
            gen_hydro_other_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    for cc in ("CZ", "DE", "AT", "PL", "SK"):
        op.execute(f"""
            CREATE TABLE entsoe_generation_actual_60min_{cc.lower()}
            PARTITION OF entsoe_generation_actual_60min
            FOR VALUES IN ('{cc}');
        """)
    op.execute("CREATE INDEX ix_entsoe_generation_actual_60min_trade_date ON entsoe_generation_actual_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_generation_actual_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_generation_actual_60min_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_day_ahead_prices_60min — partitioned by country_code
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_day_ahead_prices_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            price_eur_mwh NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    for cc in ("DE", "AT", "HU"):
        op.execute(f"""
            CREATE TABLE entsoe_day_ahead_prices_60min_{cc.lower()}
            PARTITION OF entsoe_day_ahead_prices_60min
            FOR VALUES IN ('{cc}');
        """)
    op.execute("CREATE INDEX ix_entsoe_day_ahead_prices_60min_trade_date ON entsoe_day_ahead_prices_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_day_ahead_prices_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_day_ahead_prices_60min_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_imbalance_prices_60min — partitioned by country_code
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_imbalance_prices_60min (
            id SERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            pos_imb_price_mwh NUMERIC(15, 3),
            pos_imb_scarcity_mwh NUMERIC(15, 3),
            pos_imb_incentive_mwh NUMERIC(15, 3),
            pos_imb_financial_neutrality_mwh NUMERIC(15, 3),
            neg_imb_price_mwh NUMERIC(15, 3),
            neg_imb_scarcity_mwh NUMERIC(15, 3),
            neg_imb_incentive_mwh NUMERIC(15, 3),
            neg_imb_financial_neutrality_mwh NUMERIC(15, 3),
            imbalance_mwh NUMERIC(12, 5),
            difference_mwh NUMERIC(12, 5),
            situation VARCHAR,
            status VARCHAR,
            currency VARCHAR(3) NOT NULL,
            delivery_datetime TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    for cc in ("CZ", "DE", "AT", "PL", "SK", "HU"):
        op.execute(f"""
            CREATE TABLE entsoe_imbalance_prices_60min_{cc.lower()}
            PARTITION OF entsoe_imbalance_prices_60min
            FOR VALUES IN ('{cc}');
        """)
    op.execute("CREATE INDEX ix_entsoe_imbalance_prices_60min_trade_date ON entsoe_imbalance_prices_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_imbalance_prices_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_imbalance_prices_60min_id_seq TO user_finance;")


def downgrade() -> None:
    for table in [
        "entsoe_imbalance_prices_60min",
        "entsoe_day_ahead_prices_60min",
        "entsoe_generation_actual_60min",
        "entsoe_scheduled_cross_border_flows_60min",
        "entsoe_cross_border_flows_60min",
        "entsoe_generation_forecast_60min",
        "entsoe_load_60min",
    ]:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
