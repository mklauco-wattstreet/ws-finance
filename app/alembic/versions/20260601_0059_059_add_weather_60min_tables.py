"""Add weather_current_60min and weather_forecast_60min tables.

Revision ID: 059
Revises: 058
Create Date: 2026-06-01

60-minute weather aggregations for central Czechia (lat=49.80, lon=15.47).
See docs/60min_tables_plan.md §4.3.

All variables aggregated by arithmetic mean across the four 15-min
quarter-hour rows of each hour.
"""

from alembic import op


revision = '059'
down_revision = '058'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # weather_current_60min
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE weather_current_60min (
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            temperature_2m_degc NUMERIC(6, 2),
            shortwave_radiation_wm2 NUMERIC(8, 2),
            direct_radiation_wm2 NUMERIC(8, 2),
            cloud_cover_pct NUMERIC(5, 2),
            wind_speed_10m_kmh NUMERIC(6, 2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval)
        );
    """)
    op.execute("""
        CREATE INDEX ix_weather_current_60min_trade_date
        ON weather_current_60min (trade_date);
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE weather_current_60min TO user_finance;")

    # ------------------------------------------------------------------
    # weather_forecast_60min
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE weather_forecast_60min (
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            forecast_made_at TIMESTAMP WITH TIME ZONE NOT NULL,
            temperature_2m_degc NUMERIC(6, 2),
            shortwave_radiation_wm2 NUMERIC(8, 2),
            direct_radiation_wm2 NUMERIC(8, 2),
            cloud_cover_pct NUMERIC(5, 2),
            wind_speed_10m_kmh NUMERIC(6, 2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, forecast_made_at)
        );
    """)
    op.execute("""
        CREATE INDEX ix_weather_forecast_60min_trade_date
        ON weather_forecast_60min (trade_date);
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE weather_forecast_60min TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS weather_forecast_60min CASCADE;")
    op.execute("DROP TABLE IF EXISTS weather_current_60min CASCADE;")
