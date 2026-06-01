"""Add DA period summary and curve depth 60-min tables.

Revision ID: 057
Revises: 056
Create Date: 2026-06-01

Materialized 60-minute aggregations of the day-ahead analytics.
See docs/60min_tables_plan.md §4.1.

Keyed solely on (delivery_date, time_interval). No `period` column —
60-min tables drop the 15-min ordinal in favour of the textual interval.
"""

from alembic import op


revision = '057'
down_revision = '056'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # da_period_summary_60min — clearing summary per hour
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE da_period_summary_60min (
            delivery_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            clearing_price NUMERIC(10, 2),
            clearing_volume NUMERIC(12, 3),
            supply_next_price NUMERIC(10, 2),
            supply_next_volume NUMERIC(12, 3),
            supply_price_gap NUMERIC(10, 2),
            supply_volume_gap NUMERIC(12, 3),
            demand_next_price NUMERIC(10, 2),
            demand_next_volume NUMERIC(12, 3),
            demand_price_gap NUMERIC(10, 2),
            demand_volume_gap NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (delivery_date, time_interval)
        );
    """)
    op.execute("""
        CREATE INDEX ix_da_period_summary_60min_delivery_date
        ON da_period_summary_60min (delivery_date);
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE da_period_summary_60min TO user_finance;")

    # ------------------------------------------------------------------
    # da_curve_depth_60min — supply/demand walls per hour
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE da_curve_depth_60min (
            delivery_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            clearing_price NUMERIC(10, 2) NOT NULL,
            supply_mw_from_clearing NUMERIC(12, 3),
            supply_price_from_clearing NUMERIC(10, 2),
            supply_slope NUMERIC(10, 4),
            supply_matched_mw_from_clearing NUMERIC(12, 3),
            supply_matched_price_from_clearing NUMERIC(10, 2),
            supply_matched_slope NUMERIC(10, 4),
            demand_mw_from_clearing NUMERIC(12, 3),
            demand_price_from_clearing NUMERIC(10, 2),
            demand_slope NUMERIC(10, 4),
            demand_matched_mw_from_clearing NUMERIC(12, 3),
            demand_matched_price_from_clearing NUMERIC(10, 2),
            demand_matched_slope NUMERIC(10, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (delivery_date, time_interval)
        );
    """)
    op.execute("""
        CREATE INDEX ix_da_curve_depth_60min_delivery_date
        ON da_curve_depth_60min (delivery_date);
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE da_curve_depth_60min TO user_finance;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS da_curve_depth_60min CASCADE;")
    op.execute("DROP TABLE IF EXISTS da_period_summary_60min CASCADE;")
