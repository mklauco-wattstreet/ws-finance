"""Add ceps_derived_features_15min table for rolling memory and forecast surprise.

Revision ID: 043
Revises: 042
Create Date: 2026-03-07

Stores rolling imbalance statistics (2h/4h windows) and forecast surprise
features (solar/wind/total generation error) at 15-minute resolution.
Computed from existing 15-min tables, no new API fetches required.
"""

from alembic import op

revision = '043'
down_revision = '042'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE finance.ceps_derived_features_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            -- Rolling memory (from ceps_actual_imbalance_15min)
            imb_roll_2h NUMERIC(12,5),
            imb_roll_4h NUMERIC(12,5),
            imb_integral_4h NUMERIC(15,5),
            -- Forecast surprise (generation_15min vs generation_res_15min / generation_plan_15min)
            solar_error_mw NUMERIC(12,3),
            wind_error_mw NUMERIC(12,3),
            gen_total_error_mw NUMERIC(12,3),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, id),
            CONSTRAINT uq_ceps_derived_features_15min UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    for year in range(2024, 2029):
        op.execute(f"""
            CREATE TABLE finance.ceps_derived_features_15min_{year}
            PARTITION OF finance.ceps_derived_features_15min
            FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01');
        """)

    op.execute("""
        CREATE INDEX ix_ceps_derived_features_15min_trade_date
        ON finance.ceps_derived_features_15min (trade_date);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS finance.ceps_derived_features_15min CASCADE;")
