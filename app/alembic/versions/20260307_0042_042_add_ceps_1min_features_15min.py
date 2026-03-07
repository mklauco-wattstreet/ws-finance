"""Add ceps_1min_features_15min table for distributional/volatility features.

Revision ID: 042
Revises: 041
Create Date: 2026-03-07

Stores statistical distribution, threshold counts, and temporal trend features
computed from CEPS 1-minute data (imbalance, RE prices, export/import SVR)
aggregated to 15-minute intervals.
"""

from alembic import op

revision = '042'
down_revision = '041'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE finance.ceps_1min_features_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            minute_count SMALLINT,
            -- aFRR+ price distribution
            afrr_plus_min_eur NUMERIC(15,3),
            afrr_plus_max_eur NUMERIC(15,3),
            afrr_plus_std_eur NUMERIC(15,5),
            afrr_plus_skew NUMERIC(10,5),
            -- aFRR- price distribution
            afrr_minus_min_eur NUMERIC(15,3),
            afrr_minus_max_eur NUMERIC(15,3),
            afrr_minus_std_eur NUMERIC(15,5),
            afrr_minus_skew NUMERIC(10,5),
            -- mFRR+ price distribution
            mfrr_plus_min_eur NUMERIC(15,3),
            mfrr_plus_max_eur NUMERIC(15,3),
            mfrr_plus_std_eur NUMERIC(15,5),
            mfrr_plus_skew NUMERIC(10,5),
            -- mFRR- price distribution
            mfrr_minus_min_eur NUMERIC(15,3),
            mfrr_minus_max_eur NUMERIC(15,3),
            mfrr_minus_std_eur NUMERIC(15,5),
            mfrr_minus_skew NUMERIC(10,5),
            -- Imbalance distribution
            imbalance_range_mw NUMERIC(12,5),
            imbalance_std_mw NUMERIC(12,5),
            imbalance_slope NUMERIC(12,8),
            -- Threshold counts
            minutes_at_floor SMALLINT,
            minutes_near_peak SMALLINT,
            saturation_count SMALLINT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, id),
            CONSTRAINT uq_ceps_1min_features_15min UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)

    for year in range(2024, 2029):
        op.execute(f"""
            CREATE TABLE finance.ceps_1min_features_15min_{year}
            PARTITION OF finance.ceps_1min_features_15min
            FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01');
        """)

    op.execute("""
        CREATE INDEX ix_ceps_1min_features_15min_trade_date
        ON finance.ceps_1min_features_15min (trade_date);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS finance.ceps_1min_features_15min CASCADE;")
