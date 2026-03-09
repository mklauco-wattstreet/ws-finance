"""Add Golden Trio derived features to ceps_1min_features_15min.

Revision ID: 044
Revises: 043
Create Date: 2026-03-08

New columns capture the three most predictive market-tightness signals:
- total_active_mean/std_mw  : mean/std of (aFRR+ + mFRR+) activation per 15-min
- platform_active_count     : minutes with non-zero PICASSO/MARI import flow
- afrr_mfrr_{plus,minus}_spread_{mean,std}_eur : price spread between aFRR and mFRR
  (wide spread = thin market = large price spikes on small imbalance errors)
"""

from alembic import op

revision = '044'
down_revision = '043'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE finance.ceps_1min_features_15min
            ADD COLUMN total_active_mean_mw NUMERIC(12,3),
            ADD COLUMN total_active_std_mw NUMERIC(12,5),
            ADD COLUMN platform_active_count SMALLINT,
            ADD COLUMN afrr_mfrr_plus_spread_mean_eur NUMERIC(15,3),
            ADD COLUMN afrr_mfrr_plus_spread_std_eur NUMERIC(15,5),
            ADD COLUMN afrr_mfrr_minus_spread_mean_eur NUMERIC(15,3),
            ADD COLUMN afrr_mfrr_minus_spread_std_eur NUMERIC(15,5);
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE finance.ceps_1min_features_15min
            DROP COLUMN IF EXISTS total_active_mean_mw,
            DROP COLUMN IF EXISTS total_active_std_mw,
            DROP COLUMN IF EXISTS platform_active_count,
            DROP COLUMN IF EXISTS afrr_mfrr_plus_spread_mean_eur,
            DROP COLUMN IF EXISTS afrr_mfrr_plus_spread_std_eur,
            DROP COLUMN IF EXISTS afrr_mfrr_minus_spread_mean_eur,
            DROP COLUMN IF EXISTS afrr_mfrr_minus_spread_std_eur;
    """)
