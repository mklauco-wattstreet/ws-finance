"""Add updated_at column to all CEPS tables.

Revision ID: 050
Revises: 049
Create Date: 2026-04-04

Preserves created_at as the original insert timestamp.
Adds updated_at to track when rows are modified by upserts.
Existing rows get NULL (meaning 'never updated since migration').
"""

from alembic import op

revision = '050'
down_revision = '049'
branch_labels = None
depends_on = None

TABLES = [
    'ceps_actual_imbalance_1min',
    'ceps_actual_imbalance_15min',
    'ceps_actual_re_price_1min',
    'ceps_actual_re_price_15min',
    'ceps_svr_activation_1min',
    'ceps_svr_activation_15min',
    'ceps_export_import_svr_1min',
    'ceps_export_import_svr_15min',
    'ceps_generation_res_1min',
    'ceps_generation_res_15min',
    'ceps_generation_15min',
    'ceps_generation_plan_15min',
    'ceps_estimated_imbalance_price_15min',
    'ceps_1min_features_15min',
    'ceps_derived_features_15min',
]


def upgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")


def downgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS updated_at;")
