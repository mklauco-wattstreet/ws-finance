"""Add entsoe_areas lookup table.

Revision ID: 012
Revises: 011
Create Date: 2024-12-22

Creates a central lookup table for delivery area metadata (EIC codes).
Pre-populates with CZ and neighbor areas (DE, AT, PL, SK).
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '012'
down_revision = '011'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create entsoe_areas lookup table
    op.create_table(
        'entsoe_areas',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('code', sa.String(20), nullable=False),
        sa.Column('country_name', sa.String(100), nullable=False),
        sa.Column('country_code', sa.String(5), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.PrimaryKeyConstraint('id', name='entsoe_areas_pkey'),
        sa.UniqueConstraint('code', name='entsoe_areas_code_key'),
    )

    # Pre-populate with CZ and neighbor areas
    # IMPORTANT: IDs must remain stable for partitioning
    op.execute("""
        INSERT INTO entsoe_areas (id, code, country_name, country_code, is_active) VALUES
            (1, '10YCZ-CEPS-----N', 'Czech Republic', 'CZ', true),
            (2, '10YDE-EON------1', 'Germany (TenneT)', 'DE', true),
            (3, '10YAT-APG------L', 'Austria', 'AT', true),
            (4, '10YPL-AREA-----S', 'Poland', 'PL', true),
            (5, '10YSK-SEPS-----K', 'Slovakia', 'SK', true);
    """)

    # Update the sequence to start after our manually inserted IDs
    op.execute("SELECT setval('entsoe_areas_id_seq', 5);")

    # Create index for active areas lookup
    op.create_index(
        'ix_entsoe_areas_is_active',
        'entsoe_areas',
        ['is_active'],
        unique=False
    )


def downgrade() -> None:
    op.drop_index('ix_entsoe_areas_is_active', table_name='entsoe_areas')
    op.drop_table('entsoe_areas')
