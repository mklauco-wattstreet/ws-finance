"""Add remaining German TSO areas.

Revision ID: 016
Revises: 015
Create Date: 2025-12-22

Germany has 4 TSOs. We previously only had TenneT (area_id=2).
This migration adds the remaining 3:
- 50Hertz (area_id=6)
- Amprion (area_id=7)
- TransnetBW (area_id=8)

This allows fetching generation data for all of Germany.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '016'
down_revision = '015'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add 3 additional German TSO areas
    op.execute("""
        INSERT INTO entsoe_areas (id, code, country_name, country_code, is_active) VALUES
            (6, '10YDE-VE-------2', 'Germany (50Hertz)', 'DE', true),
            (7, '10YDE-RWENET---I', 'Germany (Amprion)', 'DE', true),
            (8, '10YDE-ENBW-----N', 'Germany (TransnetBW)', 'DE', true);
    """)

    # Update sequence
    op.execute("SELECT setval('entsoe_areas_id_seq', 8);")

    # Rename existing DE partition for clarity
    op.execute("""
        ALTER TABLE entsoe_generation_actual_de
        RENAME TO entsoe_generation_actual_de_tennet;
    """)

    # Create partitions for new German TSO areas
    op.execute("""
        CREATE TABLE entsoe_generation_actual_de_50hertz
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (6);
    """)

    op.execute("""
        CREATE TABLE entsoe_generation_actual_de_amprion
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (7);
    """)

    op.execute("""
        CREATE TABLE entsoe_generation_actual_de_transnetbw
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (8);
    """)


def downgrade() -> None:
    # Drop new partitions
    op.execute("DROP TABLE IF EXISTS entsoe_generation_actual_de_50hertz;")
    op.execute("DROP TABLE IF EXISTS entsoe_generation_actual_de_amprion;")
    op.execute("DROP TABLE IF EXISTS entsoe_generation_actual_de_transnetbw;")

    # Rename partition back
    op.execute("""
        ALTER TABLE entsoe_generation_actual_de_tennet
        RENAME TO entsoe_generation_actual_de;
    """)

    # Remove German TSO areas
    op.execute("DELETE FROM entsoe_areas WHERE id IN (6, 7, 8);")
