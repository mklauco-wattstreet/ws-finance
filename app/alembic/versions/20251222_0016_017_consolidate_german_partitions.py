"""Consolidate German TSO partitions into single country partition.

Revision ID: 017
Revises: 016
Create Date: 2025-12-23

Merges the 4 separate German TSO partitions into a single country partition:
- entsoe_generation_actual_de_tennet (area_id=2)
- entsoe_generation_actual_de_50hertz (area_id=6)
- entsoe_generation_actual_de_amprion (area_id=7)
- entsoe_generation_actual_de_transnetbw (area_id=8)

Into:
- entsoe_generation_actual_de FOR VALUES IN (2, 6, 7, 8)

This provides cleaner partitioning by country rather than by TSO.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '017'
down_revision = '016'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Detach all 4 German partitions (they become regular tables)
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_de_tennet;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_de_50hertz;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_de_amprion;")
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_de_transnetbw;")

    # Step 2: Create single German partition for all 4 TSO area_ids
    op.execute("""
        CREATE TABLE entsoe_generation_actual_de
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (2, 6, 7, 8);
    """)

    # Step 3: Migrate data from detached tables to new partition
    op.execute("INSERT INTO entsoe_generation_actual_de SELECT * FROM entsoe_generation_actual_de_tennet;")
    op.execute("INSERT INTO entsoe_generation_actual_de SELECT * FROM entsoe_generation_actual_de_50hertz;")
    op.execute("INSERT INTO entsoe_generation_actual_de SELECT * FROM entsoe_generation_actual_de_amprion;")
    op.execute("INSERT INTO entsoe_generation_actual_de SELECT * FROM entsoe_generation_actual_de_transnetbw;")

    # Step 4: Drop old detached tables
    op.execute("DROP TABLE entsoe_generation_actual_de_tennet;")
    op.execute("DROP TABLE entsoe_generation_actual_de_50hertz;")
    op.execute("DROP TABLE entsoe_generation_actual_de_amprion;")
    op.execute("DROP TABLE entsoe_generation_actual_de_transnetbw;")


def downgrade() -> None:
    # Detach consolidated partition
    op.execute("ALTER TABLE entsoe_generation_actual DETACH PARTITION entsoe_generation_actual_de;")

    # Recreate individual TSO partitions
    op.execute("""
        CREATE TABLE entsoe_generation_actual_de_tennet
        PARTITION OF entsoe_generation_actual
        FOR VALUES IN (2);
    """)
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

    # Migrate data back
    op.execute("INSERT INTO entsoe_generation_actual_de_tennet SELECT * FROM entsoe_generation_actual_de WHERE area_id = 2;")
    op.execute("INSERT INTO entsoe_generation_actual_de_50hertz SELECT * FROM entsoe_generation_actual_de WHERE area_id = 6;")
    op.execute("INSERT INTO entsoe_generation_actual_de_amprion SELECT * FROM entsoe_generation_actual_de WHERE area_id = 7;")
    op.execute("INSERT INTO entsoe_generation_actual_de_transnetbw SELECT * FROM entsoe_generation_actual_de WHERE area_id = 8;")

    # Drop consolidated table
    op.execute("DROP TABLE entsoe_generation_actual_de;")
