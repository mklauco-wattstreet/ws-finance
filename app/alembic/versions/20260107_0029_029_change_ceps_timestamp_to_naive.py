"""029_change_ceps_timestamp_to_naive

Revision ID: 029
Revises: 028
Create Date: 2026-01-07

Changes delivery_timestamp from TIMESTAMPTZ to TIMESTAMP (no timezone)
to prevent timezone conversion issues.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '029'
down_revision = '028'
branch_labels = None
depends_on = None


def upgrade():
    """Convert delivery_timestamp from TIMESTAMPTZ to TIMESTAMP."""

    # print("Starting migration: Converting delivery_timestamp from TIMESTAMPTZ to TIMESTAMP")
    # print("Note: This requires recreating the table due to partition key constraints")

    # Step 1: Create new table with TIMESTAMP (no timezone)
    # Partition by delivery_timestamp directly (not by expression) to allow UNIQUE constraint
    op.execute("""
        CREATE TABLE finance.ceps_actual_imbalance_1min_new (
            id BIGSERIAL,
            delivery_timestamp TIMESTAMP NOT NULL,
            load_mw NUMERIC(12,5) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_ceps_1min_delivery_timestamp_new UNIQUE (delivery_timestamp)
        ) PARTITION BY RANGE (delivery_timestamp);
    """)
    # print("✓ Created new table with TIMESTAMP column")

    # Step 2: Create partitions for new table (2024-2028)
    # Use timestamp boundaries for each year
    for year in range(2024, 2029):
        op.execute(f"""
            CREATE TABLE finance.ceps_actual_imbalance_1min_new_{year}
            PARTITION OF finance.ceps_actual_imbalance_1min_new
            FOR VALUES FROM ('{year}-01-01 00:00:00') TO ('{year + 1}-01-01 00:00:00');
        """)
    # print("✓ Created partitions for years 2024-2028")

    # Step 3: Copy data with timezone conversion
    op.execute("""
        INSERT INTO finance.ceps_actual_imbalance_1min_new
            (delivery_timestamp, load_mw, created_at)
        SELECT
            delivery_timestamp AT TIME ZONE 'Europe/Prague' AS delivery_timestamp,
            load_mw,
            created_at
        FROM finance.ceps_actual_imbalance_1min
        ORDER BY delivery_timestamp;
    """)
    # print("✓ Copied data with timezone conversion (UTC -> Europe/Prague local time)")

    # Step 4: Create indexes on new table
    op.execute("""
        CREATE INDEX idx_ceps_1min_delivery_timestamp_new
        ON finance.ceps_actual_imbalance_1min_new (delivery_timestamp);
    """)
    op.execute("""
        CREATE INDEX idx_ceps_1min_created_at_new
        ON finance.ceps_actual_imbalance_1min_new (created_at);
    """)
    # print("✓ Created indexes")

    # Step 5: Drop old table and its partitions
    op.execute("DROP TABLE finance.ceps_actual_imbalance_1min CASCADE;")
    # print("✓ Dropped old table")

    # Step 6: Rename new table to original name
    op.execute("""
        ALTER TABLE finance.ceps_actual_imbalance_1min_new
        RENAME TO ceps_actual_imbalance_1min;
    """)
    op.execute("""
        ALTER TABLE finance.ceps_actual_imbalance_1min
        RENAME CONSTRAINT uq_ceps_1min_delivery_timestamp_new
        TO uq_ceps_1min_delivery_timestamp;
    """)
    op.execute("""
        ALTER INDEX finance.idx_ceps_1min_delivery_timestamp_new
        RENAME TO idx_ceps_1min_delivery_timestamp;
    """)
    op.execute("""
        ALTER INDEX finance.idx_ceps_1min_created_at_new
        RENAME TO idx_ceps_1min_created_at;
    """)

    # Rename partitions
    for year in range(2024, 2029):
        op.execute(f"""
            ALTER TABLE finance.ceps_actual_imbalance_1min_new_{year}
            RENAME TO ceps_actual_imbalance_1min_{year};
        """)

    # print("✓ Renamed table and indexes to original names")
    # print("")
    # print("=" * 80)
    # print("MIGRATION COMPLETED SUCCESSFULLY")
    # print("=" * 80)
    # print("delivery_timestamp is now TIMESTAMP (no timezone)")
    # print("All data has been preserved and converted to Europe/Prague local time")
    # print("=" * 80)


def downgrade():
    """Convert delivery_timestamp back to TIMESTAMPTZ."""

    # print("Downgrade not supported for this migration")
    # print("Reason: Requires recreating partitioned table")
    # print("To rollback: restore from backup or recreate table manually")
    raise NotImplementedError("Downgrade not supported - table recreation required")
