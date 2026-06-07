"""Drop entsoe_outages.source_xml (redundant raw-XML bloat).

Revision ID: 065
Revises: 064
Create Date: 2026-06-07

Every field of the outage document is already extracted into typed columns
(entsoe_outages) and the full curve into entsoe_outage_points, so the raw
source_xml blob adds no information and bloats the table. Drop it.

Downgrade recreates the (nullable) column but cannot restore its contents.
"""

from alembic import op


revision = '065'
down_revision = '064'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE entsoe_outages DROP COLUMN IF EXISTS source_xml;")


def downgrade() -> None:
    op.execute("ALTER TABLE entsoe_outages ADD COLUMN source_xml TEXT;")
