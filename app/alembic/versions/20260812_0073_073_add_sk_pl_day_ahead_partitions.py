"""Add SK and PL partitions to the ENTSO-E day-ahead price tables.

Revision ID: 073
Revises: 072
Create Date: 2026-08-12

entsoe_day_ahead_prices (and its 60min mirror) are LIST-partitioned by
country_code and until now only carried AT, DE and HU. SK and PL are being
added to ACTIVE_DAY_AHEAD_AREAS in app/entsoe/constants.py; without a matching
partition every insert for those countries fails with "no partition of relation
... found for row".

BOTH tables need partitions. The 60min aggregator (backfill_entsoe_60min)
writes into entsoe_day_ahead_prices_60min, so partitioning only the 15-min
table would move the failure one step downstream instead of fixing it.

CZ is deliberately absent from these tables - Czech day-ahead prices come
directly from OTE into ote_prices_day_ahead, not via ENTSO-E.
"""

from alembic import op


revision = '073'
down_revision = '072'
branch_labels = None
depends_on = None


PARENTS = ('entsoe_day_ahead_prices', 'entsoe_day_ahead_prices_60min')
COUNTRIES = ('SK', 'PL')


def upgrade() -> None:
    for parent in PARENTS:
        for country in COUNTRIES:
            partition = f"{parent}_{country.lower()}"
            op.execute(f"""
                CREATE TABLE IF NOT EXISTS {partition}
                PARTITION OF {parent}
                FOR VALUES IN ('{country}');
            """)
            op.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {partition} "
                f"TO user_finance;"
            )


def downgrade() -> None:
    for parent in PARENTS:
        for country in COUNTRIES:
            op.execute(f"DROP TABLE IF EXISTS {parent}_{country.lower()};")
