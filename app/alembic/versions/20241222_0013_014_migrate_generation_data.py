"""Migrate generation data to partitioned table.

Revision ID: 014
Revises: 013
Create Date: 2024-12-22

Migrates existing data from entsoe_generation_actual_old (CZ)
and entsoe_germany_wind (DE) to the new partitioned table.

NOTE: This migration performs data copy which may take >30 seconds
for large datasets. Run manually if needed:

    docker compose exec entsoe-ote-data-uploader python3 -c "
    from alembic import command
    from alembic.config import Config
    cfg = Config('/app/alembic.ini')
    command.upgrade(cfg, '014')
    "
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '014'
down_revision = '013'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Migrate CZ data (area_id = 1)
    op.execute("""
        INSERT INTO entsoe_generation_actual
            (trade_date, period, area_id, time_interval,
             gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
             gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw,
             created_at)
        SELECT
            trade_date, period, 1 AS area_id, time_interval,
            gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
            gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw,
            created_at
        FROM entsoe_generation_actual_old;
    """)

    # Migrate DE wind data (area_id = 2)
    # Maps wind_onshore_mw -> gen_wind_mw, wind_offshore_mw -> gen_wind_offshore_mw
    op.execute("""
        INSERT INTO entsoe_generation_actual
            (trade_date, period, area_id, time_interval,
             gen_wind_mw, gen_wind_offshore_mw, created_at)
        SELECT
            trade_date, period, 2 AS area_id, time_interval,
            wind_onshore_mw, wind_offshore_mw, created_at
        FROM entsoe_germany_wind
        ON CONFLICT (trade_date, period, area_id) DO UPDATE SET
            gen_wind_mw = EXCLUDED.gen_wind_mw,
            gen_wind_offshore_mw = EXCLUDED.gen_wind_offshore_mw;
    """)


def downgrade() -> None:
    # Delete migrated data from partitioned table
    op.execute("DELETE FROM entsoe_generation_actual WHERE area_id IN (1, 2);")
