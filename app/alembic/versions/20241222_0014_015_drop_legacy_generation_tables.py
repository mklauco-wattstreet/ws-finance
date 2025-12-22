"""Drop legacy generation tables after migration verification.

Revision ID: 015
Revises: 014
Create Date: 2024-12-22

Drops the legacy tables after data migration is verified:
- entsoe_generation_actual_old (CZ data, now in partitioned table)
- entsoe_germany_wind (DE wind data, now in partitioned table)

IMPORTANT: Only run this after verifying data integrity:

    -- Verify CZ data count matches
    SELECT COUNT(*) FROM entsoe_generation_actual WHERE area_id = 1;
    SELECT COUNT(*) FROM entsoe_generation_actual_old;

    -- Verify DE data count matches
    SELECT COUNT(*) FROM entsoe_generation_actual WHERE area_id = 2;
    SELECT COUNT(*) FROM entsoe_germany_wind;
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '015'
down_revision = '014'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop legacy CZ generation table
    op.drop_table('entsoe_generation_actual_old')

    # Drop legacy DE wind table
    op.drop_table('entsoe_germany_wind')


def downgrade() -> None:
    # Recreate entsoe_germany_wind table
    op.create_table(
        'entsoe_germany_wind',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date(), nullable=False),
        sa.Column('period', sa.Integer(), nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        sa.Column('wind_onshore_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('wind_offshore_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('wind_total_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_germany_wind_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_germany_wind_trade_date_period_key'),
    )

    # Recreate entsoe_generation_actual_old table (original CZ table)
    op.create_table(
        'entsoe_generation_actual_old',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('trade_date', sa.Date(), nullable=False),
        sa.Column('period', sa.Integer(), nullable=False),
        sa.Column('time_interval', sa.String(11), nullable=False),
        sa.Column('gen_nuclear_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('gen_coal_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('gen_gas_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('gen_solar_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('gen_wind_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('gen_hydro_pumped_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('gen_biomass_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('gen_hydro_other_mw', sa.Numeric(12, 3), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('id', name='entsoe_generation_actual_pkey'),
        sa.UniqueConstraint('trade_date', 'period', name='entsoe_generation_actual_trade_date_period_key'),
        sa.UniqueConstraint('trade_date', 'time_interval', name='entsoe_generation_actual_trade_date_time_interval_key'),
    )

    # Restore data from partitioned table
    op.execute("""
        INSERT INTO entsoe_generation_actual_old
            (trade_date, period, time_interval,
             gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
             gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw,
             created_at)
        SELECT
            trade_date, period, time_interval,
            gen_nuclear_mw, gen_coal_mw, gen_gas_mw, gen_solar_mw,
            gen_wind_mw, gen_hydro_pumped_mw, gen_biomass_mw, gen_hydro_other_mw,
            created_at
        FROM entsoe_generation_actual
        WHERE area_id = 1;
    """)

    op.execute("""
        INSERT INTO entsoe_germany_wind
            (trade_date, period, time_interval,
             wind_onshore_mw, wind_offshore_mw, wind_total_mw, created_at)
        SELECT
            trade_date, period, time_interval,
            gen_wind_mw, gen_wind_offshore_mw,
            COALESCE(gen_wind_mw, 0) + COALESCE(gen_wind_offshore_mw, 0),
            created_at
        FROM entsoe_generation_actual
        WHERE area_id = 2;
    """)
