"""Correct balancing price columns; add bids, 4-second aFRR prices and price parameters.

Revision ID: 093
Revises: 092
Create Date: 2026-11-25

1. entsoe_balancing_energy: since commit 89e0997 (2025-12-22) the parser used the
   wrong ENTSO-E business types, so every value sits one product too high:

       column    held           correct source
       afrr_*    A95 = FCR      A96
       mfrr_*    A96 = aFRR     A97
       rr_*      A97 = mFRR     A98 (was dropped)

   The existing values are moved to the right columns; FCR gets new fcr_* columns.
   rr_* becomes NULL (A98 was never stored; a backfill fills it). updated_at is
   left alone. New nullable columns:
       fcr_*            A95
       mfrr_specific_*  A97 of a specific product (original_MarketProduct A02); until
                        now it overwrote the standard product, so mfrr_* of CZ and SK
                        stays wrong (mostly 0) until the backfill has run.
       afrr_local_*     A96 under processType A68 (local selection)
   Plain afrr_*, mfrr_*, rr_*, fcr_* come from processType A16.

2. entsoe_balancing_bids          individual bids (A37 / B74), process afrr | mfrr
3. entsoe_afrr_price_4s           4-second aFRR prices (A84 / A67)
4. imbalance_price_parameters     constants of the Czech imbalance price formula

Deployment: the new runner code must be in place BEFORE this migration (it fails
without writing until the columns exist); old code running after it would write
into the wrong columns again.
"""

from alembic import op


revision = '093'
down_revision = '092'
branch_labels = None
depends_on = None


_AUDIT = """created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"""

_NEW_PRICE_COLUMNS = [
    f"{product}_{direction}_price_eur"
    for product in ("fcr", "mfrr_specific", "afrr_local")
    for direction in ("up", "down")
]


def _grant(table: str) -> None:
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO user_finance;")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")

    # 1. entsoe_balancing_energy ------------------------------------------------
    op.execute(
        "ALTER TABLE entsoe_balancing_energy "
        + ", ".join(f"ADD COLUMN {c} NUMERIC(15, 3)" for c in _NEW_PRICE_COLUMNS)
    )
    op.execute("""
        UPDATE entsoe_balancing_energy SET
            fcr_up_price_eur = afrr_up_price_eur,
            fcr_down_price_eur = afrr_down_price_eur,
            afrr_up_price_eur = mfrr_up_price_eur,
            afrr_down_price_eur = mfrr_down_price_eur,
            mfrr_up_price_eur = rr_up_price_eur,
            mfrr_down_price_eur = rr_down_price_eur,
            rr_up_price_eur = NULL,
            rr_down_price_eur = NULL
    """)
    op.execute(
        "COMMENT ON TABLE entsoe_balancing_energy IS "
        "'ENTSO-E A84 activated balancing energy prices, EUR/MWh, 15 min. processType A16: "
        "fcr_* = A95, afrr_* = A96, mfrr_* = A97 standard product, mfrr_specific_* = A97 specific "
        "product, rr_* = A98. afrr_local_* = A96 under processType A68 (local selection). "
        "Before migration 093 the afrr/mfrr/rr columns were shifted by one product.';"
    )

    # 2. entsoe_balancing_bids --------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_balancing_bids (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            process VARCHAR(12) NOT NULL,
            bid_mrid VARCHAR(80) NOT NULL,
            delivery_end TIMESTAMPTZ NOT NULL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            direction VARCHAR(4) NOT NULL,
            product VARCHAR(10),
            acquiring_eic VARCHAR(16),
            quantity_mw NUMERIC(12, 3),
            price_eur NUMERIC(15, 3),
            divisible BOOLEAN,
            status VARCHAR(3),
            {_AUDIT},
            CONSTRAINT entsoe_balancing_bids_pkey
                PRIMARY KEY (delivery_datetime, area_id, country_code, process, bid_mrid),
            CONSTRAINT entsoe_balancing_bids_area_fkey FOREIGN KEY (area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_balancing_bids_process_check CHECK (process IN ('afrr', 'mfrr')),
            CONSTRAINT entsoe_balancing_bids_direction_check CHECK (direction IN ('up', 'down'))
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("CREATE TABLE entsoe_balancing_bids_cz PARTITION OF entsoe_balancing_bids FOR VALUES IN ('CZ');")
    op.execute(
        "COMMENT ON TABLE entsoe_balancing_bids IS "
        "'Individual balancing energy bids (ENTSO-E A37/B74, processType A51 afrr / A47 mfrr), one row per bid. "
        "aFRR bids are published twice with different ids, once with acquiring_eic 10Y1001C--00090V (PICASSO) and "
        "once with the own area: filter on acquiring_eic before summing. product standard / specific; "
        "status is the published code, unchanged.';"
    )
    _grant("entsoe_balancing_bids")

    # 3. entsoe_afrr_price_4s ---------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_afrr_price_4s (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            trade_date DATE NOT NULL,
            afrr_up_price_eur NUMERIC(15, 3),
            afrr_down_price_eur NUMERIC(15, 3),
            {_AUDIT},
            CONSTRAINT entsoe_afrr_price_4s_pkey PRIMARY KEY (delivery_datetime, area_id, country_code),
            CONSTRAINT entsoe_afrr_price_4s_area_fkey FOREIGN KEY (area_id) REFERENCES entsoe_areas (id)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("CREATE TABLE entsoe_afrr_price_4s_cz PARTITION OF entsoe_afrr_price_4s FOR VALUES IN ('CZ');")
    op.execute(
        "COMMENT ON TABLE entsoe_afrr_price_4s IS "
        "'4-second aFRR activation prices (ENTSO-E A84/A96, processType A67), EUR/MWh. "
        "NULL = published without a price.';"
    )
    _grant("entsoe_afrr_price_4s")

    # 4. imbalance_price_parameters ---------------------------------------------
    op.execute(f"""
        CREATE TABLE imbalance_price_parameters (
            parameter VARCHAR(50) NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE,
            value NUMERIC(15, 4) NOT NULL,
            unit VARCHAR(20) NOT NULL,
            source TEXT NOT NULL,
            {_AUDIT},
            CONSTRAINT imbalance_price_parameters_pkey PRIMARY KEY (parameter, valid_from)
        );
    """)
    op.execute("""
        INSERT INTO imbalance_price_parameters (parameter, valid_from, value, unit, source) VALUES
            ('intraday_vwap_offset', '2026-01-01', 250, 'CZK/MWh', 'ERU price decision 13/2025'),
            ('system_imbalance_slope_shortage', '2026-01-01', 5.5, 'CZK/MWh2', 'ERU price decision 13/2025'),
            ('system_imbalance_slope_surplus', '2026-01-01', 3.5, 'CZK/MWh2', 'ERU price decision 13/2025'),
            ('extreme_price_threshold', '2026-01-01', 20000, 'CZK/MWh', 'ERU price decision 13/2025'),
            ('intraday_vwap_min_volume', '2026-01-01', 25, 'MWh', 'decree 408/2015 Annex 8; valid_from not verified'),
            ('emergency_vwap_cap', '2026-01-01', 110, '%', 'decree 408/2015 Annex 8; valid_from not verified');
    """)
    op.execute(
        "COMMENT ON TABLE imbalance_price_parameters IS "
        "'Constants of the Czech imbalance price formula. valid_to NULL = still in force (inclusive end date).';"
    )
    _grant("imbalance_price_parameters")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.execute("DROP TABLE IF EXISTS imbalance_price_parameters;")
    op.execute("DROP TABLE IF EXISTS entsoe_afrr_price_4s CASCADE;")
    op.execute("DROP TABLE IF EXISTS entsoe_balancing_bids CASCADE;")
    op.execute("""
        UPDATE entsoe_balancing_energy SET
            rr_up_price_eur = mfrr_up_price_eur,
            rr_down_price_eur = mfrr_down_price_eur,
            mfrr_up_price_eur = afrr_up_price_eur,
            mfrr_down_price_eur = afrr_down_price_eur,
            afrr_up_price_eur = fcr_up_price_eur,
            afrr_down_price_eur = fcr_down_price_eur
    """)
    op.execute(
        "ALTER TABLE entsoe_balancing_energy "
        + ", ".join(f"DROP COLUMN {c}" for c in _NEW_PRICE_COLUMNS)
    )
