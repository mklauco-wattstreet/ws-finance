"""Add ENTSO-E outages tables (A77 production/generation unavailability).

Revision ID: 064
Revises: 063
Create Date: 2026-06-07

Four tables for ENTSO-E unavailability data (documentType A77; the schema is
A80-ready via the ``doc_type`` + ``power_system_resource_*`` columns). All are
LIST-partitioned by ``country_code`` (CZ partition created now; add more with a
one-line migration when other areas are switched on).

Grain & philosophy
------------------
Outages are EVENT-based, not a 96-period time series: one document = one asset
outage with its own ``unavail_start``/``unavail_end`` (minutes .. years), an
``mRID`` + ``revision_number`` that get revised/withdrawn over time, and a
PT1M ``Available_Period`` curve of *available* MW (unavailable = nominal-available).

  - entsoe_outages          raw event header rows  (one per doc TimeSeries; lossless, keeps source_xml)
  - entsoe_outage_points    full PT1M available-capacity curve (one row per Point)
  - entsoe_outages_15min    PRIMARY feature: total OUT MW per 15-min MTU (deduped per unit, active)
  - entsoe_outages_60min    hourly aggregate (mean MW for level signals, max for peak, max for counts)

Point-in-time correctness
-------------------------
``created_datetime`` (publication time) is stored on every raw event so feature
queries can gate ``WHERE created_datetime <= :decision_time`` for honest, no-
lookahead backtests. The 15min/60min aggregates are the *realized* view anchored
to the outage window; the runner can also build a point-in-time view from the raw
events when needed.

Aggregation rules (15min -> 60min)
----------------------------------
MW-offline is a power level -> ``*_mw`` columns aggregate by MEAN over the four
MTUs (energy-consistent hourly average). ``total_out_mw_max`` keeps the in-hour
peak; ``n_units_max`` keeps the peak distinct-unit count. The aggregator applies
the standard 60min completeness gate (HAVING COUNT(DISTINCT period)=4).
"""

from alembic import op


revision = '064'
down_revision = '063'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # entsoe_outages — raw event header rows (partitioned by country_code)
    #   natural key: (doc_mrid, revision_number, timeseries_mrid, country_code)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_outages (
            id BIGSERIAL,
            doc_mrid VARCHAR(60) NOT NULL,
            revision_number INTEGER NOT NULL,
            timeseries_mrid VARCHAR(60) NOT NULL,
            doc_type VARCHAR(3) NOT NULL,                 -- A77 (A80-ready)
            business_type VARCHAR(3),                     -- A53 planned / A54 forced
            doc_status VARCHAR(3),                        -- A05 Active / A09 Cancelled / A13 Withdrawn; NULL = Active (no docStatus tag). Default API query returns Active+Cancelled only.
            process_type VARCHAR(3),                      -- e.g. A26
            created_datetime TIMESTAMP WITH TIME ZONE,    -- publication time (point-in-time gate)
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            biddingzone_domain VARCHAR(20),               -- EIC of the bidding zone
            production_resource_mrid VARCHAR(40),
            production_resource_name VARCHAR(120),
            location_name VARCHAR(120),
            psr_type VARCHAR(3),                          -- fuel code (B02 lignite, B12 hydro, ...)
            power_system_resource_mrid VARCHAR(40),       -- generation unit EIC (A80; NULL for A77)
            power_system_resource_name VARCHAR(120),
            nominal_power_mw NUMERIC(12, 3),
            quantity_unit VARCHAR(10),                    -- MAW
            curve_type VARCHAR(3),                        -- A03
            unavail_start TIMESTAMP WITH TIME ZONE NOT NULL,
            unavail_end TIMESTAMP WITH TIME ZONE NOT NULL,
            min_available_mw NUMERIC(12, 3),             -- min available across the curve (worst case)
            max_unavailable_mw NUMERIC(12, 3),           -- nominal - min_available (convenience)
            reason_code VARCHAR(10),
            reason_text VARCHAR(255),
            source_xml TEXT,                             -- full raw document, for digging out future features
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (doc_mrid, revision_number, timeseries_mrid, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("CREATE TABLE entsoe_outages_cz PARTITION OF entsoe_outages FOR VALUES IN ('CZ');")
    # query accelerators: overlap-by-window, point-in-time, and per-unit lookups
    op.execute("CREATE INDEX ix_entsoe_outages_window ON entsoe_outages (country_code, unavail_start, unavail_end);")
    op.execute("CREATE INDEX ix_entsoe_outages_created ON entsoe_outages (country_code, created_datetime);")
    op.execute("CREATE INDEX ix_entsoe_outages_resource ON entsoe_outages (country_code, production_resource_mrid);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_outages TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_outages_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_outage_points — full PT1M available-capacity curve
    #   key: (doc_mrid, revision_number, timeseries_mrid, position, country_code)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_outage_points (
            id BIGSERIAL,
            doc_mrid VARCHAR(60) NOT NULL,
            revision_number INTEGER NOT NULL,
            timeseries_mrid VARCHAR(60) NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            point_start TIMESTAMP WITH TIME ZONE NOT NULL,   -- Available_Period interval start
            point_end TIMESTAMP WITH TIME ZONE,              -- Available_Period interval end
            resolution VARCHAR(10),                          -- PT1M / PT15M / PT60M
            position INTEGER NOT NULL,
            available_mw NUMERIC(12, 3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (doc_mrid, revision_number, timeseries_mrid, position, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("CREATE TABLE entsoe_outage_points_cz PARTITION OF entsoe_outage_points FOR VALUES IN ('CZ');")
    op.execute("CREATE INDEX ix_entsoe_outage_points_start ON entsoe_outage_points (country_code, point_start);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_outage_points TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_outage_points_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_outages_15min — PRIMARY feature: total OUT MW per 15-min MTU
    #   key: (trade_date, period, area_id, country_code)   period = 1..96 (Prague)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_outages_15min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            period SMALLINT NOT NULL,                     -- 1..96
            time_interval VARCHAR(11) NOT NULL,           -- HH:MM-HH:MM
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            delivery_datetime TIMESTAMP WITH TIME ZONE,   -- start of the MTU
            total_out_mw NUMERIC(12, 3),                  -- total capacity offline (deduped per unit, active)
            planned_out_mw NUMERIC(12, 3),                -- A53
            forced_out_mw NUMERIC(12, 3),                 -- A54
            n_units INTEGER,                              -- distinct units offline
            out_lignite_mw NUMERIC(12, 3),               -- B02
            out_hard_coal_mw NUMERIC(12, 3),             -- B05
            out_gas_mw NUMERIC(12, 3),                    -- B04
            out_nuclear_mw NUMERIC(12, 3),               -- B14
            out_hydro_mw NUMERIC(12, 3),                  -- B10/B11/B12
            out_other_mw NUMERIC(12, 3),                  -- everything else
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, period, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("CREATE TABLE entsoe_outages_15min_cz PARTITION OF entsoe_outages_15min FOR VALUES IN ('CZ');")
    op.execute("CREATE INDEX ix_entsoe_outages_15min_trade_date ON entsoe_outages_15min (trade_date);")
    op.execute("CREATE INDEX ix_entsoe_outages_15min_delivery ON entsoe_outages_15min (country_code, delivery_datetime);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_outages_15min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_outages_15min_id_seq TO user_finance;")

    # ------------------------------------------------------------------
    # entsoe_outages_60min — hourly aggregate (mean for levels, max for peak/counts)
    #   key: (trade_date, time_interval, area_id, country_code)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE entsoe_outages_60min (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,           -- HH:MM-HH:MM
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            delivery_datetime TIMESTAMP WITH TIME ZONE,
            total_out_mw NUMERIC(12, 3),                  -- MEAN of the 4 MTU total_out_mw (avg MW offline)
            total_out_mw_max NUMERIC(12, 3),              -- in-hour peak MW offline
            planned_out_mw NUMERIC(12, 3),                -- mean A53
            forced_out_mw NUMERIC(12, 3),                 -- mean A54
            n_units_max INTEGER,                          -- peak distinct units offline within the hour
            out_lignite_mw NUMERIC(12, 3),               -- mean B02
            out_hard_coal_mw NUMERIC(12, 3),             -- mean B05
            out_gas_mw NUMERIC(12, 3),                    -- mean B04
            out_nuclear_mw NUMERIC(12, 3),               -- mean B14
            out_hydro_mw NUMERIC(12, 3),                  -- mean B10/B11/B12
            out_other_mw NUMERIC(12, 3),                  -- mean other
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, area_id, country_code)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute("CREATE TABLE entsoe_outages_60min_cz PARTITION OF entsoe_outages_60min FOR VALUES IN ('CZ');")
    op.execute("CREATE INDEX ix_entsoe_outages_60min_trade_date ON entsoe_outages_60min (trade_date);")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE entsoe_outages_60min TO user_finance;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE entsoe_outages_60min_id_seq TO user_finance;")


def downgrade() -> None:
    for table in [
        "entsoe_outages_60min",
        "entsoe_outages_15min",
        "entsoe_outage_points",
        "entsoe_outages",
    ]:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
