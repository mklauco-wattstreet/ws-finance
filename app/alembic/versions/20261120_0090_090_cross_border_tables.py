"""Add cross-border tables: border register, ENTSO-E border data, JAO Core.

Revision ID: 090
Revises: 089
Create Date: 2026-11-20

Plan: docs/cross_border_plan.md section 2.1. Every cross-border signal for the
CZ borders (level 0) goes into tables that take the borders of CZ's neighbours
(level 1) by adding rows in entsoe_borders / new JAO rows, not new columns.

Register
  entsoe_borders                  one row per border (bidding-zone or control-area
                                  pair), level 0 = CZ borders, level 1 = borders of
                                  CZ's neighbours (seeded inactive).

ENTSO-E data tables. Every row is DIRECTED: energy or capacity goes from
from_area_id to to_area_id (ENTSO-E out_Domain -> in_Domain). Timestamps are
TIMESTAMPTZ; trade_date / period / time_interval follow the convention of
entsoe_cross_border_flows (Prague-local trade date, period counted from local
midnight 1..92/96/100, time_interval 'HH:MM-HH:MM'), descriptive only - the
key is the UTC instant.
  entsoe_border_flows             15 min physical + scheduled flows per direction
  entsoe_border_capacity          mixed resolution capacity / auction items
  entsoe_net_positions            15 min, LIST-partitioned by country_code (CZ)
  entsoe_transmission_outages     A78 events (header)
  entsoe_transmission_outage_points  A78 capacity curve points
  entsoe_congestion_costs         monthly (A92)

JAO Core publication tool tables (jao_core_*): JAO's own hub codes as text,
everything a response contains is stored, so level 1 comes for free.

created_at / updated_at are TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP; there is no
trigger, the upserts set updated_at (same as the sibling tables).

No existing table is touched. Downgrade drops exactly these 14 tables (7
ENTSO-E tables incl. the register, plus 7 JAO tables; the CZ partition of
entsoe_net_positions goes with its parent).
"""

from alembic import op


revision = '090'
down_revision = '089'
branch_labels = None
depends_on = None


_AUDIT = """
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"""

_CAPACITY_ITEMS = (
    "'ntc_forecast', 'offered_explicit', 'offered_intraday', 'already_allocated', "
    "'nominated', 'use_requested', 'use_allocated', 'auction_price', 'auction_revenue'"
)
_CAPACITY_HORIZONS = "'day', 'week', 'month', 'year', 'idct', 'ida1', 'ida2', 'ida3'"
_JAO_MARKETS = "'da', 'ida1', 'ida2', 'ida3'"
_JAO_ROUNDS = "'A', 'B', 'C', 'D', 'E'"

_NEW_TABLES_DROP_ORDER = [
    "jao_core_allocation_constraint",
    "jao_core_intraday_atc",
    "jao_core_price_spread",
    "jao_core_scheduled_exchange",
    "jao_core_net_position",
    "jao_core_max_exchange",
    "jao_core_max_net_position",
    "entsoe_congestion_costs",
    "entsoe_transmission_outage_points",
    "entsoe_transmission_outages",
    "entsoe_net_positions",
    "entsoe_border_capacity",
    "entsoe_border_flows",
    "entsoe_borders",
]


def _grant(table: str, sequence: str | None = None) -> None:
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO user_finance;")
    if sequence:
        op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {sequence} TO user_finance;")


def upgrade() -> None:
    # ------------------------------------------------------------------
    # entsoe_borders - border register
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_borders (
            border_id SMALLINT NOT NULL,
            border_code TEXT NOT NULL,
            area_a_id INTEGER NOT NULL,
            area_b_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            level SMALLINT NOT NULL,
            capacity_region TEXT,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            {_AUDIT.strip()},
            CONSTRAINT entsoe_borders_pkey PRIMARY KEY (border_id),
            CONSTRAINT entsoe_borders_border_code_key UNIQUE (border_code),
            CONSTRAINT entsoe_borders_areas_key UNIQUE (area_a_id, area_b_id),
            CONSTRAINT entsoe_borders_area_a_fkey FOREIGN KEY (area_a_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_borders_area_b_fkey FOREIGN KEY (area_b_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_borders_kind_check CHECK (kind IN ('bidding_zone', 'control_area')),
            CONSTRAINT entsoe_borders_areas_differ_check CHECK (area_a_id <> area_b_id)
        );
    """)
    op.execute("""
        INSERT INTO entsoe_borders
            (border_id, border_code, area_a_id, area_b_id, kind, level, capacity_region, is_active)
        VALUES
            (1,  'CZ-DE_LU',      1, 10, 'bidding_zone', 0, 'Core', TRUE),
            (2,  'CZ-DE_TENNET',  1,  2, 'control_area', 0, 'Core', TRUE),
            (3,  'CZ-DE_50HERTZ', 1,  6, 'control_area', 0, 'Core', TRUE),
            (4,  'CZ-AT',         1,  3, 'bidding_zone', 0, 'Core', TRUE),
            (5,  'CZ-PL',         1,  4, 'bidding_zone', 0, 'Core', TRUE),
            (6,  'CZ-SK',         1,  5, 'bidding_zone', 0, 'Core', TRUE),
            (7,  'DE_LU-AT',     10,  3, 'bidding_zone', 1, 'Core', FALSE),
            (8,  'DE_LU-PL',     10,  4, 'bidding_zone', 1, 'Core', FALSE),
            (9,  'AT-HU',         3,  9, 'bidding_zone', 1, 'Core', FALSE),
            (10, 'SK-HU',         5,  9, 'bidding_zone', 1, 'Core', FALSE),
            (11, 'PL-SK',         4,  5, 'bidding_zone', 1, 'Core', FALSE);
    """)
    op.execute(
        "COMMENT ON TABLE entsoe_borders IS "
        "'Border register (app/entsoe/borders.py). level 0 = CZ borders, level 1 = borders "
        "of CZ neighbours. Runners fetch the is_active rows, both directions.';"
    )
    _grant("entsoe_borders")

    # ------------------------------------------------------------------
    # entsoe_border_flows - 15 min, directed
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_border_flows (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            from_area_id INTEGER NOT NULL,
            to_area_id INTEGER NOT NULL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            physical_mw NUMERIC(12, 3),
            scheduled_da_mw NUMERIC(12, 3),
            scheduled_total_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT entsoe_border_flows_pkey PRIMARY KEY (delivery_datetime, from_area_id, to_area_id),
            CONSTRAINT entsoe_border_flows_from_fkey FOREIGN KEY (from_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_border_flows_to_fkey FOREIGN KEY (to_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_border_flows_areas_differ_check CHECK (from_area_id <> to_area_id)
        );
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_border_flows_border_time "
        "ON entsoe_border_flows (from_area_id, to_area_id, delivery_datetime);"
    )
    op.execute(
        "COMMENT ON TABLE entsoe_border_flows IS "
        "'Directed 15-min flows from_area_id -> to_area_id (ENTSO-E out_Domain -> in_Domain): "
        "physical (A11), scheduled day-ahead (A09/A01), scheduled total (A09/A05).';"
    )
    _grant("entsoe_border_flows")

    # ------------------------------------------------------------------
    # entsoe_border_capacity - mixed resolution, directed
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_border_capacity (
            period_start TIMESTAMPTZ NOT NULL,
            from_area_id INTEGER NOT NULL,
            to_area_id INTEGER NOT NULL,
            item VARCHAR(20) NOT NULL,
            horizon VARCHAR(10) NOT NULL,
            period_end TIMESTAMPTZ NOT NULL,
            resolution VARCHAR(10),
            value NUMERIC(16, 4),
            unit VARCHAR(10),
            {_AUDIT.strip()},
            CONSTRAINT entsoe_border_capacity_pkey PRIMARY KEY (period_start, from_area_id, to_area_id, item, horizon),
            CONSTRAINT entsoe_border_capacity_from_fkey FOREIGN KEY (from_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_border_capacity_to_fkey FOREIGN KEY (to_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_border_capacity_areas_differ_check CHECK (from_area_id <> to_area_id),
            CONSTRAINT entsoe_border_capacity_item_check CHECK (item IN ({_CAPACITY_ITEMS})),
            CONSTRAINT entsoe_border_capacity_horizon_check CHECK (horizon IN ({_CAPACITY_HORIZONS}))
        );
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_border_capacity_border_item_time "
        "ON entsoe_border_capacity (from_area_id, to_area_id, item, horizon, period_start);"
    )
    op.execute(
        "COMMENT ON TABLE entsoe_border_capacity IS "
        "'Directed capacity / auction items per period. value is MW except auction_price "
        "(EUR/MW) and auction_revenue (EUR); unit says which. resolution = source resolution "
        "(PT15M, PT60M, P1D, P7D, P1M, P1Y).';"
    )
    _grant("entsoe_border_capacity")

    # ------------------------------------------------------------------
    # entsoe_net_positions - 15 min, LIST-partitioned by country_code
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_net_positions (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            net_position_da_mw NUMERIC(12, 3),
            net_position_intraday_mw NUMERIC(12, 3),
            sched_net_position_da_mw NUMERIC(12, 3),
            sched_net_position_total_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT entsoe_net_positions_pkey PRIMARY KEY (delivery_datetime, area_id, country_code),
            CONSTRAINT entsoe_net_positions_area_fkey FOREIGN KEY (area_id) REFERENCES entsoe_areas (id)
        ) PARTITION BY LIST (country_code);
    """)
    op.execute(
        "CREATE TABLE entsoe_net_positions_cz PARTITION OF entsoe_net_positions FOR VALUES IN ('CZ');"
    )
    op.execute(
        "CREATE INDEX ix_entsoe_net_positions_country_trade_date "
        "ON entsoe_net_positions (country_code, trade_date);"
    )
    op.execute(
        "COMMENT ON TABLE entsoe_net_positions IS "
        "'15-min net positions (positive = net export): day-ahead and intraday allocation "
        "(A25/B09), scheduled (A09/B09 day-ahead and total).';"
    )
    _grant("entsoe_net_positions")

    # ------------------------------------------------------------------
    # entsoe_transmission_outages / _points - A78 events
    #   natural key (doc_mrid, revision_number, timeseries_mrid); surrogate id
    #   is what the points table references (as in migration 064).
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_transmission_outages (
            id BIGSERIAL NOT NULL,
            doc_mrid VARCHAR(60) NOT NULL,
            revision_number INTEGER NOT NULL,
            timeseries_mrid VARCHAR(60) NOT NULL,
            from_area_id INTEGER NOT NULL,
            to_area_id INTEGER NOT NULL,
            business_type VARCHAR(3),
            doc_status VARCHAR(3),
            created_datetime TIMESTAMPTZ,
            asset_mrid VARCHAR(60),
            asset_name VARCHAR(200),
            asset_location VARCHAR(200),
            asset_type VARCHAR(10),
            unavail_start TIMESTAMPTZ NOT NULL,
            unavail_end TIMESTAMPTZ NOT NULL,
            quantity_unit VARCHAR(10),
            curve_type VARCHAR(3),
            min_capacity_mw NUMERIC(12, 3),
            reason_code VARCHAR(10),
            reason_text TEXT,
            {_AUDIT.strip()},
            CONSTRAINT entsoe_transmission_outages_pkey PRIMARY KEY (doc_mrid, revision_number, timeseries_mrid),
            CONSTRAINT entsoe_transmission_outages_id_key UNIQUE (id),
            CONSTRAINT entsoe_transmission_outages_from_fkey FOREIGN KEY (from_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_transmission_outages_to_fkey FOREIGN KEY (to_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_transmission_outages_areas_differ_check CHECK (from_area_id <> to_area_id)
        );
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_transmission_outages_border_window "
        "ON entsoe_transmission_outages (from_area_id, to_area_id, unavail_start, unavail_end);"
    )
    op.execute(
        "COMMENT ON TABLE entsoe_transmission_outages IS "
        "'ENTSO-E A78 transmission unavailability, one row per document TimeSeries and revision, "
        "directed from_area_id (out_Domain) -> to_area_id (in_Domain). created_datetime is the "
        "publication time (point-in-time gate). min_capacity_mw = minimum available across the curve.';"
    )
    _grant("entsoe_transmission_outages", "entsoe_transmission_outages_id_seq")

    op.execute("""
        CREATE TABLE entsoe_transmission_outage_points (
            outage_id BIGINT NOT NULL,
            point_start TIMESTAMPTZ NOT NULL,
            point_end TIMESTAMPTZ,
            capacity_mw NUMERIC(12, 3),
            CONSTRAINT entsoe_transmission_outage_points_pkey PRIMARY KEY (outage_id, point_start),
            CONSTRAINT entsoe_transmission_outage_points_outage_fkey
                FOREIGN KEY (outage_id) REFERENCES entsoe_transmission_outages (id) ON DELETE CASCADE
        );
    """)
    op.execute(
        "COMMENT ON TABLE entsoe_transmission_outage_points IS "
        "'Available-capacity curve of a transmission outage, one row per point.';"
    )
    _grant("entsoe_transmission_outage_points")

    # ------------------------------------------------------------------
    # entsoe_congestion_costs - monthly (A92)
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_congestion_costs (
            period_start TIMESTAMPTZ NOT NULL,
            area_id INTEGER NOT NULL,
            cost_type VARCHAR(30) NOT NULL,
            period_end TIMESTAMPTZ NOT NULL,
            amount_eur NUMERIC(18, 3),
            {_AUDIT.strip()},
            CONSTRAINT entsoe_congestion_costs_pkey PRIMARY KEY (period_start, area_id, cost_type),
            CONSTRAINT entsoe_congestion_costs_area_fkey FOREIGN KEY (area_id) REFERENCES entsoe_areas (id)
        );
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_congestion_costs_area_time "
        "ON entsoe_congestion_costs (area_id, period_start);"
    )
    op.execute(
        "COMMENT ON TABLE entsoe_congestion_costs IS "
        "'Monthly congestion management costs per area (A92); cost_type names the item.';"
    )
    _grant("entsoe_congestion_costs")

    # ------------------------------------------------------------------
    # JAO Core publication tool tables
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE jao_core_max_net_position (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            hub VARCHAR(20) NOT NULL,
            min_mw NUMERIC(12, 3),
            max_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT jao_core_max_net_position_pkey PRIMARY KEY (delivery_datetime, hub)
        );
    """)
    op.execute(
        "CREATE INDEX ix_jao_core_max_net_position_hub_time "
        "ON jao_core_max_net_position (hub, delivery_datetime);"
    )
    _grant("jao_core_max_net_position")

    op.execute(f"""
        CREATE TABLE jao_core_max_exchange (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            from_hub VARCHAR(20) NOT NULL,
            to_hub VARCHAR(20) NOT NULL,
            max_bex_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT jao_core_max_exchange_pkey PRIMARY KEY (delivery_datetime, from_hub, to_hub),
            CONSTRAINT jao_core_max_exchange_hubs_differ_check CHECK (from_hub <> to_hub)
        );
    """)
    op.execute(
        "CREATE INDEX ix_jao_core_max_exchange_border_time "
        "ON jao_core_max_exchange (from_hub, to_hub, delivery_datetime);"
    )
    _grant("jao_core_max_exchange")

    op.execute(f"""
        CREATE TABLE jao_core_net_position (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            hub VARCHAR(20) NOT NULL,
            market VARCHAR(5) NOT NULL,
            net_position_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT jao_core_net_position_pkey PRIMARY KEY (delivery_datetime, hub, market),
            CONSTRAINT jao_core_net_position_market_check CHECK (market IN ({_JAO_MARKETS}))
        );
    """)
    op.execute(
        "CREATE INDEX ix_jao_core_net_position_hub_time "
        "ON jao_core_net_position (hub, market, delivery_datetime);"
    )
    _grant("jao_core_net_position")

    op.execute(f"""
        CREATE TABLE jao_core_scheduled_exchange (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            from_hub VARCHAR(20) NOT NULL,
            to_hub VARCHAR(20) NOT NULL,
            market VARCHAR(5) NOT NULL,
            scheduled_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT jao_core_scheduled_exchange_pkey PRIMARY KEY (delivery_datetime, from_hub, to_hub, market),
            CONSTRAINT jao_core_scheduled_exchange_hubs_differ_check CHECK (from_hub <> to_hub),
            CONSTRAINT jao_core_scheduled_exchange_market_check CHECK (market IN ({_JAO_MARKETS}))
        );
    """)
    op.execute(
        "CREATE INDEX ix_jao_core_scheduled_exchange_border_time "
        "ON jao_core_scheduled_exchange (from_hub, to_hub, market, delivery_datetime);"
    )
    _grant("jao_core_scheduled_exchange")

    op.execute(f"""
        CREATE TABLE jao_core_price_spread (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            from_hub VARCHAR(20) NOT NULL,
            to_hub VARCHAR(20) NOT NULL,
            market VARCHAR(5) NOT NULL,
            spread_eur NUMERIC(14, 4),
            {_AUDIT.strip()},
            CONSTRAINT jao_core_price_spread_pkey PRIMARY KEY (delivery_datetime, from_hub, to_hub, market),
            CONSTRAINT jao_core_price_spread_hubs_differ_check CHECK (from_hub <> to_hub),
            CONSTRAINT jao_core_price_spread_market_check CHECK (market IN ({_JAO_MARKETS}))
        );
    """)
    op.execute(
        "CREATE INDEX ix_jao_core_price_spread_border_time "
        "ON jao_core_price_spread (from_hub, to_hub, market, delivery_datetime);"
    )
    _grant("jao_core_price_spread")

    op.execute(f"""
        CREATE TABLE jao_core_intraday_atc (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            from_hub VARCHAR(20) NOT NULL,
            to_hub VARCHAR(20) NOT NULL,
            calc_round VARCHAR(1) NOT NULL,
            atc_mw NUMERIC(12, 3),
            ntc_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT jao_core_intraday_atc_pkey PRIMARY KEY (delivery_datetime, from_hub, to_hub, calc_round),
            CONSTRAINT jao_core_intraday_atc_hubs_differ_check CHECK (from_hub <> to_hub),
            CONSTRAINT jao_core_intraday_atc_round_check CHECK (calc_round IN ({_JAO_ROUNDS}))
        );
    """)
    op.execute(
        "CREATE INDEX ix_jao_core_intraday_atc_border_time "
        "ON jao_core_intraday_atc (from_hub, to_hub, calc_round, delivery_datetime);"
    )
    _grant("jao_core_intraday_atc")

    op.execute(f"""
        CREATE TABLE jao_core_allocation_constraint (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            hub VARCHAR(20) NOT NULL,
            market VARCHAR(5) NOT NULL,
            limit_up_mw NUMERIC(12, 3),
            limit_down_mw NUMERIC(12, 3),
            {_AUDIT.strip()},
            CONSTRAINT jao_core_allocation_constraint_pkey PRIMARY KEY (delivery_datetime, hub, market),
            CONSTRAINT jao_core_allocation_constraint_market_check CHECK (market IN ({_JAO_MARKETS}))
        );
    """)
    op.execute(
        "CREATE INDEX ix_jao_core_allocation_constraint_hub_time "
        "ON jao_core_allocation_constraint (hub, market, delivery_datetime);"
    )
    _grant("jao_core_allocation_constraint")


def downgrade() -> None:
    for table in _NEW_TABLES_DROP_ORDER:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
