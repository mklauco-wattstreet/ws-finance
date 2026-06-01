"""Backfill ENTSO-E 60-min tables from 15-min sources.

Per docs/60min_tables_plan.md §4.5 — 7 tables.

Aggregation rules:
- All MW / price columns:           AVG()
- entsoe_imbalance_prices_60min:
    * pos_imb_*, neg_imb_* prices:  AVG()
    * imbalance_mwh, difference_mwh: SUM()
    * situation, status:            last (ARRAY_AGG ORDER BY time_interval DESC)
    * currency:                     MAX() (identical across the four quarters)
    * delivery_datetime:            MIN() (hour start)
- entsoe_cross_border_flows_60min:
    * delivery_datetime: date_trunc('hour', MIN(delivery_datetime))

Partitioning is preserved: INSERT goes to the parent partitioned
table; PostgreSQL routes each row to the right partition.

Usage:
    python3 -m backfill.backfill_entsoe_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import (
    HOUR_GROUP_SQL,
    HOUR_INTERVAL_SQL,
    parse_args,
    run_backfill,
    setup_logging,
)


# -------------------- entsoe_load_60min --------------------
LOAD_SQL = f"""
INSERT INTO entsoe_load_60min (
    trade_date, time_interval, actual_load_mw, forecast_load_mw
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    AVG(actual_load_mw),
    AVG(forecast_load_mw)
FROM entsoe_load
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    actual_load_mw = EXCLUDED.actual_load_mw,
    forecast_load_mw = EXCLUDED.forecast_load_mw,
    updated_at = CURRENT_TIMESTAMP
"""


# -------------------- entsoe_generation_forecast_60min --------------------
GEN_FORECAST_SQL = f"""
INSERT INTO entsoe_generation_forecast_60min (
    trade_date, time_interval,
    forecast_solar_mw, forecast_wind_mw, forecast_wind_offshore_mw
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    AVG(forecast_solar_mw),
    AVG(forecast_wind_mw),
    AVG(forecast_wind_offshore_mw)
FROM entsoe_generation_forecast
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    forecast_solar_mw = EXCLUDED.forecast_solar_mw,
    forecast_wind_mw = EXCLUDED.forecast_wind_mw,
    forecast_wind_offshore_mw = EXCLUDED.forecast_wind_offshore_mw,
    updated_at = CURRENT_TIMESTAMP
"""


# -------------------- entsoe_generation_actual_60min (partitioned by country_code) --------------------
_GEN_ACTUAL_COLS = [
    "gen_nuclear_mw", "gen_coal_mw", "gen_gas_mw",
    "gen_solar_mw", "gen_wind_mw", "gen_wind_offshore_mw",
    "gen_hydro_pumped_mw", "gen_biomass_mw", "gen_hydro_other_mw",
]
GEN_ACTUAL_SQL = f"""
INSERT INTO entsoe_generation_actual_60min (
    trade_date, time_interval, area_id, country_code,
    {", ".join(_GEN_ACTUAL_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    area_id,
    country_code,
    {", ".join(f"AVG({c})" for c in _GEN_ACTUAL_COLS)}
FROM entsoe_generation_actual
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}, area_id, country_code
ON CONFLICT (trade_date, time_interval, area_id, country_code) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in _GEN_ACTUAL_COLS)},
    updated_at = CURRENT_TIMESTAMP
"""


# -------------------- entsoe_cross_border_flows_60min --------------------
_FLOW_COLS = ["flow_de_mw", "flow_at_mw", "flow_pl_mw", "flow_sk_mw", "flow_total_net_mw"]
CROSS_BORDER_FLOWS_SQL = f"""
INSERT INTO entsoe_cross_border_flows_60min (
    trade_date, time_interval, delivery_datetime, area_id,
    {", ".join(_FLOW_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    date_trunc('hour', MIN(delivery_datetime)),
    area_id,
    {", ".join(f"AVG({c})" for c in _FLOW_COLS)}
FROM entsoe_cross_border_flows
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}, area_id
ON CONFLICT (trade_date, time_interval, area_id) DO UPDATE SET
    delivery_datetime = EXCLUDED.delivery_datetime,
    {", ".join(f"{c} = EXCLUDED.{c}" for c in _FLOW_COLS)},
    updated_at = CURRENT_TIMESTAMP
"""


# -------------------- entsoe_scheduled_cross_border_flows_60min --------------------
_SCHED_FLOW_COLS = ["scheduled_de_mw", "scheduled_at_mw", "scheduled_pl_mw",
                    "scheduled_sk_mw", "scheduled_total_net_mw"]
SCHEDULED_CROSS_BORDER_FLOWS_SQL = f"""
INSERT INTO entsoe_scheduled_cross_border_flows_60min (
    trade_date, time_interval,
    {", ".join(_SCHED_FLOW_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    {", ".join(f"AVG({c})" for c in _SCHED_FLOW_COLS)}
FROM entsoe_scheduled_cross_border_flows
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in _SCHED_FLOW_COLS)},
    updated_at = CURRENT_TIMESTAMP
"""


# -------------------- entsoe_day_ahead_prices_60min (partitioned by country_code) --------------------
DAY_AHEAD_PRICES_SQL = f"""
INSERT INTO entsoe_day_ahead_prices_60min (
    trade_date, time_interval, area_id, country_code, price_eur_mwh
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    area_id,
    country_code,
    AVG(price_eur_mwh)
FROM entsoe_day_ahead_prices
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}, area_id, country_code
ON CONFLICT (trade_date, time_interval, area_id, country_code) DO UPDATE SET
    price_eur_mwh = EXCLUDED.price_eur_mwh,
    updated_at = CURRENT_TIMESTAMP
"""


# -------------------- entsoe_imbalance_prices_60min (partitioned by country_code) --------------------
_IMB_PRICE_COLS = [
    "pos_imb_price_mwh", "pos_imb_scarcity_mwh",
    "pos_imb_incentive_mwh", "pos_imb_financial_neutrality_mwh",
    "neg_imb_price_mwh", "neg_imb_scarcity_mwh",
    "neg_imb_incentive_mwh", "neg_imb_financial_neutrality_mwh",
]
IMBALANCE_PRICES_SQL = f"""
INSERT INTO entsoe_imbalance_prices_60min (
    trade_date, time_interval, area_id, country_code,
    {", ".join(_IMB_PRICE_COLS)},
    imbalance_mwh, difference_mwh,
    situation, status, currency, delivery_datetime
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    area_id,
    country_code,
    {", ".join(f"AVG({c})" for c in _IMB_PRICE_COLS)},
    SUM(imbalance_mwh),
    SUM(difference_mwh),
    (ARRAY_AGG(situation ORDER BY time_interval DESC))[1],
    (ARRAY_AGG(status    ORDER BY time_interval DESC))[1],
    MAX(currency),
    date_trunc('hour', MIN(delivery_datetime))
FROM entsoe_imbalance_prices
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}, area_id, country_code
ON CONFLICT (trade_date, time_interval, area_id, country_code) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in _IMB_PRICE_COLS)},
    imbalance_mwh = EXCLUDED.imbalance_mwh,
    difference_mwh = EXCLUDED.difference_mwh,
    situation = EXCLUDED.situation,
    status = EXCLUDED.status,
    currency = EXCLUDED.currency,
    delivery_datetime = EXCLUDED.delivery_datetime,
    updated_at = CURRENT_TIMESTAMP
"""


def main():
    args = parse_args("ENTSO-E")
    logger = setup_logging("backfill_entsoe_60min", args.debug)
    run_backfill(
        label="ENTSO-E",
        queries=[
            ("entsoe_load_60min",                         LOAD_SQL),
            ("entsoe_generation_forecast_60min",          GEN_FORECAST_SQL),
            ("entsoe_generation_actual_60min",            GEN_ACTUAL_SQL),
            ("entsoe_cross_border_flows_60min",           CROSS_BORDER_FLOWS_SQL),
            ("entsoe_scheduled_cross_border_flows_60min", SCHEDULED_CROSS_BORDER_FLOWS_SQL),
            ("entsoe_day_ahead_prices_60min",             DAY_AHEAD_PRICES_SQL),
            ("entsoe_imbalance_prices_60min",             IMBALANCE_PRICES_SQL),
        ],
        args=args,
        logger=logger,
    )


if __name__ == "__main__":
    main()
