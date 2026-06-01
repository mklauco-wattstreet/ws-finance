"""Backfill da_period_summary_60min and da_curve_depth_60min from 15-min sources.

Per docs/60min_tables_plan.md §4.1:
- da_period_summary_60min: price columns mean, volume/gap columns sum
- da_curve_depth_60min: all wall columns mean (clearing_price is mean)

Usage:
    python3 -m backfill.backfill_da_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
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


PERIOD_SUMMARY_SQL = f"""
INSERT INTO da_period_summary_60min (
    delivery_date, time_interval,
    clearing_price, clearing_volume,
    supply_next_price, supply_next_volume,
    supply_price_gap, supply_volume_gap,
    demand_next_price, demand_next_volume,
    demand_price_gap, demand_volume_gap
)
SELECT
    delivery_date,
    {HOUR_INTERVAL_SQL},
    AVG(clearing_price),
    SUM(clearing_volume),
    AVG(supply_next_price),
    SUM(supply_next_volume),
    AVG(supply_price_gap),
    SUM(supply_volume_gap),
    AVG(demand_next_price),
    SUM(demand_next_volume),
    AVG(demand_price_gap),
    SUM(demand_volume_gap)
FROM da_period_summary
WHERE delivery_date = %s
GROUP BY delivery_date, {HOUR_GROUP_SQL}
ON CONFLICT (delivery_date, time_interval) DO UPDATE SET
    clearing_price = EXCLUDED.clearing_price,
    clearing_volume = EXCLUDED.clearing_volume,
    supply_next_price = EXCLUDED.supply_next_price,
    supply_next_volume = EXCLUDED.supply_next_volume,
    supply_price_gap = EXCLUDED.supply_price_gap,
    supply_volume_gap = EXCLUDED.supply_volume_gap,
    demand_next_price = EXCLUDED.demand_next_price,
    demand_next_volume = EXCLUDED.demand_next_volume,
    demand_price_gap = EXCLUDED.demand_price_gap,
    demand_volume_gap = EXCLUDED.demand_volume_gap
"""


CURVE_DEPTH_SQL = f"""
INSERT INTO da_curve_depth_60min (
    delivery_date, time_interval, clearing_price,
    supply_mw_from_clearing, supply_price_from_clearing, supply_slope,
    supply_matched_mw_from_clearing, supply_matched_price_from_clearing, supply_matched_slope,
    demand_mw_from_clearing, demand_price_from_clearing, demand_slope,
    demand_matched_mw_from_clearing, demand_matched_price_from_clearing, demand_matched_slope
)
SELECT
    delivery_date,
    {HOUR_INTERVAL_SQL},
    AVG(clearing_price),
    AVG(supply_mw_from_clearing),
    AVG(supply_price_from_clearing),
    AVG(supply_slope),
    AVG(supply_matched_mw_from_clearing),
    AVG(supply_matched_price_from_clearing),
    AVG(supply_matched_slope),
    AVG(demand_mw_from_clearing),
    AVG(demand_price_from_clearing),
    AVG(demand_slope),
    AVG(demand_matched_mw_from_clearing),
    AVG(demand_matched_price_from_clearing),
    AVG(demand_matched_slope)
FROM da_curve_depth
WHERE delivery_date = %s
GROUP BY delivery_date, {HOUR_GROUP_SQL}
ON CONFLICT (delivery_date, time_interval) DO UPDATE SET
    clearing_price = EXCLUDED.clearing_price,
    supply_mw_from_clearing = EXCLUDED.supply_mw_from_clearing,
    supply_price_from_clearing = EXCLUDED.supply_price_from_clearing,
    supply_slope = EXCLUDED.supply_slope,
    supply_matched_mw_from_clearing = EXCLUDED.supply_matched_mw_from_clearing,
    supply_matched_price_from_clearing = EXCLUDED.supply_matched_price_from_clearing,
    supply_matched_slope = EXCLUDED.supply_matched_slope,
    demand_mw_from_clearing = EXCLUDED.demand_mw_from_clearing,
    demand_price_from_clearing = EXCLUDED.demand_price_from_clearing,
    demand_slope = EXCLUDED.demand_slope,
    demand_matched_mw_from_clearing = EXCLUDED.demand_matched_mw_from_clearing,
    demand_matched_price_from_clearing = EXCLUDED.demand_matched_price_from_clearing,
    demand_matched_slope = EXCLUDED.demand_matched_slope
"""


def main():
    args = parse_args("DA")
    logger = setup_logging("backfill_da_60min", args.debug)
    run_backfill(
        label="DA",
        queries=[
            ("da_period_summary_60min", PERIOD_SUMMARY_SQL),
            ("da_curve_depth_60min", CURVE_DEPTH_SQL),
        ],
        args=args,
        logger=logger,
    )


if __name__ == "__main__":
    main()
