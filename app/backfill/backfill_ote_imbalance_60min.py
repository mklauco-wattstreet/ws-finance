"""Backfill ote_prices_imbalance_60min from the 15-min source.

Per docs/60min_tables_plan.md §4.7:
- Volumes (system_imbalance_mwh, absolute_imbalance_sum_mwh,
  positive_imbalance_mwh, negative_imbalance_mwh,
  rounded_imbalance_mwh): sum across the 4 quarters
- Costs (cost_of_be_czk, cost_of_imbalance_czk): sum
- Prices (settlement_price_*, price_*_component_czk_mwh,
  price_not_performed_activation_czk_mwh): mean

Usage:
    python3 -m backfill.backfill_ote_imbalance_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import (
    HOUR_COMPLETE_HAVING,
    HOUR_GROUP_SQL,
    HOUR_INTERVAL_SQL,
    parse_args,
    run_backfill,
    setup_logging,
)


IMBALANCE_SQL = f"""
INSERT INTO ote_prices_imbalance_60min (
    trade_date, time_interval,
    system_imbalance_mwh, absolute_imbalance_sum_mwh,
    positive_imbalance_mwh, negative_imbalance_mwh, rounded_imbalance_mwh,
    cost_of_be_czk, cost_of_imbalance_czk,
    settlement_price_imbalance_czk_mwh,
    settlement_price_counter_imbalance_czk_mwh,
    price_protective_be_component_czk_mwh,
    price_be_component_czk_mwh,
    price_im_component_czk_mwh,
    price_si_component_czk_mwh,
    price_not_performed_activation_czk_mwh
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    SUM(system_imbalance_mwh),
    SUM(absolute_imbalance_sum_mwh),
    SUM(positive_imbalance_mwh),
    SUM(negative_imbalance_mwh),
    SUM(rounded_imbalance_mwh),
    SUM(cost_of_be_czk),
    SUM(cost_of_imbalance_czk),
    AVG(settlement_price_imbalance_czk_mwh),
    AVG(settlement_price_counter_imbalance_czk_mwh),
    AVG(price_protective_be_component_czk_mwh),
    AVG(price_be_component_czk_mwh),
    AVG(price_im_component_czk_mwh),
    AVG(price_si_component_czk_mwh),
    AVG(price_not_performed_activation_czk_mwh)
FROM ote_prices_imbalance
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{HOUR_COMPLETE_HAVING}
ON CONFLICT (trade_date, time_interval) DO UPDATE SET
    system_imbalance_mwh = EXCLUDED.system_imbalance_mwh,
    absolute_imbalance_sum_mwh = EXCLUDED.absolute_imbalance_sum_mwh,
    positive_imbalance_mwh = EXCLUDED.positive_imbalance_mwh,
    negative_imbalance_mwh = EXCLUDED.negative_imbalance_mwh,
    rounded_imbalance_mwh = EXCLUDED.rounded_imbalance_mwh,
    cost_of_be_czk = EXCLUDED.cost_of_be_czk,
    cost_of_imbalance_czk = EXCLUDED.cost_of_imbalance_czk,
    settlement_price_imbalance_czk_mwh = EXCLUDED.settlement_price_imbalance_czk_mwh,
    settlement_price_counter_imbalance_czk_mwh = EXCLUDED.settlement_price_counter_imbalance_czk_mwh,
    price_protective_be_component_czk_mwh = EXCLUDED.price_protective_be_component_czk_mwh,
    price_be_component_czk_mwh = EXCLUDED.price_be_component_czk_mwh,
    price_im_component_czk_mwh = EXCLUDED.price_im_component_czk_mwh,
    price_si_component_czk_mwh = EXCLUDED.price_si_component_czk_mwh,
    price_not_performed_activation_czk_mwh = EXCLUDED.price_not_performed_activation_czk_mwh,
    updated_at = CURRENT_TIMESTAMP
"""


def main():
    args = parse_args("OTE imbalance")
    logger = setup_logging("backfill_ote_imbalance_60min", args.debug)
    run_backfill(
        label="OTE imbalance",
        queries=[("ote_prices_imbalance_60min", IMBALANCE_SQL)],
        args=args,
        logger=logger,
    )


if __name__ == "__main__":
    main()
