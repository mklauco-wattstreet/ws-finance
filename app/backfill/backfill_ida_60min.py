"""Backfill ote_prices_ida_60min from 15-min source.

Per docs/60min_tables_plan.md §4.2 (provisional, aggregation-from-quarters):
- price_eur_mwh: VWAP weighted by volume_mwh
- volume_mwh, saldo_dm_mwh, export_mwh, import_mwh: sum

GROUP BY (trade_date, hour of delivery_datetime, ida_idx).

Usage:
    python3 -m backfill.backfill_ida_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import (
    HOUR_COMPLETE_DT_HAVING,
    HOUR_INTERVAL_DT_SQL,
    HOUR_START_DT_SQL,
    parse_args,
    run_backfill,
    setup_logging,
)


IDA_SQL = f"""
INSERT INTO ote_prices_ida_60min (
    trade_date, time_interval, delivery_datetime, ida_idx,
    price_eur_mwh, volume_mwh,
    saldo_dm_mwh, export_mwh, import_mwh
)
SELECT
    trade_date,
    {HOUR_INTERVAL_DT_SQL},
    {HOUR_START_DT_SQL},
    ida_idx,
    SUM(price_eur_mwh * volume_mwh) / NULLIF(SUM(volume_mwh), 0),
    SUM(volume_mwh),
    SUM(saldo_dm_mwh),
    SUM(export_mwh),
    SUM(import_mwh)
FROM ote_prices_ida
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_START_DT_SQL}, ida_idx
{HOUR_COMPLETE_DT_HAVING}
ON CONFLICT (delivery_datetime, ida_idx) DO UPDATE SET
    trade_date = EXCLUDED.trade_date,
    time_interval = EXCLUDED.time_interval,
    price_eur_mwh = EXCLUDED.price_eur_mwh,
    volume_mwh = EXCLUDED.volume_mwh,
    saldo_dm_mwh = EXCLUDED.saldo_dm_mwh,
    export_mwh = EXCLUDED.export_mwh,
    import_mwh = EXCLUDED.import_mwh,
    updated_at = CURRENT_TIMESTAMP
"""


def main():
    args = parse_args("IDA")
    logger = setup_logging("backfill_ida_60min", args.debug)
    run_backfill(
        label="IDA",
        queries=[("ote_prices_ida_60min", IDA_SQL)],
        args=args,
        logger=logger,
    )


if __name__ == "__main__":
    main()
