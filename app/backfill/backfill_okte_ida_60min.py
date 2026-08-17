"""Backfill okte_prices_ida_60min from 15-min source.

Mirrors backfill_ida_60min.py (the OTE-CR counterpart):
- price_eur_mwh: VWAP weighted by volume_mwh
- volume_mwh, saldo_dm_mwh, export_mwh, import_mwh: sum
- flow_sk_cz, flow_cz_sk, flow_sk_hu, flow_hu_sk, flow_sk_pl, flow_pl_sk: sum
  (same treatment as volume - border flows are additive across quarters)

GROUP BY (trade_date, hour, ida_idx).

Usage:
    python3 -m backfill.backfill_okte_ida_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
    python3 -m backfill.backfill_okte_ida_60min --auto [--debug] [--dry-run]
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


OKTE_IDA_SQL = f"""
INSERT INTO okte_prices_ida_60min (
    trade_date, time_interval, ida_idx,
    price_eur_mwh, volume_mwh,
    saldo_dm_mwh, export_mwh, import_mwh,
    flow_sk_cz, flow_cz_sk, flow_sk_hu, flow_hu_sk, flow_sk_pl, flow_pl_sk
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    ida_idx,
    SUM(price_eur_mwh * volume_mwh) / NULLIF(SUM(volume_mwh), 0),
    SUM(volume_mwh),
    SUM(saldo_dm_mwh),
    SUM(export_mwh),
    SUM(import_mwh),
    SUM(flow_sk_cz),
    SUM(flow_cz_sk),
    SUM(flow_sk_hu),
    SUM(flow_hu_sk),
    SUM(flow_sk_pl),
    SUM(flow_pl_sk)
FROM okte_prices_ida
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}, ida_idx
{HOUR_COMPLETE_HAVING}
ON CONFLICT (trade_date, time_interval, ida_idx) DO UPDATE SET
    price_eur_mwh = EXCLUDED.price_eur_mwh,
    volume_mwh = EXCLUDED.volume_mwh,
    saldo_dm_mwh = EXCLUDED.saldo_dm_mwh,
    export_mwh = EXCLUDED.export_mwh,
    import_mwh = EXCLUDED.import_mwh,
    flow_sk_cz = EXCLUDED.flow_sk_cz,
    flow_cz_sk = EXCLUDED.flow_cz_sk,
    flow_sk_hu = EXCLUDED.flow_sk_hu,
    flow_hu_sk = EXCLUDED.flow_hu_sk,
    flow_sk_pl = EXCLUDED.flow_sk_pl,
    flow_pl_sk = EXCLUDED.flow_pl_sk,
    updated_at = CURRENT_TIMESTAMP
"""


def main():
    args = parse_args("OKTE IDA")
    logger = setup_logging("backfill_okte_ida_60min", args.debug)
    run_backfill(
        label="OKTE IDA",
        queries=[("okte_prices_ida_60min", OKTE_IDA_SQL)],
        args=args,
        logger=logger,
    )


if __name__ == "__main__":
    main()
