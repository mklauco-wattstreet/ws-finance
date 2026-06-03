"""One-shot re-derivation of da_period_summary and da_curve_depth from da_bid.

Use after fixing the MAX(matched-sell-price) clearing_price bug in
upload_dam_curves.py. The raw bid stack (da_bid) is untouched by the bug —
only the derived tables had wrong clearing_price (and downstream gap /
*_from_clearing columns).

This script:
  1. Reads distinct delivery_dates present in da_bid
  2. For each date, calls compute_and_upsert_period_summary
     and compute_and_upsert_curve_depth from upload_dam_curves
     (now using the fixed JOIN-to-OTE logic)
  3. Logs per-date row counts and total

UPSERT semantics: existing rows get overwritten with corrected values.
Periods without a matching ote_prices_day_ahead row are skipped (e.g.
DST-skipped hour 02 on spring-forward day) — the fixed query uses INNER JOIN.

Usage:
    python3 -m backfill.recompute_da_period_summary_and_curve_depth                 # all dates
    python3 -m backfill.recompute_da_period_summary_and_curve_depth 2026-01-01 2026-06-03
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import get_db_connection, setup_logging, print_banner
from upload_dam_curves import (
    compute_and_upsert_period_summary,
    compute_and_upsert_curve_depth,
)


def main():
    parser = argparse.ArgumentParser(
        description="Re-derive da_period_summary + da_curve_depth from da_bid"
    )
    parser.add_argument('start', nargs='?', help='YYYY-MM-DD (default: min(da_bid.delivery_date))')
    parser.add_argument('end',   nargs='?', help='YYYY-MM-DD (default: max(da_bid.delivery_date))')
    args = parser.parse_args()

    logger = setup_logging("recompute_da")
    print_banner("Re-derive da_period_summary + da_curve_depth from da_bid")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Discover the date range to process from da_bid
            range_clause = []
            params = []
            if args.start:
                range_clause.append("delivery_date >= %s")
                params.append(datetime.strptime(args.start, '%Y-%m-%d').date())
            if args.end:
                range_clause.append("delivery_date <= %s")
                params.append(datetime.strptime(args.end, '%Y-%m-%d').date())
            where = ("WHERE " + " AND ".join(range_clause)) if range_clause else ""
            cur.execute(
                f"SELECT DISTINCT delivery_date FROM da_bid {where} ORDER BY delivery_date",
                params,
            )
            dates = [r[0] for r in cur.fetchall()]

        if not dates:
            logger.warning("No dates found in da_bid for the given range — nothing to do")
            return

        logger.info(f"Processing {len(dates)} dates from {dates[0]} to {dates[-1]}")

        period_total = 0
        depth_total = 0
        for i, d in enumerate(dates, 1):
            try:
                p = compute_and_upsert_period_summary(d, conn, logger)
                c = compute_and_upsert_curve_depth(d, conn, logger)
                period_total += p
                depth_total += c
                if i % 10 == 0:
                    logger.info(
                        f"  {i}/{len(dates)} dates processed "
                        f"(period_summary={period_total}, curve_depth={depth_total})"
                    )
            except Exception as e:
                conn.rollback()
                logger.error(f"  {d} failed: {e}")
                raise

    logger.info("")
    logger.info(f"  da_period_summary: {period_total} rows upserted across {len(dates)} dates")
    logger.info(f"  da_curve_depth:    {depth_total} rows upserted across {len(dates)} dates")
    logger.info("Recompute complete")


if __name__ == "__main__":
    main()
