"""Delete partial-hour rows from all _60min tables.

A row is "partial" if its corresponding hour in the source has fewer
than 4 distinct 15-min `time_interval` values — or fewer than 60 minutes
for `ceps_1min_features_60min`, whose source is the 1-min table.

These rows were written by the bootstrap backfill runs before the HAVING
gate (HOUR_COMPLETE_HAVING in _common.py) was added; they must be deleted
before the live aggregators take over.

After cleanup, re-run the 7 backfill scripts over their full source range
to re-emit only complete-hour rows. The HAVING gate guarantees no
partials are re-written.

Usage:
    python3 -m backfill.cleanup_partial_60min_rows [--dry-run]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import get_db_connection, setup_logging, print_banner


# Each entry: (target_60min, source, date_col, extra_keys_csv).
# `extra_keys_csv` lists the non-time keys that must match between source
# and target — partition columns, area_id, ida_idx, forecast_made_at, etc.
QUARTERLY_TABLES = [
    # DA analytics — date column is `delivery_date`
    ("da_period_summary_60min",                   "da_period_summary",                    "delivery_date", ""),
    ("da_curve_depth_60min",                      "da_curve_depth",                       "delivery_date", ""),

    # OTE
    ("ote_prices_ida_60min",                      "ote_prices_ida",                       "trade_date",    "ida_idx"),
    ("ote_prices_imbalance_60min",                "ote_prices_imbalance",                 "trade_date",    ""),

    # Weather
    ("weather_current_60min",                     "weather_current",                      "trade_date",    ""),
    ("weather_forecast_60min",                    "weather_forecast",                     "trade_date",    "forecast_made_at"),

    # CEPS (15-min sources)
    ("ceps_actual_imbalance_60min",               "ceps_actual_imbalance_15min",          "trade_date",    ""),
    ("ceps_estimated_imbalance_price_60min",      "ceps_estimated_imbalance_price_15min", "trade_date",    ""),
    ("ceps_actual_re_price_60min",                "ceps_actual_re_price_15min",           "trade_date",    ""),
    ("ceps_svr_activation_60min",                 "ceps_svr_activation_15min",            "trade_date",    ""),
    ("ceps_export_import_svr_60min",              "ceps_export_import_svr_15min",         "trade_date",    ""),
    ("ceps_generation_60min",                     "ceps_generation_15min",                "trade_date",    ""),
    ("ceps_generation_plan_60min",                "ceps_generation_plan_15min",           "trade_date",    ""),
    ("ceps_generation_res_60min",                 "ceps_generation_res_15min",            "trade_date",    ""),
    ("ceps_derived_features_60min",               "ceps_derived_features_15min",          "trade_date",    ""),

    # ENTSO-E
    ("entsoe_load_60min",                         "entsoe_load",                          "trade_date",    "area_id, country_code"),
    ("entsoe_generation_forecast_60min",          "entsoe_generation_forecast",           "trade_date",    "area_id, country_code"),
    ("entsoe_generation_actual_60min",            "entsoe_generation_actual",             "trade_date",    "area_id, country_code"),
    ("entsoe_cross_border_flows_60min",           "entsoe_cross_border_flows",            "trade_date",    "area_id"),
    ("entsoe_scheduled_cross_border_flows_60min", "entsoe_scheduled_cross_border_flows",  "trade_date",    ""),
    ("entsoe_day_ahead_prices_60min",             "entsoe_day_ahead_prices",              "trade_date",    "area_id, country_code"),
    ("entsoe_imbalance_prices_60min",             "entsoe_imbalance_prices",              "trade_date",    "area_id, country_code"),
]


def _not_exists_clause(target: str, source: str, date_col: str, extra_keys_csv: str) -> str:
    """Build the NOT EXISTS predicate that flags partial-hour 60-min rows.

    Extra-key comparisons are cast to text on both sides. This is a no-op
    when source and target types match, and bridges the one schema mismatch
    (entsoe_cross_border_flows.area_id is INTEGER, _60min.area_id is VARCHAR)
    without per-table special-casing. Safe for timestamps too — both sides
    use the same session formatting.
    """
    extra_predicate_lines = ""
    extra_group = ""
    if extra_keys_csv:
        keys = [k.strip() for k in extra_keys_csv.split(",")]
        extra_predicate_lines = "\n".join(
            f"      AND src.{k}::text = t60.{k}::text" for k in keys
        )
        extra_group = ", " + ", ".join(f"src.{k}" for k in keys)
    return f"""NOT EXISTS (
    SELECT 1
    FROM {source} src
    WHERE src.{date_col} = t60.{date_col}
{extra_predicate_lines}
      AND SUBSTRING(src.time_interval, 1, 2) = SUBSTRING(t60.time_interval, 1, 2)
    GROUP BY src.{date_col}{extra_group}, SUBSTRING(src.time_interval, 1, 2)
    HAVING COUNT(DISTINCT src.time_interval) = 4
)"""


def _build_queries(target: str, source: str, date_col: str, extra_keys_csv: str):
    """Returns (count_sql, delete_sql)."""
    where = _not_exists_clause(target, source, date_col, extra_keys_csv)
    return (
        f"SELECT COUNT(*) FROM {target} t60 WHERE {where}",
        f"DELETE FROM {target} t60 WHERE {where}",
    )


# ceps_1min_features_60min: source is the 1-min table; "complete" = 60 minutes.
CEPS_1MIN_FEATURES_NOT_EXISTS = """NOT EXISTS (
    SELECT 1
    FROM ceps_actual_re_price_1min src
    WHERE src.delivery_timestamp::date = t60.trade_date
      AND TO_CHAR(DATE_TRUNC('hour', src.delivery_timestamp), 'HH24') =
          SUBSTRING(t60.time_interval, 1, 2)
    GROUP BY src.delivery_timestamp::date, DATE_TRUNC('hour', src.delivery_timestamp)
    HAVING COUNT(*) = 60
)"""
CEPS_1MIN_FEATURES_COUNT = (
    f"SELECT COUNT(*) FROM ceps_1min_features_60min t60 "
    f"WHERE {CEPS_1MIN_FEATURES_NOT_EXISTS}"
)
CEPS_1MIN_FEATURES_DELETE = (
    f"DELETE FROM ceps_1min_features_60min t60 "
    f"WHERE {CEPS_1MIN_FEATURES_NOT_EXISTS}"
)


def main():
    parser = argparse.ArgumentParser(
        description="Delete partial-hour rows from 60-min tables"
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Report counts without deleting')
    args = parser.parse_args()

    logger = setup_logging("cleanup_partial_60min_rows")
    print_banner(
        f"Cleanup partial 60-min rows{' (DRY RUN)' if args.dry_run else ''}"
    )

    plan: list[tuple[str, str, str]] = []
    for target, source, date_col, extra_keys in QUARTERLY_TABLES:
        count_sql, delete_sql = _build_queries(target, source, date_col, extra_keys)
        plan.append((target, count_sql, delete_sql))
    plan.append((
        "ceps_1min_features_60min",
        CEPS_1MIN_FEATURES_COUNT,
        CEPS_1MIN_FEATURES_DELETE,
    ))

    grand_total = 0
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for target, count_sql, delete_sql in plan:
                logger.info(f"  Scanning {target} ...")
                cur.execute(count_sql)
                (n_partial,) = cur.fetchone()
                grand_total += n_partial
                if n_partial == 0:
                    logger.info(f"  {target}: 0 partial rows — skip")
                    continue
                if args.dry_run:
                    logger.info(f"  {target}: {n_partial} partial rows (DRY RUN — not deleted)")
                else:
                    cur.execute(delete_sql)
                    conn.commit()
                    logger.info(f"  {target}: deleted {cur.rowcount} partial rows")

    logger.info("")
    if args.dry_run:
        logger.info(f"DRY RUN complete — {grand_total} partial rows would be deleted")
    else:
        logger.info(f"Cleanup complete — {grand_total} partial rows deleted")
        logger.info(
            "Re-run the backfill scripts over the full source range to "
            "repopulate complete hours that the cleanup did not touch."
        )


if __name__ == "__main__":
    main()
