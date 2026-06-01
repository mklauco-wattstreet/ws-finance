"""Backfill CEPS 60-min tables from 15-min sources.

Per docs/60min_tables_plan.md §4.4 — 9 of 10 tables. The 10th
(ceps_1min_features_60min) requires native re-aggregation from the
1-min source and is intentionally NOT in this script; it's the
subject of a separate PR alongside its live aggregator.

Aggregation rules (mapped from plan §4.4):
- _mean_ columns:      AVG()
- _median_ columns:    AVG() — see note below
- _last_at_interval_:  ARRAY_AGG(... ORDER BY time_interval DESC)[1]
- derived rolling/error cols (ceps_derived_features_60min): last

Note on "_median_" columns: in the 15-min source these are the
median *across the 1-min window of that 15-min row*. The plan
calls for `mean` across the four 15-min rows; aggregating
medians by AVG is a defensible proxy. Re-aggregating from the
1-min source would be cleaner but is out of scope here.

Usage:
    python3 -m backfill.backfill_ceps_60min YYYY-MM-DD YYYY-MM-DD [--debug] [--dry-run]
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


def _last(col: str) -> str:
    """SQL fragment that picks the value of the last 15-min row in the hour."""
    return f"(ARRAY_AGG({col} ORDER BY time_interval DESC))[1]"


def _on_conflict_update(*cols: str) -> str:
    """ON CONFLICT clause body that updates each named column from EXCLUDED."""
    lines = [f"    {c} = EXCLUDED.{c}" for c in cols] + ["    updated_at = CURRENT_TIMESTAMP"]
    return "ON CONFLICT (trade_date, time_interval) DO UPDATE SET\n" + ",\n".join(lines)


# -------------------- ceps_actual_imbalance_60min --------------------
ACTUAL_IMBALANCE_SQL = f"""
INSERT INTO ceps_actual_imbalance_60min (
    trade_date, time_interval, load_mean_mw, load_median_mw
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    AVG(load_mean_mw),
    AVG(load_median_mw)
FROM ceps_actual_imbalance_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update("load_mean_mw", "load_median_mw")}
"""


# -------------------- ceps_estimated_imbalance_price_60min --------------------
ESTIMATED_PRICE_SQL = f"""
INSERT INTO ceps_estimated_imbalance_price_60min (
    trade_date, time_interval, estimated_price_czk_mwh
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    AVG(estimated_price_czk_mwh)
FROM ceps_estimated_imbalance_price_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update("estimated_price_czk_mwh")}
"""


# -------------------- ceps_actual_re_price_60min --------------------
_RE_PRICE_GROUPS = [
    ("price_afrr_plus_mean_eur_mwh",   "price_afrr_plus_median_eur_mwh",   "price_afrr_plus_last_at_interval_eur_mwh"),
    ("price_afrr_minus_mean_eur_mwh",  "price_afrr_minus_median_eur_mwh",  "price_afrr_minus_last_at_interval_eur_mwh"),
    ("price_mfrr_plus_mean_eur_mwh",   "price_mfrr_plus_median_eur_mwh",   "price_mfrr_plus_last_at_interval_eur_mwh"),
    ("price_mfrr_minus_mean_eur_mwh",  "price_mfrr_minus_median_eur_mwh",  "price_mfrr_minus_last_at_interval_eur_mwh"),
    ("price_mfrr_5_mean_eur_mwh",      "price_mfrr_5_median_eur_mwh",      "price_mfrr_5_last_at_interval_eur_mwh"),
]
_RE_PRICE_COLS = [c for grp in _RE_PRICE_GROUPS for c in grp]
_RE_PRICE_SELECT = ",\n    ".join(
    f"AVG({mean}),\n    AVG({median}),\n    {_last(last)}"
    for mean, median, last in _RE_PRICE_GROUPS
)
RE_PRICE_SQL = f"""
INSERT INTO ceps_actual_re_price_60min (
    trade_date, time_interval,
    {", ".join(_RE_PRICE_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    {_RE_PRICE_SELECT}
FROM ceps_actual_re_price_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update(*_RE_PRICE_COLS)}
"""


# -------------------- ceps_svr_activation_60min --------------------
_SVR_GROUPS = [
    ("afrr_plus_mean_mw",  "afrr_plus_median_mw",  "afrr_plus_last_at_interval_mw"),
    ("afrr_minus_mean_mw", "afrr_minus_median_mw", "afrr_minus_last_at_interval_mw"),
    ("mfrr_plus_mean_mw",  "mfrr_plus_median_mw",  "mfrr_plus_last_at_interval_mw"),
    ("mfrr_minus_mean_mw", "mfrr_minus_median_mw", "mfrr_minus_last_at_interval_mw"),
]
_SVR_COLS = [c for grp in _SVR_GROUPS for c in grp]
_SVR_SELECT = ",\n    ".join(
    f"AVG({mean}),\n    AVG({median}),\n    {_last(last)}"
    for mean, median, last in _SVR_GROUPS
)
SVR_ACTIVATION_SQL = f"""
INSERT INTO ceps_svr_activation_60min (
    trade_date, time_interval,
    {", ".join(_SVR_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    {_SVR_SELECT}
FROM ceps_svr_activation_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update(*_SVR_COLS)}
"""


# -------------------- ceps_export_import_svr_60min --------------------
_EX_GROUPS = [
    ("imbalance_netting_mean_mw", "imbalance_netting_median_mw", "imbalance_netting_last_at_interval_mw"),
    ("mari_mfrr_mean_mw",         "mari_mfrr_median_mw",         "mari_mfrr_last_at_interval_mw"),
    ("picasso_afrr_mean_mw",      "picasso_afrr_median_mw",      "picasso_afrr_last_at_interval_mw"),
    ("sum_exchange_mean_mw",      "sum_exchange_median_mw",      "sum_exchange_last_at_interval_mw"),
]
_EX_COLS = [c for grp in _EX_GROUPS for c in grp]
_EX_SELECT = ",\n    ".join(
    f"AVG({mean}),\n    AVG({median}),\n    {_last(last)}"
    for mean, median, last in _EX_GROUPS
)
EXPORT_IMPORT_SVR_SQL = f"""
INSERT INTO ceps_export_import_svr_60min (
    trade_date, time_interval,
    {", ".join(_EX_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    {_EX_SELECT}
FROM ceps_export_import_svr_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update(*_EX_COLS)}
"""


# -------------------- ceps_generation_60min --------------------
_GEN_COLS = ["tpp_mw", "ccgt_mw", "npp_mw", "hpp_mw", "pspp_mw",
             "altpp_mw", "appp_mw", "wpp_mw", "pvpp_mw"]
GENERATION_SQL = f"""
INSERT INTO ceps_generation_60min (
    trade_date, time_interval,
    {", ".join(_GEN_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    {", ".join(f"AVG({c})" for c in _GEN_COLS)}
FROM ceps_generation_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update(*_GEN_COLS)}
"""


# -------------------- ceps_generation_plan_60min --------------------
GENERATION_PLAN_SQL = f"""
INSERT INTO ceps_generation_plan_60min (trade_date, time_interval, total_mw)
SELECT trade_date, {HOUR_INTERVAL_SQL}, AVG(total_mw)
FROM ceps_generation_plan_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update("total_mw")}
"""


# -------------------- ceps_generation_res_60min --------------------
_RES_GROUPS = [
    ("wind_mean_mw",  "wind_median_mw",  "wind_last_at_interval_mw"),
    ("solar_mean_mw", "solar_median_mw", "solar_last_at_interval_mw"),
]
_RES_COLS = [c for grp in _RES_GROUPS for c in grp]
_RES_SELECT = ",\n    ".join(
    f"AVG({mean}),\n    AVG({median}),\n    {_last(last)}"
    for mean, median, last in _RES_GROUPS
)
GENERATION_RES_SQL = f"""
INSERT INTO ceps_generation_res_60min (
    trade_date, time_interval,
    {", ".join(_RES_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    {_RES_SELECT}
FROM ceps_generation_res_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update(*_RES_COLS)}
"""


# -------------------- ceps_derived_features_60min --------------------
_DERIVED_COLS = ["imb_roll_2h", "imb_roll_4h", "imb_integral_4h",
                 "solar_error_mw", "wind_error_mw", "gen_total_error_mw"]
DERIVED_FEATURES_SQL = f"""
INSERT INTO ceps_derived_features_60min (
    trade_date, time_interval,
    {", ".join(_DERIVED_COLS)}
)
SELECT
    trade_date,
    {HOUR_INTERVAL_SQL},
    {", ".join(_last(c) for c in _DERIVED_COLS)}
FROM ceps_derived_features_15min
WHERE trade_date = %s
GROUP BY trade_date, {HOUR_GROUP_SQL}
{_on_conflict_update(*_DERIVED_COLS)}
"""


def main():
    args = parse_args("CEPS")
    logger = setup_logging("backfill_ceps_60min", args.debug)
    run_backfill(
        label="CEPS",
        queries=[
            ("ceps_actual_imbalance_60min",          ACTUAL_IMBALANCE_SQL),
            ("ceps_estimated_imbalance_price_60min", ESTIMATED_PRICE_SQL),
            ("ceps_actual_re_price_60min",           RE_PRICE_SQL),
            ("ceps_svr_activation_60min",            SVR_ACTIVATION_SQL),
            ("ceps_export_import_svr_60min",         EXPORT_IMPORT_SVR_SQL),
            ("ceps_generation_60min",                GENERATION_SQL),
            ("ceps_generation_plan_60min",           GENERATION_PLAN_SQL),
            ("ceps_generation_res_60min",            GENERATION_RES_SQL),
            ("ceps_derived_features_60min",          DERIVED_FEATURES_SQL),
        ],
        args=args,
        logger=logger,
    )


if __name__ == "__main__":
    main()
