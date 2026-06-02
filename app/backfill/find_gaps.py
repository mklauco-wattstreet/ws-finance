"""Scan 15-min source tables for coverage gaps over a date range.

Reports per-table coverage:
- Fully missing days (no rows at all)
- Partial days (some hours present, others missing, or some quarters missing within hours)

Partitioned tables (entsoe by country_code, ote_prices_ida by ida_idx,
entsoe_cross_border_flows by area_id) are reported per partition.

Usage:
    python3 -m backfill.find_gaps YYYY-MM-DD YYYY-MM-DD [--table NAME] [--verbose]
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill._common import get_db_connection


# (table_name, date_column, partition_column_or_None)
TABLES = [
    ('da_period_summary',                    'delivery_date', None),
    ('da_curve_depth',                       'delivery_date', None),
    ('ote_prices_imbalance',                 'trade_date',    None),
    ('ote_prices_ida',                       'trade_date',    'ida_idx'),
    ('weather_current',                      'trade_date',    None),
    ('weather_forecast',                     'trade_date',    None),
    ('ceps_actual_imbalance_15min',          'trade_date',    None),
    ('ceps_estimated_imbalance_price_15min', 'trade_date',    None),
    ('ceps_actual_re_price_15min',           'trade_date',    None),
    ('ceps_svr_activation_15min',            'trade_date',    None),
    ('ceps_export_import_svr_15min',         'trade_date',    None),
    ('ceps_generation_15min',                'trade_date',    None),
    ('ceps_generation_plan_15min',           'trade_date',    None),
    ('ceps_generation_res_15min',            'trade_date',    None),
    ('ceps_1min_features_15min',             'trade_date',    None),
    ('ceps_derived_features_15min',          'trade_date',    None),
    ('entsoe_load',                          'trade_date',    None),
    ('entsoe_generation_forecast',           'trade_date',    None),
    ('entsoe_generation_actual',             'trade_date',    'country_code'),
    ('entsoe_cross_border_flows',            'trade_date',    'area_id'),
    ('entsoe_scheduled_cross_border_flows',  'trade_date',    None),
    ('entsoe_day_ahead_prices',              'trade_date',    'country_code'),
    ('entsoe_imbalance_prices',              'trade_date',    'country_code'),
]


def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def scan_table(conn, table, date_col, partition_col, start_date, end_date):
    """Return dict: {partition_value: {date: {hour: quarter_count}}}.

    When the table has no partition column, all rows are bucketed under
    the synthetic partition key '(all)'.
    """
    if partition_col:
        sql = (
            f"SELECT {date_col}, {partition_col}, "
            f"       SUBSTRING(time_interval, 1, 2) AS hh, "
            f"       COUNT(*) "
            f"FROM finance.{table} "
            f"WHERE {date_col} BETWEEN %s AND %s "
            f"GROUP BY {date_col}, {partition_col}, SUBSTRING(time_interval, 1, 2)"
        )
    else:
        sql = (
            f"SELECT {date_col}, "
            f"       SUBSTRING(time_interval, 1, 2) AS hh, "
            f"       COUNT(*) "
            f"FROM finance.{table} "
            f"WHERE {date_col} BETWEEN %s AND %s "
            f"GROUP BY {date_col}, SUBSTRING(time_interval, 1, 2)"
        )

    with conn.cursor() as cur:
        cur.execute(sql, (start_date, end_date))
        rows = cur.fetchall()

    coverage = defaultdict(lambda: defaultdict(dict))
    if partition_col:
        for d, part, hh, n in rows:
            coverage[part][d][hh] = n
    else:
        for d, hh, n in rows:
            coverage["(all)"][d][hh] = n
    return dict(coverage)


def _format_date_list(dates, verbose):
    """Compact representation of a date list — full if verbose, preview otherwise."""
    if verbose or len(dates) <= 6:
        return ", ".join(str(d) for d in dates)
    head = ", ".join(str(d) for d in dates[:3])
    tail = ", ".join(str(d) for d in dates[-3:])
    return f"{head}, … ({len(dates) - 6} more) …, {tail}"


def analyze_table(table, partition_col, start_date, end_date, coverage, verbose):
    """Print findings for one table. Return True if any gaps."""
    total_days = (end_date - start_date).days + 1

    if not coverage:
        print(f"  {table}: NO ROWS at all in range ({total_days} days) ❌")
        return True

    any_gaps = False
    for part in sorted(coverage.keys()):
        day_map = coverage[part]
        fully_covered = 0
        missing = []
        partial = []
        for d in daterange(start_date, end_date):
            hours = day_map.get(d, {})
            full_hours = sum(1 for n in hours.values() if n >= 4)
            if not hours:
                missing.append(d)
            elif len(hours) < 24 or full_hours < 24:
                partial.append((d, len(hours), full_hours))
            else:
                fully_covered += 1

        label = f"{table} [{partition_col}={part}]" if partition_col else table
        if fully_covered == total_days:
            print(f"  {label}: {fully_covered}/{total_days} ✓")
            continue

        any_gaps = True
        print(f"  {label}: {fully_covered}/{total_days} ⚠")
        if missing:
            print(f"    Fully missing ({len(missing)} day(s)): {_format_date_list(missing, verbose)}")
        if partial:
            print(f"    Partial coverage ({len(partial)} day(s)):")
            shown = partial if verbose else partial[:5]
            for d, hour_count, full_hour_count in shown:
                print(f"      {d}: {hour_count}/24 hours present, {full_hour_count}/24 fully covered")
            if not verbose and len(partial) > 5:
                print(f"      … ({len(partial) - 5} more — use --verbose to see all)")

    return any_gaps


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('start', help='Start date YYYY-MM-DD (inclusive)')
    parser.add_argument('end',   help='End date YYYY-MM-DD (inclusive)')
    parser.add_argument('--table', help='Scan only this table (omit to scan all)')
    parser.add_argument('--verbose', '-v', action='store_true', help='List every gap date')
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
    except ValueError as e:
        parser.error(f"Invalid date format: {e}")
    if end_date < start_date:
        parser.error("end must be >= start")

    tables_to_scan = [t for t in TABLES if not args.table or t[0] == args.table]
    if not tables_to_scan:
        valid = ", ".join(t[0] for t in TABLES)
        parser.error(f"Unknown table {args.table!r}. Known: {valid}")

    total_days = (end_date - start_date).days + 1
    print(f"Coverage scan: {start_date} to {end_date} ({total_days} days)")
    print(f"Scanning {len(tables_to_scan)} table(s)")
    print('=' * 70)

    any_gaps_overall = False
    with get_db_connection() as conn:
        for table, date_col, partition_col in tables_to_scan:
            coverage = scan_table(conn, table, date_col, partition_col,
                                  start_date, end_date)
            had_gaps = analyze_table(table, partition_col, start_date, end_date,
                                     coverage, args.verbose)
            any_gaps_overall = any_gaps_overall or had_gaps

    print('=' * 70)
    if any_gaps_overall:
        print("Gaps found. Suggested next steps:")
        print("  - CEPS gaps:    re-run ceps.ceps_soap_pipeline --dataset all --start <D1> --end <D2>")
        print("  - ENTSO-E gaps: re-run the corresponding runners.entsoe_*_runner --start <D1> --end <D2>")
        print("  - Then propagate to 60-min: python3 -m backfill.backfill_<source> <D1> <D2>")
        sys.exit(1)
    else:
        print("✓ No gaps found across all scanned tables.")
        sys.exit(0)


if __name__ == '__main__':
    main()
