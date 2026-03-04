#!/usr/bin/env python3
"""
CEPS Data Consistency Check

Checks data completeness for all CEPS datasets:

1-Minute Tables (with 15-min aggregation):
1. System Imbalance (imbalance) - 1440 records/day
2. RE Prices (re_price) - 1440 records/day
3. SVR Activation (svr_activation) - 1440 records/day
4. Export/Import SVR (export_import_svr) - 1440 records/day
5. Generation RES (generation_res) - 1440 records/day

Native 15-Minute Tables:
6. Generation by Plant Type (generation) - 96 records/day
7. Generation Plan (generation_plan) - 96 records/day
8. Estimated Imbalance Price (estimated_imbalance_price) - 96 records/day

Expected: All data from 2024-12-01 00:00 to current time
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
import psycopg2

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT


def get_missing_dates_1min(table_name: str, start_date: date, end_date: date, current_time: datetime, conn) -> list:
    """
    Get list of dates with missing or incomplete data for 1-minute tables.
    Expected: 1440 records per day.
    """
    query = f"""
        WITH date_series AS (
            SELECT generate_series(
                %s::date,
                %s::date,
                '1 day'::interval
            )::date AS check_date
        ),
        daily_counts AS (
            SELECT
                DATE(delivery_timestamp) AS data_date,
                COUNT(*) AS record_count
            FROM finance.{table_name}
            WHERE DATE(delivery_timestamp) BETWEEN %s AND %s
            GROUP BY DATE(delivery_timestamp)
        )
        SELECT
            ds.check_date,
            COALESCE(dc.record_count, 0) AS record_count
        FROM date_series ds
        LEFT JOIN daily_counts dc ON ds.check_date = dc.data_date
        ORDER BY ds.check_date;
    """

    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date, start_date, end_date))
        all_dates = cur.fetchall()

    missing_dates = []
    for check_date, record_count in all_dates:
        if check_date < end_date:
            expected = 1440
            if record_count < expected:
                missing_dates.append((check_date, record_count, expected))
        else:
            minutes_elapsed = current_time.hour * 60 + current_time.minute + 1
            expected = minutes_elapsed
            if record_count < expected:
                missing_dates.append((check_date, record_count, expected))

    return missing_dates


def get_missing_dates_15min(table_name: str, start_date: date, end_date: date, current_time: datetime, conn) -> list:
    """
    Get list of dates with missing or incomplete data for native 15-minute tables.
    Expected: 96 records per day.
    """
    query = f"""
        WITH date_series AS (
            SELECT generate_series(
                %s::date,
                %s::date,
                '1 day'::interval
            )::date AS check_date
        ),
        daily_counts AS (
            SELECT
                trade_date AS data_date,
                COUNT(*) AS record_count
            FROM finance.{table_name}
            WHERE trade_date BETWEEN %s AND %s
            GROUP BY trade_date
        )
        SELECT
            ds.check_date,
            COALESCE(dc.record_count, 0) AS record_count
        FROM date_series ds
        LEFT JOIN daily_counts dc ON ds.check_date = dc.data_date
        ORDER BY ds.check_date;
    """

    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date, start_date, end_date))
        all_dates = cur.fetchall()

    missing_dates = []
    for check_date, record_count in all_dates:
        if check_date < end_date:
            expected = 96
            if record_count < expected:
                missing_dates.append((check_date, record_count, expected))
        else:
            # Today: calculate expected based on current time (15-min intervals)
            intervals_elapsed = (current_time.hour * 60 + current_time.minute) // 15 + 1
            expected = intervals_elapsed
            if record_count < expected:
                missing_dates.append((check_date, record_count, expected))

    return missing_dates


def get_summary_stats_1min(table_name: str, start_date: date, end_date: date, conn) -> dict:
    """Get summary statistics for a 1-minute table."""
    query = f"""
        SELECT
            COUNT(DISTINCT DATE(delivery_timestamp)) AS days_with_data,
            COUNT(*) AS total_records,
            MIN(delivery_timestamp) AS first_record,
            MAX(delivery_timestamp) AS last_record
        FROM finance.{table_name}
        WHERE DATE(delivery_timestamp) BETWEEN %s AND %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date))
        row = cur.fetchone()

        if row:
            return {
                'days_with_data': row[0] or 0,
                'total_records': row[1] or 0,
                'first_record': row[2],
                'last_record': row[3]
            }
        return {'days_with_data': 0, 'total_records': 0, 'first_record': None, 'last_record': None}


def get_summary_stats_15min(table_name: str, start_date: date, end_date: date, conn) -> dict:
    """Get summary statistics for a native 15-minute table."""
    query = f"""
        SELECT
            COUNT(DISTINCT trade_date) AS days_with_data,
            COUNT(*) AS total_records,
            MIN(trade_date || ' ' || time_interval) AS first_record,
            MAX(trade_date || ' ' || time_interval) AS last_record
        FROM finance.{table_name}
        WHERE trade_date BETWEEN %s AND %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date))
        row = cur.fetchone()

        if row:
            return {
                'days_with_data': row[0] or 0,
                'total_records': row[1] or 0,
                'first_record': row[2],
                'last_record': row[3]
            }
        return {'days_with_data': 0, 'total_records': 0, 'first_record': None, 'last_record': None}


def get_last_12h_stats_1min(table_name: str, current_time: datetime, conn) -> dict:
    """Get statistics for the last 12 hours for 1-minute tables."""
    twelve_hours_ago = current_time.replace(microsecond=0) - timedelta(hours=12)

    query = f"""
        SELECT
            COUNT(*) AS total_records,
            MIN(delivery_timestamp) AS first_record,
            MAX(delivery_timestamp) AS last_record
        FROM finance.{table_name}
        WHERE delivery_timestamp >= %s
          AND delivery_timestamp <= %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (twelve_hours_ago, current_time))
        row = cur.fetchone()

        if row:
            return {
                'total_records': row[0] or 0,
                'first_record': row[1],
                'last_record': row[2]
            }
        return {'total_records': 0, 'first_record': None, 'last_record': None}


def get_last_12h_stats_15min(table_name: str, current_time: datetime, conn) -> dict:
    """Get statistics for the last 12 hours for native 15-minute tables."""
    twelve_hours_ago = current_time.replace(microsecond=0) - timedelta(hours=12)
    today = current_time.date()
    yesterday = today - timedelta(days=1)

    query = f"""
        SELECT
            COUNT(*) AS total_records,
            MIN(trade_date || ' ' || time_interval) AS first_record,
            MAX(trade_date || ' ' || time_interval) AS last_record
        FROM finance.{table_name}
        WHERE trade_date >= %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (yesterday,))
        row = cur.fetchone()

        if row:
            return {
                'total_records': row[0] or 0,
                'first_record': row[1],
                'last_record': row[2]
            }
        return {'total_records': 0, 'first_record': None, 'last_record': None}


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='CEPS Data Consistency Check')
    parser.add_argument('--start', type=str, default='2024-12-01', help='Start date (YYYY-MM-DD), default: 2024-12-01')
    parser.add_argument('--end', type=str, default=None, help='End date (YYYY-MM-DD), default: today')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    current_time = datetime.now()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else current_time.date()

    num_days = (end_date - start_date).days + 1
    completed_days = num_days - 1
    minutes_today = current_time.hour * 60 + current_time.minute + 1
    intervals_today = (current_time.hour * 60 + current_time.minute) // 15 + 1

    # Expected totals
    expected_1min = (completed_days * 1440) + minutes_today
    expected_15min = (completed_days * 96) + intervals_today

    print("=" * 80)
    print("CEPS DATA CONSISTENCY CHECK")
    print("=" * 80)
    print(f"Date Range: {start_date} to {current_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"Expected Days: {num_days} ({completed_days} complete + today)")
    print(f"Expected 1-min Records: {expected_1min:,} (1440/day)")
    print(f"Expected 15-min Records: {expected_15min:,} (96/day)")
    print()

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        )
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Define all 8 datasets
        datasets = [
            # 1-minute tables
            {'name': 'System Imbalance', 'table': 'ceps_actual_imbalance_1min', 'type': '1min', 'key': 'imbalance'},
            {'name': 'RE Prices', 'table': 'ceps_actual_re_price_1min', 'type': '1min', 'key': 're_price'},
            {'name': 'SVR Activation', 'table': 'ceps_svr_activation_1min', 'type': '1min', 'key': 'svr_activation'},
            {'name': 'Export/Import SVR', 'table': 'ceps_export_import_svr_1min', 'type': '1min', 'key': 'export_import_svr'},
            {'name': 'Generation RES', 'table': 'ceps_generation_res_1min', 'type': '1min', 'key': 'generation_res'},
            # Native 15-minute tables
            {'name': 'Generation (by plant)', 'table': 'ceps_generation_15min', 'type': '15min', 'key': 'generation'},
            {'name': 'Generation Plan', 'table': 'ceps_generation_plan_15min', 'type': '15min', 'key': 'generation_plan'},
            {'name': 'Est. Imbalance Price', 'table': 'ceps_estimated_imbalance_price_15min', 'type': '15min', 'key': 'estimated_imbalance_price'},
        ]

        # Check each dataset
        for dataset in datasets:
            is_1min = dataset['type'] == '1min'
            expected_total = expected_1min if is_1min else expected_15min
            records_per_day = 1440 if is_1min else 96

            print("=" * 80)
            print(f"DATASET: {dataset['name']} ({dataset['key']}) [{dataset['type']}]")
            print("=" * 80)
            print()

            # Get summary stats
            if is_1min:
                stats = get_summary_stats_1min(dataset['table'], start_date, end_date, conn)
            else:
                stats = get_summary_stats_15min(dataset['table'], start_date, end_date, conn)

            print(f"Days with Data:    {stats['days_with_data']:,} / {num_days:,}")
            print(f"Total Records:     {stats['total_records']:,} / {expected_total:,}")
            print(f"Records per Day:   {records_per_day}")

            if stats['first_record']:
                print(f"First Record:      {stats['first_record']}")
            else:
                print(f"First Record:      (no data)")

            if stats['last_record']:
                print(f"Last Record:       {stats['last_record']}")
            else:
                print(f"Last Record:       (no data)")

            completeness = (stats['total_records'] / expected_total * 100) if expected_total > 0 else 0
            print(f"Completeness:      {completeness:.2f}%")
            print()

            # Get missing dates
            if is_1min:
                missing = get_missing_dates_1min(dataset['table'], start_date, end_date, current_time, conn)
            else:
                missing = get_missing_dates_15min(dataset['table'], start_date, end_date, current_time, conn)

            if missing:
                print(f"MISSING OR INCOMPLETE DATA: {len(missing)} days")
                print()
                print(f"{'Date':<12} | {'Records':<10} | {'Expected':<10} | {'Missing':<10}")
                print("-" * 50)

                # Show only first 10 and last 5 if too many
                if len(missing) > 15:
                    for missing_date, record_count, expected_count in missing[:10]:
                        missing_count = expected_count - record_count
                        status = " (TODAY)" if missing_date == end_date else ""
                        print(f"{missing_date} | {record_count:>10,} | {expected_count:>10,} | {missing_count:>10,}{status}")
                    print(f"... ({len(missing) - 15} more days) ...")
                    for missing_date, record_count, expected_count in missing[-5:]:
                        missing_count = expected_count - record_count
                        status = " (TODAY)" if missing_date == end_date else ""
                        print(f"{missing_date} | {record_count:>10,} | {expected_count:>10,} | {missing_count:>10,}{status}")
                else:
                    for missing_date, record_count, expected_count in missing:
                        missing_count = expected_count - record_count
                        status = " (TODAY)" if missing_date == end_date else ""
                        print(f"{missing_date} | {record_count:>10,} | {expected_count:>10,} | {missing_count:>10,}{status}")

                print()
            else:
                print("ALL DATA COMPLETE - No missing dates!")
                print()

        # ====================================================================
        # LAST 12 HOURS HEALTH CHECK
        # ====================================================================
        print("=" * 80)
        print("LAST 12 HOURS HEALTH CHECK")
        print("=" * 80)
        twelve_hours_ago = current_time - timedelta(hours=12)
        expected_12h_1min = 12 * 60  # 720 minutes
        expected_12h_15min = 12 * 4  # 48 intervals
        print(f"Period: {twelve_hours_ago.strftime('%Y-%m-%d %H:%M')} to {current_time.strftime('%Y-%m-%d %H:%M')}")
        print()

        for dataset in datasets:
            is_1min = dataset['type'] == '1min'
            expected_12h = expected_12h_1min if is_1min else expected_12h_15min

            if is_1min:
                stats_12h = get_last_12h_stats_1min(dataset['table'], current_time, conn)
            else:
                stats_12h = get_last_12h_stats_15min(dataset['table'], current_time, conn)

            completeness_12h = (stats_12h['total_records'] / expected_12h * 100) if expected_12h > 0 else 0

            if completeness_12h >= 99:
                status = "OK"
            elif completeness_12h >= 90:
                status = "WARN"
            else:
                status = "FAIL"

            print(f"{dataset['name']:<25} {completeness_12h:>6.1f}% ({stats_12h['total_records']:>4}/{expected_12h:>4}) [{status}]")

        print()

        # ====================================================================
        # OVERALL SUMMARY
        # ====================================================================
        print("=" * 80)
        print("OVERALL SUMMARY")
        print("=" * 80)
        print()

        for dataset in datasets:
            is_1min = dataset['type'] == '1min'
            expected_total = expected_1min if is_1min else expected_15min

            if is_1min:
                stats = get_summary_stats_1min(dataset['table'], start_date, end_date, conn)
                missing = get_missing_dates_1min(dataset['table'], start_date, end_date, current_time, conn)
            else:
                stats = get_summary_stats_15min(dataset['table'], start_date, end_date, conn)
                missing = get_missing_dates_15min(dataset['table'], start_date, end_date, current_time, conn)

            completeness = (stats['total_records'] / expected_total * 100) if expected_total > 0 else 0

            if completeness >= 99:
                status = "OK"
            elif completeness >= 90:
                status = "WARN"
            else:
                status = "FAIL"

            missing_full_days = len([m for m in missing if m[1] == 0])

            print(f"{dataset['name']:<25} {completeness:>6.1f}% [{status}]")
            if missing_full_days > 0:
                print(f"  -> {missing_full_days} days completely missing")

        print()
        print("=" * 80)
        print("Backfill command:")
        print("docker compose exec entsoe-ote-data-uploader python3 /app/scripts/ceps/ceps_soap_pipeline.py --dataset <KEY> --start YYYY-MM-DD --end YYYY-MM-DD")
        print("=" * 80)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
