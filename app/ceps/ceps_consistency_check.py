#!/usr/bin/env python3
"""
CEPS Data Consistency Check

Checks data completeness for all CEPS datasets:
1. System Imbalance (AktualniSystemovaOdchylkaCR)
2. RE Prices (AktualniCenaRE)
3. SVR Activation (AktivaceSVRvCR)
4. Export/Import SVR (ExportImportSVR)

Expected: All data from 2024-12-01 00:00 to current time (1440 records per day)
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta
import psycopg2

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT


def get_missing_dates(table_name: str, start_date: date, end_date: date, current_time: datetime, conn) -> list:
    """
    Get list of dates with missing or incomplete data.

    Args:
        table_name: Name of the 1min table
        start_date: Start date to check
        end_date: End date to check (today)
        current_time: Current datetime (for today's expected records)
        conn: Database connection

    Returns:
        List of (date, record_count, expected_count) tuples for dates with incomplete data
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

    # Filter based on expected counts
    missing_dates = []
    for check_date, record_count in all_dates:
        if check_date < end_date:
            # Past days: expect 1440 records
            expected = 1440
            if record_count < expected:
                missing_dates.append((check_date, record_count, expected))
        else:
            # Today: calculate expected based on current time
            minutes_elapsed = current_time.hour * 60 + current_time.minute + 1  # +1 for current minute
            expected = minutes_elapsed
            if record_count < expected:
                missing_dates.append((check_date, record_count, expected))

    return missing_dates


def get_summary_stats(table_name: str, start_date: date, end_date: date, conn) -> dict:
    """
    Get summary statistics for a table.

    Args:
        table_name: Name of the 1min table
        start_date: Start date to check
        end_date: End date to check
        conn: Database connection

    Returns:
        Dict with summary statistics
    """
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
        return {
            'days_with_data': 0,
            'total_records': 0,
            'first_record': None,
            'last_record': None
        }


def get_last_12h_stats(table_name: str, current_time: datetime, conn) -> dict:
    """
    Get statistics for the last 12 hours.

    Args:
        table_name: Name of the 1min table
        current_time: Current datetime
        conn: Database connection

    Returns:
        Dict with last 12h statistics
    """
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
        return {
            'total_records': 0,
            'first_record': None,
            'last_record': None
        }


def main():
    """Main entry point."""
    # Date range: 2024-12-01 to current time
    start_date = date(2024, 12, 1)
    current_time = datetime.now()
    end_date = current_time.date()

    # Calculate expected metrics
    num_days = (end_date - start_date).days + 1
    expected_records_per_day = 1440  # 1 minute resolution

    # For completed days + today's partial
    completed_days = num_days - 1
    minutes_today = current_time.hour * 60 + current_time.minute + 1
    expected_total_records = (completed_days * expected_records_per_day) + minutes_today

    print("=" * 80)
    print("CEPS DATA CONSISTENCY CHECK")
    print("=" * 80)
    print(f"Date Range: {start_date} to {current_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"Expected Days: {num_days} ({completed_days} complete + today)")
    print(f"Expected Records per Day: {expected_records_per_day:,}")
    print(f"Expected Total Records: {expected_total_records:,}")
    print()

    # Connect to database
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
        # Define datasets to check
        datasets = [
            {
                'name': 'System Imbalance',
                'table': 'ceps_actual_imbalance_1min',
                'tag': 'AktualniSystemovaOdchylkaCR'
            },
            {
                'name': 'RE Prices',
                'table': 'ceps_actual_re_price_1min',
                'tag': 'AktualniCenaRE'
            },
            {
                'name': 'SVR Activation',
                'table': 'ceps_svr_activation_1min',
                'tag': 'AktivaceSVRvCR'
            },
            {
                'name': 'Export/Import SVR',
                'table': 'ceps_export_import_svr_1min',
                'tag': 'ExportImportSVR'
            }
        ]

        # Check each dataset
        for dataset in datasets:
            print("=" * 80)
            print(f"DATASET: {dataset['name']} ({dataset['tag']})")
            print("=" * 80)
            print()

            # Get summary stats
            stats = get_summary_stats(dataset['table'], start_date, end_date, conn)

            print(f"Days with Data:    {stats['days_with_data']:,} / {num_days:,}")
            print(f"Total Records:     {stats['total_records']:,} / {expected_total_records:,}")

            if stats['first_record']:
                print(f"First Record:      {stats['first_record']}")
            else:
                print(f"First Record:      (no data)")

            if stats['last_record']:
                print(f"Last Record:       {stats['last_record']}")
            else:
                print(f"Last Record:       (no data)")

            # Calculate completeness
            completeness = (stats['total_records'] / expected_total_records * 100) if expected_total_records > 0 else 0
            print(f"Completeness:      {completeness:.2f}%")
            print()

            # Get missing dates
            missing = get_missing_dates(dataset['table'], start_date, end_date, current_time, conn)

            if missing:
                print(f"⚠ MISSING OR INCOMPLETE DATA: {len(missing)} days")
                print()
                print(f"{'Date':<12} | {'Records':<10} | {'Expected':<10} | {'Missing':<10}")
                print("-" * 50)

                for missing_date, record_count, expected_count in missing:
                    missing_count = expected_count - record_count
                    status = " (TODAY)" if missing_date == end_date else ""
                    print(f"{missing_date} | {record_count:>10,} | {expected_count:>10,} | {missing_count:>10,}{status}")

                print()

                # Group consecutive missing dates for easier reading
                consecutive_ranges = []
                current_range_start = None
                current_range_end = None

                for i, (missing_date, record_count, expected_count) in enumerate(missing):
                    # Only consider completely missing days (0 records)
                    if record_count == 0:
                        if current_range_start is None:
                            current_range_start = missing_date
                            current_range_end = missing_date
                        elif missing_date == current_range_end + timedelta(days=1):
                            current_range_end = missing_date
                        else:
                            consecutive_ranges.append((current_range_start, current_range_end))
                            current_range_start = missing_date
                            current_range_end = missing_date

                if current_range_start is not None:
                    consecutive_ranges.append((current_range_start, current_range_end))

                if consecutive_ranges:
                    print("MISSING DATE RANGES (0 records):")
                    for range_start, range_end in consecutive_ranges:
                        if range_start == range_end:
                            print(f"  • {range_start}")
                        else:
                            num_days_in_range = (range_end - range_start).days + 1
                            print(f"  • {range_start} to {range_end} ({num_days_in_range} days)")
                    print()

            else:
                print("✓ ALL DATA COMPLETE - No missing dates!")
                print()

        # ====================================================================
        # LAST 12 HOURS HEALTH CHECK
        # ====================================================================
        print("=" * 80)
        print("LAST 12 HOURS HEALTH CHECK (Most Critical)")
        print("=" * 80)
        twelve_hours_ago = current_time - timedelta(hours=12)
        expected_12h = 12 * 60  # 720 minutes
        print(f"Period: {twelve_hours_ago.strftime('%Y-%m-%d %H:%M')} to {current_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"Expected: {expected_12h} records per dataset")
        print()

        for dataset in datasets:
            stats_12h = get_last_12h_stats(dataset['table'], current_time, conn)
            completeness_12h = (stats_12h['total_records'] / expected_12h * 100) if expected_12h > 0 else 0

            # Determine status emoji
            if completeness_12h >= 99:
                status = "✅"
            elif completeness_12h >= 95:
                status = "⚠️"
            else:
                status = "❌"

            print(f"{dataset['name']}: {completeness_12h:.2f}% Complete {status}")
            print(f"  Records: {stats_12h['total_records']:,} / {expected_12h:,}")
            print(f"  Missing: {expected_12h - stats_12h['total_records']:,} records")

            if stats_12h['first_record']:
                print(f"  First: {stats_12h['first_record']}")
            if stats_12h['last_record']:
                print(f"  Last: {stats_12h['last_record']}")

            print()

        # ====================================================================
        # OVERALL SUMMARY
        # ====================================================================
        print("=" * 80)
        print("OVERALL SUMMARY (Since 2024-12-01)")
        print("=" * 80)
        print()

        for dataset in datasets:
            stats = get_summary_stats(dataset['table'], start_date, end_date, conn)
            completeness = (stats['total_records'] / expected_total_records * 100) if expected_total_records > 0 else 0

            # Determine status emoji
            if completeness >= 99:
                status = "✅"
            elif completeness >= 90:
                status = "⚠️"
            else:
                status = "❌"

            print(f"{dataset['name']}: {completeness:.2f}% Complete {status}")
            print(f"  Records: {stats['total_records']:,} / {expected_total_records:,}")

            # Get missing dates for summary
            missing = get_missing_dates(dataset['table'], start_date, end_date, current_time, conn)
            missing_full_days = [m for m in missing if m[1] == 0]  # Only completely missing days

            if missing:
                print(f"  Missing/Incomplete: {len(missing)} days")
                if missing_full_days:
                    print(f"  Completely Missing: {len(missing_full_days)} days")
            else:
                print(f"  Status: ALL DATA COMPLETE")

            if stats['last_record']:
                print(f"  Last Record: {stats['last_record']}")

            print()

    finally:
        conn.close()

    print("=" * 80)
    print("END OF CONSISTENCY CHECK")
    print("=" * 80)


if __name__ == "__main__":
    main()
