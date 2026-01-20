#!/usr/bin/env python3
"""
ENTSO-E Data Consistency Check

Checks data completeness for all ENTSO-E datasets:
1. Generation Actual (A75)
2. Generation Forecast (A69)
3. Generation Scheduled (A71)
4. Load (A65)
5. Cross-Border Flows (A11)
6. Scheduled Cross-Border Flows (A09)
7. Balancing Energy (A84)
8. Imbalance Prices (A85/A86)

Expected: 96 periods per day per area (15-min resolution)
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta
import psycopg2

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT

# ENTSO-E datasets configuration
DATASETS = [
    {
        'name': 'Generation Actual',
        'table': 'entsoe_generation_actual',
        'doc_type': 'A75'
    },
    {
        'name': 'Generation Forecast',
        'table': 'entsoe_generation_forecast',
        'doc_type': 'A69'
    },
    {
        'name': 'Generation Scheduled',
        'table': 'entsoe_generation_scheduled',
        'doc_type': 'A71'
    },
    {
        'name': 'Load',
        'table': 'entsoe_load',
        'doc_type': 'A65'
    },
    {
        'name': 'Cross-Border Flows',
        'table': 'entsoe_cross_border_flows',
        'doc_type': 'A11'
    },
    {
        'name': 'Scheduled Flows',
        'table': 'entsoe_scheduled_cross_border_flows',
        'doc_type': 'A09'
    },
    {
        'name': 'Balancing Energy',
        'table': 'entsoe_balancing_energy',
        'doc_type': 'A84'
    },
    {
        'name': 'Imbalance Prices',
        'table': 'entsoe_imbalance_prices',
        'doc_type': 'A85/A86'
    }
]

COUNTRIES = ['CZ', 'DE', 'AT', 'PL', 'SK']
PERIODS_PER_DAY = 96


def get_summary_stats(table_name: str, country_code: str, start_date: date, end_date: date, conn) -> dict:
    """Get summary statistics for a table and country."""
    query = f"""
        SELECT
            COUNT(DISTINCT trade_date) AS days_with_data,
            COUNT(*) AS total_records,
            MIN(trade_date) AS first_date,
            MAX(trade_date) AS last_date
        FROM finance.{table_name}
        WHERE country_code = %s
          AND trade_date BETWEEN %s AND %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (country_code, start_date, end_date))
        row = cur.fetchone()
        if row:
            return {
                'days_with_data': row[0] or 0,
                'total_records': row[1] or 0,
                'first_date': row[2],
                'last_date': row[3]
            }
    return {'days_with_data': 0, 'total_records': 0, 'first_date': None, 'last_date': None}


def get_missing_dates(table_name: str, country_code: str, start_date: date, end_date: date, conn) -> list:
    """Get dates with missing or incomplete data."""
    query = f"""
        WITH date_series AS (
            SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date AS check_date
        ),
        daily_counts AS (
            SELECT trade_date, COUNT(DISTINCT period) AS period_count
            FROM finance.{table_name}
            WHERE country_code = %s AND trade_date BETWEEN %s AND %s
            GROUP BY trade_date
        )
        SELECT ds.check_date, COALESCE(dc.period_count, 0) AS period_count
        FROM date_series ds
        LEFT JOIN daily_counts dc ON ds.check_date = dc.trade_date
        WHERE COALESCE(dc.period_count, 0) < {PERIODS_PER_DAY}
        ORDER BY ds.check_date;
    """
    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date, country_code, start_date, end_date))
        return cur.fetchall()


def get_last_24h_stats(table_name: str, country_code: str, current_time: datetime, conn) -> dict:
    """Get statistics for the last 24 hours."""
    yesterday = (current_time - timedelta(days=1)).date()
    today = current_time.date()

    query = f"""
        SELECT COUNT(*) AS total_records, MAX(trade_date) AS last_date
        FROM finance.{table_name}
        WHERE country_code = %s AND trade_date >= %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (country_code, yesterday))
        row = cur.fetchone()
        if row:
            return {'total_records': row[0] or 0, 'last_date': row[1]}
    return {'total_records': 0, 'last_date': None}


def print_dataset_summary(dataset: dict, start_date: date, end_date: date, num_days: int, conn):
    """Print summary for a single dataset across all countries."""
    print("=" * 80)
    print(f"DATASET: {dataset['name']} ({dataset['doc_type']})")
    print(f"Table: finance.{dataset['table']}")
    print("=" * 80)
    print()

    print(f"{'Country':<10} | {'Days':<12} | {'Records':<15} | {'Complete':<10} | {'Status':<8}")
    print("-" * 70)

    for country in COUNTRIES:
        stats = get_summary_stats(dataset['table'], country, start_date, end_date, conn)
        expected_records = num_days * PERIODS_PER_DAY
        completeness = (stats['total_records'] / expected_records * 100) if expected_records > 0 else 0

        if completeness >= 99:
            status = "OK"
        elif completeness >= 90:
            status = "WARN"
        elif completeness > 0:
            status = "GAPS"
        else:
            status = "EMPTY"

        print(f"{country:<10} | {stats['days_with_data']:>5}/{num_days:<5} | {stats['total_records']:>7,}/{expected_records:<7,} | {completeness:>7.1f}% | {status:<8}")

    print()


def print_missing_dates_detail(dataset: dict, start_date: date, end_date: date, conn):
    """Print detailed missing dates for a dataset."""
    for country in COUNTRIES:
        missing = get_missing_dates(dataset['table'], country, start_date, end_date, conn)
        if missing:
            completely_missing = [m for m in missing if m[1] == 0]
            if completely_missing:
                print(f"  {country}: {len(completely_missing)} days completely missing")
                # Show first 5 missing dates
                for check_date, _ in completely_missing[:5]:
                    print(f"    - {check_date}")
                if len(completely_missing) > 5:
                    print(f"    ... and {len(completely_missing) - 5} more")


def main():
    """Main entry point."""
    start_date = date(2024, 12, 1)
    current_time = datetime.now()
    end_date = current_time.date()
    num_days = (end_date - start_date).days + 1

    print("=" * 80)
    print("ENTSO-E DATA CONSISTENCY CHECK")
    print("=" * 80)
    print(f"Date Range: {start_date} to {end_date}")
    print(f"Total Days: {num_days}")
    print(f"Expected Periods per Day: {PERIODS_PER_DAY}")
    print(f"Countries: {', '.join(COUNTRIES)}")
    print()

    try:
        conn = psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
            dbname=DB_NAME, port=DB_PORT
        )
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Summary for each dataset
        for dataset in DATASETS:
            print_dataset_summary(dataset, start_date, end_date, num_days, conn)

        # Last 24h health check
        print("=" * 80)
        print("LAST 24 HOURS HEALTH CHECK")
        print("=" * 80)
        expected_24h = PERIODS_PER_DAY * 2  # Yesterday + today partial
        print()

        for dataset in DATASETS:
            print(f"{dataset['name']} ({dataset['doc_type']}):")
            for country in COUNTRIES:
                stats = get_last_24h_stats(dataset['table'], country, current_time, conn)
                pct = (stats['total_records'] / expected_24h * 100) if expected_24h > 0 else 0
                status = "OK" if pct >= 90 else ("WARN" if pct >= 50 else "LOW")
                print(f"  {country}: {stats['total_records']:>4} records ({pct:>5.1f}%) [{status}]")
            print()

        # Missing dates detail
        print("=" * 80)
        print("MISSING DATA DETAILS (Completely Missing Days)")
        print("=" * 80)
        print()

        for dataset in DATASETS:
            print(f"{dataset['name']}:")
            print_missing_dates_detail(dataset, start_date, end_date, conn)
            print()

    finally:
        conn.close()

    print("=" * 80)
    print("END OF CONSISTENCY CHECK")
    print("=" * 80)


if __name__ == "__main__":
    main()
