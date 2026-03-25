#!/usr/bin/env python3
"""
Upload intraday market price reports from Excel files to PostgreSQL database.

Usage:
    python3 upload_intraday_prices.py PATH_TO_DIRECTORY

Example:
    python3 upload_intraday_prices.py 2025/10
    python3 upload_intraday_prices.py 2025/09
"""

import sys
import os
import re
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2 import extras
import pandas as pd

# Import database configuration
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA


def parse_date_from_filename(filename):
    """
    Extract trade date from filename.
    Expected format: IM_15MIN_DD_MM_YYYY_EN.xlsx

    Args:
        filename: Name of the Excel file

    Returns:
        datetime.date object or None if parsing fails
    """
    pattern = r'IM_15MIN_(\d{2})_(\d{2})_(\d{4})_EN\.xlsx'
    match = re.match(pattern, filename)

    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day)).date()
        except ValueError:
            return None
    return None


def clean_numeric_value(value):
    """
    Clean numeric values from Excel (remove commas, handle spaces).

    Args:
        value: Raw value from Excel

    Returns:
        float or None
    """
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    # Convert to string and clean
    str_value = str(value).strip()
    # Remove thousands separators (both comma and space)
    str_value = str_value.replace(',', '').replace(' ', '')

    try:
        return float(str_value)
    except ValueError:
        return None


def read_intraday_file(file_path, trade_date):
    """
    Read intraday market data from Excel file.

    Args:
        file_path: Path to the Excel file
        trade_date: Trade date extracted from filename

    Returns:
        list of dictionaries containing the data
    """
    # Read Excel file, skipping first 5 rows (header is on row 6, index 5)
    df = pd.read_excel(file_path, skiprows=5)

    records = []

    for _, row in df.iterrows():
        # Skip empty rows
        if pd.isna(row.get('Period')):
            continue

        # Skip rows where Period is not numeric (e.g., footer text rows)
        try:
            period = int(row['Period'])
        except (ValueError, TypeError):
            # This row contains text (like "The results contain only traded hourly contracts.")
            continue

        record = {
            'trade_date': trade_date,
            'period': period,
            'time_interval': str(row['Time interval']).strip(),
        }

        # Process numeric columns with exact column names from Excel
        numeric_columns = [
            ('Traded volume\n(MWh)', 'traded_volume_mwh'),
            ('Traded volume - purchase\n(MWh)', 'traded_volume_purchased_mwh'),
            ('Traded volume - sold\n(MWh)', 'traded_volume_sold_mwh'),
            ('Average price\n(EUR/MWh)', 'weighted_avg_price_eur_mwh'),
            ('Minimal price\n(EUR/MWh)', 'min_price_eur_mwh'),
            ('Maximal price\n(EUR/MWh)', 'max_price_eur_mwh'),
            ('Last price (EUR/MWh)', 'last_price_eur_mwh'),
        ]

        for excel_col, db_col in numeric_columns:
            record[db_col] = clean_numeric_value(row.get(excel_col))

        records.append(record)

    return records


def upload_to_database(records, conn, trade_date):
    """
    Upload records to the database using bulk upsert (INSERT ... ON CONFLICT DO UPDATE).
    This allows re-uploading today's data as it gets updated throughout the day.

    Args:
        records: List of dictionaries containing the data (96 rows expected)
        conn: Database connection
        trade_date: Trade date for logging

    Returns:
        int: Number of records upserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    # Use UPSERT to handle updates to intraday data (which changes throughout the day)
    upsert_query = """
        INSERT INTO ote_prices_intraday_market (
            trade_date, period, time_interval,
            traded_volume_mwh, traded_volume_purchased_mwh,
            traded_volume_sold_mwh, weighted_avg_price_eur_mwh,
            min_price_eur_mwh, max_price_eur_mwh,
            last_price_eur_mwh
        ) VALUES %s
        ON CONFLICT (trade_date, period) DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            traded_volume_mwh = EXCLUDED.traded_volume_mwh,
            traded_volume_purchased_mwh = EXCLUDED.traded_volume_purchased_mwh,
            traded_volume_sold_mwh = EXCLUDED.traded_volume_sold_mwh,
            weighted_avg_price_eur_mwh = EXCLUDED.weighted_avg_price_eur_mwh,
            min_price_eur_mwh = EXCLUDED.min_price_eur_mwh,
            max_price_eur_mwh = EXCLUDED.max_price_eur_mwh,
            last_price_eur_mwh = EXCLUDED.last_price_eur_mwh
    """

    # Prepare data as tuples for bulk upsert
    values = []
    for record in records:
        values.append((
            record['trade_date'],
            record['period'],
            record['time_interval'],
            record['traded_volume_mwh'],
            record['traded_volume_purchased_mwh'],
            record['traded_volume_sold_mwh'],
            record['weighted_avg_price_eur_mwh'],
            record['min_price_eur_mwh'],
            record['max_price_eur_mwh'],
            record['last_price_eur_mwh'],
        ))

    try:
        # Use execute_values for efficient bulk upsert
        extras.execute_values(cursor, upsert_query, values)
        conn.commit()
        upserted = len(values)
        cursor.close()
        return upserted

    except Exception as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"Database error: {e}")


def process_directory(directory_path):
    """
    Process Excel files in a directory or a single file.

    Args:
        directory_path: Path to directory containing Excel files, or path to a single .xlsx file
    """
    dir_path = Path(directory_path)

    if not dir_path.exists():
        print(f"Error: '{directory_path}' does not exist")
        return False

    # Single file mode
    if dir_path.is_file() and dir_path.name.startswith("IM_15MIN_") and dir_path.suffix == ".xlsx":
        excel_files = [dir_path]
    elif dir_path.is_dir():
        excel_files = list(dir_path.glob("IM_15MIN_*.xlsx"))
        if not excel_files:
            print(f"No intraday market Excel files in '{directory_path}'")
            return False
    else:
        print(f"Error: '{directory_path}' is not a directory or valid IM_15MIN file")
        return False

    # Connect to database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            connect_timeout=10
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA}")
        conn.autocommit = False
    except Exception as e:
        print(f"DB connection failed: {e}")
        return False

    total_upserted = 0
    files_processed = 0
    files_failed = 0

    try:
        for excel_file in sorted(excel_files):
            trade_date = parse_date_from_filename(excel_file.name)

            if not trade_date:
                print(f"Bad filename: {excel_file.name}")
                files_failed += 1
                continue

            try:
                records = read_intraday_file(excel_file, trade_date)

                if not records:
                    print(f"No records: {excel_file.name}")
                    files_failed += 1
                    continue

                upserted = upload_to_database(records, conn, trade_date)
                total_upserted += upserted
                files_processed += 1

            except Exception as e:
                print(f"Error {excel_file.name}: {e}")
                files_failed += 1
                continue

        # Compact summary
        summary = f"OTE Intraday upload: {files_processed} files, {total_upserted} rows upserted"
        if files_failed > 0:
            summary += f" ({files_failed} failed)"
        print(summary)

        return True

    finally:
        conn.close()


def main():
    """Main function."""
    if len(sys.argv) != 2:
        print("Usage: python3 upload_intraday_prices.py PATH_TO_DIRECTORY")
        print("\nExamples:")
        print("  python3 upload_intraday_prices.py 2025/10")
        print("  python3 upload_intraday_prices.py 2025/09")
        sys.exit(1)

    directory_path = sys.argv[1]

    try:
        success = process_directory(directory_path)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
