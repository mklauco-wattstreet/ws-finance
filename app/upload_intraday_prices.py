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
    print(f"  Reading file: {file_path.name}")

    # Read Excel file, skipping first 5 rows (header is on row 6, index 5)
    # Data starts on row 7 (index 6)
    df = pd.read_excel(file_path, skiprows=5)

    print(f"  Found {len(df)} rows of data")

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
    Upload records to the database using bulk insert.

    Args:
        records: List of dictionaries containing the data (96 rows expected)
        conn: Database connection
        trade_date: Trade date for logging

    Returns:
        int: Number of records inserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO ote_prices_intraday_market (
            trade_date, period, time_interval,
            traded_volume_mwh, traded_volume_purchased_mwh,
            traded_volume_sold_mwh, weighted_avg_price_eur_mwh,
            min_price_eur_mwh, max_price_eur_mwh,
            last_price_eur_mwh
        ) VALUES %s
    """

    # Prepare data as tuples for bulk insert
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
        # Use execute_values for efficient bulk insert
        extras.execute_values(cursor, insert_query, values)
        conn.commit()
        inserted = len(values)
        cursor.close()
        return inserted

    except psycopg2.IntegrityError as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"Conflict detected - data for {trade_date} already exists in database: {e}")
    except Exception as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"Database error: {e}")


def process_directory(directory_path):
    """
    Process all Excel files in a directory.

    Args:
        directory_path: Path to directory containing Excel files
    """
    dir_path = Path(directory_path)

    if not dir_path.exists():
        print(f"Error: Directory '{directory_path}' does not exist")
        return False

    if not dir_path.is_dir():
        print(f"Error: '{directory_path}' is not a directory")
        return False

    # Find all Excel files
    excel_files = list(dir_path.glob("IM_15MIN_*.xlsx"))

    if not excel_files:
        print(f"No intraday market Excel files found in '{directory_path}'")
        return False

    print(f"╔══════════════════════════════════════════════════════════╗")
    print(f"║  Intraday Market Price Data Uploader                     ║")
    print(f"╚══════════════════════════════════════════════════════════╝")
    print(f"\nDirectory: {dir_path.absolute()}")
    print(f"Found {len(excel_files)} Excel file(s)\n")

    # Connect to database
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            connect_timeout=10,
            options=f'-c search_path={DB_SCHEMA}'
        )
        print("✓ Database connection established\n")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

    total_inserted = 0
    files_processed = 0
    files_failed = 0

    try:
        for excel_file in sorted(excel_files):
            print(f"{'─' * 60}")
            print(f"Processing: {excel_file.name}")

            # Extract trade date from filename
            trade_date = parse_date_from_filename(excel_file.name)

            if not trade_date:
                print(f"  ✗ Failed to extract date from filename")
                files_failed += 1
                continue

            print(f"  Trade date: {trade_date}")

            try:
                # Read data from Excel
                records = read_intraday_file(excel_file, trade_date)

                if not records:
                    print(f"  ⚠️  No valid records found")
                    files_failed += 1
                    continue

                print(f"  Expected rows: 96, Found: {len(records)}")

                # Upload to database using bulk insert
                print(f"  Uploading {len(records)} records to database (bulk insert)...")
                inserted = upload_to_database(records, conn, trade_date)

                total_inserted += inserted
                files_processed += 1

                print(f"  ✓ Complete - Inserted: {inserted} records")

            except Exception as e:
                print(f"  ✗ Error processing file: {e}")
                files_failed += 1
                continue

        # Summary
        print(f"\n{'═' * 60}")
        print(f"UPLOAD SUMMARY")
        print(f"{'═' * 60}")
        print(f"Files processed successfully: {files_processed}")
        print(f"Files failed: {files_failed}")
        print(f"Total records inserted: {total_inserted}")
        print(f"{'═' * 60}\n")

        return True

    finally:
        conn.close()
        print("Database connection closed.")


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
        print("\n\n⚠️  Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
