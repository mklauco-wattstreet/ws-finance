#!/usr/bin/env python3
"""
Upload day-ahead price reports from Excel files to PostgreSQL database.

Usage:
    python3 upload_day_ahead_prices.py PATH_TO_DIRECTORY [--debug]

Example:
    python3 upload_day_ahead_prices.py 2025/10
    python3 upload_day_ahead_prices.py 2025/09 --debug
"""

import sys
import os
import re
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2 import extras
import pandas as pd

# Import database configuration and logging
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA
from common import setup_logging, print_banner

# Cutoff date for format change
CUTOFF_DATE = datetime(2025, 10, 1).date()


def parse_date_from_filename(filename):
    """
    Extract trade date from filename.
    Expected formats:
    - Old: DM_DD_MM_YYYY_EN.xlsx (before Oct 1, 2025)
    - New: DM_15MIN_DD_MM_YYYY_EN.xlsx (from Oct 1, 2025)

    Args:
        filename: Name of the Excel file

    Returns:
        datetime.date object or None if parsing fails
    """
    # Try new format first (DM_15MIN_DD_MM_YYYY_EN.xlsx)
    pattern_new = r'DM_15MIN_(\d{2})_(\d{2})_(\d{4})_EN\.xlsx'
    match = re.match(pattern_new, filename)

    if not match:
        # Try old format (DM_DD_MM_YYYY_EN.xlsx)
        pattern_old = r'DM_(\d{2})_(\d{2})_(\d{4})_EN\.xlsx'
        match = re.match(pattern_old, filename)

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


def generate_time_interval(period):
    """
    Generate time interval string for a given period (1-96).

    Args:
        period: Period number (1-96)

    Returns:
        str: Time interval in format "HH:MM-HH:MM"
    """
    # Each period is 15 minutes, starting from 00:00
    start_minutes = (period - 1) * 15
    end_minutes = period * 15

    start_hour = start_minutes // 60
    start_min = start_minutes % 60
    end_hour = end_minutes // 60
    end_min = end_minutes % 60

    return f"{start_hour:02d}:{start_min:02d}-{end_hour:02d}:{end_min:02d}"


def read_legacy_day_ahead_file(file_path, trade_date):
    """
    Read legacy day-ahead data from Excel file (hourly resolution, before Oct 1, 2025).
    Expands 24 hours into 96 15-minute periods.

    Args:
        file_path: Path to the Excel file
        trade_date: Trade date extracted from filename

    Returns:
        list of dictionaries containing the data (96 records)
    """
    print(f"  Reading LEGACY file (hourly data): {file_path.name}")

    # Read Excel file, skipping first 21 rows
    df = pd.read_excel(file_path, sheet_name='Day-Ahead Market CZ Results', skiprows=21)

    print(f"  Found {len(df)} rows (including header rows)")

    records = []

    for _, row in df.iterrows():
        # Skip empty rows and header rows
        if pd.isna(row.get('Hour')) or row.get('Volume\n(MWh)') == 'Sale':
            continue

        hour = int(row['Hour'])
        price = clean_numeric_value(row.get('Price (EUR/MWh)'))
        volume = clean_numeric_value(row.get('Volume\n(MWh)'))
        saldo_dm = clean_numeric_value(row.get('Saldo DM'))
        export = clean_numeric_value(row.get('Export'))
        import_val = clean_numeric_value(row.get('Import'))

        # Expand this hour into 4 periods (15-minute intervals)
        for quarter in range(4):
            period = (hour - 1) * 4 + quarter + 1  # hour 1 -> periods 1-4, hour 2 -> periods 5-8, etc.
            time_interval = generate_time_interval(period)

            record = {
                'trade_date': trade_date,
                'period': period,
                'time_interval': time_interval,
                'price_15min_eur_mwh': price,  # Same price for all 4 quarters
                'volume_mwh': volume / 4 if volume is not None else None,  # Divide by 4
                'purchase_15min_products_mwh': 0,  # Not available in legacy
                'purchase_60min_products_mwh': volume / 4 if volume is not None else None,  # Assume all purchases were 60-min
                'sale_15min_products_mwh': 0,  # Not available in legacy
                'sale_60min_products_mwh': volume / 4 if volume is not None else None,  # Assume all sales were 60-min
                'saldo_dm_mwh': saldo_dm / 4 if saldo_dm is not None else None,  # Divide by 4
                'export_mwh': export / 4 if export is not None else None,  # Divide by 4
                'import_mwh': import_val / 4 if import_val is not None else None,  # Divide by 4
                'price_60min_ref_eur_mwh': price,  # Same as 15-min price
                'is_15min': False  # Legacy data is hourly
            }

            records.append(record)

    return records


def read_new_day_ahead_file(file_path, trade_date):
    """
    Read new day-ahead data from Excel file (15-minute resolution, from Oct 1, 2025).

    Args:
        file_path: Path to the Excel file
        trade_date: Trade date extracted from filename

    Returns:
        list of dictionaries containing the data (96 records)
    """
    print(f"  Reading NEW file (15-minute data): {file_path.name}")

    # Read Excel file, skipping first 21 rows (header is on row 22, index 21)
    # Data starts on row 24 (index 23), but there's an empty row after header
    df = pd.read_excel(file_path, sheet_name='Day-Ahead Market CZ Results', skiprows=21)

    print(f"  Found {len(df)} rows (including empty rows)")

    records = []

    for _, row in df.iterrows():
        # Skip empty rows
        if pd.isna(row.get('Period')):
            continue

        # Clean time interval (remove any extra spaces or newlines)
        time_interval = str(row['Time interval']).strip() if not pd.isna(row.get('Time interval')) else None

        if not time_interval:
            continue

        record = {
            'trade_date': trade_date,
            'period': int(row['Period']),
            'time_interval': time_interval,
        }

        # Process numeric columns with exact column names from Excel
        # Column names have newlines in them
        numeric_columns = [
            ('15 min price\n(EUR/MWh)', 'price_15min_eur_mwh'),
            ('Volume\n(MWh)', 'volume_mwh'),
            ('Purchase 15min products\n(MWh)', 'purchase_15min_products_mwh'),
            ('Purchase 60min products\n(MWh)', 'purchase_60min_products_mwh'),
            ('Sale 15min products\n(MWh)', 'sale_15min_products_mwh'),
            ('Sale 60min products\n(MWh)', 'sale_60min_products_mwh'),
            ('Saldo DM\n(MWh)', 'saldo_dm_mwh'),
            ('Export\n(MWh)', 'export_mwh'),
            ('Import\n(MWh)', 'import_mwh'),
            ('60 min price reference\n(EUR/MWh)', 'price_60min_ref_eur_mwh'),
        ]

        for excel_col, db_col in numeric_columns:
            record[db_col] = clean_numeric_value(row.get(excel_col))

        record['is_15min'] = True  # New data is 15-minute resolution

        records.append(record)

    return records


def print_debug_info(records, trade_date, file_path):
    """
    Print debug information for the first 2 hours (8 periods) of data.

    Args:
        records: List of records to debug
        trade_date: Trade date for display
        file_path: Path to Excel file for reading original data
    """
    print(f"\n{'═' * 120}")
    print(f"DEBUG MODE - First 2 hours of data for {trade_date}")
    print(f"{'═' * 120}")
    print(f"is_15min: {records[0]['is_15min']}")
    print(f"Total records: {len(records)}")

    # If legacy format, show original hourly data first
    if not records[0]['is_15min']:
        print(f"\n{'─' * 120}")
        print("ORIGINAL HOURLY DATA FROM EXCEL:")
        print(f"{'─' * 120}")

        # Read original hourly data
        df = pd.read_excel(file_path, sheet_name='Day-Ahead Market CZ Results', skiprows=21)

        print(f"{'Hour':<6} {'Price (EUR/MWh)':<18} {'Volume (MWh)':<15} {'Saldo DM':<12} {'Export':<12} {'Import':<12}")
        print(f"{'─' * 120}")

        for _, row in df.iterrows():
            if pd.isna(row.get('Hour')) or row.get('Volume\n(MWh)') == 'Sale':
                continue

            hour = int(row['Hour'])
            if hour > 2:  # Only show first 2 hours
                break

            price = row.get('Price (EUR/MWh)')
            volume = row.get('Volume\n(MWh)')
            saldo = row.get('Saldo DM')
            export = row.get('Export')
            import_val = row.get('Import')

            print(f"{hour:<6} {price:<18.2f} {volume:<15.2f} {saldo:<12.2f} {export:<12.2f} {import_val:<12.2f}")

    # Print mapped 15-minute data
    print(f"\n{'─' * 120}")
    print("MAPPED 15-MINUTE DATA (as it will be inserted into PostgreSQL):")
    print(f"{'─' * 120}")

    # Table header
    header = (
        f"{'Per':<4} {'Time':<11} "
        f"{'Price15':<8} {'Price60':<8} {'Vol':<8} "
        f"{'Pur15':<8} {'Pur60':<8} {'Sale15':<8} {'Sale60':<8} "
        f"{'Saldo':<8} {'Export':<8} {'Import':<8}"
    )
    print(header)
    print(f"{'─' * 120}")

    # Print first 8 records (2 hours)
    for record in records[:8]:
        line = (
            f"{record['period']:<4} {record['time_interval']:<11} "
            f"{record['price_15min_eur_mwh']:<8.2f} "
            f"{record['price_60min_ref_eur_mwh']:<8.2f} "
            f"{record['volume_mwh']:<8.2f} "
            f"{record['purchase_15min_products_mwh']:<8.2f} "
            f"{record['purchase_60min_products_mwh']:<8.2f} "
            f"{record['sale_15min_products_mwh']:<8.2f} "
            f"{record['sale_60min_products_mwh']:<8.2f} "
            f"{record['saldo_dm_mwh']:<8.2f} "
            f"{record['export_mwh']:<8.2f} "
            f"{record['import_mwh']:<8.2f}"
        )
        print(line)

    print(f"\n{'─' * 120}")
    print("Column Legend:")
    print("  Per = period | Price15 = price_15min_eur_mwh | Price60 = price_60min_ref_eur_mwh")
    print("  Vol = volume_mwh | Pur15 = purchase_15min_products_mwh | Pur60 = purchase_60min_products_mwh")
    print("  Sale15 = sale_15min_products_mwh | Sale60 = sale_60min_products_mwh")
    print("  Saldo = saldo_dm_mwh | Export = export_mwh | Import = import_mwh")

    if not records[0]['is_15min']:
        print(f"\n{'─' * 120}")
        print("VERIFICATION NOTE (Legacy Format):")
        print("  - All 15-min values should be 1/4 of the hourly value (except prices which stay the same)")
        print("  - Example: Hour 1 Volume 3341.80 ÷ 4 = 835.45 for each of periods 1-4")
        print("  - purchase_15min_products_mwh and sale_15min_products_mwh are set to 0 (not available)")
        print("  - purchase_60min_products_mwh and sale_60min_products_mwh equal volume_mwh (assumption)")

    print(f"\n{'─' * 120}")
    print(f"Showing 8 periods (2 hours) out of {len(records)} total periods")
    print(f"{'═' * 120}\n")


def read_day_ahead_file(file_path, trade_date):
    """
    Read day-ahead data from Excel file (detects format automatically).

    Args:
        file_path: Path to the Excel file
        trade_date: Trade date extracted from filename

    Returns:
        list of dictionaries containing the data
    """
    # Determine format based on trade date
    if trade_date < CUTOFF_DATE:
        return read_legacy_day_ahead_file(file_path, trade_date)
    else:
        return read_new_day_ahead_file(file_path, trade_date)


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
        INSERT INTO ote_prices_day_ahead (
            trade_date, period, time_interval,
            price_15min_eur_mwh, volume_mwh,
            purchase_15min_products_mwh, purchase_60min_products_mwh,
            sale_15min_products_mwh, sale_60min_products_mwh,
            saldo_dm_mwh, export_mwh, import_mwh,
            price_60min_ref_eur_mwh, is_15min
        ) VALUES %s
    """

    # Prepare data as tuples for bulk insert
    values = []
    for record in records:
        values.append((
            record['trade_date'],
            record['period'],
            record['time_interval'],
            record['price_15min_eur_mwh'],
            record['volume_mwh'],
            record['purchase_15min_products_mwh'],
            record['purchase_60min_products_mwh'],
            record['sale_15min_products_mwh'],
            record['sale_60min_products_mwh'],
            record['saldo_dm_mwh'],
            record['export_mwh'],
            record['import_mwh'],
            record['price_60min_ref_eur_mwh'],
            record['is_15min'],
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


def process_directory(directory_path, logger, debug_mode=False):
    """
    Process all Excel files in a directory.

    Args:
        directory_path: Path to directory containing Excel files
        logger: Logger instance
        debug_mode: If True, print debug info and don't insert to database
    """
    dir_path = Path(directory_path)

    if not dir_path.exists():
        logger.error(f"Directory '{directory_path}' does not exist")
        return False

    if not dir_path.is_dir():
        logger.error(f"'{directory_path}' is not a directory")
        return False

    # Find all Excel files (both old and new formats)
    excel_files = list(dir_path.glob("DM_*.xlsx"))

    if not excel_files:
        logger.warning(f"No day-ahead Excel files found in '{directory_path}'")
        return True  # Not an error, just nothing to upload

    logger.info(f"\nDirectory: {dir_path.absolute()}")
    logger.info(f"Found {len(excel_files)} Excel file(s)\n")

    # Connect to database only if not in debug mode
    conn = None
    if not debug_mode:
        logger.info("Connecting to database...")
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
            logger.info("✓ Database connection established\n")
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            return False

    total_inserted = 0
    files_processed = 0
    files_failed = 0

    try:
        for excel_file in sorted(excel_files):
            logger.info(f"{'─' * 60}")
            logger.info(f"Processing: {excel_file.name}")

            # Extract trade date from filename
            trade_date = parse_date_from_filename(excel_file.name)

            if not trade_date:
                logger.warning(f"  ✗ Failed to extract date from filename")
                files_failed += 1
                continue

            logger.info(f"  Trade date: {trade_date}")
            logger.info(f"  Format: {'LEGACY (hourly)' if trade_date < CUTOFF_DATE else 'NEW (15-minute)'}")

            try:
                # Read data from Excel
                records = read_day_ahead_file(excel_file, trade_date)

                if not records:
                    logger.warning(f"  ⚠️  No valid records found")
                    files_failed += 1
                    continue

                logger.info(f"  Expected rows: 96, Found: {len(records)}")

                if debug_mode:
                    # Print debug info for first 2 hours
                    print_debug_info(records, trade_date, excel_file)
                    files_processed += 1
                else:
                    # Upload to database using bulk insert
                    logger.info(f"  Uploading {len(records)} records to database (bulk insert)...")
                    inserted = upload_to_database(records, conn, trade_date)

                    total_inserted += inserted
                    files_processed += 1

                    logger.info(f"  ✓ Complete - Inserted: {inserted} records")

            except Exception as e:
                logger.error(f"  ✗ Error processing file: {e}")
                files_failed += 1
                continue

        # Summary
        logger.info(f"\n{'═' * 60}")
        logger.info(f"UPLOAD SUMMARY")
        logger.info(f"{'═' * 60}")
        logger.info(f"Files processed successfully: {files_processed}")
        logger.info(f"Files failed: {files_failed}")
        if not debug_mode:
            logger.info(f"Total records inserted: {total_inserted}")
        else:
            logger.info(f"DEBUG MODE - No records inserted to database")
        logger.info(f"{'═' * 60}\n")

        return True

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def main():
    """Main function."""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python3 upload_day_ahead_prices.py PATH_TO_DIRECTORY [--debug]")
        print("\nExamples:")
        print("  python3 upload_day_ahead_prices.py 2025/10")
        print("  python3 upload_day_ahead_prices.py 2025/09 --debug")
        sys.exit(1)

    directory_path = sys.argv[1]
    debug_mode = len(sys.argv) == 3 and sys.argv[2] == '--debug'

    # Setup logging
    logger = setup_logging(debug=debug_mode)

    print_banner("Day-Ahead Price Data Uploader", debug_mode)

    try:
        success = process_directory(directory_path, logger, debug_mode)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("\n\nUpload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
