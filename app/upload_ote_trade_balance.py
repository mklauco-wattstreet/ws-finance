#!/usr/bin/env python3
"""
Upload OTE trade balance reports from Excel files to PostgreSQL database.

Usage:
    python3 upload_ote_trade_balance.py PATH_TO_DIRECTORY [--debug]

Example:
    python3 upload_ote_trade_balance.py ote_files/2025/11
    python3 upload_ote_trade_balance.py ote_files/2025/11 --debug
"""

import sys
import os
import re
from pathlib import Path
from datetime import datetime, time
import psycopg2
from psycopg2 import extras
import pandas as pd

# Import database configuration and logging
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT
from common import setup_logging, print_banner


def parse_date_from_filename(filename):
    """
    Extract delivery date from filename.
    Expected format: Trade_balance_YYYYMMDD_HHMM.xlsx

    Args:
        filename: Name of the Excel file

    Returns:
        datetime.date object or None if parsing fails
    """
    pattern = r'Trade_balance_(\d{4})(\d{2})(\d{2})_\d{4}\.xlsx'
    match = re.match(pattern, filename)

    if match:
        year, month, day = match.groups()
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


def parse_time_interval_to_period(time_interval):
    """
    Convert time interval string to period number (1-96).

    Args:
        time_interval: Time interval in format "HH:MM - HH:MM" or "HH:MM-HH:MM"

    Returns:
        int: Period number (1-96)
    """
    # Extract start time from interval (handle both "HH:MM - HH:MM" and "HH:MM-HH:MM")
    if ' - ' in time_interval:
        start_time_str = time_interval.split(' - ')[0].strip()
    else:
        start_time_str = time_interval.split('-')[0].strip()

    # Parse hours and minutes
    hour, minute = map(int, start_time_str.split(':'))

    # Calculate period: each period is 15 minutes
    # 00:00 = period 1, 00:15 = period 2, etc.
    period = (hour * 4) + (minute // 15) + 1

    return period


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


def read_trade_balance_file(file_path, delivery_date):
    """
    Read trade balance data from Excel file and ensure all 96 periods are present.

    Args:
        file_path: Path to the Excel file
        delivery_date: Delivery date extracted from filename

    Returns:
        list of dictionaries containing the data (always 96 records)
    """
    print(f"  Reading file: {file_path.name}")

    # Read Excel file with multi-level headers
    df = pd.read_excel(file_path, sheet_name=0, header=[0, 1])

    print(f"  Found {len(df)} rows in Excel")

    # Create a dictionary to store records by period number
    records_by_period = {}

    # Track the actual delivery date from the Excel data
    actual_delivery_date = None

    for _, row in df.iterrows():
        # Get delivery day and period (time interval)
        delivery_day = row[('Unnamed: 0_level_0', 'Delivery day')]
        time_interval = row[('Unnamed: 1_level_0', 'Period')]

        # Skip empty rows
        if pd.isna(delivery_day) or pd.isna(time_interval):
            continue

        # Convert time_interval to period number
        period = parse_time_interval_to_period(str(time_interval))

        # Parse delivery date from the row (this is the actual date, not from filename)
        if isinstance(delivery_day, str):
            row_delivery_date = datetime.strptime(delivery_day, '%Y-%m-%d').date()
        else:
            row_delivery_date = delivery_day.date() if hasattr(delivery_day, 'date') else delivery_date

        # Store the actual delivery date (from first valid row)
        if actual_delivery_date is None:
            actual_delivery_date = row_delivery_date

        # Clean time_interval: remove spaces around dash to fit varchar(11)
        # "01:00 - 01:15" (13 chars) -> "01:00-01:15" (11 chars)
        cleaned_time_interval = str(time_interval).replace(' - ', '-')

        record = {
            'delivery_date': row_delivery_date,
            'time_interval': cleaned_time_interval,
            'period': period,
        }

        # Process MW columns
        mw_columns = [
            (('Total', 'Buy (MW)'), 'total_buy_mw'),
            (('Total', 'Sell (MW)'), 'total_sell_mw'),
            (('Daily market', 'Buy (MW)'), 'daily_market_buy_mw'),
            (('Daily market', 'Sell (MW)'), 'daily_market_sell_mw'),
            (('Intraday auction', 'Buy (MW)'), 'intraday_auction_buy_mw'),
            (('Intraday auction', 'Sell (MW)'), 'intraday_auction_sell_mw'),
            (('Intraday market', 'Buy (MW)'), 'intraday_market_buy_mw'),
            (('Intraday market', 'Sell (MW)'), 'intraday_market_sell_mw'),
            (('Realization diagrams', 'Buy (MW)'), 'realization_diagrams_buy_mw'),
            (('Realization diagrams', 'Sell (MW)'), 'realization_diagrams_sell_mw'),
        ]

        for excel_col, db_col in mw_columns:
            record[db_col] = clean_numeric_value(row.get(excel_col))

        # Process MWh columns
        mwh_columns = [
            (('Total', 'Buy (MWh)'), 'total_buy_mwh'),
            (('Total', 'Sell (MWh)'), 'total_sell_mwh'),
            (('Daily market', 'Buy (MWh)'), 'daily_market_buy_mwh'),
            (('Daily market', 'Sell (MWh)'), 'daily_market_sell_mwh'),
            (('Intraday auction', 'Buy (MWh)'), 'intraday_auction_buy_mwh'),
            (('Intraday auction', 'Sell (MWh)'), 'intraday_auction_sell_mwh'),
            (('Intraday market', 'Buy (MWh)'), 'intraday_market_buy_mwh'),
            (('Intraday market', 'Sell (MWh)'), 'intraday_market_sell_mwh'),
            (('Realization diagrams', 'Buy (MWh)'), 'realization_diagrams_buy_mwh'),
            (('Realization diagrams', 'Sell (MWh)'), 'realization_diagrams_sell_mwh'),
        ]

        for excel_col, db_col in mwh_columns:
            record[db_col] = clean_numeric_value(row.get(excel_col))

        records_by_period[period] = record

    # Use actual delivery date from Excel data, fallback to filename date if not found
    if actual_delivery_date is None:
        actual_delivery_date = delivery_date
        print(f"  Warning: No delivery date found in Excel, using filename date: {delivery_date}")
    else:
        print(f"  Actual delivery date from Excel: {actual_delivery_date}")

    # Now create all 96 periods, filling missing ones with zeros
    complete_records = []
    for period in range(1, 97):
        if period in records_by_period:
            # Use data from Excel
            complete_records.append(records_by_period[period])
        else:
            # Create record with zeros for missing period
            time_interval = generate_time_interval(period)
            record = {
                'delivery_date': actual_delivery_date,
                'time_interval': time_interval,
                'period': period,
                'total_buy_mw': 0.0,
                'total_sell_mw': 0.0,
                'daily_market_buy_mw': 0.0,
                'daily_market_sell_mw': 0.0,
                'intraday_auction_buy_mw': 0.0,
                'intraday_auction_sell_mw': 0.0,
                'intraday_market_buy_mw': 0.0,
                'intraday_market_sell_mw': 0.0,
                'realization_diagrams_buy_mw': 0.0,
                'realization_diagrams_sell_mw': 0.0,
                'total_buy_mwh': 0.0,
                'total_sell_mwh': 0.0,
                'daily_market_buy_mwh': 0.0,
                'daily_market_sell_mwh': 0.0,
                'intraday_auction_buy_mwh': 0.0,
                'intraday_auction_sell_mwh': 0.0,
                'intraday_market_buy_mwh': 0.0,
                'intraday_market_sell_mwh': 0.0,
                'realization_diagrams_buy_mwh': 0.0,
                'realization_diagrams_sell_mwh': 0.0,
            }
            complete_records.append(record)

    print(f"  Complete records with all 96 periods: {len(complete_records)} (from Excel: {len(records_by_period)}, filled with zeros: {96 - len(records_by_period)})")

    return complete_records


def print_debug_info(records, delivery_date):
    """
    Print debug information for the first 8 periods of data.

    Args:
        records: List of records to debug
        delivery_date: Delivery date for display (may differ from actual date in records)
    """
    # Get actual delivery date from first record
    actual_date = records[0]['delivery_date'] if records else delivery_date

    print(f"\n{'═' * 120}")
    print(f"DEBUG MODE - First 8 periods of data for {actual_date}")
    print(f"{'═' * 120}")
    print(f"Total records: {len(records)}")

    print(f"\n{'─' * 120}")
    print("MAPPED DATA (as it will be inserted into PostgreSQL):")
    print(f"{'─' * 120}")

    # Table header
    header = (
        f"{'Delivery Date':<13} {'Per':<4} {'Time Interval':<13} "
        f"{'TotBuy(MW)':<11} {'TotSel(MW)':<11} "
        f"{'DMBuy(MW)':<11} {'DMSel(MW)':<11} "
        f"{'TotBuy(MWh)':<12} {'TotSel(MWh)':<12}"
    )
    print(header)
    print(f"{'─' * 120}")

    # Print first 8 records
    for record in records[:8]:
        line = (
            f"{str(record['delivery_date']):<13} {record['period']:<4} {record['time_interval']:<13} "
            f"{record['total_buy_mw']:<11.2f} "
            f"{record['total_sell_mw']:<11.2f} "
            f"{record['daily_market_buy_mw']:<11.2f} "
            f"{record['daily_market_sell_mw']:<11.2f} "
            f"{record['total_buy_mwh']:<12.2f} "
            f"{record['total_sell_mwh']:<12.2f}"
        )
        print(line)

    print(f"\n{'─' * 120}")
    print("Column Legend:")
    print("  Delivery Date = delivery_date | Per = period | TotBuy = total_buy | TotSel = total_sell")
    print("  DMBuy = daily_market_buy | DMSel = daily_market_sell | (showing MW and MWh columns)")
    print(f"\nShowing 8 periods out of {len(records)} total periods")
    print(f"{'═' * 120}\n")


def upload_to_database(records, conn, delivery_date):
    """
    Upload records to the database using UPSERT (INSERT ... ON CONFLICT UPDATE).

    Args:
        records: List of dictionaries containing the data
        conn: Database connection
        delivery_date: Delivery date for logging (may differ from actual date in records)

    Returns:
        tuple: (inserted_count, updated_count)
    """
    if not records:
        return 0, 0

    # Get actual delivery date from first record
    actual_date = records[0]['delivery_date'] if records else delivery_date
    if actual_date != delivery_date:
        print(f"  Note: Excel contains data for {actual_date} (filename suggests {delivery_date})")

    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO ote_trade_balance (
            delivery_date, time_interval, period,
            total_buy_mw, total_sell_mw,
            daily_market_buy_mw, daily_market_sell_mw,
            intraday_auction_buy_mw, intraday_auction_sell_mw,
            intraday_market_buy_mw, intraday_market_sell_mw,
            realization_diagrams_buy_mw, realization_diagrams_sell_mw,
            total_buy_mwh, total_sell_mwh,
            daily_market_buy_mwh, daily_market_sell_mwh,
            intraday_auction_buy_mwh, intraday_auction_sell_mwh,
            intraday_market_buy_mwh, intraday_market_sell_mwh,
            realization_diagrams_buy_mwh, realization_diagrams_sell_mwh
        ) VALUES %s
        ON CONFLICT (delivery_date, time_interval)
        DO UPDATE SET
            period = EXCLUDED.period,
            total_buy_mw = EXCLUDED.total_buy_mw,
            total_sell_mw = EXCLUDED.total_sell_mw,
            daily_market_buy_mw = EXCLUDED.daily_market_buy_mw,
            daily_market_sell_mw = EXCLUDED.daily_market_sell_mw,
            intraday_auction_buy_mw = EXCLUDED.intraday_auction_buy_mw,
            intraday_auction_sell_mw = EXCLUDED.intraday_auction_sell_mw,
            intraday_market_buy_mw = EXCLUDED.intraday_market_buy_mw,
            intraday_market_sell_mw = EXCLUDED.intraday_market_sell_mw,
            realization_diagrams_buy_mw = EXCLUDED.realization_diagrams_buy_mw,
            realization_diagrams_sell_mw = EXCLUDED.realization_diagrams_sell_mw,
            total_buy_mwh = EXCLUDED.total_buy_mwh,
            total_sell_mwh = EXCLUDED.total_sell_mwh,
            daily_market_buy_mwh = EXCLUDED.daily_market_buy_mwh,
            daily_market_sell_mwh = EXCLUDED.daily_market_sell_mwh,
            intraday_auction_buy_mwh = EXCLUDED.intraday_auction_buy_mwh,
            intraday_auction_sell_mwh = EXCLUDED.intraday_auction_sell_mwh,
            intraday_market_buy_mwh = EXCLUDED.intraday_market_buy_mwh,
            intraday_market_sell_mwh = EXCLUDED.intraday_market_sell_mwh,
            realization_diagrams_buy_mwh = EXCLUDED.realization_diagrams_buy_mwh,
            realization_diagrams_sell_mwh = EXCLUDED.realization_diagrams_sell_mwh,
            uploaded_at = CURRENT_TIMESTAMP
        WHERE (
            ote_trade_balance.period IS DISTINCT FROM EXCLUDED.period OR
            ote_trade_balance.total_buy_mw IS DISTINCT FROM EXCLUDED.total_buy_mw OR
            ote_trade_balance.total_sell_mw IS DISTINCT FROM EXCLUDED.total_sell_mw OR
            ote_trade_balance.daily_market_buy_mw IS DISTINCT FROM EXCLUDED.daily_market_buy_mw OR
            ote_trade_balance.daily_market_sell_mw IS DISTINCT FROM EXCLUDED.daily_market_sell_mw OR
            ote_trade_balance.intraday_auction_buy_mw IS DISTINCT FROM EXCLUDED.intraday_auction_buy_mw OR
            ote_trade_balance.intraday_auction_sell_mw IS DISTINCT FROM EXCLUDED.intraday_auction_sell_mw OR
            ote_trade_balance.intraday_market_buy_mw IS DISTINCT FROM EXCLUDED.intraday_market_buy_mw OR
            ote_trade_balance.intraday_market_sell_mw IS DISTINCT FROM EXCLUDED.intraday_market_sell_mw OR
            ote_trade_balance.realization_diagrams_buy_mw IS DISTINCT FROM EXCLUDED.realization_diagrams_buy_mw OR
            ote_trade_balance.realization_diagrams_sell_mw IS DISTINCT FROM EXCLUDED.realization_diagrams_sell_mw OR
            ote_trade_balance.total_buy_mwh IS DISTINCT FROM EXCLUDED.total_buy_mwh OR
            ote_trade_balance.total_sell_mwh IS DISTINCT FROM EXCLUDED.total_sell_mwh OR
            ote_trade_balance.daily_market_buy_mwh IS DISTINCT FROM EXCLUDED.daily_market_buy_mwh OR
            ote_trade_balance.daily_market_sell_mwh IS DISTINCT FROM EXCLUDED.daily_market_sell_mwh OR
            ote_trade_balance.intraday_auction_buy_mwh IS DISTINCT FROM EXCLUDED.intraday_auction_buy_mwh OR
            ote_trade_balance.intraday_auction_sell_mwh IS DISTINCT FROM EXCLUDED.intraday_auction_sell_mwh OR
            ote_trade_balance.intraday_market_buy_mwh IS DISTINCT FROM EXCLUDED.intraday_market_buy_mwh OR
            ote_trade_balance.intraday_market_sell_mwh IS DISTINCT FROM EXCLUDED.intraday_market_sell_mwh OR
            ote_trade_balance.realization_diagrams_buy_mwh IS DISTINCT FROM EXCLUDED.realization_diagrams_buy_mwh OR
            ote_trade_balance.realization_diagrams_sell_mwh IS DISTINCT FROM EXCLUDED.realization_diagrams_sell_mwh
        )
    """

    # Prepare data as tuples for bulk upsert
    values = []
    for record in records:
        values.append((
            record['delivery_date'],
            record['time_interval'],
            record['period'],
            record['total_buy_mw'],
            record['total_sell_mw'],
            record['daily_market_buy_mw'],
            record['daily_market_sell_mw'],
            record['intraday_auction_buy_mw'],
            record['intraday_auction_sell_mw'],
            record['intraday_market_buy_mw'],
            record['intraday_market_sell_mw'],
            record['realization_diagrams_buy_mw'],
            record['realization_diagrams_sell_mw'],
            record['total_buy_mwh'],
            record['total_sell_mwh'],
            record['daily_market_buy_mwh'],
            record['daily_market_sell_mwh'],
            record['intraday_auction_buy_mwh'],
            record['intraday_auction_sell_mwh'],
            record['intraday_market_buy_mwh'],
            record['intraday_market_sell_mwh'],
            record['realization_diagrams_buy_mwh'],
            record['realization_diagrams_sell_mwh'],
        ))

    try:
        # Use execute_values for efficient bulk upsert
        extras.execute_values(cursor, upsert_query, values)
        affected_rows = cursor.rowcount
        conn.commit()
        cursor.close()

        # Note: PostgreSQL doesn't easily distinguish between inserts and updates with ON CONFLICT
        # We return the total affected rows
        return affected_rows, 0

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

    # Find all Trade Balance Excel files
    excel_files = list(dir_path.glob("Trade_balance_*.xlsx"))

    if not excel_files:
        logger.warning(f"No Trade Balance Excel files found in '{directory_path}'")
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
                connect_timeout=10
            )
            logger.info("✓ Database connection established\n")
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            return False

    total_upserted = 0
    files_processed = 0
    files_failed = 0

    try:
        for excel_file in sorted(excel_files):
            logger.info(f"{'─' * 60}")
            logger.info(f"Processing: {excel_file.name}")

            # Extract delivery date from filename
            delivery_date = parse_date_from_filename(excel_file.name)

            if not delivery_date:
                logger.warning(f"  ✗ Failed to extract date from filename")
                files_failed += 1
                continue

            logger.info(f"  Delivery date (from filename): {delivery_date}")

            try:
                # Read data from Excel
                records = read_trade_balance_file(excel_file, delivery_date)

                if not records:
                    logger.warning(f"  ⚠️  No valid records found")
                    files_failed += 1
                    continue

                logger.info(f"  Found: {len(records)} records")

                if debug_mode:
                    # Print debug info for first 8 periods
                    print_debug_info(records, delivery_date)
                    files_processed += 1
                else:
                    # Upload to database using bulk upsert
                    logger.info(f"  Uploading {len(records)} records to database (bulk upsert)...")
                    affected, _ = upload_to_database(records, conn, delivery_date)

                    total_upserted += affected
                    files_processed += 1

                    logger.info(f"  ✓ Complete - Affected: {affected} records")

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
            logger.info(f"Total records affected: {total_upserted}")
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
        print("Usage: python3 upload_ote_trade_balance.py PATH_TO_DIRECTORY [--debug]")
        print("\nExamples:")
        print("  python3 upload_ote_trade_balance.py ote_files/2025/11")
        print("  python3 upload_ote_trade_balance.py ote_files/2025/11 --debug")
        sys.exit(1)

    directory_path = sys.argv[1]
    debug_mode = len(sys.argv) == 3 and sys.argv[2] == '--debug'

    # Setup logging
    logger = setup_logging(debug=debug_mode)

    print_banner("OTE Trade Balance Data Uploader", debug_mode)

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
