#!/usr/bin/env python3
"""
Upload day-ahead 60-minute price reports from Excel files to PostgreSQL database.

Usage:
    python3 upload_day_ahead_60min_prices.py PATH_TO_DIRECTORY [--debug]

Example:
    python3 upload_day_ahead_60min_prices.py 2026/03
    python3 upload_day_ahead_60min_prices.py 2026/03 --debug
"""

import sys
import re
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2 import extras
import pandas as pd

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA
from common import setup_logging, print_banner


def parse_date_from_filename(filename):
    """
    Extract trade date from filename.
    Expected format: DM_60MIN_DD_MM_YYYY_EN.xlsx (or .xls)

    Args:
        filename: Name of the Excel file

    Returns:
        datetime.date object or None if parsing fails
    """
    pattern = r'DM_60MIN_(\d{2})_(\d{2})_(\d{4})_EN\.xlsx?'
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

    str_value = str(value).strip()
    str_value = str_value.replace(',', '').replace(' ', '')

    try:
        return float(str_value)
    except ValueError:
        return None


def convert_time_interval(raw):
    """
    Convert '00-01' to '00:00-01:00' format.

    Args:
        raw: Raw time interval string from Excel (e.g., '00-01', '23-24')

    Returns:
        str: Time interval in 'HH:MM-HH:MM' format
    """
    parts = raw.strip().split('-')
    start = f"{int(parts[0]):02d}:00"
    end = f"{int(parts[1]):02d}:00"
    return f"{start}-{end}"


def read_day_ahead_60min_file(file_path, trade_date):
    """
    Read day-ahead 60-minute data from Excel file.

    Args:
        file_path: Path to the Excel file
        trade_date: Trade date extracted from filename

    Returns:
        list of dictionaries containing the data (24 records)
    """
    df = pd.read_excel(file_path, sheet_name='Day-Ahead Market CZ Results', skiprows=21)

    records = []

    for _, row in df.iterrows():
        if pd.isna(row.get('Period')):
            continue

        time_interval_raw = str(row['Time interval']).strip() if not pd.isna(row.get('Time interval')) else None
        if not time_interval_raw:
            continue

        record = {
            'trade_date': trade_date,
            'period_60': int(row['Period']),
            'time_interval': convert_time_interval(time_interval_raw),
        }

        numeric_columns = [
            ('60 min price\n(EUR/MWh)', 'price_60min_eur_mwh'),
            ('Volume\n(MWh)', 'volume_mwh'),
            ('Purchase 15min products\n(MWh)', 'purchase_15min_products_mwh'),
            ('Purchase 60min products\n(MWh)', 'purchase_60min_products_mwh'),
            ('Sale 15min products\n(MWh)', 'sale_15min_products_mwh'),
            ('Sale 60min products\n(MWh)', 'sale_60min_products_mwh'),
            ('Saldo DM\n(MWh)', 'saldo_dm_mwh'),
            ('Export\n(MWh)', 'export_mwh'),
            ('Import\n(MWh)', 'import_mwh'),
        ]

        for excel_col, db_col in numeric_columns:
            record[db_col] = clean_numeric_value(row.get(excel_col))

        records.append(record)

    return records


def upload_to_database(records, conn, trade_date):
    """
    Upload records to the database using bulk upsert.

    Args:
        records: List of dictionaries containing the data (24 rows expected)
        conn: Database connection
        trade_date: Trade date for logging

    Returns:
        int: Number of records upserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO ote_prices_day_ahead_60min (
            trade_date, period_60, time_interval,
            price_60min_eur_mwh, volume_mwh,
            purchase_15min_products_mwh, purchase_60min_products_mwh,
            sale_15min_products_mwh, sale_60min_products_mwh,
            saldo_dm_mwh, export_mwh, import_mwh
        ) VALUES %s
        ON CONFLICT (trade_date, period_60) DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            price_60min_eur_mwh = EXCLUDED.price_60min_eur_mwh,
            volume_mwh = EXCLUDED.volume_mwh,
            purchase_15min_products_mwh = EXCLUDED.purchase_15min_products_mwh,
            purchase_60min_products_mwh = EXCLUDED.purchase_60min_products_mwh,
            sale_15min_products_mwh = EXCLUDED.sale_15min_products_mwh,
            sale_60min_products_mwh = EXCLUDED.sale_60min_products_mwh,
            saldo_dm_mwh = EXCLUDED.saldo_dm_mwh,
            export_mwh = EXCLUDED.export_mwh,
            import_mwh = EXCLUDED.import_mwh
    """

    values = []
    for record in records:
        values.append((
            record['trade_date'],
            record['period_60'],
            record['time_interval'],
            record['price_60min_eur_mwh'],
            record['volume_mwh'],
            record['purchase_15min_products_mwh'],
            record['purchase_60min_products_mwh'],
            record['sale_15min_products_mwh'],
            record['sale_60min_products_mwh'],
            record['saldo_dm_mwh'],
            record['export_mwh'],
            record['import_mwh'],
        ))

    try:
        extras.execute_values(cursor, upsert_query, values)
        conn.commit()
        upserted = len(values)
        cursor.close()
        return upserted

    except Exception as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"Database error: {e}")


def process_directory(directory_path, logger, debug_mode=False):
    """
    Process all 60-minute Excel files in a directory.

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

    excel_files = list(dir_path.glob("DM_60MIN_*.xlsx")) + list(dir_path.glob("DM_60MIN_*.xls"))

    if not excel_files:
        logger.warning(f"No day-ahead 60min Excel files in '{directory_path}'")
        return True

    conn = None
    if not debug_mode:
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
            logger.error(f"DB connection failed: {e}")
            return False

    total_upserted = 0
    files_processed = 0
    files_failed = 0

    try:
        for excel_file in sorted(excel_files):
            trade_date = parse_date_from_filename(excel_file.name)

            if not trade_date:
                logger.warning(f"Bad filename: {excel_file.name}")
                files_failed += 1
                continue

            try:
                records = read_day_ahead_60min_file(excel_file, trade_date)

                if not records:
                    logger.warning(f"No records: {excel_file.name}")
                    files_failed += 1
                    continue

                if debug_mode:
                    print(f"\n{'═' * 100}")
                    print(f"DEBUG - {trade_date} - {len(records)} records from {excel_file.name}")
                    print(f"{'─' * 100}")
                    print(f"{'Per':<4} {'Time':<11} {'Price60':<10} {'Vol':<10} {'Pur15':<10} {'Pur60':<10} {'Sale15':<10} {'Sale60':<10} {'Saldo':<10} {'Export':<10} {'Import':<10}")
                    print(f"{'─' * 100}")
                    for r in records[:4]:
                        print(f"{r['period_60']:<4} {r['time_interval']:<11} {r['price_60min_eur_mwh']:<10.2f} {r['volume_mwh']:<10.3f} {r['purchase_15min_products_mwh']:<10.3f} {r['purchase_60min_products_mwh']:<10.3f} {r['sale_15min_products_mwh']:<10.3f} {r['sale_60min_products_mwh']:<10.3f} {r['saldo_dm_mwh']:<10.3f} {r['export_mwh']:<10.3f} {r['import_mwh']:<10.3f}")
                    print(f"... showing 4 of {len(records)} records")
                    print(f"{'═' * 100}\n")
                    files_processed += 1
                else:
                    upserted = upload_to_database(records, conn, trade_date)
                    total_upserted += upserted
                    files_processed += 1

            except Exception as e:
                logger.error(f"Error {excel_file.name}: {e}")
                files_failed += 1
                continue

        if debug_mode:
            print(f"DayAhead60 upload: {files_processed} files (debug mode)")
        else:
            summary = f"OTE DayAhead60 upload: {files_processed} files, {total_upserted} rows upserted"
            if files_failed > 0:
                summary += f" ({files_failed} failed)"
            print(summary)

        return True

    finally:
        if conn:
            conn.close()


def main():
    """Main function."""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python3 upload_day_ahead_60min_prices.py PATH_TO_DIRECTORY [--debug]")
        print("\nExamples:")
        print("  python3 upload_day_ahead_60min_prices.py 2026/03")
        print("  python3 upload_day_ahead_60min_prices.py 2026/03 --debug")
        sys.exit(1)

    directory_path = sys.argv[1]
    debug_mode = len(sys.argv) == 3 and sys.argv[2] == '--debug'

    logger = setup_logging(debug=debug_mode)

    print_banner("Day-Ahead 60min Price Data Uploader", debug_mode)

    try:
        success = process_directory(directory_path, logger, debug_mode)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
