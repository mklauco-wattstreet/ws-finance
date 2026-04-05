#!/usr/bin/env python3
"""
Upload OTE Intraday Auction (IDA) price reports from Excel files to PostgreSQL database.

Usage:
    python3 upload_ida_prices.py PATH_TO_DIRECTORY --ida N

Example:
    python3 upload_ida_prices.py IDA1/2026/03 --ida 1
    python3 upload_ida_prices.py IDA2/2026/03 --ida 2
    python3 upload_ida_prices.py IDA3/2026/03 --ida 3
"""

import sys
import re
from pathlib import Path
import psycopg2
from psycopg2 import extras
import pandas as pd

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA


def parse_date_from_filename(filename, ida_idx):
    """
    Extract trade date from filename.
    Expected format: IDA{n}_{DD}_{MM}_{YYYY}_EN.xlsx
    """
    pattern = rf'IDA{ida_idx}_(\d{{2}})_(\d{{2}})_(\d{{4}})_EN\.xlsx'
    match = re.match(pattern, filename)
    if match:
        day, month, year = match.groups()
        try:
            from datetime import datetime
            return datetime(int(year), int(month), int(day)).date()
        except ValueError:
            return None
    return None


def clean_numeric_value(value):
    """Clean numeric values from Excel (remove commas, handle spaces)."""
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


def read_ida_file(file_path, trade_date, ida_idx):
    """
    Read IDA market data from Excel file.

    Args:
        file_path: Path to the Excel file
        trade_date: Trade date extracted from filename
        ida_idx: IDA index (1, 2, or 3)

    Returns:
        list of tuples for bulk insert
    """
    # Header row position varies by IDA type (IDA1/IDA2 have more summary rows than IDA3).
    # Scan for the row containing "Period" to find the header dynamically.
    df_raw = pd.read_excel(file_path, header=None)
    header_row = None
    for idx, row in df_raw.iterrows():
        if row.iloc[0] == 'Period':
            header_row = idx
            break
    if header_row is None:
        return []
    df = pd.read_excel(file_path, skiprows=header_row)

    records = []

    for _, row in df.iterrows():
        if pd.isna(row.get('Period')):
            continue
        try:
            period = int(row['Period'])
        except (ValueError, TypeError):
            continue

        time_interval = str(row['Time interval']).strip()

        price = clean_numeric_value(row.get('Price (EUR/MWh)'))
        volume = clean_numeric_value(row.get('Volume\n(MWh)'))
        saldo = clean_numeric_value(row.get('Saldo DM\n(MWh)'))
        export_val = clean_numeric_value(row.get('Export\n(MWh)'))
        import_val = clean_numeric_value(row.get('Import\n(MWh)'))

        records.append((
            trade_date,
            period,
            ida_idx,
            time_interval,
            price,
            volume,
            saldo,
            export_val,
            import_val,
        ))

    return records


def upload_to_database(records, conn, trade_date, ida_idx):
    """
    Upload records to the database using bulk upsert.

    Returns:
        int: Number of records upserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO ote_prices_ida (
            trade_date, period, ida_idx, time_interval,
            price_eur_mwh, volume_mwh, saldo_dm_mwh,
            export_mwh, import_mwh
        ) VALUES %s
        ON CONFLICT (trade_date, period, ida_idx) DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            price_eur_mwh = EXCLUDED.price_eur_mwh,
            volume_mwh = EXCLUDED.volume_mwh,
            saldo_dm_mwh = EXCLUDED.saldo_dm_mwh,
            export_mwh = EXCLUDED.export_mwh,
            import_mwh = EXCLUDED.import_mwh,
            updated_at = CURRENT_TIMESTAMP
    """

    try:
        extras.execute_values(cursor, upsert_query, records)
        conn.commit()
        upserted = len(records)
        cursor.close()
        return upserted
    except Exception as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"Database error: {e}")


def process_directory(directory_path, ida_idx):
    """Process all IDA Excel files in a directory."""
    dir_path = Path(directory_path)

    if not dir_path.exists():
        print(f"Error: Directory '{directory_path}' does not exist")
        return False

    if not dir_path.is_dir():
        print(f"Error: '{directory_path}' is not a directory")
        return False

    file_pattern = f"IDA{ida_idx}_*.xlsx"
    excel_files = list(dir_path.glob(file_pattern))

    if not excel_files:
        print(f"No IDA{ida_idx} Excel files in '{directory_path}'")
        return False

    try:
        conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT, connect_timeout=10)
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
            trade_date = parse_date_from_filename(excel_file.name, ida_idx)

            if not trade_date:
                print(f"Bad filename: {excel_file.name}")
                files_failed += 1
                continue

            try:
                records = read_ida_file(excel_file, trade_date, ida_idx)

                if not records:
                    print(f"No records: {excel_file.name}")
                    files_failed += 1
                    continue

                upserted = upload_to_database(records, conn, trade_date, ida_idx)
                total_upserted += upserted
                files_processed += 1

            except Exception as e:
                print(f"Error {excel_file.name}: {e}")
                files_failed += 1
                continue

        summary = f"OTE IDA{ida_idx} upload: {files_processed} files, {total_upserted} rows upserted"
        if files_failed > 0:
            summary += f" ({files_failed} failed)"
        print(summary)

        return True

    finally:
        conn.close()


def main():
    """Main function."""
    # Parse --ida argument
    ida_idx = None
    remaining_args = []
    args = sys.argv[1:]

    i = 0
    while i < len(args):
        if args[i] == '--ida' and i + 1 < len(args):
            try:
                ida_idx = int(args[i + 1])
            except ValueError:
                print("Error: --ida must be followed by 1, 2, or 3")
                sys.exit(1)
            i += 2
        else:
            remaining_args.append(args[i])
            i += 1

    if ida_idx not in (1, 2, 3):
        print("Error: --ida N is required (N must be 1, 2, or 3)")
        print("\nUsage: python3 upload_ida_prices.py PATH_TO_DIRECTORY --ida N")
        print("\nExamples:")
        print("  python3 upload_ida_prices.py IDA1/2026/03 --ida 1")
        print("  python3 upload_ida_prices.py IDA2/2026/03 --ida 2")
        sys.exit(1)

    if len(remaining_args) != 1:
        print("Usage: python3 upload_ida_prices.py PATH_TO_DIRECTORY --ida N")
        sys.exit(1)

    directory_path = remaining_args[0]

    try:
        success = process_directory(directory_path, ida_idx)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
