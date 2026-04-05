#!/usr/bin/env python3
"""
One-time backfill: Upload VDT_STANDARD_OBCHODY Excel files to pblctradeconf table.

Usage:
    python3 backfill_upload_pblctradeconf.py [DIRECTORY]

Examples:
    python3 backfill_upload_pblctradeconf.py pblctradeconf/2025
    python3 backfill_upload_pblctradeconf.py pblctradeconf/2025/03

Parses Excel files downloaded by backfill_download_pblctradeconf.py and inserts
into public.pblctradeconf. Uses DELETE+INSERT per day for idempotency.
"""

import sys
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2 import extras

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT


def parse_date_from_filename(filename):
    """Extract date from VDT_STANDARD_OBCHODY_DD_MM_YYYY_CZ.xlsx."""
    match = re.match(r'VDT_STANDARD_OBCHODY_(\d{2})_(\d{2})_(\d{4})_CZ\.xlsx', filename)
    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day)).date()
        except ValueError:
            return None
    return None


def parse_contract(contract_str):
    """
    Parse contract string like '20251201 10:30-20251201 10:45'.

    Returns:
        (contract_start: datetime, contract_end: datetime, duration_minutes: int)
    """
    parts = contract_str.split('-')
    if len(parts) != 2:
        return None, None, None
    fmt = '%Y%m%d %H:%M'
    try:
        start = datetime.strptime(parts[0].strip(), fmt)
        end = datetime.strptime(parts[1].strip(), fmt)
        duration = int((end - start).total_seconds() / 60)
        return start, end, duration
    except ValueError:
        return None, None, None


def read_excel_file(file_path, trade_date):
    """
    Read VDT_STANDARD_OBCHODY Excel file and return list of record tuples.

    Args:
        file_path: Path to Excel file
        trade_date: date object from filename

    Returns:
        list of tuples ready for bulk insert
    """
    col_names = [
        'contract', 'period_from', 'period_to', 'aggressor',
        'qty_mw', 'qty_mwh', 'price_eur_mwh', 'amount_eur', 'exec_time'
    ]
    df = pd.read_excel(file_path, skiprows=4, header=None, names=col_names)

    # Filter junk rows: repeated headers and fully-NaN rows
    df = df.dropna(how='all')
    df = df[df['contract'] != 'Kontrakt']
    df = df[df['contract'].notna()]

    # Coerce numeric columns — handles 'SN' (Storno/cancelled) and other non-numeric values
    for col in ('qty_mw', 'qty_mwh', 'price_eur_mwh', 'amount_eur'):
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows with no valid qty_mw (SN rows, residual headers)
    df = df[df['qty_mw'].notna()]

    date_str = trade_date.strftime('%Y%m%d')
    records = []

    for row_idx, (_, row) in enumerate(df.iterrows()):
        contract_str = str(row['contract']).strip()
        c_start, c_end, c_duration = parse_contract(contract_str)
        if c_start is None:
            continue

        # Synthetic tradeId: YYYYMMDD * 100000 + row_index (unique per day)
        trade_id = int(date_str) * 100000 + row_idx

        # exec_time: use as-is if already datetime, otherwise parse
        exec_time = row['exec_time']
        if isinstance(exec_time, str):
            try:
                exec_time = datetime.strptime(exec_time.strip(), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                exec_time = datetime.combine(trade_date, datetime.min.time())
        elif pd.isna(exec_time):
            exec_time = datetime.combine(trade_date, datetime.min.time())

        px = float(row['price_eur_mwh']) if pd.notna(row['price_eur_mwh']) else None
        qty_mw = float(row['qty_mw'])
        qty_mwh = float(row['qty_mwh']) if pd.notna(row['qty_mwh']) else None

        records.append((
            0.0,                    # ws_ote_ts
            0.0,                    # ws_our_ts
            exec_time,              # ws_ote_dt
            exec_time,              # ws_our_dt
            'xlsx_backfill',        # ws_headers
            'xlsx_backfill',        # ws_routing_key
            'xlsx_backfill',        # ws_marketID
            exec_time,              # tradeExecTime
            contract_str,           # contract
            c_start,                # contract_start
            c_end,                  # contract_end
            c_duration,             # contract_duration
            px,                     # px_eur_mwh
            qty_mwh,                # qty_mwh
            qty_mw,                 # qty_mw
            trade_id,               # tradeId
            1,                      # revisionNo
            'ACTI',                 # state
        ))

    return records


INSERT_QUERY = """
    INSERT INTO public.pblctradeconf (
        "ws_ote_ts", "ws_our_ts", "ws_ote_dt", "ws_our_dt",
        "ws_headers", "ws_routing_key", "ws_marketID",
        "tradeExecTime", contract, contract_start, contract_end,
        contract_duration, "px_eur_mwh", "qty_mwh", "qty_mw",
        "tradeId", "revisionNo", state
    ) VALUES %s
"""

DELETE_QUERY = """
    DELETE FROM public.pblctradeconf
    WHERE "ws_marketID" = 'xlsx_backfill'
      AND contract_start >= %s::date
      AND contract_start < %s::date + INTERVAL '1 day'
"""


def upload_day(records, conn, trade_date):
    """Delete existing backfill rows for the date, then bulk insert."""
    if not records:
        return 0
    cursor = conn.cursor()
    try:
        cursor.execute(DELETE_QUERY, (trade_date, trade_date))
        deleted = cursor.rowcount
        extras.execute_values(cursor, INSERT_QUERY, records)
        conn.commit()
        if deleted > 0:
            print(f"    Replaced {deleted} existing backfill rows")
        return len(records)
    except Exception as e:
        conn.rollback()
        raise Exception(f"DB error for {trade_date}: {e}")
    finally:
        cursor.close()


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 backfill_upload_pblctradeconf.py PATH")
        print("  PATH can be a directory or a single .xlsx file")
        print("  e.g. python3 backfill_upload_pblctradeconf.py pblctradeconf/2025")
        print("  e.g. python3 backfill_upload_pblctradeconf.py pblctradeconf/2025/07/VDT_STANDARD_OBCHODY_04_07_2025_CZ.xlsx")
        sys.exit(1)

    target = Path(sys.argv[1])
    if not target.exists():
        print(f"Error: '{target}' does not exist")
        sys.exit(1)

    if target.is_file() and target.suffix == '.xlsx':
        excel_files = [target]
    else:
        excel_files = sorted(target.glob("**/VDT_STANDARD_OBCHODY_*.xlsx"))

    if not excel_files:
        print(f"No VDT_STANDARD_OBCHODY files found in '{target}'")
        sys.exit(1)

    print(f"Found {len(excel_files)} Excel file(s)")

    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT, connect_timeout=10,
    )

    total_inserted = 0
    files_ok = 0
    files_fail = 0

    try:
        for excel_file in excel_files:
            trade_date = parse_date_from_filename(excel_file.name)
            if not trade_date:
                print(f"  SKIP {excel_file.name} - cannot parse date")
                files_fail += 1
                continue

            try:
                records = read_excel_file(excel_file, trade_date)
                if not records:
                    print(f"  SKIP {excel_file.name} - no valid records")
                    files_fail += 1
                    continue

                inserted = upload_day(records, conn, trade_date)
                total_inserted += inserted
                files_ok += 1
                print(f"  OK   {trade_date} - {inserted} trades")

            except Exception as e:
                print(f"  FAIL {excel_file.name} - {e}")
                files_fail += 1

        print(f"\n{'=' * 60}")
        print(f"UPLOAD SUMMARY")
        print(f"{'=' * 60}")
        print(f"Files OK:        {files_ok}")
        print(f"Files failed:    {files_fail}")
        print(f"Total inserted:  {total_inserted}")
        print(f"{'=' * 60}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
