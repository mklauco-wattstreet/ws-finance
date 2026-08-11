#!/usr/bin/env python3
"""
Upload Slovak OKTE intraday market (IDM) CSV files to PostgreSQL.

Usage:
    python3 upload_okte_idm.py PATH_TO_CSV

Example:
    python3 upload_okte_idm.py okte/2026/08/IDM_15MIN_09_08_2026.csv
    python3 upload_okte_idm.py okte/2026/08/IDM_60MIN_09_08_2026.csv
"""

import sys
import csv
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2 import extras

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

# 15MIN -> quarter-hourly order book (volume columns suffixed _mw)
# 60MIN -> hourly order book (volume columns suffixed _mwh)
TABLE_CONFIG = {
    'IDM_15MIN_': ('okte_prices_intraday_market', 'mw'),
    'IDM_60MIN_': ('okte_prices_intraday_market_60min', 'mwh'),
}


def clean_numeric(value):
    """Parse sk-SK formatted numbers ('165370,4', with possible NBSP/space) -> float or None."""
    if value is None:
        return None
    str_value = value.replace('\xa0', '').replace(' ', '').strip()
    if not str_value:
        return None
    str_value = str_value.replace(',', '.')
    try:
        return float(str_value)
    except ValueError:
        return None


def parse_trade_date(value):
    """Parse 'D.M.YYYY' (not zero-padded) -> date."""
    return datetime.strptime(value.strip(), '%d.%m.%Y').date()


def read_okte_csv(file_path, volume_suffix):
    """
    Parse an OKTE IDM CSV file (';' delimited, CRLF, sk-SK decimals) into records.
    Columns are mapped by position, not by header text.
    """
    records = []

    with open(file_path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, delimiter=';')
        next(reader, None)  # skip header row

        for row in reader:
            if not row or len(row) < 16:
                continue

            record = {
                'trade_date': parse_trade_date(row[0]),
                'period': int(row[1].strip()),
                'time_interval': row[2].replace(' ', ''),
                f'buy_orders_{volume_suffix}': clean_numeric(row[3]),
                f'sell_orders_{volume_suffix}': clean_numeric(row[4]),
                f'buy_trades_{volume_suffix}': clean_numeric(row[5]),
                f'sell_trades_{volume_suffix}': clean_numeric(row[6]),
                f'traded_quantity_diff_{volume_suffix}': clean_numeric(row[7]),
                'avg_price_eur_mwh': clean_numeric(row[8]),
                'weighted_avg_price_eur_mwh': clean_numeric(row[9]),
                'min_price_eur_mwh': clean_numeric(row[10]),
                'max_price_eur_mwh': clean_numeric(row[11]),
                'last_price_eur_mwh': clean_numeric(row[12]),
                f'total_traded_quantity_{volume_suffix}': clean_numeric(row[13]),
                'simple_orders_vwap_eur_mwh': clean_numeric(row[14]),
                f'simple_orders_quantity_{volume_suffix}': clean_numeric(row[15]),
            }
            records.append(record)

    return records


def upload_to_database(records, conn, table_name, volume_suffix):
    """Bulk upsert records via execute_values. Returns number of rows upserted."""
    if not records:
        return 0

    cursor = conn.cursor()

    columns = [
        'trade_date', 'period', 'time_interval',
        f'buy_orders_{volume_suffix}', f'sell_orders_{volume_suffix}',
        f'buy_trades_{volume_suffix}', f'sell_trades_{volume_suffix}',
        f'traded_quantity_diff_{volume_suffix}',
        'avg_price_eur_mwh', 'weighted_avg_price_eur_mwh',
        'min_price_eur_mwh', 'max_price_eur_mwh', 'last_price_eur_mwh',
        f'total_traded_quantity_{volume_suffix}',
        'simple_orders_vwap_eur_mwh', f'simple_orders_quantity_{volume_suffix}',
    ]
    update_columns = [c for c in columns if c not in ('trade_date', 'period')]

    upsert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (trade_date, period) DO UPDATE SET
            {', '.join(f'{c} = EXCLUDED.{c}' for c in update_columns)},
            updated_at = CURRENT_TIMESTAMP
    """

    values = [tuple(record[c] for c in columns) for record in records]

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


def resolve_table(filename):
    """Pick target table + volume column suffix from filename."""
    for prefix, (table_name, volume_suffix) in TABLE_CONFIG.items():
        if filename.startswith(prefix):
            return table_name, volume_suffix
    return None, None


def process_file(file_path):
    """Parse and upload a single OKTE IDM CSV file."""
    path = Path(file_path)

    if not path.exists():
        print(f"Error: '{file_path}' does not exist")
        return False

    table_name, volume_suffix = resolve_table(path.name)
    if not table_name:
        print(f"Error: unrecognized filename '{path.name}' (expected IDM_15MIN_* or IDM_60MIN_*)")
        return False

    try:
        records = read_okte_csv(path, volume_suffix)
    except Exception as e:
        print(f"Error parsing {path.name}: {e}")
        return False

    if not records:
        print(f"No records: {path.name}")
        return False

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
            cur.execute("SET statement_timeout TO '30s'")
        conn.autocommit = False
    except Exception as e:
        print(f"DB connection failed: {e}")
        return False

    try:
        upserted = upload_to_database(records, conn, table_name, volume_suffix)
        print(f"OKTE IDM upload: {path.name} -> {table_name}: {upserted} rows upserted")
        return True
    except Exception as e:
        print(f"Error {path.name}: {e}")
        return False
    finally:
        conn.close()


def main():
    """Main function."""
    if len(sys.argv) != 2:
        print("Usage: python3 upload_okte_idm.py PATH_TO_CSV")
        print("\nExamples:")
        print("  python3 upload_okte_idm.py okte/2026/08/IDM_15MIN_09_08_2026.csv")
        print("  python3 upload_okte_idm.py okte/2026/08/IDM_60MIN_09_08_2026.csv")
        sys.exit(1)

    try:
        success = process_file(sys.argv[1])
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
