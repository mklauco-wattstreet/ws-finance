#!/usr/bin/env python3
"""Import a CSV file into a table using COPY via temp table. Skips id/created_at columns automatically.

Usage: python3 import_csv.py TABLE_NAME /path/to/file.csv
"""
import sys
import csv
import psycopg2
from psycopg2 import extras
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

if len(sys.argv) != 3:
    print("Usage: python3 import_csv.py TABLE_NAME /path/to/file.csv")
    sys.exit(1)

table_name = sys.argv[1]
csv_path = sys.argv[2]

SKIP_COLS = {'id', 'created_at'}

conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, port=DB_PORT)
cur = conn.cursor()
cur.execute(f"SET search_path TO {DB_SCHEMA}")

with open(csv_path) as f:
    reader = csv.DictReader(f)
    columns = [c for c in reader.fieldnames if c not in SKIP_COLS]
    col_list = ','.join(columns)
    placeholders = ','.join(['%s'] * len(columns))

    rows = []
    for row in reader:
        rows.append(tuple(row[c] if row[c] != '' else None for c in columns))

    query = f"INSERT INTO {table_name} ({col_list}) VALUES %s ON CONFLICT DO NOTHING"
    extras.execute_values(cur, query, rows, page_size=1000)

conn.commit()
print(f"{cur.rowcount} rows imported into {table_name}")
conn.close()
