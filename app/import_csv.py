#!/usr/bin/env python3
"""Import a CSV into a table using COPY. Automatically skips id and created_at columns.

Usage: python3 import_csv.py TABLE_NAME /path/to/file.csv
"""
import sys
import csv
import io
import psycopg2
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

table_name = sys.argv[1]
csv_path = sys.argv[2]

SKIP = {'id', 'created_at'}

with open(csv_path) as f:
    reader = csv.reader(f)
    header = next(reader)
    keep = [i for i, col in enumerate(header) if col not in SKIP]
    columns = [header[i] for i in keep]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in reader:
        writer.writerow([row[i] for i in keep])

buf.seek(0)

conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, port=DB_PORT)
cur = conn.cursor()
cur.execute(f"SET search_path TO {DB_SCHEMA}")
cur.execute(f"TRUNCATE {table_name}")
cur.copy_expert(f"COPY {table_name} ({','.join(columns)}) FROM STDIN WITH CSV HEADER", buf)
conn.commit()
print(f"{cur.rowcount} rows imported")
conn.close()
