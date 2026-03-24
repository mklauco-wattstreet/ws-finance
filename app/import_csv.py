#!/usr/bin/env python3
"""Import a CSV file into a table using COPY. Usage: python3 import_csv.py TABLE_NAME /path/to/file.csv"""
import sys
import os
import psycopg2
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA

if len(sys.argv) != 3:
    print("Usage: python3 import_csv.py TABLE_NAME /path/to/file.csv")
    sys.exit(1)

table_name = sys.argv[1]
csv_path = sys.argv[2]

conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, port=DB_PORT)
cur = conn.cursor()
cur.execute(f"SET search_path TO {DB_SCHEMA}")
with open(csv_path) as f:
    cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
conn.commit()
print(f"{cur.rowcount} rows imported into {table_name}")
conn.close()
