"""Tiny ad-hoc SQL runner — credentials come from config/.env, never the CLI.

Usage (always via docker):
  docker compose exec -w /app/scripts entsoe-ote-data-uploader python3 dbq.py "SELECT count(*) FROM entsoe_outages"

search_path is set to the finance schema, so table names need no prefix.
"""
import sys
import psycopg2
import config

if len(sys.argv) < 2:
    sys.exit("usage: python3 dbq.py \"<SQL>\"")

sql = sys.argv[1]
conn = psycopg2.connect(
    host=config.DB_HOST, port=config.DB_PORT, dbname=config.DB_NAME,
    user=config.DB_USER, password=config.DB_PASSWORD, connect_timeout=10,
)
try:
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {config.DB_SCHEMA};")
        cur.execute(sql)
        if cur.description:  # SELECT-like
            cols = [d.name for d in cur.description]
            print(" | ".join(cols))
            print("-" * 60)
            for row in cur.fetchall():
                print(" | ".join("" if v is None else str(v) for v in row))
        else:
            print(f"OK: {cur.rowcount} rows affected")
    conn.commit()
finally:
    conn.close()
