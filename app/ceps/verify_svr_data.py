#!/usr/bin/env python3
"""Quick verification script for SVR activation data."""
import sys
from pathlib import Path
import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT

conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, port=DB_PORT)
cur = conn.cursor()

# Count 1min records
cur.execute("SELECT COUNT(*) FROM finance.ceps_svr_activation_1min WHERE DATE(delivery_timestamp) = '2026-01-07'")
print(f"1min records: {cur.fetchone()[0]}")

# Count 15min intervals
cur.execute("SELECT COUNT(*) FROM finance.ceps_svr_activation_15min WHERE trade_date = '2026-01-07'")
print(f"15min intervals: {cur.fetchone()[0]}")

# Show first 5 records
print("\nFirst 5 1-minute records:")
cur.execute("""
    SELECT delivery_timestamp, afrr_plus_mw, afrr_minus_mw, mfrr_plus_mw, mfrr_minus_mw, mfrr_5_mw
    FROM finance.ceps_svr_activation_1min
    WHERE DATE(delivery_timestamp) = '2026-01-07'
    ORDER BY delivery_timestamp
    LIMIT 5
""")
for row in cur.fetchall():
    print(f"  {row[0]}: aFRR+={row[1]}, aFRR-={row[2]}, mFRR+={row[3]}, mFRR-={row[4]}, mFRR5={row[5]}")

# Show first 3 15min intervals
print("\nFirst 3 15-minute intervals:")
cur.execute("""
    SELECT time_interval, afrr_plus_mean_mw, afrr_minus_mean_mw, mfrr_plus_mean_mw, mfrr_minus_mean_mw, mfrr_5_mean_mw
    FROM finance.ceps_svr_activation_15min
    WHERE trade_date = '2026-01-07'
    ORDER BY time_interval
    LIMIT 3
""")
for row in cur.fetchall():
    print(f"  {row[0]}: aFRR+={row[1]:.3f}, aFRR-={row[2]:.3f}, mFRR+={row[3]:.3f}, mFRR-={row[4]:.3f}, mFRR5={row[5]:.3f}")

conn.close()
print("\n✓ Verification complete")
