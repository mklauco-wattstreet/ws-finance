#!/usr/bin/env python3
"""
Upload OTE "Fin. security trend for limit IM" XLSX exports to
finance.ote_intraday_limit_utilization.

Usage:
    python3 upload_intraday_limit.py PATH [--debug] [--dry-run]

PATH is a single .xlsx file or a directory; a directory is scanned for
Intraday_limit_*.xlsx (the name ote_intraday_limit_downloader.py writes).

The XLSX layout is fixed (both CZ and EN exports share it):
    row 2  : report title
    row 3  : period from / to
    row 6  : column headers (merged cells, so header and value columns differ)
    row 8+ : data, one row per order event

Only the XLSX export is accepted. The CSV export truncates the timestamp to
whole seconds and then collides on the (event_timestamp, order_id) key.
"""

import sys
import re
from decimal import Decimal
from pathlib import Path

import warnings

import openpyxl
import psycopg2
from psycopg2 import extras

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA
from common import setup_logging

TABLE = "ote_intraday_limit_utilization"

# Header label -> value column index (0-based). The header cell sits at a
# different index than the value for merged columns, so both are listed.
HEADER_TIMESTAMP = {"Timestamp", "Datumová značka"}
COL_TIMESTAMP = 2
COL_ORDER_TYPE = 5
COL_ORDER_ID = 6
COL_DELIVERY_DATE = 8
COL_CHANGE_COMMODITY = 14
COL_CHANGE_IMBALANCE = 19
COL_CHANGE_TOTAL = 22
COL_UTIL_TOTAL = 25
COL_LIMIT_TOTAL = 28
COL_LIMIT_AVAILABLE = 31

COLUMNS = (
    "event_timestamp", "order_type", "order_id", "delivery_date",
    "utilization_change_commodity", "utilization_change_imbalance",
    "utilization_change_total", "utilization_total", "limit_total", "limit_available",
)
UPDATE_COLUMNS = COLUMNS[1:2] + COLUMNS[3:]

UPSERT_SQL = f"""
    INSERT INTO {TABLE} AS t ({", ".join(COLUMNS)})
    VALUES %s
    ON CONFLICT (event_timestamp, order_id) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS)},
        updated_at = CURRENT_TIMESTAMP
    WHERE ({", ".join(f"t.{c}" for c in UPDATE_COLUMNS)})
        IS DISTINCT FROM ({", ".join(f"EXCLUDED.{c}" for c in UPDATE_COLUMNS)})
    RETURNING (xmax = 0) AS inserted
"""


def _num(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    cleaned = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".")
    return Decimal(cleaned)


def parse_xlsx(path, logger):
    """Return (rows, meta). rows are tuples in COLUMNS order, deduplicated on key."""
    # Not read_only: the portal writes a bogus <dimension> tag and read-only
    # mode then yields a single empty row.
    with warnings.catch_warnings():
        # the portal's XLSX has no default style; openpyxl warns and applies its own
        warnings.simplefilter("ignore", UserWarning)
        wb = openpyxl.load_workbook(path, data_only=True)
    try:
        ws = wb.worksheets[0]
        all_rows = ws.iter_rows(values_only=True)

        title = None
        period = None
        header_seen = False
        records = {}
        dupes = 0
        skipped = 0

        for row in all_rows:
            cells = list(row) + [None] * (COL_LIMIT_AVAILABLE + 1 - len(row))
            if not header_seen:
                if title is None and cells[1] and isinstance(cells[1], str):
                    title = cells[1].strip()
                if period is None and cells[29] is not None and cells[32] is not None:
                    period = (cells[29], cells[32])
                if isinstance(cells[COL_TIMESTAMP], str) and cells[COL_TIMESTAMP].strip() in HEADER_TIMESTAMP:
                    header_seen = True
                continue

            ts = cells[COL_TIMESTAMP]
            if not hasattr(ts, "year"):
                if ts is not None:
                    skipped += 1
                continue
            delivery = cells[COL_DELIVERY_DATE]
            if not hasattr(delivery, "year"):
                skipped += 1
                logger.warning(f"Row at {ts}: unparseable delivery day {delivery!r}, skipped")
                continue

            order_id = int(_num(cells[COL_ORDER_ID]))
            record = (
                ts,
                str(cells[COL_ORDER_TYPE]).strip(),
                order_id,
                delivery.date() if hasattr(delivery, "date") else delivery,
                _num(cells[COL_CHANGE_COMMODITY]),
                _num(cells[COL_CHANGE_IMBALANCE]),
                _num(cells[COL_CHANGE_TOTAL]),
                _num(cells[COL_UTIL_TOTAL]),
                _num(cells[COL_LIMIT_TOTAL]),
                _num(cells[COL_LIMIT_AVAILABLE]),
            )
            key = (ts, order_id)
            if key in records:
                dupes += 1
            records[key] = record

        if not header_seen:
            raise ValueError("header row not found (expected 'Timestamp' / 'Datumová značka')")
        if dupes:
            logger.warning(f"{path.name}: {dupes} duplicate (timestamp, order_id) rows collapsed - "
                           f"is this the XLSX export and not the CSV?")
        if skipped:
            logger.debug(f"{path.name}: {skipped} non-data rows skipped")
        return list(records.values()), {"title": title, "period": period}
    finally:
        wb.close()


def upsert(conn, rows, logger):
    """Bulk upsert; returns (inserted, updated)."""
    with conn.cursor() as cur:
        result = extras.execute_values(cur, UPSERT_SQL, rows, page_size=1000, fetch=True)
    inserted = sum(1 for (flag,) in result if flag)
    updated = len(result) - inserted
    return inserted, updated


def collect_files(path):
    p = Path(path)
    if p.is_file():
        return [p]
    if p.is_dir():
        return sorted(p.glob("Intraday_limit_*.xlsx"))
    return []


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    debug = "--debug" in sys.argv
    dry_run = "--dry-run" in sys.argv
    logger = setup_logging(debug=debug)

    if not args:
        logger.error("Usage: upload_intraday_limit.py PATH [--debug] [--dry-run]")
        sys.exit(2)

    files = collect_files(args[0])
    if not files:
        logger.error(f"No Intraday_limit_*.xlsx under {args[0]}")
        sys.exit(1)

    parsed = []
    for f in files:
        try:
            rows, meta = parse_xlsx(f, logger)
        except Exception as e:
            logger.error(f"{f.name}: parse failed: {e}")
            sys.exit(1)
        period = meta["period"]
        period_str = f"{period[0]:%Y-%m-%d} .. {period[1]:%Y-%m-%d}" if period else "?"
        logger.info(f"{f.name}: '{meta['title']}' period {period_str}, {len(rows)} rows")
        if rows:
            dates = sorted({r[3] for r in rows})
            logger.debug(f"  delivery dates: {dates[0]} .. {dates[-1]}, "
                         f"events {min(r[0] for r in rows)} .. {max(r[0] for r in rows)}")
        parsed.append((f, rows))

    if dry_run:
        logger.info("Dry run: skipping database upload")
        return

    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT, connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA}")
        total_ins = total_upd = 0
        for f, rows in parsed:
            if not rows:
                continue
            ins, upd = upsert(conn, rows, logger)
            conn.commit()
            total_ins += ins
            total_upd += upd
            logger.info(f"{f.name}: inserted {ins}, updated {upd}, unchanged {len(rows) - ins - upd}")
        logger.info(f"OTE IntradayLimit upload: {total_ins} inserted, {total_upd} updated "
                    f"across {len(parsed)} file(s)")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
