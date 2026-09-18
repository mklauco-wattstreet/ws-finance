#!/usr/bin/env python3
"""
Upload OTE "Fin. security trend" XLSX exports to finance.ote_financial_security_trend.

Usage:
    python3 upload_financial_security_trend.py PATH [--debug] [--dry-run]

PATH is a single .xlsx file or a directory; a directory is scanned for
FS_trend_*.xlsx and for the portal's own financial_security_trend_main_*.xlsx.

Not to be confused with "Fin. security trend for limit IM"
(financial_security_trend_offline_main) - that one is upload_intraday_limit.py.
The two reports differ in title, columns and key; this parser refuses a file
whose header row does not carry the Utilization type / Typ utilizace column.

XLSX layout (CZ and EN exports share it):
    row 2  : report title
    row 3  : period from / to
    row 6  : column headers (merged cells, so header and value columns differ)
    row 8+ : data, one row per event x utilization bucket
"""

import re
import sys
import warnings
from decimal import Decimal
from pathlib import Path

import openpyxl
import psycopg2
from psycopg2 import extras

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA
from common import setup_logging

TABLE = "ote_financial_security_trend"

HEADER_TIMESTAMP = {"Timestamp", "Datumová značka"}
HEADER_UTIL_TYPE = {"Utilization type", "Typ utilizace"}
COL_TIMESTAMP = 2
COL_UTIL_TYPE = 5
COL_TRADE_TYPE = 7
COL_TRADE_ID = 9
COL_DELIVERY_DATE = 11
COL_TRADE = 15
COL_UTILIZATION = 18
COL_TOTAL_BY_TYPE = 23
COL_TOTAL = 25
COL_STATIC_LIMIT = 28
COL_DYNAMIC_LIMIT = 30
COL_TOTAL_LIMIT = 34
COL_FREE = 37
COL_PERIOD_FROM = 32
COL_PERIOD_TO = 38
PAD_TO = max(COL_FREE, COL_PERIOD_TO) + 1

# "10.40 EUR", "1,000.00 EUR", "-12.50 CZK"
TRADE_RE = re.compile(r"^\s*(-?[\d,\s\xa0]*\d(?:\.\d+)?)\s*([A-Z]{3})\s*$")

COLUMNS = (
    "event_timestamp", "utilization_type", "trade_type", "trade_id", "delivery_date",
    "trade_price", "trade_currency",
    "utilization_czk", "total_utilization_by_type_czk", "total_utilization_czk",
    "static_limit_czk", "dynamic_limit_czk", "total_limit_czk", "free_resources_czk",
)
KEY_COLUMNS = ("event_timestamp", "utilization_type", "trade_id")
UPDATE_COLUMNS = tuple(c for c in COLUMNS if c not in KEY_COLUMNS)

UPSERT_SQL = f"""
    INSERT INTO {TABLE} AS t ({", ".join(COLUMNS)})
    VALUES %s
    ON CONFLICT ({", ".join(KEY_COLUMNS)}) DO UPDATE SET
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
    cleaned = str(value).replace("\xa0", "").replace(" ", "").replace(",", "")
    return Decimal(cleaned)


def _trade(value):
    """'1,000.00 EUR' -> (Decimal('1000.00'), 'EUR'); None/blank -> (None, None)."""
    if value is None or str(value).strip() == "":
        return None, None
    m = TRADE_RE.match(str(value))
    if not m:
        raise ValueError(f"unparseable trade price {value!r}")
    return _num(m.group(1)), m.group(2)


def parse_xlsx(path, logger):
    """Return (rows, meta). rows are tuples in COLUMNS order, deduplicated on key."""
    with warnings.catch_warnings():
        # the portal's XLSX has no default style; openpyxl warns and applies its own
        warnings.simplefilter("ignore", UserWarning)
        wb = openpyxl.load_workbook(path, data_only=True)
    try:
        ws = wb.worksheets[0]
        title = None
        period = None
        header_seen = False
        records = {}
        dupes = 0
        skipped = 0

        for row in ws.iter_rows(values_only=True):
            cells = list(row) + [None] * (PAD_TO - len(row))
            if not header_seen:
                if title is None and cells[1] and isinstance(cells[1], str):
                    title = cells[1].strip()
                if period is None and cells[COL_PERIOD_FROM] is not None and cells[COL_PERIOD_TO] is not None:
                    period = (cells[COL_PERIOD_FROM], cells[COL_PERIOD_TO])
                if isinstance(cells[COL_TIMESTAMP], str) and cells[COL_TIMESTAMP].strip() in HEADER_TIMESTAMP:
                    util_header = cells[COL_UTIL_TYPE]
                    if not (isinstance(util_header, str) and util_header.strip() in HEADER_UTIL_TYPE):
                        raise ValueError(
                            f"header row found but column {COL_UTIL_TYPE} is {util_header!r}, "
                            "not 'Utilization type' - is this the 'for limit IM' report? "
                            "(use upload_intraday_limit.py for that one)"
                        )
                    header_seen = True
                continue

            ts = cells[COL_TIMESTAMP]
            if not hasattr(ts, "year"):
                if ts is not None:
                    skipped += 1
                continue

            delivery = cells[COL_DELIVERY_DATE]
            if delivery is not None and not hasattr(delivery, "year"):
                raise ValueError(f"row at {ts}: unparseable delivery day {delivery!r}")
            trade_price, trade_currency = _trade(cells[COL_TRADE])
            trade_id = int(_num(cells[COL_TRADE_ID]))
            utilization_type = str(cells[COL_UTIL_TYPE]).strip()

            record = (
                ts,
                utilization_type,
                str(cells[COL_TRADE_TYPE]).strip(),
                trade_id,
                delivery.date() if delivery is not None and hasattr(delivery, "date") else delivery,
                trade_price,
                trade_currency,
                _num(cells[COL_UTILIZATION]),
                _num(cells[COL_TOTAL_BY_TYPE]),
                _num(cells[COL_TOTAL]),
                _num(cells[COL_STATIC_LIMIT]),
                _num(cells[COL_DYNAMIC_LIMIT]),
                _num(cells[COL_TOTAL_LIMIT]),
                _num(cells[COL_FREE]),
            )
            key = (ts, utilization_type, trade_id)
            if key in records:
                dupes += 1
            records[key] = record

        if not header_seen:
            raise ValueError("header row not found (expected 'Timestamp' / 'Datumová značka')")
        if dupes:
            logger.warning(f"{path.name}: {dupes} duplicate (timestamp, utilization_type, trade_id) "
                           f"rows collapsed to the last occurrence")
        if skipped:
            logger.debug(f"{path.name}: {skipped} non-data rows skipped")
        return list(records.values()), {"title": title, "period": period}
    finally:
        wb.close()


def upsert(conn, rows):
    """Bulk upsert; returns (inserted, updated)."""
    with conn.cursor() as cur:
        result = extras.execute_values(cur, UPSERT_SQL, rows, page_size=1000, fetch=True)
    inserted = sum(1 for (flag,) in result if flag)
    return inserted, len(result) - inserted


def collect_files(path):
    p = Path(path)
    if p.is_file():
        return [p]
    if p.is_dir():
        return sorted(set(p.glob("FS_trend_*.xlsx")) | set(p.glob("financial_security_trend_main_*.xlsx")))
    return []


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    debug = "--debug" in sys.argv
    dry_run = "--dry-run" in sys.argv
    logger = setup_logging(debug=debug)

    if not args:
        logger.error("Usage: upload_financial_security_trend.py PATH [--debug] [--dry-run]")
        sys.exit(2)

    files = collect_files(args[0])
    if not files:
        logger.error(f"No FS_trend_*.xlsx / financial_security_trend_main_*.xlsx under {args[0]}")
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
            logger.debug(f"  events {min(r[0] for r in rows)} .. {max(r[0] for r in rows)}")
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
            ins, upd = upsert(conn, rows)
            conn.commit()
            total_ins += ins
            total_upd += upd
            logger.info(f"{f.name}: inserted {ins}, updated {upd}, unchanged {len(rows) - ins - upd}")
        logger.info(f"OTE FinancialSecurityTrend upload: {total_ins} inserted, {total_upd} updated "
                    f"across {len(parsed)} file(s)")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
