#!/usr/bin/env python3
"""
Upload DAM matching curve XML files to PostgreSQL database.

Parses OTE-CR matching curve XML, upserts into da_bid table,
then computes and upserts da_period_summary analytics.

Usage:
    python3 upload_dam_curves.py PATH_TO_DIRECTORY [--debug]

Example:
    python3 upload_dam_curves.py 2026/03
    python3 upload_dam_curves.py 2026/01 --debug
"""

import sys
import re
from decimal import Decimal
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import extras

from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA
from common import setup_logging, print_banner

# Fixed jump threshold for future filtering/flagging
JUMP_THRESHOLD_EUR = Decimal('20.00')

# MW offsets for curve depth analysis (extend here, no schema change needed)
CURVE_DEPTH_OFFSETS_MW = [50, 100, 200, 500, 1000]

# Mapping from XML trade_type to DB side
TRADE_TYPE_MAP = {
    'B': 'buy',
    'S': 'sell',
}

# Mapping from XML order_resolution to DB value
ORDER_RESOLUTION_MAP = {
    'PT15M': '15min',
    'PT60M': '60min',
}


def generate_time_interval(period):
    """
    Generate time interval string for a given period (1-96).

    Args:
        period: Period number (1-96)

    Returns:
        str: Time interval in format "HH:MM-HH:MM"
    """
    start_minutes = (period - 1) * 15
    end_minutes = period * 15

    start_hour = start_minutes // 60
    start_min = start_minutes % 60
    end_hour = end_minutes // 60
    end_min = end_minutes % 60

    return f"{start_hour:02d}:{start_min:02d}-{end_hour:02d}:{end_min:02d}"


def parse_date_from_filename(filename):
    """
    Extract delivery date from MC XML filename.

    Expected format: MC_DD_MM_YYYY_EN.xml

    Args:
        filename: Name of the XML file

    Returns:
        datetime.date object or None if parsing fails
    """
    pattern = r'MC_(\d{2})_(\d{2})_(\d{4})_EN\.xml'
    match = re.match(pattern, filename)

    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day)).date()
        except ValueError:
            return None
    return None


def parse_xml_file(file_path, delivery_date):
    """
    Parse DAM matching curve XML file into bid records.

    XML structure per row:
    <data date-time="YYYY-MM-DD" period="1-96" time_interval="HH:MM-HH:MM"
          price="..." energy_order="..." energy_match="..."
          trade_type="B|S" order_resolution="PT15M|PT60M"/>

    Period in XML is already global 1-96 (sequential across all hours).

    Args:
        file_path: Path to XML file
        delivery_date: Expected delivery date (for validation)

    Returns:
        list of tuples for da_bid upsert
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Handle namespace if present
    ns = ''
    if '}' in root.tag:
        ns = root.tag.split('}')[0] + '}'

    data_elements = root.findall(f'{ns}data')

    records = []
    for elem in data_elements:
        attrs = elem.attrib

        # Map trade_type and order_resolution
        trade_type = attrs.get('trade_type', '')
        side = TRADE_TYPE_MAP.get(trade_type)
        if side is None:
            continue

        raw_resolution = attrs.get('order_resolution', '')
        order_resolution = ORDER_RESOLUTION_MAP.get(raw_resolution)
        if order_resolution is None:
            continue

        # Parse price and volumes
        price = Decimal(attrs.get('price', '0'))
        volume_bid = Decimal(attrs.get('energy_order', '0'))
        volume_matched = Decimal(attrs.get('energy_match', '0'))

        # XML period is already global 1-96
        global_period = int(attrs.get('period', '1'))
        time_interval = generate_time_interval(global_period)

        records.append((
            delivery_date,
            global_period,
            time_interval,
            side,
            price,
            volume_bid,
            volume_matched,
            order_resolution,
        ))

    return records


def upsert_da_bid(records, conn, logger):
    """
    Bulk upsert bid records into da_bid table.

    Args:
        records: List of tuples (delivery_date, period, time_interval, side, price, volume_bid, volume_matched, order_resolution)
        conn: Database connection
        logger: Logger instance

    Returns:
        int: Number of records upserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO da_bid (
            delivery_date, period, time_interval, side, price,
            volume_bid, volume_matched, order_resolution
        ) VALUES %s
        ON CONFLICT (delivery_date, period, side, price, order_resolution) DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            volume_bid = EXCLUDED.volume_bid,
            volume_matched = EXCLUDED.volume_matched
    """

    try:
        extras.execute_values(cursor, upsert_query, records)
        conn.commit()
        count = len(records)
        cursor.close()
        return count
    except Exception as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"da_bid upsert error: {e}")


def compute_and_upsert_period_summary(delivery_date, conn, logger):
    """
    Compute da_period_summary from da_bid data and upsert results.

    For each period:
    - clearing_price: MAX(price) WHERE side='sell' AND volume_matched > 0
    - clearing_volume: SUM(volume_matched) WHERE side='sell' AND volume_matched > 0
    - supply_next: first unmatched sell bid above clearing price
    - demand_next: first unmatched buy bid below clearing price

    Args:
        delivery_date: Date to compute summary for
        conn: Database connection
        logger: Logger instance

    Returns:
        int: Number of summary records upserted
    """
    cursor = conn.cursor()

    # CTE-based query to compute all summary metrics in one pass
    summary_query = """
        WITH clearing AS (
            SELECT
                period,
                MIN(time_interval) AS time_interval,
                MAX(price) AS clearing_price,
                SUM(volume_matched) AS clearing_volume
            FROM da_bid
            WHERE delivery_date = %s
              AND side = 'sell'
              AND volume_matched > 0
            GROUP BY period
        ),
        supply_next AS (
            SELECT DISTINCT ON (b.period)
                b.period,
                b.price AS supply_next_price,
                b.volume_bid AS supply_next_volume
            FROM da_bid b
            JOIN clearing c ON c.period = b.period
            WHERE b.delivery_date = %s
              AND b.side = 'sell'
              AND b.volume_matched = 0
              AND b.price > c.clearing_price
            ORDER BY b.period, b.price ASC
        ),
        demand_next AS (
            SELECT DISTINCT ON (b.period)
                b.period,
                b.price AS demand_next_price,
                b.volume_bid AS demand_next_volume
            FROM da_bid b
            JOIN clearing c ON c.period = b.period
            WHERE b.delivery_date = %s
              AND b.side = 'buy'
              AND b.volume_matched = 0
              AND b.price < c.clearing_price
            ORDER BY b.period, b.price DESC
        ),
        supply_gap AS (
            SELECT
                b.period,
                SUM(b.volume_bid) AS supply_volume_gap
            FROM da_bid b
            JOIN clearing c ON c.period = b.period
            JOIN supply_next sn ON sn.period = b.period
            WHERE b.delivery_date = %s
              AND b.side = 'sell'
              AND b.volume_matched = 0
              AND b.price > c.clearing_price
              AND b.price < sn.supply_next_price
            GROUP BY b.period
        ),
        demand_gap AS (
            SELECT
                b.period,
                SUM(b.volume_bid) AS demand_volume_gap
            FROM da_bid b
            JOIN clearing c ON c.period = b.period
            JOIN demand_next dn ON dn.period = b.period
            WHERE b.delivery_date = %s
              AND b.side = 'buy'
              AND b.volume_matched = 0
              AND b.price < c.clearing_price
              AND b.price > dn.demand_next_price
            GROUP BY b.period
        )
        SELECT
            c.period,
            c.time_interval,
            c.clearing_price,
            c.clearing_volume,
            sn.supply_next_price,
            sn.supply_next_volume,
            sn.supply_next_price - c.clearing_price AS supply_price_gap,
            COALESCE(sg.supply_volume_gap, 0) AS supply_volume_gap,
            dn.demand_next_price,
            dn.demand_next_volume,
            c.clearing_price - dn.demand_next_price AS demand_price_gap,
            COALESCE(dg.demand_volume_gap, 0) AS demand_volume_gap
        FROM clearing c
        LEFT JOIN supply_next sn ON sn.period = c.period
        LEFT JOIN demand_next dn ON dn.period = c.period
        LEFT JOIN supply_gap sg ON sg.period = c.period
        LEFT JOIN demand_gap dg ON dg.period = c.period
        ORDER BY c.period
    """

    try:
        cursor.execute(summary_query, (delivery_date, delivery_date, delivery_date, delivery_date, delivery_date))
        rows = cursor.fetchall()

        if not rows:
            cursor.close()
            return 0

        # Build upsert values
        summary_records = []
        for row in rows:
            summary_records.append((
                delivery_date,      # delivery_date
                row[0],             # period
                row[1],             # time_interval
                row[2],             # clearing_price
                row[3],             # clearing_volume
                row[4],             # supply_next_price
                row[5],             # supply_next_volume
                row[6],             # supply_price_gap
                row[7],             # supply_volume_gap
                row[8],             # demand_next_price
                row[9],             # demand_next_volume
                row[10],            # demand_price_gap
                row[11],            # demand_volume_gap
            ))

        upsert_summary = """
            INSERT INTO da_period_summary (
                delivery_date, period, time_interval,
                clearing_price, clearing_volume,
                supply_next_price, supply_next_volume, supply_price_gap, supply_volume_gap,
                demand_next_price, demand_next_volume, demand_price_gap, demand_volume_gap
            ) VALUES %s
            ON CONFLICT (delivery_date, period) DO UPDATE SET
                time_interval = EXCLUDED.time_interval,
                clearing_price = EXCLUDED.clearing_price,
                clearing_volume = EXCLUDED.clearing_volume,
                supply_next_price = EXCLUDED.supply_next_price,
                supply_next_volume = EXCLUDED.supply_next_volume,
                supply_price_gap = EXCLUDED.supply_price_gap,
                supply_volume_gap = EXCLUDED.supply_volume_gap,
                demand_next_price = EXCLUDED.demand_next_price,
                demand_next_volume = EXCLUDED.demand_next_volume,
                demand_price_gap = EXCLUDED.demand_price_gap,
                demand_volume_gap = EXCLUDED.demand_volume_gap
        """

        extras.execute_values(cursor, upsert_summary, summary_records)
        conn.commit()
        count = len(summary_records)
        cursor.close()
        return count

    except Exception as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"da_period_summary compute error: {e}")


def compute_and_upsert_curve_depth(delivery_date, conn, logger):
    """
    Compute da_curve_depth from da_bid data and upsert results.

    For each period and side, walks the unmatched bid stack in price order
    (ascending for sell, descending for buy) and finds the price where
    cumulative unmatched volume first reaches each offset in CURVE_DEPTH_OFFSETS_MW.

    Args:
        delivery_date: Date to compute depth for
        conn: Database connection
        logger: Logger instance

    Returns:
        int: Number of depth records upserted
    """
    cursor = conn.cursor()

    # Build VALUES list for offsets: (50),(100),(200),(500),(1000)
    offset_values = ','.join(f'({o})' for o in CURVE_DEPTH_OFFSETS_MW)

    depth_query = f"""
        WITH clearing AS (
            SELECT
                period,
                MIN(time_interval) AS time_interval,
                MAX(price) AS clearing_price
            FROM da_bid
            WHERE delivery_date = %s
              AND side = 'sell'
              AND volume_matched > 0
            GROUP BY period
        ),
        unmatched_sell AS (
            SELECT
                b.period,
                b.price,
                b.volume_bid,
                SUM(b.volume_bid) OVER (PARTITION BY b.period ORDER BY b.price ASC) AS cum_vol
            FROM da_bid b
            JOIN clearing c ON c.period = b.period
            WHERE b.delivery_date = %s
              AND b.side = 'sell'
              AND b.volume_matched = 0
              AND b.price > c.clearing_price
        ),
        unmatched_buy AS (
            SELECT
                b.period,
                b.price,
                b.volume_bid,
                SUM(b.volume_bid) OVER (PARTITION BY b.period ORDER BY b.price DESC) AS cum_vol
            FROM da_bid b
            JOIN clearing c ON c.period = b.period
            WHERE b.delivery_date = %s
              AND b.side = 'buy'
              AND b.volume_matched = 0
              AND b.price < c.clearing_price
        ),
        sell_total AS (
            SELECT period, SUM(volume_bid) AS total_vol
            FROM da_bid
            WHERE delivery_date = %s AND side = 'sell' AND volume_matched = 0
            GROUP BY period
        ),
        buy_total AS (
            SELECT period, SUM(volume_bid) AS total_vol
            FROM da_bid
            WHERE delivery_date = %s AND side = 'buy' AND volume_matched = 0
            GROUP BY period
        ),
        offsets(offset_mw) AS (VALUES {offset_values}),
        sell_crossing AS (
            SELECT DISTINCT ON (us.period, o.offset_mw)
                us.period, o.offset_mw, us.price AS price_at_offset
            FROM unmatched_sell us
            CROSS JOIN offsets o
            WHERE us.cum_vol >= o.offset_mw
            ORDER BY us.period, o.offset_mw, us.price ASC
        ),
        buy_crossing AS (
            SELECT DISTINCT ON (ub.period, o.offset_mw)
                ub.period, o.offset_mw, ub.price AS price_at_offset
            FROM unmatched_buy ub
            CROSS JOIN offsets o
            WHERE ub.cum_vol >= o.offset_mw
            ORDER BY ub.period, o.offset_mw, ub.price DESC
        ),
        all_combos AS (
            SELECT c.period, c.time_interval, 'sell' AS side, o.offset_mw
            FROM clearing c CROSS JOIN offsets o
            UNION ALL
            SELECT c.period, c.time_interval, 'buy' AS side, o.offset_mw
            FROM clearing c CROSS JOIN offsets o
        )
        SELECT
            ac.period,
            ac.time_interval,
            ac.side,
            ac.offset_mw,
            CASE ac.side
                WHEN 'sell' THEN sc.price_at_offset
                WHEN 'buy' THEN bc.price_at_offset
            END AS price_at_offset,
            CASE ac.side
                WHEN 'sell' THEN st.total_vol
                WHEN 'buy' THEN bt.total_vol
            END AS volume_available
        FROM all_combos ac
        LEFT JOIN sell_crossing sc ON sc.period = ac.period AND sc.offset_mw = ac.offset_mw AND ac.side = 'sell'
        LEFT JOIN buy_crossing bc ON bc.period = ac.period AND bc.offset_mw = ac.offset_mw AND ac.side = 'buy'
        LEFT JOIN sell_total st ON st.period = ac.period AND ac.side = 'sell'
        LEFT JOIN buy_total bt ON bt.period = ac.period AND ac.side = 'buy'
        ORDER BY ac.period, ac.side, ac.offset_mw
    """

    try:
        cursor.execute(depth_query, (delivery_date,) * 5)
        rows = cursor.fetchall()

        if not rows:
            cursor.close()
            return 0

        records = []
        for row in rows:
            records.append((
                delivery_date,
                row[0],  # period
                row[1],  # time_interval
                row[2],  # side
                row[3],  # offset_mw
                row[4],  # price_at_offset (NULL if exhausted)
                row[5],  # volume_available
            ))

        upsert_depth = """
            INSERT INTO da_curve_depth (
                delivery_date, period, time_interval, side,
                offset_mw, price_at_offset, volume_available
            ) VALUES %s
            ON CONFLICT (delivery_date, period, side, offset_mw) DO UPDATE SET
                time_interval = EXCLUDED.time_interval,
                price_at_offset = EXCLUDED.price_at_offset,
                volume_available = EXCLUDED.volume_available
        """

        extras.execute_values(cursor, upsert_depth, records)
        conn.commit()
        count = len(records)
        cursor.close()
        return count

    except Exception as e:
        conn.rollback()
        cursor.close()
        raise Exception(f"da_curve_depth compute error: {e}")


def print_debug_info(records, delivery_date):
    """
    Print debug information for parsed bid records.

    Args:
        records: List of bid record tuples
        delivery_date: Delivery date for display
    """
    print(f"\n{'═' * 100}")
    print(f"DEBUG MODE - DAM Matching Curve for {delivery_date}")
    print(f"{'═' * 100}")
    print(f"Total bid records: {len(records)}")

    # Count by side and resolution
    buy_count = sum(1 for r in records if r[3] == 'buy')
    sell_count = sum(1 for r in records if r[3] == 'sell')
    res_15 = sum(1 for r in records if r[7] == '15min')
    res_60 = sum(1 for r in records if r[7] == '60min')
    print(f"Buy bids: {buy_count}, Sell bids: {sell_count}")
    print(f"15min products: {res_15}, 60min products: {res_60}")

    # Show first 20 records
    print(f"\n{'─' * 100}")
    print(f"{'Date':<12} {'Per':>3} {'Time':<11} {'Side':<5} {'Price':>8} {'VolBid':>10} {'VolMatch':>10} {'Res':<5}")
    print(f"{'─' * 100}")

    for r in records[:20]:
        print(f"{r[0]!s:<12} {r[1]:>3} {r[2]:<11} {r[3]:<5} {r[4]:>8} {r[5]:>10} {r[6]:>10} {r[7]:<5}")

    if len(records) > 20:
        print(f"  ... ({len(records) - 20} more records)")

    print(f"{'═' * 100}\n")


def process_directory(directory_path, logger, debug_mode=False):
    """
    Process all MC XML files in a directory.

    Args:
        directory_path: Path to directory containing XML files
        logger: Logger instance
        debug_mode: If True, print debug info and don't insert to database
    """
    dir_path = Path(directory_path)

    if not dir_path.exists():
        logger.error(f"Directory '{directory_path}' does not exist")
        return False

    if not dir_path.is_dir():
        logger.error(f"'{directory_path}' is not a directory")
        return False

    xml_files = sorted(dir_path.glob("MC_*.xml"))

    if not xml_files:
        logger.warning(f"No MC XML files found in '{directory_path}'")
        return True

    logger.info(f"\nDirectory: {dir_path.absolute()}")
    logger.info(f"Found {len(xml_files)} XML file(s)\n")

    conn = None
    if not debug_mode:
        logger.info("Connecting to database...")
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                port=DB_PORT,
                connect_timeout=10,
                options=f'-c search_path={DB_SCHEMA}'
            )
            logger.info("Database connection established\n")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    total_bids = 0
    total_summaries = 0
    total_depths = 0
    files_processed = 0
    files_failed = 0

    try:
        for xml_file in xml_files:
            logger.info(f"{'─' * 60}")
            logger.info(f"Processing: {xml_file.name}")

            delivery_date = parse_date_from_filename(xml_file.name)

            if not delivery_date:
                logger.warning(f"  Failed to extract date from filename")
                files_failed += 1
                continue

            logger.info(f"  Delivery date: {delivery_date}")

            try:
                records = parse_xml_file(xml_file, delivery_date)

                if not records:
                    logger.warning(f"  No valid records found")
                    files_failed += 1
                    continue

                logger.info(f"  Parsed {len(records)} bid records")

                if debug_mode:
                    print_debug_info(records, delivery_date)
                    files_processed += 1
                else:
                    # Upsert bids
                    bid_count = upsert_da_bid(records, conn, logger)
                    total_bids += bid_count
                    logger.info(f"  Upserted {bid_count} bid records")

                    # Compute and upsert period summary
                    summary_count = compute_and_upsert_period_summary(delivery_date, conn, logger)
                    total_summaries += summary_count
                    logger.info(f"  Computed {summary_count} period summaries")

                    # Compute and upsert curve depth
                    depth_count = compute_and_upsert_curve_depth(delivery_date, conn, logger)
                    total_depths += depth_count
                    logger.info(f"  Computed {depth_count} curve depth records")

                    files_processed += 1

            except Exception as e:
                logger.error(f"  Error processing file: {e}")
                files_failed += 1
                continue

        logger.info(f"\n{'═' * 60}")
        logger.info(f"UPLOAD SUMMARY")
        logger.info(f"{'═' * 60}")
        logger.info(f"Files processed successfully: {files_processed}")
        logger.info(f"Files failed: {files_failed}")
        if not debug_mode:
            logger.info(f"Total bid records upserted: {total_bids}")
            logger.info(f"Total period summaries computed: {total_summaries}")
            logger.info(f"Total curve depth records computed: {total_depths}")
        else:
            logger.info(f"DEBUG MODE - No records upserted to database")
        logger.info(f"{'═' * 60}\n")

        return True

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def main():
    """Main function."""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python3 upload_dam_curves.py PATH_TO_DIRECTORY [--debug]")
        print("\nExamples:")
        print("  python3 upload_dam_curves.py 2026/03")
        print("  python3 upload_dam_curves.py 2026/01 --debug")
        sys.exit(1)

    directory_path = sys.argv[1]
    debug_mode = len(sys.argv) == 3 and sys.argv[2] == '--debug'

    logger = setup_logging(debug=debug_mode)

    print_banner("DAM Matching Curve Uploader", debug_mode)

    try:
        success = process_directory(directory_path, logger, debug_mode)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("\n\nUpload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
