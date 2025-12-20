#!/usr/bin/env python3
"""
Upload OTE Daily Payments XML file to PostgreSQL database.

Usage: python ote_upload_daily_payments.py <xml_file_path>
Example: python ote_upload_daily_payments.py /app/ote_files/2025/11/daily_payments_20251111_211920.xml
"""

import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2 import OperationalError, DatabaseError, IntegrityError

# Import database configuration and logging
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, DB_SCHEMA
from common import setup_logging

# XML namespace for OTE portal exports
NAMESPACE = {'ns': 'http://www.ote-cr.cz/xmlschemas/grid/xmlexport'}


def parse_date(date_str):
    """
    Parse date from DD/MM/YYYY format to YYYY-MM-DD.

    Args:
        date_str: Date string in DD/MM/YYYY format

    Returns:
        str: Date in YYYY-MM-DD format or None if invalid
    """
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), '%d/%m/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError as e:
        return None


def parse_float(value_str):
    """
    Parse float value, handling commas and empty strings.

    Args:
        value_str: String representation of float

    Returns:
        float or None
    """
    if not value_str or value_str.strip() == '':
        return None
    try:
        # Remove comma thousands separators before parsing
        cleaned_value = value_str.replace(',', '')
        return float(cleaned_value)
    except ValueError:
        return None


def parse_xml_file(xml_filename, logger):
    """
    Parse OTE Daily Payments XML file and extract payment records.

    Args:
        xml_filename: Path to XML file
        logger: Logger instance

    Returns:
        list: List of dictionaries containing payment records
    """
    logger.info(f"Parsing XML file: {xml_filename}")

    try:
        tree = ET.parse(xml_filename)
        root = tree.getroot()

        records = []

        # Find all Row elements in Data section
        for row in root.findall('.//ns:Data/ns:Row', NAMESPACE):
            # Extract column values
            columns = {}
            for col in row.findall('ns:Column', NAMESPACE):
                col_id = col.get('id')
                col_value = col.get('value', '')
                columns[col_id] = col_value

            # Map XML columns to database fields
            # Column mapping based on OTE portal export format:
            # 1: Delivery day (DD/MM/YYYY)
            # 2: Settlement version
            # 3: Settlement item
            # 4: Type of payment
            # 5: Volume (MWh)
            # 6: Amount excl. VAT
            # 7: Currency of payment
            # 8: Currency rate
            # 9: System
            # 10: Message

            record = {
                'delivery_day': parse_date(columns.get('1', '')),
                'settlement_version': columns.get('2', None) if columns.get('2', '').strip() else None,
                'settlement_item': columns.get('3', None) if columns.get('3', '').strip() else None,
                'type_of_payment': columns.get('4', None) if columns.get('4', '').strip() else None,
                'volume_mwh': parse_float(columns.get('5', '')),
                'amount_excl_vat': parse_float(columns.get('6', '')),
                'currency_of_payment': columns.get('7', None) if columns.get('7', '').strip() else None,
                'currency_rate': parse_float(columns.get('8', '')),
                'system': columns.get('9', None) if columns.get('9', '').strip() else None,
                'message': columns.get('10', None) if columns.get('10', '').strip() else None
            }

            records.append(record)

        logger.info(f"✓ Found {len(records)} records in XML file")
        return records

    except ET.ParseError as e:
        logger.error(f"✗ XML parsing error: {e}")
        return None
    except Exception as e:
        logger.error(f"✗ Unexpected error while parsing XML: {e}")
        return None


def insert_records(records, logger):
    """
    Insert records into PostgreSQL database with duplicate checking.

    Args:
        records: List of dictionaries containing payment records
        logger: Logger instance

    Returns:
        bool: True if successful, False otherwise
    """
    if not records:
        logger.warning("No records to insert")
        return False

    try:
        # Connect to database
        logger.info("Connecting to database...")
        logger.info(f"  Host: {DB_HOST}")
        logger.info(f"  Database: {DB_NAME}")
        logger.info(f"  User: {DB_USER}")

        connection = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            connect_timeout=10,
            options=f'-c search_path={DB_SCHEMA}'
        )

        logger.info("✓ Database connection established")

        cursor = connection.cursor()

        # Prepare duplicate check query
        # Check for duplicates based on delivery_day, settlement_version, settlement_item, and type_of_payment
        check_query = """
            SELECT COUNT(*) FROM ote_daily_payments
            WHERE delivery_day = %s
              AND settlement_version = %s
              AND settlement_item = %s
              AND type_of_payment = %s
        """

        # Prepare INSERT statement
        insert_query = """
            INSERT INTO ote_daily_payments (
                delivery_day,
                settlement_version,
                settlement_item,
                type_of_payment,
                volume_mwh,
                amount_excl_vat,
                currency_of_payment,
                currency_rate,
                system,
                message
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        # Insert records with duplicate checking
        inserted_count = 0
        skipped_count = 0
        error_count = 0

        logger.info(f"\nProcessing {len(records)} records:")
        logger.info("─" * 80)

        for idx, record in enumerate(records, 1):
            try:
                # Check if record already exists
                cursor.execute(check_query, (
                    record['delivery_day'],
                    record['settlement_version'],
                    record['settlement_item'],
                    record['type_of_payment']
                ))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    skipped_count += 1
                    logger.debug(f"  [{idx}/{len(records)}] SKIP: {record['delivery_day']} | "
                               f"{record['settlement_version']} | {record['settlement_item']} | "
                               f"{record['type_of_payment']}")
                    continue

                # Insert new record
                cursor.execute(insert_query, (
                    record['delivery_day'],
                    record['settlement_version'],
                    record['settlement_item'],
                    record['type_of_payment'],
                    record['volume_mwh'],
                    record['amount_excl_vat'],
                    record['currency_of_payment'],
                    record['currency_rate'],
                    record['system'],
                    record['message']
                ))
                inserted_count += 1
                logger.debug(f"  [{idx}/{len(records)}] INSERT: {record['delivery_day']} | "
                           f"{record['settlement_version']} | {record['settlement_item']} | "
                           f"{record['type_of_payment']}")

            except DatabaseError as e:
                error_count += 1
                logger.error(f"  [{idx}/{len(records)}] ERROR: {e}")
                logger.error(f"  Record: {record}")
                connection.rollback()
                continue

        # Commit transaction
        connection.commit()

        # Summary
        logger.info("─" * 80)
        logger.info(f"\nUpload Summary:")
        logger.info(f"  Total records: {len(records)}")
        logger.info(f"  ✓ Inserted: {inserted_count}")
        if skipped_count > 0:
            logger.info(f"  ⊘ Skipped (duplicates): {skipped_count}")
        if error_count > 0:
            logger.warning(f"  ✗ Errors: {error_count}")

        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("✓ Database connection closed")

        return error_count == 0

    except OperationalError as e:
        logger.error(f"✗ Database connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ Unexpected database error: {e}")
        return False


def main():
    """Main function."""
    # Setup logging
    debug_mode = '--debug' in sys.argv
    logger = setup_logging(debug=debug_mode)

    # Parse arguments
    if len(sys.argv) < 2:
        logger.error("Usage: python ote_upload_daily_payments.py <xml_file_path> [--debug]")
        logger.error("Example: python ote_upload_daily_payments.py /app/ote_files/2025/11/daily_payments_20251111_211920.xml")
        sys.exit(1)

    xml_filename = sys.argv[1]

    logger.info("=" * 80)
    logger.info("OTE Daily Payments Upload")
    logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    logger.info(f"Input file: {xml_filename}")

    # Check if file exists
    xml_path = Path(xml_filename)
    if not xml_path.exists():
        logger.error(f"✗ File not found: {xml_filename}")
        sys.exit(1)

    if not xml_path.is_file():
        logger.error(f"✗ Not a file: {xml_filename}")
        sys.exit(1)

    logger.info(f"File size: {xml_path.stat().st_size:,} bytes")
    logger.info("")

    # Parse XML file
    records = parse_xml_file(xml_filename, logger)

    if records is None:
        logger.error("✗ Failed to parse XML file")
        sys.exit(1)

    if not records:
        logger.warning("⚠ No records found in XML file")
        sys.exit(0)

    logger.info("")

    # Insert records into database
    logger.info("Uploading records to database...")
    logger.info("─" * 80)
    success = insert_records(records, logger)

    logger.info("=" * 80)
    if success:
        logger.info("✓ Upload completed successfully!")
        logger.info("=" * 80)
        sys.exit(0)
    else:
        logger.error("✗ Upload completed with errors")
        logger.info("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
