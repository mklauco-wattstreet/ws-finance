#!/usr/bin/env python3
"""
Fetch ENTSO-E data for a specific period and parse it.

Usage:
    python3 fetch_and_parse.py <start_period> <end_period>

Example:
    python3 fetch_and_parse.py 202511101630 202511102115
"""

import sys
from pathlib import Path
from datetime import datetime

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from entsoe.entsoe_client import EntsoeClient
from entsoe.parse_imbalance_to_db import ImbalanceDataParser


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 fetch_and_parse.py <start_period> <end_period>")
        print("Example: python3 fetch_and_parse.py 202511101630 202511102115")
        sys.exit(1)

    start_str = sys.argv[1]
    end_str = sys.argv[2]

    # Parse datetime
    start_dt = datetime.strptime(start_str, '%Y%m%d%H%M')
    end_dt = datetime.strptime(end_str, '%Y%m%d%H%M')

    print(f"\n{'=' * 80}")
    print(f"ENTSO-E Fetch and Parse Pipeline")
    print(f"{'=' * 80}")
    print(f"Period: {start_dt} to {end_dt}")
    print(f"{'=' * 80}\n")

    # Setup output directory
    output_dir = Path(f'/app/scripts/entsoe/data/{start_dt.year}/{start_dt.month:02d}')
    output_dir.mkdir(parents=True, exist_ok=True)

    prices_file = output_dir / f'entsoe_imbalance_prices_{start_str}_{end_str}.xml'
    volumes_file = output_dir / f'entsoe_imbalance_volumes_{start_str}_{end_str}.xml'

    # Initialize client
    client = EntsoeClient()

    # Fetch prices (A85)
    print(f"[1/4] Fetching Imbalance Prices (A85)...")
    try:
        xml_prices = client.fetch_data('A85', start_dt, end_dt)
        with open(prices_file, 'w') as f:
            f.write(xml_prices)
        print(f"      ✓ Saved: {prices_file}")
    except Exception as e:
        print(f"      ✗ Error: {e}")
        sys.exit(1)

    # Fetch volumes (A86)
    print(f"\n[2/4] Fetching Imbalance Volumes (A86)...")
    try:
        xml_volumes = client.fetch_data('A86', start_dt, end_dt)
        with open(volumes_file, 'w') as f:
            f.write(xml_volumes)
        print(f"      ✓ Saved: {volumes_file}")
    except Exception as e:
        print(f"      ✗ Error: {e}")
        sys.exit(1)

    # Parse the data
    print(f"\n[3/4] Parsing XML data...")
    parser = ImbalanceDataParser()

    try:
        parser.parse_prices_xml(str(prices_file))
        parser.parse_volumes_xml(str(volumes_file))
        parser.combine_data()
        print(f"      ✓ Parsed {len(parser.combined_data)} combined records")
    except Exception as e:
        print(f"      ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Display results
    print(f"\n[4/4] Displaying results...")
    parser.display_as_markdown_table()

    # Generate SQL
    print(f"\n{'=' * 80}")
    print(f"SQL QUERIES")
    print(f"{'=' * 80}")
    queries = parser.generate_sql_queries()
    print(f"\nGenerated {len(queries)} SQL INSERT queries with UPSERT logic")
    print(f"\nExample query (first record):")
    if queries:
        query, values = queries[0]
        print(query)
        print(f"\nValues: {values}")

    print(f"\n{'=' * 80}")
    print(f"✓ Pipeline completed successfully")
    print(f"{'=' * 80}\n")


if __name__ == '__main__':
    main()
