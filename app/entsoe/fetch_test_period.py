#!/usr/bin/env python3
"""Fetch test data for specific period."""
import sys
sys.path.insert(0, '/app/scripts')
from entsoe.client import EntsoeClient
from datetime import datetime
from pathlib import Path

client = EntsoeClient()
start = datetime(2025, 11, 10, 8, 0)
end = datetime(2025, 11, 10, 12, 0)

# Create output directory
output_dir = Path('/app/scripts/entsoe/data/2025/11')
output_dir.mkdir(parents=True, exist_ok=True)

print(f'Fetching A85 (prices) for {start} to {end}')
xml_prices = client.fetch_data('A85', start, end)
prices_file = output_dir / 'entsoe_imbalance_prices_202511100800_202511101200.xml'
with open(prices_file, 'w') as f:
    f.write(xml_prices)
print(f'Prices saved to {prices_file}')

print(f'\nFetching A86 (volumes) for {start} to {end}')
xml_volumes = client.fetch_data('A86', start, end)
volumes_file = output_dir / 'entsoe_imbalance_volumes_202511100800_202511101200.xml'
with open(volumes_file, 'w') as f:
    f.write(xml_volumes)
print(f'Volumes saved to {volumes_file}')

print('\n✓ Test data fetched successfully')
