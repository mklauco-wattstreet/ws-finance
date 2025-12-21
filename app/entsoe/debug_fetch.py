#!/usr/bin/env python3
"""Debug script to check ENTSO-E API response"""

import sys
sys.path.insert(0, '/app/scripts')
from entsoe.client import EntsoeClient
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

client = EntsoeClient()
period_start, period_end = client.get_preceding_hour_range()

print(f"Fetching data for period: {period_start} to {period_end}")
xml_content = client.fetch_data('A85', period_start, period_end)

print('\n=== XML CONTENT ===')
print(f"Length: {len(xml_content)} bytes")
print("\nFirst 1000 characters:")
print(xml_content[:1000])

# Parse XML to see structure
try:
    root = ET.fromstring(xml_content)
    print(f"\n=== XML STRUCTURE ===")
    print(f"Root tag: {root.tag}")
    print(f"Root attributes: {root.attrib}")

    # Print all child elements
    print("\nChild elements:")
    for child in root:
        print(f"  - {child.tag}")
        for subchild in child:
            print(f"    - {subchild.tag}")
            if len(subchild) > 0:
                for subsubchild in subchild:
                    print(f"      - {subsubchild.tag}")

    # Check for namespaces
    print("\n=== NAMESPACES ===")
    # Extract namespaces from root tag
    if '{' in root.tag:
        ns_url = root.tag.split('}')[0].strip('{')
        print(f"Default namespace: {ns_url}")

    # Try different namespace patterns
    print("\nTrying to find TimeSeries with different patterns:")

    # Pattern 1: With explicit namespace
    ns1 = {'ns': 'urn:iec62325.351:tc57wg16:451-6:balancingdocument:3:0'}
    ts1 = root.findall('.//ns:TimeSeries', ns1)
    print(f"  Pattern 1 (balancingdocument): found {len(ts1)} TimeSeries")

    # Pattern 2: Without namespace
    ts2 = root.findall('.//TimeSeries')
    print(f"  Pattern 2 (no namespace): found {len(ts2)} TimeSeries")

    # Pattern 3: With wildcard namespace
    ts3 = root.findall('.//{*}TimeSeries')
    print(f"  Pattern 3 (wildcard namespace): found {len(ts3)} TimeSeries")

    # If we found TimeSeries, show their structure
    if ts3:
        print("\n=== FIRST TIMESERIES STRUCTURE ===")
        ts = ts3[0]
        for elem in ts:
            print(f"  - {elem.tag}")
            if elem.tag.endswith('Period'):
                for period_elem in elem:
                    print(f"    - {period_elem.tag}")

except Exception as e:
    print(f"\nError parsing XML: {e}")

# Save to file
with open('/tmp/response.xml', 'w') as f:
    f.write(xml_content)
print('\n=== Saved full response to /tmp/response.xml ===')