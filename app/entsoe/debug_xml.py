#!/usr/bin/env python3
"""Debug script to check actual XML structure from ENTSO-E"""

import sys
sys.path.insert(0, '/app/scripts')
from entsoe.entsoe_client import EntsoeClient
import xml.etree.ElementTree as ET

client = EntsoeClient()
period_start, period_end = client.get_preceding_hour_range()

print(f"Fetching data for period: {period_start} to {period_end}")
xml_content = client.fetch_data('A85', period_start, period_end)

print(f"\n=== XML CONTENT ({len(xml_content)} bytes) ===")
print("First 2000 characters:")
print(xml_content[:2000])

# Parse and explore structure
root = ET.fromstring(xml_content)
print(f"\n=== ROOT ===")
print(f"Tag: {root.tag}")

# Extract namespace from root
namespace = ""
if '{' in root.tag:
    namespace = root.tag.split('}')[0].strip('{')
    print(f"Namespace: {namespace}")

# Try to find TimeSeries with wildcard
print("\n=== SEARCHING FOR TIMESERIES ===")
timeseries_wildcard = root.findall('.//{*}TimeSeries')
print(f"Found {len(timeseries_wildcard)} TimeSeries with wildcard")

if timeseries_wildcard:
    print("\n=== FIRST TIMESERIES STRUCTURE ===")
    ts = timeseries_wildcard[0]
    for child in ts:
        tag_name = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        print(f"  {tag_name}: {child.text if child.text and len(child.text) < 50 else '...'}")

        if tag_name == 'Period':
            print("    Period children:")
            for period_child in child:
                pc_name = period_child.tag.split('}')[-1] if '}' in period_child.tag else period_child.tag
                print(f"      {pc_name}: {period_child.text if period_child.text and len(period_child.text) < 50 else '...'}")

                if pc_name == 'Point':
                    print("        Point children:")
                    for point_child in period_child:
                        pt_name = point_child.tag.split('}')[-1] if '}' in point_child.tag else point_child.tag
                        print(f"          {pt_name}: {point_child.text}")

# Try with correct namespace
if namespace:
    print(f"\n=== TRYING WITH NAMESPACE: {namespace} ===")
    ns = {'ns': namespace}
    timeseries_ns = root.findall('.//ns:TimeSeries', ns)
    print(f"Found {len(timeseries_ns)} TimeSeries with namespace")

    if timeseries_ns:
        # Check for Period
        period = timeseries_ns[0].find('.//ns:Period', ns)
        if period:
            points = period.findall('ns:Point', ns)
            print(f"Found {len(points)} Points in first Period")

            # Check first point for price field
            if points:
                print("\nFirst Point structure:")
                for elem in points[0]:
                    tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    print(f"  {tag_name}: {elem.text}")

# Save for inspection
with open('/tmp/response_full.xml', 'w') as f:
    f.write(xml_content)
print("\n=== Saved to /tmp/response_full.xml ===")