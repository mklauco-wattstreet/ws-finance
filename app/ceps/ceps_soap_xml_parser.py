#!/usr/bin/env python3
"""
CEPS SOAP XML Parser

Parses XML responses from CEPS SOAP API and converts to structured data records.
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional


# XML namespace for CEPS data
NS = {
    'ns0': 'https://www.ceps.cz/CepsData/',
    'ns1': 'https://www.ceps.cz/CepsData/StructuredData/1.0'
}


def parse_datetime(date_str: str) -> datetime:
    """
    Parse CEPS datetime string to naive datetime (Europe/Prague).

    Args:
        date_str: ISO datetime string (e.g., "2025-11-01T00:00:00+01:00")

    Returns:
        Naive datetime object
    """
    # Parse with timezone info, then make naive (remove timezone)
    dt = datetime.fromisoformat(date_str)
    return dt.replace(tzinfo=None)


def parse_imbalance_xml(xml_root: ET.Element) -> List[Dict]:
    """
    Parse System Imbalance XML data.

    Expected structure:
    <root>
      <data>
        <item date="..." value1="..." />  <!-- value1 = Aktuální odchylka [MW] -->
      </data>
    </root>

    Args:
        xml_root: XML root element from SOAP response

    Returns:
        List of dicts with keys: delivery_timestamp, load_mw
    """
    records = []

    # Find all data items
    data_items = xml_root.findall('.//ns1:item', NS)

    for item in data_items:
        date_str = item.get('date')
        value1 = item.get('value1')

        if date_str and value1:
            try:
                records.append({
                    'delivery_timestamp': parse_datetime(date_str),
                    'load_mw': float(value1)
                })
            except (ValueError, TypeError) as e:
                # Skip invalid records
                continue

    return records


def parse_re_price_xml(xml_root: ET.Element) -> List[Dict]:
    """
    Parse RE Price XML data.

    Expected structure:
    <root>
      <data>
        <item date="..." value1="..." value2="..." value3="..." value4="..." />
        <!-- value1 = aFRR [EUR/MWh] - applies to BOTH aFRR+ and aFRR- -->
        <!-- value2 = mFRR+ [EUR/MWh] -->
        <!-- value3 = mFRR- [EUR/MWh] -->
        <!-- value4 = mFRR5 [EUR/MWh] -->
      </data>
    </root>

    Args:
        xml_root: XML root element from SOAP response

    Returns:
        List of dicts with keys: delivery_timestamp, price_afrr_plus_eur_mwh, price_afrr_minus_eur_mwh,
                                 price_mfrr_plus_eur_mwh, price_mfrr_minus_eur_mwh, price_mfrr_5_eur_mwh
    """
    records = []

    # Find all data items
    data_items = xml_root.findall('.//ns1:item', NS)

    for item in data_items:
        date_str = item.get('date')
        value1 = item.get('value1')  # aFRR (applies to both + and -)
        value2 = item.get('value2')  # mFRR+
        value3 = item.get('value3')  # mFRR-
        value4 = item.get('value4')  # mFRR5

        if date_str:
            try:
                # IMPORTANT: Single aFRR value applies to BOTH aFRR+ and aFRR-
                afrr_value = float(value1) if value1 else None

                records.append({
                    'delivery_timestamp': parse_datetime(date_str),
                    'price_afrr_plus_eur_mwh': afrr_value,
                    'price_afrr_minus_eur_mwh': afrr_value,  # Same value
                    'price_mfrr_plus_eur_mwh': float(value2) if value2 else None,
                    'price_mfrr_minus_eur_mwh': float(value3) if value3 else None,
                    'price_mfrr_5_eur_mwh': float(value4) if value4 else None
                })
            except (ValueError, TypeError) as e:
                # Skip invalid records
                continue

    return records


def parse_svr_activation_xml(xml_root: ET.Element) -> List[Dict]:
    """
    Parse SVR Activation XML data.

    Expected structure:
    <root>
      <data>
        <item date="..." value1="..." value2="..." value3="..." value4="..." value5="..." />
        <!-- value1 = aFRR+ [MW] -->
        <!-- value2 = aFRR- [MW] -->
        <!-- value3 = mFRR+ [MW] -->
        <!-- value4 = mFRR- [MW] -->
        <!-- value5 = mFRR5 [MW] -->
      </data>
    </root>

    Args:
        xml_root: XML root element from SOAP response

    Returns:
        List of dicts with keys: delivery_timestamp, afrr_plus_mw, afrr_minus_mw,
                                 mfrr_plus_mw, mfrr_minus_mw, mfrr_5_mw
    """
    records = []

    # Find all data items
    data_items = xml_root.findall('.//ns1:item', NS)

    for item in data_items:
        date_str = item.get('date')
        value1 = item.get('value1')  # aFRR+
        value2 = item.get('value2')  # aFRR-
        value3 = item.get('value3')  # mFRR+
        value4 = item.get('value4')  # mFRR-
        value5 = item.get('value5')  # mFRR5

        if date_str:
            try:
                records.append({
                    'delivery_timestamp': parse_datetime(date_str),
                    'afrr_plus_mw': float(value1) if value1 else None,
                    'afrr_minus_mw': float(value2) if value2 else None,
                    'mfrr_plus_mw': float(value3) if value3 else None,
                    'mfrr_minus_mw': float(value4) if value4 else None,
                    'mfrr_5_mw': float(value5) if value5 else None
                })
            except (ValueError, TypeError) as e:
                # Skip invalid records
                continue

    return records


def parse_export_import_svr_xml(xml_root: ET.Element) -> List[Dict]:
    """
    Parse Export/Import SVR XML data.

    Expected structure:
    <root>
      <data>
        <item date="..." value2="..." value3="..." value4="..." value5="..." />
        <!-- value2 = Imbalance netting [MW] -->
        <!-- value3 = Mari (mFRR) [MW] -->
        <!-- value4 = Picasso (aFRR) [MW] -->
        <!-- value5 = Sum exchange European platforms [MW] -->
      </data>
    </root>

    Args:
        xml_root: XML root element from SOAP response

    Returns:
        List of dicts with keys: delivery_timestamp, imbalance_netting_mw, mari_mfrr_mw,
                                 picasso_afrr_mw, sum_exchange_european_platforms_mw
    """
    records = []

    # Find all data items
    data_items = xml_root.findall('.//ns1:item', NS)

    for item in data_items:
        date_str = item.get('date')
        value2 = item.get('value2')  # Imbalance netting
        value3 = item.get('value3')  # Mari (mFRR)
        value4 = item.get('value4')  # Picasso (aFRR)
        value5 = item.get('value5')  # Sum exchange

        if date_str:
            try:
                records.append({
                    'delivery_timestamp': parse_datetime(date_str),
                    'imbalance_netting_mw': float(value2) if value2 else None,
                    'mari_mfrr_mw': float(value3) if value3 else None,
                    'picasso_afrr_mw': float(value4) if value4 else None,
                    'sum_exchange_european_platforms_mw': float(value5) if value5 else None
                })
            except (ValueError, TypeError) as e:
                # Skip invalid records
                continue

    return records


def parse_soap_xml(dataset: str, xml_root: ET.Element) -> List[Dict]:
    """
    Parse SOAP XML response for any dataset.

    Args:
        dataset: Dataset key ('imbalance', 're_price', 'svr_activation', 'export_import_svr')
        xml_root: XML root element from SOAP response

    Returns:
        List of parsed records
    """
    if dataset == 'imbalance':
        return parse_imbalance_xml(xml_root)
    elif dataset == 're_price':
        return parse_re_price_xml(xml_root)
    elif dataset == 'svr_activation':
        return parse_svr_activation_xml(xml_root)
    elif dataset == 'export_import_svr':
        return parse_export_import_svr_xml(xml_root)
    else:
        raise ValueError(f"Unknown dataset: {dataset}")
