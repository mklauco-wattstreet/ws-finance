#!/usr/bin/env python3
"""
CEPS cross-border datasets (SOAP operations CrossborderPowerFlows, EmergencyExchange).

Kept out of ceps_soap_xml_parser.py / ceps_soap_uploader.py so that those stay small;
the pipeline (ceps_soap_pipeline.py) dispatches to this module. Both datasets are
runnable only explicitly (--dataset crossborder_flows | emergency_exchange) and are
NOT part of --dataset all.

crossborder_flows   CrossborderPowerFlows, agregation=MI function=AVG version=RT
    1-min series per border profile (PSE=PL, SEPS=SK, APG=AT, TenneT=DE,
    50HzT=DE) as Actual and Planned, plus CEPS = their sum. The Planned values
    are an hourly step function that the service repeats on every minute.
    Sign: positive = import into CZ, negative = export from CZ (checked against
    ENTSO-E physical flow_pl_mw). Stored in ceps_crossborder_flows_1min and
    aggregated to ceps_crossborder_flows_15min (mean / median / last), exactly like
    the other 1-min datasets.
    The `version` parameter makes no difference to the data (RT and DF return
    identical values); RT is sent.

emergency_exchange  EmergencyExchange, agregation=HR function=SUM version=RT
    Sparse emergency / redispatch deliveries per "border and direction"
    (e.g. CAD_CEPS_50HZT = CEPS -> 50Hertz). The service replicates the hourly
    sum on QH rows and returns nothing on MI, so the hourly sum is the finest
    resolution that can be stored; it is a long table (one row per hour, border
    and direction, exchange type) because the series are categories, not columns.
    Empty for most days (last event 2026-01-13).
"""

import sys
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from psycopg2.extras import execute_values

from ceps.ceps_soap_uploader import (
    AggColumns, _aggregate_1min_to_15min, _upsert_1min, get_affected_intervals,
)
from ceps.ceps_soap_xml_parser import NS, parse_times
from dst_calendar import PRAGUE_TZ, local_label

logger = logging.getLogger("CEPS-Pipeline")

_BASE = 'https://www.ceps.cz/CepsData'

# Pipeline dataset name -> SOAP configuration (merged into ceps_soap_pipeline.DATASETS)
CROSSBORDER_DATASETS = {
    'crossborder_flows': {
        'operation': 'CrossborderPowerFlows',
        'soap_action': f'{_BASE}/CrossborderPowerFlows',
    },
    'emergency_exchange': {
        'operation': 'EmergencyExchange',
        'soap_action': f'{_BASE}/EmergencyExchange',
    },
}

# Fields (besides delivery_datetime) that identify a record, for in-memory dedup
RECORD_KEY_FIELDS: Dict[str, Tuple[str, ...]] = {
    'crossborder_flows': (),
    'emergency_exchange': ('border_direction', 'exchange_type'),
}

# value1..value12 of CrossborderPowerFlows, in series order
FLOW_COLUMNS: List[str] = [
    f'{border}_{kind}_mw'
    for border in ('pse', 'seps', 'apg', 'tennet', 'fiftyhertz', 'ceps')
    for kind in ('actual', 'planned')
]

FLOW_AGG_COLUMNS: AggColumns = [
    (col,
     col.replace('_mw', '_mean_mw'),
     col.replace('_mw', '_median_mw'),
     col.replace('_mw', '_last_at_interval_mw'))
    for col in FLOW_COLUMNS
]

EMERGENCY_VALUE_COLUMNS = ['energy_mwh']


def build_crossborder_body(operation: str, date_from: str, date_to: str) -> str:
    """SOAP body for a cross-border operation (see build_soap_envelope)."""
    if operation == 'CrossborderPowerFlows':
        extra = "<agregation>MI</agregation><function>AVG</function><version>RT</version>"
    elif operation == 'EmergencyExchange':
        extra = ("<agregation>HR</agregation><function>SUM</function><version>RT</version>"
                 "<param1>all</param1><param2>all</param2>")
    else:
        raise ValueError(f"Unknown cross-border operation: {operation}")
    return f"""
        <{operation} xmlns="{_BASE}/">
            <dateFrom>{date_from}</dateFrom>
            <dateTo>{date_to}</dateTo>
            {extra}
        </{operation}>
        """


def _to_float(raw: Optional[str]) -> Optional[float]:
    if raw is None or raw.strip() == '':
        return None
    return float(raw)


def parse_crossborder_flows_xml(xml_root: ET.Element) -> List[Dict]:
    """Parse CrossborderPowerFlows: <item date=... value1..value12=.../> per minute."""
    records = []
    for item in xml_root.findall('.//ns1:item', NS):
        date_str = item.get('date')
        if not date_str:
            continue
        try:
            record = dict(parse_times(date_str))
            for idx, col in enumerate(FLOW_COLUMNS, start=1):
                record[col] = _to_float(item.get(f'value{idx}'))
        except (ValueError, TypeError):
            continue
        records.append(record)
    return records


def parse_emergency_exchange_xml(xml_root: ET.Element) -> List[Dict]:
    """Parse EmergencyExchange: value15 = border and direction, value16 = type, value17 = value."""
    records = []
    for item in xml_root.findall('.//ns1:item', NS):
        date_str = item.get('date')
        border = item.get('value15')
        exchange_type = item.get('value16')
        if not (date_str and border and exchange_type):
            continue
        try:
            value = _to_float(item.get('value17'))
            times = parse_times(date_str)
        except (ValueError, TypeError):
            continue
        records.append({
            'delivery_datetime': times['delivery_datetime'],
            'border_direction': border.strip(),
            'exchange_type': exchange_type.strip(),
            'energy_mwh': value,
        })
    return records


def parse_crossborder(dataset: str, xml_root: ET.Element) -> List[Dict]:
    """Parse a cross-border dataset."""
    if dataset == 'crossborder_flows':
        return parse_crossborder_flows_xml(xml_root)
    if dataset == 'emergency_exchange':
        return parse_emergency_exchange_xml(xml_root)
    raise ValueError(f"Unknown cross-border dataset: {dataset}")


def upsert_crossborder_flows_data(records: List[Dict], conn, log) -> int:
    """Upsert 1-min flows and re-aggregate the affected 15-min intervals."""
    if not records:
        return 0
    affected = get_affected_intervals(records)
    _upsert_1min('ceps_crossborder_flows_1min', FLOW_COLUMNS, records, conn)
    log.debug(f" Upserted {len(records):,} records to ceps_crossborder_flows_1min")
    agg = _aggregate_1min_to_15min('ceps_crossborder_flows_1min', 'ceps_crossborder_flows_15min',
                                   FLOW_AGG_COLUMNS, affected, conn)
    log.debug(f" Aggregated {agg:,} intervals to ceps_crossborder_flows_15min")
    return len(records)


def upsert_emergency_exchange_data(records: List[Dict], conn, log) -> int:
    """Upsert hourly emergency exchange rows keyed on (delivery_datetime, trade_date, border, type)."""
    if not records:
        return 0
    values = []
    for r in records:
        start: datetime = r['delivery_datetime']
        values.append((
            start.astimezone(PRAGUE_TZ).date(),
            local_label(start, 60, midnight='00:00'),
            start,
            r['border_direction'],
            r['exchange_type'],
            r['energy_mwh'],
        ))
    query = """
        INSERT INTO finance.ceps_emergency_exchange_60min
            (trade_date, time_interval, delivery_datetime, border_direction, exchange_type, energy_mwh)
        VALUES %s
        ON CONFLICT (delivery_datetime, trade_date, border_direction, exchange_type)
        DO UPDATE SET
            time_interval = EXCLUDED.time_interval,
            energy_mwh = EXCLUDED.energy_mwh,
            updated_at = CURRENT_TIMESTAMP
        WHERE finance.ceps_emergency_exchange_60min.energy_mwh IS DISTINCT FROM EXCLUDED.energy_mwh
           OR finance.ceps_emergency_exchange_60min.time_interval IS DISTINCT FROM EXCLUDED.time_interval
    """
    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()
    log.debug(f" Upserted {len(records):,} records to ceps_emergency_exchange_60min")
    return len(records)


def upsert_crossborder(dataset: str, records: List[Dict], conn, log) -> int:
    """Upload a cross-border dataset."""
    if dataset == 'crossborder_flows':
        return upsert_crossborder_flows_data(records, conn, log)
    if dataset == 'emergency_exchange':
        return upsert_emergency_exchange_data(records, conn, log)
    raise ValueError(f"Unknown cross-border dataset: {dataset}")
