#!/usr/bin/env python3
"""
Core orchestration for expired-position liquidation.

For each expired position from the Trader API:
- Parse contract_id into (trade_date, time_interval)
- Look up OTE imbalance settlement price + CNB CZK/EUR rate from `finance`
- Skip silently if either is missing (matches the manual UI behavior)
- Skip if the position already has a successful liquidation log within 24h
- POST to the Trader API and persist an audit row in
  "trader-app".manual_position_logs
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional


CONTRACT_ID_RE = re.compile(
    r"^(\d{8})\s+(\d{2}:\d{2})-(\d{8})\s+(\d{2}:\d{2})$"
)

# Audit target value reserved for automated cron runs (distinguishes
# from the UI-driven `ta_liquidate_*` rows already in the table).
LOG_TARGET_CRON = "cron_liquidate_expired_position"

# Reuses the admin user that owns all existing manual_position_logs rows.
SYSTEM_USER_ID = 1

LIQUIDATIONS_LOG_TABLE = '"trader-app".manual_position_logs'

# Settlement source table by contract duration (minutes). 15-min MTU contracts
# settle off the raw 15-min table; 60-min (hourly) contracts off the aggregated
# 60-min table. Both expose settlement_price_imbalance_czk_mwh.
SETTLEMENT_TABLE_BY_DURATION = {
    15: "finance.ote_prices_imbalance",
    60: "finance.ote_prices_imbalance_60min",
}


@dataclass
class LiquidationResult:
    position_id: str
    contract_id: str
    side: str
    status: str
    http_status: Optional[int] = None
    detail: str = ""


def parse_contract_id(contract_id: str) -> Optional[Dict[str, Any]]:
    """Parse "20260513 22:00-20260513 22:15" into trade_date + time_interval.

    The trade_date is taken from the START of the interval (the OTE
    imbalance table keys on delivery-start, so cross-midnight contracts
    naturally line up with their start date).
    """
    if not contract_id:
        return None
    m = CONTRACT_ID_RE.match(contract_id.strip())
    if not m:
        return None
    yyyymmdd_start, hhmm_start, yyyymmdd_end, hhmm_end = m.groups()
    try:
        start_dt = datetime.strptime(f"{yyyymmdd_start} {hhmm_start}", "%Y%m%d %H:%M")
        end_dt = datetime.strptime(f"{yyyymmdd_end} {hhmm_end}", "%Y%m%d %H:%M")
    except ValueError:
        return None
    duration_minutes = int((end_dt - start_dt).total_seconds() // 60)
    if duration_minutes <= 0:
        return None
    return {
        # OTE imbalance tables key on delivery-start, so cross-midnight
        # contracts line up with their start date.
        "trade_date": start_dt.date(),
        "time_interval": f"{hhmm_start}-{hhmm_end}",
        "duration_minutes": duration_minutes,
    }


def fetch_settlement_price(
    conn, table: str, trade_date: date, time_interval: str
) -> Optional[float]:
    """Settlement price from `table` (15-min or 60-min imbalance source).

    `table` comes from SETTLEMENT_TABLE_BY_DURATION (a fixed internal map),
    never from user input, so interpolating it into the query is safe.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT settlement_price_imbalance_czk_mwh "
            f"FROM {table} "
            f"WHERE trade_date = %s AND time_interval = %s "
            f"LIMIT 1",
            (trade_date, time_interval),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def fetch_eur_czk_rate(conn, trade_date: date) -> Optional[float]:
    """CNB rate for the trade date; falls back to the most recent rate."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT czk_eur FROM finance.cnb_exchange_rate "
            "WHERE rate_date = %s LIMIT 1",
            (trade_date,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "SELECT czk_eur FROM finance.cnb_exchange_rate "
                "ORDER BY rate_date DESC LIMIT 1"
            )
            row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def already_liquidated(conn, position_id: str) -> bool:
    """Guard against re-POSTing if a prior run logged a 200 for this position.

    Looks back 24h across all liquidation targets (cron + manual UI).
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT 1 FROM {LIQUIDATIONS_LOG_TABLE}
            WHERE response_status = 200
              AND created_at > NOW() - INTERVAL '24 hours'
              AND response_body->>'position_id' = %s
            LIMIT 1
            """,
            (position_id,),
        )
        return cur.fetchone() is not None


def write_log(
    conn,
    *,
    target: str,
    contract_id: str,
    side: str,
    payload: Dict[str, Any],
    response_status: Optional[int],
    response_body: Optional[Dict[str, Any]],
    user_id: int = SYSTEM_USER_ID,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {LIQUIDATIONS_LOG_TABLE}
              (user_id, target, contract_id, side, payload,
               response_status, response_body, ip_address,
               created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, NULL,
                    NOW(), NOW())
            """,
            (
                user_id,
                target,
                contract_id,
                side,
                json.dumps(payload),
                response_status,
                json.dumps(response_body) if response_body is not None else None,
            ),
        )
    conn.commit()


def process_position(
    conn,
    client,
    position: Dict[str, Any],
    logger: logging.Logger,
    dry_run: bool,
) -> LiquidationResult:
    position_id = position.get("position_id") or ""
    contract_id = position.get("contract_id") or ""
    # manual_position_logs stores side as uppercase (BUY/SELL); normalize.
    side = (position.get("side") or "").upper()

    parsed = parse_contract_id(contract_id)
    if parsed is None:
        logger.warning(
            f"  Skipping {position_id}: unparseable contract_id={contract_id!r}"
        )
        return LiquidationResult(
            position_id, contract_id, side, "skipped_bad_contract"
        )

    trade_date = parsed["trade_date"]
    time_interval = parsed["time_interval"]
    duration = parsed["duration_minutes"]

    table = SETTLEMENT_TABLE_BY_DURATION.get(duration)
    if table is None:
        logger.warning(
            f"  Skipping {position_id} ({contract_id}): unsupported contract "
            f"duration {duration} min (supported: "
            f"{sorted(SETTLEMENT_TABLE_BY_DURATION)})"
        )
        return LiquidationResult(
            position_id, contract_id, side, "skipped_unsupported_duration",
            detail=f"{duration}min",
        )

    settlement = fetch_settlement_price(conn, table, trade_date, time_interval)
    if settlement is None:
        logger.info(
            f"  Skipping {position_id} ({contract_id}): no OTE settlement "
            f"price for {trade_date} {time_interval} ({duration}min, {table})"
        )
        return LiquidationResult(
            position_id, contract_id, side, "skipped_no_price",
            detail=f"{trade_date} {time_interval} ({duration}min)",
        )

    fx = fetch_eur_czk_rate(conn, trade_date)
    if fx is None or fx == 0:
        logger.info(
            f"  Skipping {position_id} ({contract_id}): no CZK/EUR rate "
            f"for {trade_date}"
        )
        return LiquidationResult(
            position_id, contract_id, side, "skipped_no_fx",
            detail=str(trade_date),
        )

    if already_liquidated(conn, position_id):
        logger.info(
            f"  Skipping {position_id} ({contract_id}): already liquidated "
            f"in the last 24h"
        )
        return LiquidationResult(
            position_id, contract_id, side, "skipped_idempotent"
        )

    liquidation_price = settlement / fx
    payload = {
        "settlement_price_czk_mwh": settlement,
        "eur_czk_rate": fx,
        "liquidation_price_eur_mwh": liquidation_price,
    }

    if dry_run:
        logger.info(
            f"  DRY-RUN {position_id} ({contract_id}, {side}): would POST "
            f"settlement={settlement} czk_eur={fx} -> "
            f"{liquidation_price:.6f} EUR/MWh"
        )
        return LiquidationResult(
            position_id, contract_id, side, "success",
            http_status=None, detail="dry-run",
        )

    response = client.liquidate(position_id, payload)
    status_code = response.status_code
    try:
        response_body = response.json()
    except ValueError:
        response_body = {"raw_text": response.text[:1000]}

    write_log(
        conn,
        target=LOG_TARGET_CRON,
        contract_id=contract_id,
        side=side,
        payload=payload,
        response_status=status_code,
        response_body=response_body,
    )

    if 200 <= status_code < 300:
        logger.info(
            f"  Liquidated {position_id} ({contract_id}, {side}): "
            f"status={status_code} price={liquidation_price:.6f} EUR/MWh"
        )
        return LiquidationResult(
            position_id, contract_id, side, "success",
            http_status=status_code,
        )

    logger.error(
        f"  POST failed for {position_id} ({contract_id}): "
        f"status={status_code} body={str(response_body)[:300]}"
    )
    return LiquidationResult(
        position_id, contract_id, side, "http_error",
        http_status=status_code,
        detail=str(response_body)[:300],
    )
