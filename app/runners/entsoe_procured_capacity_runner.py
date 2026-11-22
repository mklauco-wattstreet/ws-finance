#!/usr/bin/env python3
"""
ENTSO-E Procured Balancing Capacity Runner [GL EB 12.3.F].

Fetches Procured Balancing Capacity (documentType A15) for CZ only, for the
aFRR (processType A51) and mFRR (processType A47) reserve types. The market
agreement is daily (type_MarketAgreement.Type=A01); native resolution is hourly
(PT60M) priced in 4-hour blocks. Each TimeSeries is one awarded offer.

Three tables are populated per processed day:
- entsoe_procured_capacity_raw   : per-offer bid stack (replace-per-day-and-reserve snapshot)
- entsoe_procured_capacity_60min : aggregated wide summary (source-of-truth)
- entsoe_procured_capacity_15min : 60min values forward-filled into 4 quarters

The API returns a full-day document regardless of the requested sub-window, so
this runner issues one request per (day x reserve type), paged by offset when a
day exceeds the 100-TimeSeries API cap.

Every (day, reserve type) fetch ends in exactly one of three outcomes:
- OK          : data fetched and parsed. Raw rows of THAT reserve type for that day
                are replaced and only THAT reserve type's columns in the 60min/15min
                tables are updated, all in ONE transaction (one commit).
- UNAVAILABLE : ENTSO-E answered "No matching data" (not published yet). Logged at
                INFO; stored rows are left untouched.
- FAILED      : timeout, 5xx, parse error, ... Logged at WARNING per request; stored
                rows are left untouched. One ERROR is emitted per run only if EVERY
                (day, reserve type) request of the run failed (see
                _log_failure_summary); the next firing retries anyway.
Only OK outcomes ever write to the database, so a stored day is never wiped or
NULLed by an outage or a momentary "no data" answer. No DB connection is opened
until a day's fetches are finished, so no transaction is held during HTTP calls.

Usage:
    python3 -m runners.entsoe_procured_capacity_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_procured_capacity_runner --start 2026-03-01 --end 2026-03-31
"""

import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Tuple, Optional

from psycopg2 import extras

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from dst_calendar import local_label
from entsoe.client import EntsoeClient
from entsoe.parsers import ProcuredCapacityParser
from entsoe.constants import (
    ACTIVE_PROCURED_CAPACITY_AREAS,
    PROCURED_CAPACITY_PROCESS_TYPES,
    PROCURED_CAPACITY_BACKFILL_FLOOR,
)

# processType -> internal reserve label (e.g. "A51" -> "afrr")
RESERVE_TYPES = ("afrr", "mfrr")
DIRECTIONS = ("up", "down")

RAW_TABLE = "entsoe_procured_capacity_raw"
TABLE_60MIN = "entsoe_procured_capacity_60min"
TABLE_15MIN = "entsoe_procured_capacity_15min"

RAW_COLUMNS = [
    "trade_date", "time_interval", "delivery_datetime", "reserve_type", "direction",
    "offer_seq", "quantity_mw", "price_eur",
]
RAW_CONFLICT = ["delivery_datetime", "reserve_type", "direction", "offer_seq"]

# Wide summary column order (must match migration 066).
SUMMARY_VALUE_COLUMNS = [
    f"{r}_{d}_{suffix}"
    for r in RESERVE_TYPES
    for d in DIRECTIONS
    for suffix in ("mw", "price_marginal_eur", "price_avg_eur")
]
SUMMARY_COLUMNS = ["trade_date", "time_interval", "delivery_datetime"] + SUMMARY_VALUE_COLUMNS
SUMMARY_CONFLICT = ["delivery_datetime"]

# Outcome of fetching one (day, reserve type).
FETCH_OK = "ok"
FETCH_UNAVAILABLE = "unavailable"
FETCH_FAILED = "failed"


class ProcuredCapacityRunner(BaseRunner):
    """Runner for ENTSO-E Procured Balancing Capacity (A15, 12.3.F) — CZ only."""

    RUNNER_NAME = "ENTSO-E Procured Capacity Runner"
    TABLE_NAME = RAW_TABLE

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None
        self._requests_total = 0  # (day, reserve type) fetches attempted this run
        # CZ is the only area
        _, self.area_code, self.display_label, self.country_code = ACTIVE_PROCURED_CAPACITY_AREAS[0]

    def _init_client(self) -> bool:
        try:
            self.client = EntsoeClient()
            return True
        except Exception as e:
            self.logger.error(f"✗ Client initialization failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Date selection
    # ------------------------------------------------------------------
    def _target_dates(self) -> List[date]:
        """List of Prague-local dates to process, clamped to the backfill floor."""
        if self.is_backfill:
            end = self.end_date or date.today()
            start = self.start_date or end
        else:
            # Cron mode: yesterday..tomorrow (covers late + next-day publication).
            today = datetime.now(PRAGUE_TZ).date()
            start, end = today - timedelta(days=1), today + timedelta(days=1)

        if start < PROCURED_CAPACITY_BACKFILL_FLOOR:
            start = PROCURED_CAPACITY_BACKFILL_FLOOR
        if end < start:
            return []

        return [start + timedelta(days=n) for n in range((end - start).days + 1)]

    def _day_window_utc(self, target_date: date) -> Tuple[datetime, datetime]:
        """Prague-local day [00:00, 24:00) expressed as a UTC range."""
        start_prague = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        end_prague = start_prague + timedelta(days=1)
        return start_prague.astimezone(timezone.utc), end_prague.astimezone(timezone.utc)

    # ------------------------------------------------------------------
    # Fetch + parse
    # ------------------------------------------------------------------
    # Instance page size for offset paging (ENTSO-E A15 cap is 100 TimeSeries).
    PAGE_SIZE = 100
    # Safety cap on pages per (day, reserve) to avoid an unbounded loop.
    MAX_PAGES = 10

    @staticmethod
    def _is_no_data_error(error: Exception) -> bool:
        """True only for ENTSO-E's explicit "No matching data" acknowledgement.

        Deliberately narrower than BaseRunner.is_data_unavailable_error(), which
        also matches 503/404/409/timeouts/retry exhaustion - those are FAILED
        fetches here, not "ENTSO-E has no data".
        """
        return 'No matching data' in str(error)

    def _fetch_reserve_day(
        self, target_date: date, process_type: str, reserve_type: str
    ) -> Tuple[str, List[Dict], Optional[Exception]]:
        """Fetch + parse one reserve type for one Prague day (all pages).

        Pages via offset until a page returns no TimeSeries. Each page's
        TimeSeries mRIDs restart at 1, so offer_seq is globally offset by the
        page offset to stay unique across pages.

        Returns (status, rows, error):
          FETCH_OK          rows is non-empty
          FETCH_UNAVAILABLE "No matching data" on the first page (or nothing
                            usable parsed); rows is empty
          FETCH_FAILED      any other error on ANY page - rows is empty, because
                            a half-fetched day must never replace a stored one
        """
        period_start, period_end = self._day_window_utc(target_date)
        rows: List[Dict] = []

        for page in range(self.MAX_PAGES):
            offset = page * self.PAGE_SIZE
            try:
                xml_content = self.client.fetch_procured_capacity_for_domain(
                    period_start, period_end, self.area_code, process_type, offset=offset
                )
            except Exception as e:
                if self._is_no_data_error(e):
                    break  # first page: unavailable; later page: past the last page
                return FETCH_FAILED, [], e

            # TimeSeries count drives paging (the API cap is on TS instances);
            # zero-quantity offers get dropped during parsing, so we cannot
            # rely on the parsed-row count to decide whether more pages exist.
            ts_count = xml_content.count('<TimeSeries>')
            if ts_count == 0:
                break  # "No matching data" acknowledgement - past the last page.

            try:
                xml_file = self._save_xml_file(xml_content, target_date, reserve_type, offset)
                parser = ProcuredCapacityParser(reserve_type=reserve_type)
                parsed = parser.parse_xml(str(xml_file))
            except Exception as e:
                return FETCH_FAILED, [], e

            # Keep only rows on the target Prague day (drop DST edge spillover).
            parsed = [r for r in parsed if r['trade_date'] == target_date]
            for r in parsed:
                r['offer_seq'] += offset
            rows.extend(parsed)

            # A partial page (< PAGE_SIZE TimeSeries) is the last page.
            if ts_count < self.PAGE_SIZE:
                break

        self.logger.debug(f"  {reserve_type} {target_date}: {len(rows)} raw rows")
        # Nothing usable (no data, or only zero-quantity offers): treat as
        # unavailable so the stored day is left alone rather than emptied.
        return (FETCH_OK if rows else FETCH_UNAVAILABLE), rows, None

    def _save_xml_file(
        self, xml_content: str, target_date: date, reserve_type: str, offset: int = 0
    ) -> Path:
        period_start, _ = self._day_window_utc(target_date)
        suffix = f"_p{offset}" if offset else ""
        output_file = self.get_output_path(
            f"entsoe_procured_capacity_cz_{reserve_type}_{target_date.strftime('%Y%m%d')}{suffix}.xml",
            period_start,
        )
        self.save_xml(xml_content, output_file)
        return output_file

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------
    def _aggregate_60min(self, raw_rows: List[Dict]) -> List[Tuple]:
        """Aggregate per-offer rows into one wide row per hour (keyed by its UTC start)."""
        # groups[(delivery_datetime, trade_date, time_interval)][f"{reserve}_{dir}"]
        #   -> list of (qty, price); delivery_datetime first so rows sort by instant
        groups: Dict[Tuple, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        for r in raw_rows:
            key = (r['delivery_datetime'], r['trade_date'], r['time_interval'])
            groups[key][f"{r['reserve_type']}_{r['direction']}"].append(
                (r['quantity_mw'], r['price_eur'])
            )

        records = []
        for (delivery_datetime, trade_date, time_interval), buckets in sorted(groups.items()):
            values = {}
            for r in RESERVE_TYPES:
                for d in DIRECTIONS:
                    offers = buckets.get(f"{r}_{d}", [])
                    total_mw = sum(q for q, _ in offers)
                    priced = [(q, p) for q, p in offers if p is not None]
                    marginal = max((p for _, p in priced), default=None)
                    weight = sum(q for q, _ in priced)
                    avg = (sum(q * p for q, p in priced) / weight) if weight else None
                    values[f"{r}_{d}_mw"] = total_mw if offers else None
                    values[f"{r}_{d}_price_marginal_eur"] = marginal
                    values[f"{r}_{d}_price_avg_eur"] = avg

            records.append(
                (trade_date, time_interval, delivery_datetime)
                + tuple(values[c] for c in SUMMARY_VALUE_COLUMNS)
            )
        return records

    def _expand_to_15min(self, records_60min: List[Tuple]) -> List[Tuple]:
        """Forward-fill each hourly summary row into four 15-min rows."""
        records = []
        for rec in records_60min:
            trade_date, hour_start = rec[0], rec[2]
            values = rec[3:]
            for q in range(4):
                start = hour_start + timedelta(minutes=15 * q)
                records.append((trade_date, local_label(start, 15, midnight='00:00'), start) + values)
        return records

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    @staticmethod
    def _upsert(
        cur, table: str, columns: List[str], records: List[Tuple],
        conflict: List[str], update_columns: List[str],
    ) -> None:
        """Bulk upsert (execute_values) on an open cursor WITHOUT committing.

        Same SQL as BaseRunner.bulk_upsert, but the caller owns the transaction
        so several statements can be committed atomically.
        """
        if not records:
            return
        update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        update_str += ", updated_at = CURRENT_TIMESTAMP"
        query = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
            f"ON CONFLICT ({', '.join(conflict)}) DO UPDATE SET {update_str}"
        )
        extras.execute_values(cur, query, records, page_size=1000)

    def _persist_day(self, conn, target_date: date, fetched: Dict[str, List[Dict]]) -> int:
        """Write one day's successfully fetched reserve types in ONE transaction.

        `fetched` maps reserve_type -> raw rows, for OK reserve types only.
        - raw: DELETE that day's rows of those reserve types, then INSERT. The
          other reserve type's raw rows are not touched.
        - 60min/15min: aggregated from the fetched rows only, and on conflict
          only the columns of the fetched reserve types are updated, so a
          reserve type that failed/was unavailable keeps its stored values
          (no NULL overwrite). A fresh hour inserts the other reserve as NULL.
        One commit at the end: readers never see the day empty, a crash
        rolls everything back.
        """
        reserves = sorted(fetched)
        raw_rows = [r for rt in reserves for r in fetched[rt]]
        rec_60 = self._aggregate_60min(raw_rows)
        rec_15 = self._expand_to_15min(rec_60)
        update_cols = [c for c in SUMMARY_VALUE_COLUMNS if c.split('_')[0] in fetched]
        raw_records = [tuple(r[c] for c in RAW_COLUMNS) for r in raw_rows]
        raw_update = [c for c in RAW_COLUMNS if c not in RAW_CONFLICT]

        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {RAW_TABLE} WHERE trade_date = %s AND reserve_type = ANY(%s)",
                    (target_date, reserves),
                )
                self._upsert(cur, RAW_TABLE, RAW_COLUMNS, raw_records, RAW_CONFLICT, raw_update)
                self._upsert(cur, TABLE_60MIN, SUMMARY_COLUMNS, rec_60, SUMMARY_CONFLICT, update_cols)
                self._upsert(cur, TABLE_15MIN, SUMMARY_COLUMNS, rec_15, SUMMARY_CONFLICT, update_cols)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        self.logger.debug(
            f"  {target_date} [{','.join(reserves)}]: {len(raw_records)} raw, "
            f"{len(rec_60)} x60min, {len(rec_15)} x15min"
        )
        return len(raw_records)

    # ------------------------------------------------------------------
    # Failure reporting
    # ------------------------------------------------------------------
    def _log_failure_summary(self) -> None:
        """One aggregate line per run (called from print_footer).

        ERROR only when EVERY (day, reserve type) request of the run failed;
        a partial failure stays at WARNING. Per-request failures were already
        logged at WARNING by record_area_failure().
        """
        if not self.area_failures:
            return
        failed = len(self.area_failures)
        first = sorted(self.area_failures)[0]
        message = (
            f"{self.RUNNER_NAME}: {failed}/{self._requests_total} request(s) failed "
            f"[{', '.join(sorted(self.area_failures))}] - first error: {self.area_failures[first]}"
        )
        if failed >= self._requests_total:
            self.logger.error(message)
        else:
            self.logger.warning(message)

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def run(self) -> bool:
        if not self._init_client():
            return False

        dates = self._target_dates()
        if not dates:
            self.logger.info(f"{self.RUNNER_NAME}: no dates to process")
            return True

        self.logger.debug(f"Processing {len(dates)} day(s): {dates[0]}..{dates[-1]}")

        total_raw = 0
        self._requests_total = 0
        try:
            for target_date in dates:
                # 1) Fetch everything for the day first - no DB connection open.
                fetched: Dict[str, List[Dict]] = {}
                for process_type, reserve_type in PROCURED_CAPACITY_PROCESS_TYPES.items():
                    self._requests_total += 1
                    status, rows, error = self._fetch_reserve_day(
                        target_date, process_type, reserve_type
                    )
                    if status == FETCH_OK:
                        fetched[reserve_type] = rows
                    elif status == FETCH_UNAVAILABLE:
                        self.logger.info(
                            f"{self.RUNNER_NAME}: no data for {reserve_type} {target_date} "
                            f"- stored rows left untouched"
                        )
                    else:
                        self.record_area_failure(f"{reserve_type} {target_date}", error)

                if not fetched:
                    continue

                # 2) Write: short-lived connection, one transaction per day.
                if self.dry_run:
                    rec_60 = self._aggregate_60min([r for rows in fetched.values() for r in rows])
                    self.logger.info(
                        f"DRY RUN - {target_date} [{','.join(sorted(fetched))}]: "
                        f"{sum(len(v) for v in fetched.values())} raw, "
                        f"{len(rec_60)} x60min, {len(rec_60) * 4} x15min"
                    )
                    count = sum(len(v) for v in fetched.values())
                else:
                    with self.database_connection() as conn:
                        count = self._persist_day(conn, target_date, fetched)

                self.track_country(self.country_code, count)
                total_raw += count

            self.logger.info(self.format_summary(total_raw))
            all_failed = self._requests_total > 0 and len(self.area_failures) >= self._requests_total
            self.print_footer(success=not all_failed)
            return not all_failed

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            self.print_footer(success=False)
            return False


if __name__ == '__main__':
    ProcuredCapacityRunner.main()
