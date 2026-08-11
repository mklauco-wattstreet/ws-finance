#!/usr/bin/env python3
"""
ENTSO-E Procured Balancing Capacity Runner [GL EB 12.3.F].

Fetches Procured Balancing Capacity (documentType A15) for CZ only, for the
aFRR (processType A51) and mFRR (processType A47) reserve types. The market
agreement is daily (type_MarketAgreement.Type=A01); native resolution is hourly
(PT60M) priced in 4-hour blocks. Each TimeSeries is one awarded offer.

Three tables are populated per processed day:
- entsoe_procured_capacity_raw   : per-offer bid stack (replace-per-day snapshot)
- entsoe_procured_capacity_60min : aggregated wide summary (source-of-truth)
- entsoe_procured_capacity_15min : 60min values forward-filled into 4 quarters

The API returns a full-day document regardless of the requested sub-window, so
this runner issues one request per (day x reserve type) — well under the
100-instance API cap.

Usage:
    python3 -m runners.entsoe_procured_capacity_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_procured_capacity_runner --start 2026-03-01 --end 2026-03-31
"""

import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
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
    "trade_date", "time_interval", "reserve_type", "direction",
    "offer_seq", "quantity_mw", "price_eur",
]
RAW_CONFLICT = ["trade_date", "time_interval", "reserve_type", "direction", "offer_seq"]

# Wide summary column order (must match migration 066).
SUMMARY_VALUE_COLUMNS = [
    f"{r}_{d}_{suffix}"
    for r in RESERVE_TYPES
    for d in DIRECTIONS
    for suffix in ("mw", "price_marginal_eur", "price_avg_eur")
]
SUMMARY_COLUMNS = ["trade_date", "time_interval"] + SUMMARY_VALUE_COLUMNS
SUMMARY_CONFLICT = ["trade_date", "time_interval"]


class ProcuredCapacityRunner(BaseRunner):
    """Runner for ENTSO-E Procured Balancing Capacity (A15, 12.3.F) — CZ only."""

    RUNNER_NAME = "ENTSO-E Procured Capacity Runner"
    TABLE_NAME = RAW_TABLE

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None
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

    def _fetch_day_raw(self, target_date: date) -> List[Dict]:
        """Fetch + parse all reserve types for one Prague day into raw rows.

        Pages each reserve via offset until a page returns no rows. Each page's
        TimeSeries mRIDs restart at 1, so offer_seq is globally offset by the
        page offset to stay unique across pages.
        """
        period_start, period_end = self._day_window_utc(target_date)
        rows: List[Dict] = []

        for process_type, reserve_type in PROCURED_CAPACITY_PROCESS_TYPES.items():
            reserve_rows = 0
            for page in range(self.MAX_PAGES):
                offset = page * self.PAGE_SIZE
                try:
                    xml_content = self.client.fetch_procured_capacity_for_domain(
                        period_start, period_end, self.area_code, process_type, offset=offset
                    )
                except Exception as e:
                    if self.is_data_unavailable_error(e):
                        break
                    self.logger.error(
                        f"  Fetch failed {reserve_type} {target_date} offset={offset}: {e}"
                    )
                    if self.debug:
                        import traceback
                        traceback.print_exc()
                    break

                # TimeSeries count drives paging (the API cap is on TS instances);
                # zero-quantity offers get dropped during parsing, so we cannot
                # rely on the parsed-row count to decide whether more pages exist.
                ts_count = xml_content.count('<TimeSeries>')
                if ts_count == 0:
                    break  # "No matching data" acknowledgement — past the last page.

                xml_file = self._save_xml_file(xml_content, target_date, reserve_type, offset)
                parser = ProcuredCapacityParser(reserve_type=reserve_type)
                parsed = parser.parse_xml(str(xml_file))
                # Keep only rows on the target Prague day (drop DST edge spillover).
                parsed = [r for r in parsed if r['trade_date'] == target_date]

                for r in parsed:
                    r['offer_seq'] += offset
                rows.extend(parsed)
                reserve_rows += len(parsed)

                # A partial page (< PAGE_SIZE TimeSeries) is the last page.
                if ts_count < self.PAGE_SIZE:
                    break

            self.logger.debug(f"  {reserve_type} {target_date}: {reserve_rows} raw rows")

        return rows

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
        """Aggregate per-offer rows into one wide row per (trade_date, hour)."""
        # groups[(trade_date, time_interval)][f"{reserve}_{dir}"] -> list of (qty, price)
        groups: Dict[Tuple, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        for r in raw_rows:
            key = (r['trade_date'], r['time_interval'])
            groups[key][f"{r['reserve_type']}_{r['direction']}"].append(
                (r['quantity_mw'], r['price_eur'])
            )

        records = []
        for (trade_date, time_interval), buckets in sorted(groups.items()):
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
                (trade_date, time_interval) + tuple(values[c] for c in SUMMARY_VALUE_COLUMNS)
            )
        return records

    def _expand_to_15min(self, records_60min: List[Tuple]) -> List[Tuple]:
        """Forward-fill each hourly summary row into four 15-min rows."""
        records = []
        for rec in records_60min:
            trade_date, time_interval = rec[0], rec[1]
            values = rec[2:]
            start_hh = int(time_interval[:2])
            for q in range(4):
                start_min = q * 15
                end_total = start_hh * 60 + start_min + 15
                ti = (f"{start_hh:02d}:{start_min:02d}-"
                      f"{(end_total // 60) % 24:02d}:{end_total % 60:02d}")
                records.append((trade_date, ti) + values)
        return records

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _replace_raw_for_dates(self, conn, dates: List[date], raw_rows: List[Dict]) -> int:
        """Replace-per-day: delete existing raw rows for the dates, then insert."""
        if self.dry_run:
            self.logger.info(f"DRY RUN - Would replace raw rows for {len(dates)} day(s) "
                             f"with {len(raw_rows)} rows")
            return len(raw_rows)

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {RAW_TABLE} WHERE trade_date = ANY(%s)", (dates,))
        conn.commit()

        if not raw_rows:
            return 0

        records = [tuple(r[c] for c in RAW_COLUMNS) for r in raw_rows]
        return self.bulk_upsert(conn, RAW_TABLE, RAW_COLUMNS, records, RAW_CONFLICT)

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
        try:
            conn_ctx = self.database_connection() if not self.dry_run else None
            conn = conn_ctx.__enter__() if conn_ctx else None
            try:
                for target_date in dates:
                    raw_rows = self._fetch_day_raw(target_date)
                    if not raw_rows:
                        self.logger.info(f"{self.RUNNER_NAME}: no data for {target_date}")
                        # Still clear stale rows for an empty day in backfill snapshots.
                        if conn:
                            self._replace_raw_for_dates(conn, [target_date], [])
                        continue

                    rec_60 = self._aggregate_60min(raw_rows)
                    rec_15 = self._expand_to_15min(rec_60)

                    if conn:
                        self._replace_raw_for_dates(conn, [target_date], raw_rows)
                        self.bulk_upsert(conn, TABLE_60MIN, SUMMARY_COLUMNS, rec_60, SUMMARY_CONFLICT)
                        self.bulk_upsert(conn, TABLE_15MIN, SUMMARY_COLUMNS, rec_15, SUMMARY_CONFLICT)
                    else:
                        self.logger.info(
                            f"DRY RUN - {target_date}: {len(raw_rows)} raw, "
                            f"{len(rec_60)} x60min, {len(rec_15)} x15min"
                        )

                    self.track_country(self.country_code, len(raw_rows))
                    total_raw += len(raw_rows)
            finally:
                if conn_ctx:
                    conn_ctx.__exit__(None, None, None)

            self.logger.info(self.format_summary(total_raw))
            return True

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return False


if __name__ == '__main__':
    ProcuredCapacityRunner.main()
