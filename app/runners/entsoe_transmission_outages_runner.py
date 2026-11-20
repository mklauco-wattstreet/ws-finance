#!/usr/bin/env python3
"""
ENTSO-E Transmission Outages Runner (A78, unavailability of transmission
infrastructure) for the active level-0 borders.

Per border direction (both directions of every active CZ border, from
``entsoe_borders``) the runner asks for the outages that overlap a window
(``periodStart``/``periodEnd``), pages through the answer 200 ZIP members at a
time (``offset`` 0, 200, 400, ...), and stores:

    entsoe_transmission_outages        one row per (document, revision, TimeSeries)
    entsoe_transmission_outage_points  remaining capacity (MW) as validity segments

Two passes per direction and window: the default answer (active A05 plus
cancelled A09 documents) and ``DocStatus=A13`` (withdrawn documents, which are
not returned by default). All revisions are kept (the revision number is part of
the key). Direction of each row = the TimeSeries' own ``out_Domain`` ->
``in_Domain``; the two directions of a border come back as different documents
(different mRIDs), so they never collide on the primary key.

Window logic:
  * default (no args): outages overlapping [today - 1 day, today + 30 days]
    (Prague dates, inclusive), always re-fetched, because outages are revised
    and cancelled;
  * backfill (--start/--end): chunked month by month (Prague calendar months),
    start clamped to OUTAGE_BACKFILL_FLOOR like the generation outages runner.

Idempotent: rows are upserted with skip_unchanged; the points of an outage are
only replaced when they differ from what is stored.

A failed fetch for one border direction does not abort the others; failures are
counted and make the run exit non-zero.

Usage:
    python3 -m runners.entsoe_transmission_outages_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_transmission_outages_runner --start 2026-11-12 --end 2026-11-19
"""

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from psycopg2 import extras

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.borders import Border, directions, load_active_borders
from entsoe.client import EntsoeClient
from entsoe.query import fetch_document, fmt_period
from entsoe.transmission_outages_parser import parse_transmission_outage_document

OUTAGE_BACKFILL_FLOOR = date(2026, 3, 1)
DOC_TYPE_A78 = "A78"
PAGE_SIZE = 200  # ENTSO-E returns at most 200 documents per request

# Default window: today - DEFAULT_DAYS_BACK .. today + DEFAULT_DAYS_FORWARD (inclusive).
DEFAULT_DAYS_BACK = 1
DEFAULT_DAYS_FORWARD = 30

# (docStatus filter, label): None = ENTSO-E default (active + cancelled).
FETCH_SPECS: List[Tuple[Optional[str], str]] = [
    (None, "active+cancelled"),
    ("A13", "withdrawn"),
]

OUTAGE_TABLE = "entsoe_transmission_outages"
OUTAGE_COLUMNS = [
    "doc_mrid", "revision_number", "timeseries_mrid", "from_area_id", "to_area_id",
    "business_type", "doc_status", "created_datetime", "asset_mrid", "asset_name",
    "asset_location", "asset_type", "unavail_start", "unavail_end", "quantity_unit",
    "curve_type", "min_capacity_mw", "reason_code", "reason_text",
]
OUTAGE_CONFLICT = ["doc_mrid", "revision_number", "timeseries_mrid"]
POINT_TABLE = "entsoe_transmission_outage_points"

KeyT = Tuple[str, int, str]


def _prague_midnight_utc(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()).replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)


def _first_of_next_month(d: date) -> date:
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def _num(value: Any) -> Optional[Decimal]:
    return None if value is None else Decimal(str(value)).quantize(Decimal("0.001"))


class TransmissionOutagesRunner(BaseRunner):
    """Runner for ENTSO-E transmission unavailability (A78)."""

    RUNNER_NAME = "ENTSO-E Transmission Outages Runner"
    TABLE_NAME = OUTAGE_TABLE
    COLUMNS = OUTAGE_COLUMNS
    CONFLICT_COLUMNS = OUTAGE_CONFLICT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client: Optional[EntsoeClient] = None
        self.failures: List[str] = []
        self.outage_rows = 0
        self.point_rows = 0

    # ------------------------------------------------------------------ windows
    def _windows(self) -> List[Tuple[datetime, datetime]]:
        """UTC request windows: one default window, or Prague calendar months."""
        today = datetime.now(PRAGUE_TZ).date()
        if not self.is_backfill:
            return [(
                _prague_midnight_utc(today - timedelta(days=DEFAULT_DAYS_BACK)),
                _prague_midnight_utc(today + timedelta(days=DEFAULT_DAYS_FORWARD + 1)),
            )]

        start = self.start_date or OUTAGE_BACKFILL_FLOOR
        if start < OUTAGE_BACKFILL_FLOOR:
            self.logger.info(f"Clamping start {start} -> {OUTAGE_BACKFILL_FLOOR} (backfill floor)")
            start = OUTAGE_BACKFILL_FLOOR
        end = self.end_date or today
        if end < start:
            raise ValueError(f"end_date ({end}) must be >= start_date ({start})")

        windows = []
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(_first_of_next_month(chunk_start), end + timedelta(days=1))
            windows.append((_prague_midnight_utc(chunk_start), _prague_midnight_utc(chunk_end)))
            chunk_start = chunk_end
        return windows

    # -------------------------------------------------------------------- fetch
    def _fetch_all(
        self, from_eic: str, to_eic: str, start: datetime, end: datetime,
        doc_status: Optional[str],
    ) -> List[str]:
        """All documents of one direction/window/status, paged by offset.

        Raises on a failed request; the caller counts it as a failure.
        """
        documents: List[str] = []
        offset = 0
        while True:
            params = {
                "documentType": DOC_TYPE_A78,
                "in_Domain": to_eic,
                "out_Domain": from_eic,
                "periodStart": fmt_period(start),
                "periodEnd": fmt_period(end),
                "offset": str(offset),
            }
            if doc_status:
                params["DocStatus"] = doc_status
            batch = fetch_document(self.client, params)
            if not batch:
                break
            documents.extend(batch)
            self.logger.debug(f"    offset={offset}: +{len(batch)} docs (total {len(documents)})")
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return documents

    # -------------------------------------------------------------------- store
    def _store(self, conn, rows: Dict[KeyT, Dict[str, Any]]) -> Tuple[int, int]:
        """Upsert outage rows, then replace the points of rows whose points differ."""
        if not rows:
            return 0, 0
        records = [tuple(row[c] for c in OUTAGE_COLUMNS) for row in rows.values()]
        n_points = sum(len(r["points"]) for r in rows.values())
        if self.dry_run:
            self.logger.info(f"DRY RUN - Would upsert {len(records)} records to {OUTAGE_TABLE} "
                             f"and {n_points} point segments to {POINT_TABLE}")
            return len(records), n_points

        self.bulk_upsert(conn, OUTAGE_TABLE, OUTAGE_COLUMNS, records, OUTAGE_CONFLICT,
                         skip_unchanged=True)
        id_by_key = self._load_ids(conn, list(rows))
        self._replace_points(conn, rows, id_by_key)
        return len(records), n_points

    def _load_ids(self, conn, keys: List[KeyT]) -> Dict[KeyT, int]:
        """Fetch the ``id`` of the stored rows by natural key (one query per page)."""
        query = (
            f"SELECT o.id, o.doc_mrid, o.revision_number, o.timeseries_mrid FROM {OUTAGE_TABLE} o "
            f"JOIN (VALUES %s) AS v(doc_mrid, revision_number, timeseries_mrid) "
            f"ON o.doc_mrid = v.doc_mrid AND o.revision_number = v.revision_number "
            f"AND o.timeseries_mrid = v.timeseries_mrid"
        )
        with conn.cursor() as cur:
            found = extras.execute_values(
                cur, query, keys, template="(%s, %s::int, %s)", page_size=1000, fetch=True
            )
        conn.commit()
        return {(r[1], r[2], r[3]): r[0] for r in found}

    def _replace_points(self, conn, rows: Dict[KeyT, Dict[str, Any]], id_by_key: Dict[KeyT, int]) -> None:
        """Replace the point set of every outage whose stored points differ."""
        ids = list(id_by_key.values())
        stored: Dict[int, set] = {i: set() for i in ids}
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT outage_id, point_start, point_end, capacity_mw FROM {POINT_TABLE} "
                f"WHERE outage_id = ANY(%s)", (ids,)
            )
            for outage_id, p_start, p_end, capacity in cur.fetchall():
                stored[outage_id].add((p_start, p_end, capacity))
        conn.commit()

        changed_ids: List[int] = []
        new_records: List[Tuple] = []
        for key, row in rows.items():
            outage_id = id_by_key.get(key)
            if outage_id is None:
                self.logger.warning(f"No id found for outage {key}, points skipped")
                continue
            wanted = {(s, e, _num(c)) for s, e, c in row["points"]}
            if wanted == stored[outage_id]:
                continue
            changed_ids.append(outage_id)
            new_records.extend((outage_id, s, e, c) for s, e, c in wanted)

        if not changed_ids:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {POINT_TABLE} WHERE outage_id = ANY(%s)", (changed_ids,))
                if new_records:
                    extras.execute_values(
                        cur,
                        f"INSERT INTO {POINT_TABLE} (outage_id, point_start, point_end, capacity_mw) VALUES %s",
                        new_records, page_size=1000,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        self.logger.debug(f"    replaced points of {len(changed_ids)} outages "
                          f"({len(new_records)} segments)")

    # ---------------------------------------------------------------------- run
    def _process_direction(
        self, conn, border: Border, direction: Tuple[int, str, int, str],
        area_by_eic: Dict[str, int], windows: List[Tuple[datetime, datetime]],
    ) -> int:
        from_id, from_eic, to_id, to_eic = direction
        label = f"{border.border_code} {from_eic}->{to_eic}"
        parsed: Dict[KeyT, Dict[str, Any]] = {}
        failed = False

        for start, end in windows:
            for doc_status, status_label in FETCH_SPECS:
                tag = f"{border.border_code}/{from_id}->{to_id}/{status_label}"
                self.logger.debug(f"  Fetching {tag} [{fmt_period(start)} - {fmt_period(end)} UTC]")
                try:
                    documents = self._fetch_all(from_eic, to_eic, start, end, doc_status)
                except Exception as e:  # noqa: BLE001 - per-direction isolation by design
                    failed = True
                    self.failures.append(tag)
                    self.record_area_failure(tag, e)
                    continue
                self.logger.debug(f"    {tag}: {len(documents)} documents")
                for xml_text in documents:
                    try:
                        for row in parse_transmission_outage_document(
                            xml_text, area_by_eic, fallback_direction=(from_id, to_id)
                        ):
                            parsed[(row["doc_mrid"], row["revision_number"], row["timeseries_mrid"])] = row
                    except Exception as e:  # noqa: BLE001
                        failed = True
                        self.failures.append(f"{tag}/parse")
                        self.record_area_failure(f"{tag}/parse", e)

        try:
            n_rows, n_points = self._store(conn, parsed)
        except Exception as e:  # noqa: BLE001
            conn.rollback()
            self.failures.append(f"{label}/store")
            self.record_area_failure(f"{label}/store", e)
            return 0
        self.outage_rows += n_rows
        self.point_rows += n_points
        self.track_country(border.border_code, n_rows)
        self.logger.info(f"{label}: {n_rows} outage rows, {n_points} point segments"
                         f"{' (some fetches failed)' if failed else ''}")
        return n_rows

    def run(self) -> bool:
        self.print_header()
        try:
            self.client = EntsoeClient()
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            return False

        try:
            windows = self._windows()
            with self.database_connection() as conn:
                borders = load_active_borders(conn, level=0)
                area_by_eic: Dict[str, int] = {}
                for b in borders:
                    area_by_eic[b.area_a_eic] = b.area_a_id
                    area_by_eic[b.area_b_eic] = b.area_b_id
                self.logger.info(
                    f"{self.RUNNER_NAME}: {len(borders)} borders, {len(windows)} window(s)"
                    f"{' (dry-run)' if self.dry_run else ''}"
                )
                total = 0
                for border in borders:
                    for direction in directions(border):
                        total += self._process_direction(conn, border, direction, area_by_eic, windows)

            self.logger.info(self.format_summary(total) + f", {self.point_rows} point segments")
            if self.failures:
                self.logger.warning(
                    f"{self.RUNNER_NAME}: {len(self.failures)} fetch/parse/store step(s) failed: "
                    f"{', '.join(sorted(set(self.failures)))}"
                )
            self.print_footer(success=not self.failures)
            return not self.failures
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            self.print_footer(success=False)
            return False


if __name__ == "__main__":
    TransmissionOutagesRunner.main()
