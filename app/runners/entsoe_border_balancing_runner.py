#!/usr/bin/env python3
"""
ENTSO-E Border Balancing Runner: cross-border balancing limits and exchanged energy.

Datasets (``--datasets``, default: all four):

    limits            per active border, both directions, processes mfrr / afrr / netting
                      documentType A31, businessType A26, processType A47 / A51 / A63
                      (in_Domain, out_Domain)              -> entsoe_border_balancing.limit_mw
    border_volumes    per active border, both directions, processes mfrr_sa / mfrr_da /
                      netting / afrr
                      documentType A30, processType A60 / A61 / A63 / A51
                      (Acquiring_Domain, Connecting_Domain) -> entsoe_border_balancing.exchanged_mwh
    area_volumes      per home area (CZ), processes netting / mfrr_sa / mfrr_da
                      documentType B17, processType A63 / A60 / A61,
                      Acquiring_Domain = Connecting_Domain = area
                                                           -> entsoe_area_balancing_exchange
    area_volumes_afrr same for process afrr (A51): 4-second MWh points, summed per
                      15-minute interval (complete intervals only). ENTSO-E allows
                      one day per request for this process (about 6.6 MB per day).

Direction semantics (see migration 092 for the evidence):
    limits          from = out_Domain, to = in_Domain (balancing energy may flow from -> to)
    border_volumes  from = connecting_Domain (delivers), to = acquiring_Domain (receives)
    area_volumes    import_mwh = TimeSeries with acquiring_Domain = area, export_mwh =
                    TimeSeries with connecting_Domain = area

Each dataset upserts ONLY its own value columns (``update_columns``), so datasets never
overwrite one another with NULL. The values are revisable (preliminary values are later
replaced by final ones), so the whole window is always re-fetched; ``skip_unchanged=True``
leaves identical rows (and their updated_at) alone.

Structurally empty combinations (``STRUCTURALLY_EMPTY``, probed on 2026-11-19) are not
requested; ``--no-skip-map`` requests everything.

Failure handling: "No matching data" is a normal empty result. Any other failure of one
request (border, direction, process, chunk) is recorded, the remaining requests carry
on, and the run summary reports the failed count; the run then exits 1.

Default window (no arguments): yesterday 00:00 through the end of today (Prague).
Backfill (--start/--end): that date range, 7-day chunks (1-day chunks for the aFRR volumes).

Usage:
    python3 -m runners.entsoe_border_balancing_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_border_balancing_runner --start 2026-11-12 --end 2026-11-19
    python3 -m runners.entsoe_border_balancing_runner --datasets limits,area_volumes --level 0
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.border_balancing_parser import (
    AreaVolumeParser,
    BalancingParserBase,
    BorderLimitParser,
    BorderVolumeParser,
)
from entsoe.borders import Border, directions, load_active_borders
from entsoe.client import EntsoeClient
from entsoe.query import fetch_document, fmt_period

# process name (as stored) -> ENTSO-E processType
LIMIT_PROCESSES: Dict[str, str] = {"mfrr": "A47", "afrr": "A51", "netting": "A63"}
BORDER_VOLUME_PROCESSES: Dict[str, str] = {
    "mfrr_sa": "A60", "mfrr_da": "A61", "netting": "A63", "afrr": "A51",
}
AREA_VOLUME_PROCESSES: Dict[str, str] = {"netting": "A63", "mfrr_sa": "A60", "mfrr_da": "A61"}
AREA_VOLUME_AFRR_PROCESSES: Dict[str, str] = {"afrr": "A51"}

# Datasets whose aFRR requests ENTSO-E restricts to one day (the netted and exchanged
# volumes data item, per border and per area; the aFRR limits accept 7 days).
ONE_DAY_AFRR_DATASETS = {"border_volumes", "area_volumes_afrr"}

DATASET_NAMES = ["limits", "border_volumes", "area_volumes", "area_volumes_afrr"]
BORDER_DATASETS = ("limits", "border_volumes")
AREA_DATASETS = ("area_volumes", "area_volumes_afrr")

# Home area(s) of the per-area datasets: every area of an active border in these countries.
HOME_COUNTRIES = ("CZ",)

_ALL_LIMIT = set(LIMIT_PROCESSES)
_ALL_VOLUME = set(BORDER_VOLUME_PROCESSES)
# (dataset -> border_code -> processes with no data in either direction), probed 2026-11-19
# (limits, border_volumes) and 2026-11-12..2026-11-19 (border_volumes). Borders not listed
# here, including every level-1 border, are always requested.
STRUCTURALLY_EMPTY: Dict[str, Dict[str, Set[str]]] = {
    "limits": {
        "CZ-DE_LU": _ALL_LIMIT,        # nothing under the DE_LU bidding zone
        "CZ-DE_TENNET": {"mfrr"},
    },
    "border_volumes": {
        "CZ-DE_LU": _ALL_VOLUME,
        "CZ-DE_TENNET": _ALL_VOLUME,
        "CZ-DE_50HERTZ": _ALL_VOLUME,
        "CZ-AT": {"netting", "afrr"},  # only mFRR (scheduled and direct) is reported
        "CZ-PL": _ALL_VOLUME,
        "CZ-SK": _ALL_VOLUME,
    },
}

BORDER_TABLE = "entsoe_border_balancing"
BORDER_KEY = ["delivery_datetime", "from_area_id", "to_area_id", "process"]
AREA_TABLE = "entsoe_area_balancing_exchange"
AREA_KEY = ["delivery_datetime", "area_id", "country_code", "process"]
TIME_COLUMNS = ["trade_date", "period", "time_interval"]
BORDER_VALUE_COLUMN = {"limits": "limit_mw", "border_volumes": "exchanged_mwh"}
AREA_VALUE_COLUMNS = ["import_mwh", "export_mwh", "doc_status"]


class BorderBalancingRunner(BaseRunner):
    """Runner for cross-border balancing limits and exchanged energy."""

    RUNNER_NAME = "ENTSO-E Border Balancing Runner"

    DEFAULT_DAYS_BACK = 1

    def __init__(
        self,
        datasets: Optional[List[str]] = None,
        level: Optional[int] = None,
        use_skip_map: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.datasets = list(datasets) if datasets else list(DATASET_NAMES)
        self.level = level
        self.use_skip_map = use_skip_map
        self.client: Optional[EntsoeClient] = None
        self.fetches = 0
        self.empty_fetches = 0
        self.failed_fetches = 0
        self.skipped_requests = 0
        self.dataset_rows: Dict[str, int] = {}
        # (dataset, label, process) -> rows seen, for the data-availability matrix
        self.matrix: Dict[Tuple[str, str, str], int] = {}

    # ------------------------------------------------------------------ CLI

    @classmethod
    def create_argument_parser(cls):
        parser = super().create_argument_parser()
        parser.add_argument(
            "--datasets",
            type=str,
            default=",".join(DATASET_NAMES),
            help=f"Comma-separated subset of: {','.join(DATASET_NAMES)} (default: all)",
        )
        parser.add_argument(
            "--level",
            type=int,
            default=None,
            help="Only borders of this level (0 = CZ borders); default: all active borders",
        )
        parser.add_argument(
            "--no-skip-map",
            action="store_true",
            help="Also request the (border, process) combinations known to be empty",
        )
        return parser

    @classmethod
    def main(cls) -> None:
        parser = cls.create_argument_parser()
        args = parser.parse_args()

        datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
        unknown = [d for d in datasets if d not in DATASET_NAMES]
        if unknown or not datasets:
            parser.error(f"--datasets must be a subset of {','.join(DATASET_NAMES)} (got {args.datasets!r})")

        try:
            start_date = cls.parse_date(args.start)
            end_date = cls.parse_date(args.end)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        runner = cls(
            datasets=datasets,
            level=args.level,
            use_skip_map=not args.no_skip_map,
            debug=args.debug,
            dry_run=args.dry_run,
            start_date=start_date,
            end_date=end_date,
            max_runtime=args.max_runtime,
        )
        sys.exit(0 if runner.run() else 1)

    # -------------------------------------------------------------- windows

    @staticmethod
    def _prague_midnight_utc(day) -> datetime:
        return datetime.combine(day, datetime.min.time()).replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)

    def _windows(self) -> List[Tuple[datetime, datetime]]:
        """The (start, end) UTC request windows of up to MAX_CHUNK_DAYS."""
        if self.is_backfill:
            return list(self.get_backfill_chunks())
        today = datetime.now(PRAGUE_TZ).date()
        start = self._prague_midnight_utc(today - timedelta(days=self.DEFAULT_DAYS_BACK))
        end = self._prague_midnight_utc(today + timedelta(days=1))
        chunks = []
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=self.MAX_CHUNK_DAYS), end)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end
        return chunks

    def _day_chunks(self, start: datetime, end: datetime):
        """Split [start, end) at Prague midnights (DST-safe 23/24/25-hour days)."""
        current = start
        while current < end:
            next_midnight = self._prague_midnight_utc(
                current.astimezone(PRAGUE_TZ).date() + timedelta(days=1)
            )
            chunk_end = min(next_midnight, end)
            yield current, chunk_end
            current = chunk_end

    def _chunks_for(self, dataset: str, process: str, start: datetime, end: datetime):
        if process == "afrr" and dataset in ONE_DAY_AFRR_DATASETS:
            yield from self._day_chunks(start, end)
        else:
            yield start, end

    # ---------------------------------------------------------------- fetch

    def _fetch_parse(
        self,
        params: Dict[str, str],
        parser: BalancingParserBase,
        label: str,
        process: str,
        start: datetime,
        end: datetime,
    ) -> Optional[Dict[datetime, dict]]:
        """Fetch and parse one request.

        Returns the rows keyed by slot (empty dict = document without points,
        None = "No matching data"). Failures raise to the caller.
        """
        documents = fetch_document(self.client, params)
        if documents is None:
            self.empty_fetches += 1
            self.logger.info(
                f"  {label} {process}: no matching data [{start:%Y-%m-%d %H:%M} - {end:%Y-%m-%d %H:%M} UTC]"
            )
            return None
        rows: Dict[datetime, dict] = {}
        for xml in documents:
            for row in parser.parse_xml_content(xml):
                rows[row["delivery_datetime"]] = row
        if parser.skipped_series:
            self.logger.warning(
                f"  {label} {process}: {parser.skipped_series} TimeSeries skipped (domain mismatch)"
            )
        if not rows:
            self.empty_fetches += 1
            self.logger.info(f"  {label} {process}: document without points")
        return rows

    def _record(self, dataset: str, label: str, process: str, count: int, written: int, owner: str) -> None:
        self.matrix[(dataset, label, process)] = self.matrix.get((dataset, label, process), 0) + count
        self.dataset_rows[dataset] = self.dataset_rows.get(dataset, 0) + written
        self.track_country(owner, written)
        self.logger.debug(f"  {label} {process}: {count} rows")

    def _run_border(
        self,
        conn,
        dataset: str,
        border: Border,
        direction: Tuple[int, str, int, str],
        label: str,
        process: str,
        start: datetime,
        end: datetime,
    ) -> None:
        """One border direction, one process, one chunk. Failures are recorded, never raised."""
        from_id, from_eic, to_id, to_eic = direction
        if dataset == "limits":
            params = {
                "documentType": "A31", "businessType": "A26",
                "processType": LIMIT_PROCESSES[process],
                "in_Domain": to_eic, "out_Domain": from_eic,
            }
            parser: BalancingParserBase = BorderLimitParser(from_id, from_eic, to_id, to_eic)
        else:
            params = {
                "documentType": "A30",
                "processType": BORDER_VOLUME_PROCESSES[process],
                "Acquiring_Domain": to_eic, "Connecting_Domain": from_eic,
            }
            parser = BorderVolumeParser(from_id, from_eic, to_id, to_eic)
        params.update({"periodStart": fmt_period(start), "periodEnd": fmt_period(end)})
        column = BORDER_VALUE_COLUMN[dataset]
        self.fetches += 1
        try:
            rows = self._fetch_parse(params, parser, label, process, start, end)
            if not rows:
                self._record(dataset, label, process, 0, 0, border.border_code)
                return
            columns = BORDER_KEY + TIME_COLUMNS + [column]
            records = [
                (
                    r["delivery_datetime"], r["from_area_id"], r["to_area_id"], process,
                    r["trade_date"], r["period"], r["time_interval"], r["value"],
                )
                for r in (rows[k] for k in sorted(rows))
            ]
            written = self.bulk_upsert(
                conn, BORDER_TABLE, columns, records, BORDER_KEY,
                update_columns=[column], skip_unchanged=True,
            )
            self._record(dataset, label, process, len(records), written, border.border_code)
        except Exception as e:
            self.failed_fetches += 1
            self.record_area_failure(f"{label} {dataset}/{process}", e)

    def _run_area(
        self,
        conn,
        dataset: str,
        area: Tuple[int, str, str],
        process: str,
        process_type: str,
        start: datetime,
        end: datetime,
    ) -> None:
        """One area, one process, one chunk. Failures are recorded, never raised."""
        area_id, area_eic, country = area
        label = f"{country}[{area_eic}]"
        params = {
            "documentType": "B17",
            "processType": process_type,
            "Acquiring_Domain": area_eic, "Connecting_Domain": area_eic,
            "periodStart": fmt_period(start), "periodEnd": fmt_period(end),
        }
        parser = AreaVolumeParser(area_id, area_eic, country)
        self.fetches += 1
        try:
            rows = self._fetch_parse(params, parser, label, process, start, end)
            if not rows:
                self._record(dataset, label, process, 0, 0, country)
                return
            columns = AREA_KEY + TIME_COLUMNS + AREA_VALUE_COLUMNS
            records = [
                (
                    r["delivery_datetime"], r["area_id"], r["country_code"], process,
                    r["trade_date"], r["period"], r["time_interval"],
                    r["import_mwh"], r["export_mwh"], r["doc_status"],
                )
                for r in (rows[k] for k in sorted(rows))
            ]
            written = self.bulk_upsert(
                conn, AREA_TABLE, columns, records, AREA_KEY,
                update_columns=AREA_VALUE_COLUMNS, skip_unchanged=True,
            )
            self._record(dataset, label, process, len(records), written, country)
        except Exception as e:
            self.failed_fetches += 1
            self.record_area_failure(f"{label} {dataset}/{process}", e)

    # ------------------------------------------------------------- datasets

    def _skip(self, dataset: str, border: Border, process: str) -> bool:
        if not self.use_skip_map:
            return False
        return process in STRUCTURALLY_EMPTY.get(dataset, {}).get(border.border_code, set())

    def _border_dataset(self, conn, dataset: str, borders: List[Border], windows) -> None:
        processes = LIMIT_PROCESSES if dataset == "limits" else BORDER_VOLUME_PROCESSES
        for start, end in windows:
            for border in borders:
                for idx, direction in enumerate(directions(border)):
                    label = f"{border.border_code}[{'a>b' if idx == 0 else 'b>a'}]"
                    for process in processes:
                        if self._skip(dataset, border, process):
                            self.skipped_requests += 1
                            continue
                        for chunk_start, chunk_end in self._chunks_for(dataset, process, start, end):
                            if self.deadline_exceeded():
                                return
                            self._run_border(
                                conn, dataset, border, direction, label, process, chunk_start, chunk_end
                            )

    def _area_dataset(self, conn, dataset: str, areas: List[Tuple[int, str, str]], windows) -> None:
        processes = AREA_VOLUME_PROCESSES if dataset == "area_volumes" else AREA_VOLUME_AFRR_PROCESSES
        for start, end in windows:
            for area in areas:
                for process, process_type in processes.items():
                    for chunk_start, chunk_end in self._chunks_for(dataset, process, start, end):
                        if self.deadline_exceeded():
                            return
                        self._run_area(conn, dataset, area, process, process_type, chunk_start, chunk_end)

    @staticmethod
    def _home_areas(borders: List[Border]) -> List[Tuple[int, str, str]]:
        """(area_id, eic, country) of every border area in HOME_COUNTRIES, deduplicated."""
        found: Dict[int, Tuple[int, str, str]] = {}
        for b in borders:
            for area_id, eic, country in (
                (b.area_a_id, b.area_a_eic, b.area_a_country),
                (b.area_b_id, b.area_b_eic, b.area_b_country),
            ):
                if country in HOME_COUNTRIES:
                    found[area_id] = (area_id, eic, country)
        return [found[k] for k in sorted(found)]

    def _log_matrix(self) -> None:
        """Debug-level data-availability matrix: rows seen per (dataset, border direction, process)."""
        for (dataset, label, process), count in sorted(self.matrix.items()):
            self.logger.debug(f"MATRIX {dataset:<18} {label:<22} {process:<8} rows={count}")

    # ------------------------------------------------------------------ run

    def run(self) -> bool:
        self.print_header()
        try:
            self.client = EntsoeClient()
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            return False

        try:
            with self.database_connection() as conn:
                borders = load_active_borders(conn, level=self.level)
                if not borders:
                    self.logger.error(f"{self.RUNNER_NAME}: no active borders (level={self.level})")
                    self.print_footer(success=False)
                    return False
                areas = self._home_areas(borders)
                self.logger.debug(
                    f"Borders: {', '.join(b.border_code for b in borders)}; "
                    f"areas: {', '.join(a[2] for a in areas)}; datasets: {', '.join(self.datasets)}"
                )
                windows = self._windows()

                for dataset in self.datasets:
                    self.logger.debug(f"{dataset}: {len(windows)} window(s)")
                    if dataset in BORDER_DATASETS:
                        self._border_dataset(conn, dataset, borders, windows)
                    else:
                        self._area_dataset(conn, dataset, areas, windows)

            self._log_matrix()
            total = sum(self.dataset_rows.values())
            per_dataset = ", ".join(f"{d}={n}" for d, n in sorted(self.dataset_rows.items()))
            summary = (
                f"{self.format_summary(total)}; datasets: {per_dataset or 'none'}; "
                f"requests: {self.fetches} ({self.empty_fetches} empty, {self.failed_fetches} failed, "
                f"{self.skipped_requests} skipped by skip-map)"
            )
            if self.failed_fetches:
                self.logger.warning(f"{summary} - FAILED requests: {', '.join(sorted(self.area_failures))}")
            else:
                self.logger.info(summary)
            self.print_footer(success=not self.failed_fetches)
            return self.failed_fetches == 0

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            self.print_footer(success=False)
            return False


if __name__ == "__main__":
    BorderBalancingRunner.main()
