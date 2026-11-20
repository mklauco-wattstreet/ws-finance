#!/usr/bin/env python3
"""
ENTSO-E Border Flow Runner: per-border, per-direction 15-minute flows.

For every active border in ``entsoe_borders`` (both directions) it fetches
    physical         documentType=A11                              -> physical_mw
    scheduled_da     documentType=A09, contract_MarketAgreement.Type=A01 -> scheduled_da_mw
    scheduled_total  documentType=A09, contract_MarketAgreement.Type=A05 -> scheduled_total_mw
and upserts them into ``entsoe_border_flows``. Rows are directed: energy goes
from ``from_area_id`` (ENTSO-E out_Domain) to ``to_area_id`` (in_Domain), with
non-negative values exactly as published (never netted, never sign-flipped).

Each dataset upserts ONLY its own value column (``update_columns=[column]``),
so the three datasets never overwrite each other with NULL. The data is
revisable, so the whole window is always re-fetched; ``skip_unchanged=True``
keeps identical rows from being rewritten (updated_at is left alone).

Failure handling: "No matching data" is a normal empty result. Any other failure
of one (border, direction, dataset) fetch is recorded, the remaining requests
carry on, and the run summary reports the failed count; the run then exits 1.

Default window (no arguments):
    physical         D-2 00:00 (Prague) through now
    scheduled_*      D-1 00:00 through the end of D+1 (day-ahead schedules for
                     D+1 appear in the afternoon)
Backfill (--start/--end): that date range for every selected dataset, in
7-day chunks.

Usage:
    python3 -m runners.entsoe_border_flow_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_border_flow_runner --start 2026-11-12 --end 2026-11-19
    python3 -m runners.entsoe_border_flow_runner --datasets physical --level 0
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.border_flow_parser import BorderFlowParser
from entsoe.borders import Border, directions, load_active_borders
from entsoe.client import EntsoeClient
from entsoe.query import fetch_document, fmt_period

# dataset name -> (ENTSO-E query params, value column)
DATASETS: Dict[str, Tuple[Dict[str, str], str]] = {
    "physical": ({"documentType": "A11"}, "physical_mw"),
    "scheduled_da": (
        {"documentType": "A09", "contract_MarketAgreement.Type": "A01"},
        "scheduled_da_mw",
    ),
    "scheduled_total": (
        {"documentType": "A09", "contract_MarketAgreement.Type": "A05"},
        "scheduled_total_mw",
    ),
}
PHYSICAL_DATASETS = ("physical",)


class BorderFlowRunner(BaseRunner):
    """Runner for physical flows and scheduled exchanges on every active border."""

    RUNNER_NAME = "ENTSO-E Border Flow Runner"

    TABLE_NAME = "entsoe_border_flows"
    KEY_COLUMNS = ["delivery_datetime", "from_area_id", "to_area_id"]
    TIME_COLUMNS = ["trade_date", "period", "time_interval"]
    CONFLICT_COLUMNS = KEY_COLUMNS

    DEFAULT_PHYSICAL_DAYS_BACK = 2
    DEFAULT_SCHEDULED_DAYS_BACK = 1
    DEFAULT_SCHEDULED_DAYS_AHEAD = 1

    def __init__(
        self,
        datasets: Optional[List[str]] = None,
        level: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.datasets = list(datasets) if datasets else list(DATASETS)
        self.level = level
        self.client: Optional[EntsoeClient] = None
        self.fetches = 0
        self.empty_fetches = 0
        self.failed_fetches = 0
        self.dataset_rows: Dict[str, int] = {}

    # ------------------------------------------------------------------ CLI

    @classmethod
    def create_argument_parser(cls):
        parser = super().create_argument_parser()
        parser.add_argument(
            "--datasets",
            type=str,
            default=",".join(DATASETS),
            help=f"Comma-separated subset of: {','.join(DATASETS)} (default: all)",
        )
        parser.add_argument(
            "--level",
            type=int,
            default=None,
            help="Only borders of this level (0 = CZ borders); default: all active borders",
        )
        return parser

    @classmethod
    def main(cls) -> None:
        parser = cls.create_argument_parser()
        args = parser.parse_args()

        datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
        unknown = [d for d in datasets if d not in DATASETS]
        if unknown or not datasets:
            parser.error(f"--datasets must be a subset of {','.join(DATASETS)} (got {args.datasets!r})")

        try:
            start_date = cls.parse_date(args.start)
            end_date = cls.parse_date(args.end)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        runner = cls(
            datasets=datasets,
            level=args.level,
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

    def _split(self, start: datetime, end: datetime):
        """Split [start, end) into MAX_CHUNK_DAYS requests."""
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=self.MAX_CHUNK_DAYS), end)
            yield chunk_start, chunk_end
            chunk_start = chunk_end

    def _windows(self, dataset: str):
        """Yield the (start, end) UTC request chunks for one dataset."""
        if self.is_backfill:
            yield from self.get_backfill_chunks()
            return

        today = datetime.now(PRAGUE_TZ).date()
        if dataset in PHYSICAL_DATASETS:
            start = self._prague_midnight_utc(today - timedelta(days=self.DEFAULT_PHYSICAL_DAYS_BACK))
            now = datetime.now(timezone.utc)
            # Round up to the next 15-minute boundary so the running interval is requested.
            end = now.replace(second=0, microsecond=0)
            end += timedelta(minutes=(15 - end.minute % 15) % 15)
            if end <= now:
                end += timedelta(minutes=15)
        else:
            start = self._prague_midnight_utc(today - timedelta(days=self.DEFAULT_SCHEDULED_DAYS_BACK))
            end = self._prague_midnight_utc(today + timedelta(days=self.DEFAULT_SCHEDULED_DAYS_AHEAD + 1))
        yield from self._split(start, end)

    # ---------------------------------------------------------------- fetch

    def _process_direction(
        self,
        conn,
        border: Border,
        direction: Tuple[int, str, int, str],
        label: str,
        dataset: str,
        start: datetime,
        end: datetime,
    ) -> int:
        """Fetch, parse and upsert one dataset for one border direction and chunk.

        Returns rows written; any failure is recorded (not raised) so the other
        requests still run.
        """
        from_id, from_eic, to_id, to_eic = direction
        base_params, column = DATASETS[dataset]
        params = dict(base_params)
        params.update({
            "in_Domain": to_eic,
            "out_Domain": from_eic,
            "periodStart": fmt_period(start),
            "periodEnd": fmt_period(end),
        })
        failure_key = f"{label} {dataset}"
        self.fetches += 1

        try:
            documents = fetch_document(self.client, params)
            if documents is None:
                self.empty_fetches += 1
                self.logger.info(
                    f"  {label} {dataset}: no matching data "
                    f"[{start:%Y-%m-%d %H:%M} - {end:%Y-%m-%d %H:%M} UTC]"
                )
                return 0

            parser = BorderFlowParser(from_id, from_eic, to_id, to_eic)
            rows: Dict[datetime, dict] = {}
            for xml in documents:
                for row in parser.parse_xml_content(xml):
                    rows[row["delivery_datetime"]] = row
            if parser.skipped_series:
                self.logger.warning(
                    f"  {label} {dataset}: {parser.skipped_series} TimeSeries skipped "
                    f"(direction mismatch)"
                )
            if not rows:
                self.empty_fetches += 1
                self.logger.info(f"  {label} {dataset}: document without points")
                return 0

            columns = self.KEY_COLUMNS + self.TIME_COLUMNS + [column]
            records = [
                (
                    r["delivery_datetime"], r["from_area_id"], r["to_area_id"],
                    r["trade_date"], r["period"], r["time_interval"], r["value"],
                )
                for r in (rows[k] for k in sorted(rows))
            ]
            self.logger.debug(f"  {label} {dataset}: {len(records)} rows")
            written = self.bulk_upsert(
                conn,
                self.TABLE_NAME,
                columns,
                records,
                self.CONFLICT_COLUMNS,
                update_columns=[column],
                skip_unchanged=True,
            )
            self.dataset_rows[dataset] = self.dataset_rows.get(dataset, 0) + written
            self.track_country(border.border_code, written)
            return written
        except Exception as e:
            self.failed_fetches += 1
            self.record_area_failure(failure_key, e)
            return 0

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
                # End the read transaction now: nothing else commits before the first write,
                # and idle_in_transaction_session_timeout kills a session left idle in a
                # transaction while the HTTP fetches run.
                conn.rollback()
                if not borders:
                    self.logger.error(f"{self.RUNNER_NAME}: no active borders (level={self.level})")
                    self.print_footer(success=False)
                    return False
                self.logger.debug(
                    f"Borders: {', '.join(b.border_code for b in borders)}; datasets: {', '.join(self.datasets)}"
                )

                for dataset in self.datasets:
                    for start, end in self._windows(dataset):
                        self.logger.debug(f"{dataset}: {start:%Y-%m-%d %H:%M} - {end:%Y-%m-%d %H:%M} UTC")
                        for border in borders:
                            for idx, direction in enumerate(directions(border)):
                                if self.deadline_exceeded():
                                    break
                                label = f"{border.border_code}[{'a>b' if idx == 0 else 'b>a'}]"
                                self._process_direction(
                                    conn, border, direction, label, dataset, start, end
                                )

            total = sum(self.dataset_rows.values())
            per_dataset = ", ".join(f"{d}={n}" for d, n in sorted(self.dataset_rows.items()))
            summary = (
                f"{self.format_summary(total)}; datasets: {per_dataset or 'none'}; "
                f"requests: {self.fetches} ({self.empty_fetches} empty, {self.failed_fetches} failed)"
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
    BorderFlowRunner.main()
