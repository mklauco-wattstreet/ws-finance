#!/usr/bin/env python3
"""
JAO Core Runner - JAO Publication Tool, Core flow-based region.

Fetches, per Prague delivery day, the day-ahead Core results and the intraday
(Core ID) capacity and auction results, and upserts them into seven tables.
Every Core hub and border present in a response is stored (not only CZ), with
JAO's own hub codes as text.

Dataset key           Source endpoint(s)                       Table
--------------------  ---------------------------------------  ------------------------------
maxNetPos             core/maxNetPos                           jao_core_max_net_position
maxExchanges          core/maxExchanges                        jao_core_max_exchange
netPos                core/netPos                (market da)   jao_core_net_position
scheduledExchanges    core/scheduledExchanges    (market da)   jao_core_scheduled_exchange
priceSpread           core/priceSpread           (market da)   jao_core_price_spread
allocationConstraint  core/allocationConstraint  (market da)   jao_core_allocation_constraint
intradayAtc           coreID/IDCC{A..E}_intradayAtc + _intradayNtc
                                                  (round A-E)  jao_core_intraday_atc
idNetPos              coreID/ID{1,2,3}_netPos    (ida1..ida3)  jao_core_net_position
idScheduledExchanges  coreID/ID{1,2,3}_scheduledExchanges      jao_core_scheduled_exchange
idPriceSpread         coreID/ID{1,2,3}_priceSpread             jao_core_price_spread

The column mapping of every endpoint is documented in jao/parsers.py.

Window: one request per dataset per delivery day (the API refuses ranges over
two days). Default (cron) window is yesterday through tomorrow, Prague local
time (day-ahead results for D+1 exist from about 13:00 on D). --start/--end
iterate day by day. Every upsert is ON CONFLICT idempotent with
skip_unchanged=True, so re-polling never bumps updated_at on unchanged rows.

One failing dataset or day never aborts the run; failures are summarised once
in the footer. The run fails only if no request succeeded at all.

Usage:
    python3 -m runners.jao_core_runner [--debug] [--dry-run]
    python3 -m runners.jao_core_runner --start 2026-11-12 --end 2026-11-19
    python3 -m runners.jao_core_runner --datasets maxNetPos,netPos --start 2026-11-12 --end 2026-11-12
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import sentry_init  # noqa: F401,E402 - must be first to capture errors

from runners.base_runner import BaseRunner, PRAGUE_TZ  # noqa: E402
from dst_calendar import day_start_utc  # noqa: E402
from jao.client import JaoClient  # noqa: E402
from jao import parsers  # noqa: E402

# Intraday capacity-calculation rounds: calc_round -> endpoint prefix.
CAPACITY_ROUNDS: Dict[str, str] = {
    "A": "IDCCA", "B": "IDCCB", "C": "IDCCC", "D": "IDCCD", "E": "IDCCE",
}
# Intraday auctions: market -> endpoint prefix.
AUCTIONS: Dict[str, str] = {"ida1": "ID1", "ida2": "ID2", "ida3": "ID3"}

# table -> (columns, conflict columns)
TABLES: Dict[str, Tuple[List[str], List[str]]] = {
    "jao_core_max_net_position": (
        ["delivery_datetime", "hub", "min_mw", "max_mw"],
        ["delivery_datetime", "hub"],
    ),
    "jao_core_max_exchange": (
        ["delivery_datetime", "from_hub", "to_hub", "max_bex_mw"],
        ["delivery_datetime", "from_hub", "to_hub"],
    ),
    "jao_core_net_position": (
        ["delivery_datetime", "hub", "market", "net_position_mw"],
        ["delivery_datetime", "hub", "market"],
    ),
    "jao_core_scheduled_exchange": (
        ["delivery_datetime", "from_hub", "to_hub", "market", "scheduled_mw"],
        ["delivery_datetime", "from_hub", "to_hub", "market"],
    ),
    "jao_core_price_spread": (
        ["delivery_datetime", "from_hub", "to_hub", "market", "spread_eur"],
        ["delivery_datetime", "from_hub", "to_hub", "market"],
    ),
    "jao_core_allocation_constraint": (
        ["delivery_datetime", "hub", "market", "limit_up_mw", "limit_down_mw"],
        ["delivery_datetime", "hub", "market"],
    ),
    "jao_core_intraday_atc": (
        ["delivery_datetime", "from_hub", "to_hub", "calc_round", "atc_mw", "ntc_mw"],
        ["delivery_datetime", "from_hub", "to_hub", "calc_round"],
    ),
}

DATASET_KEYS: Tuple[str, ...] = (
    "maxNetPos", "maxExchanges", "netPos", "scheduledExchanges", "priceSpread",
    "allocationConstraint", "intradayAtc", "idNetPos", "idScheduledExchanges",
    "idPriceSpread",
)


@dataclass
class Batch:
    """Rows for one upsert call: explicit table, columns and conflict key."""

    dataset: str
    table: str
    columns: List[str]
    rows: List[Tuple]


class JaoCoreRunner(BaseRunner):
    """Runner for JAO Core day-ahead and intraday publication data."""

    RUNNER_NAME = "JAO Core Runner"

    # Default (non-backfill) window, Prague local days around today.
    DEFAULT_DAYS_BACK = 1
    DEFAULT_DAYS_FORWARD = 1

    def __init__(self, datasets: Optional[List[str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.datasets: List[str] = list(datasets) if datasets else list(DATASET_KEYS)
        unknown = [d for d in self.datasets if d not in DATASET_KEYS]
        if unknown:
            raise ValueError(
                f"Unknown dataset(s) {unknown}; valid: {', '.join(DATASET_KEYS)}"
            )
        self.client: Optional[JaoClient] = None
        self.unresolved_columns: Set[str] = set()
        self.fetch_ok = 0
        self.fetch_failed = 0
        self._handlers: Dict[str, Callable[[datetime, datetime], List[Batch]]] = {
            "maxNetPos": self._ds_max_net_pos,
            "maxExchanges": self._ds_max_exchanges,
            "netPos": self._ds_net_pos,
            "scheduledExchanges": self._ds_scheduled_exchanges,
            "priceSpread": self._ds_price_spread,
            "allocationConstraint": self._ds_allocation_constraint,
            "intradayAtc": self._ds_intraday_atc,
            "idNetPos": self._ds_id_net_pos,
            "idScheduledExchanges": self._ds_id_scheduled_exchanges,
            "idPriceSpread": self._ds_id_price_spread,
        }

    # ------------------------------------------------------------------
    # Fetch helpers (count successes / failures, never raise)
    # ------------------------------------------------------------------

    def _core(self, endpoint: str, start: datetime, end: datetime, key: str):
        """Fetch a Core endpoint; None when the request failed."""
        try:
            rows = self.client.fetch_core(endpoint, start, end)
        except Exception as exc:
            self._note_failure(key, exc)
            return None
        self.fetch_ok += 1
        return rows

    def _core_id(self, prefix: str, endpoint: str, start: datetime, end: datetime, key: str):
        """Fetch a Core ID endpoint; None when the request failed."""
        try:
            rows = self.client.fetch_core_id(prefix, endpoint, start, end)
        except Exception as exc:
            self._note_failure(f"{key}/{prefix}", exc)
            return None
        self.fetch_ok += 1
        return rows

    def _note_failure(self, key: str, exc: Exception) -> None:
        self.fetch_failed += 1
        if self.is_data_unavailable_error(exc):
            self.logger.info(f"  {key}: not available ({exc})")
        self.record_area_failure(key, exc)

    # ------------------------------------------------------------------
    # Dataset handlers: fetch + parse one delivery-day window -> batches
    # ------------------------------------------------------------------

    def _ds_max_net_pos(self, start: datetime, end: datetime) -> List[Batch]:
        data = self._core("maxNetPos", start, end, "maxNetPos")
        rows = parsers.parse_max_net_position(data) if data else []
        return [Batch("maxNetPos", "jao_core_max_net_position",
                      TABLES["jao_core_max_net_position"][0], rows)]

    def _ds_max_exchanges(self, start: datetime, end: datetime) -> List[Batch]:
        data = self._core("maxExchanges", start, end, "maxExchanges")
        rows = parsers.parse_max_exchange(data, self.unresolved_columns) if data else []
        return [Batch("maxExchanges", "jao_core_max_exchange",
                      TABLES["jao_core_max_exchange"][0], rows)]

    def _ds_net_pos(self, start: datetime, end: datetime) -> List[Batch]:
        data = self._core("netPos", start, end, "netPos")
        rows = parsers.parse_net_position(data, "da") if data else []
        return [Batch("netPos", "jao_core_net_position",
                      TABLES["jao_core_net_position"][0], rows)]

    def _ds_scheduled_exchanges(self, start: datetime, end: datetime) -> List[Batch]:
        data = self._core("scheduledExchanges", start, end, "scheduledExchanges")
        rows = (parsers.parse_scheduled_exchange(data, "da", self.unresolved_columns)
                if data else [])
        return [Batch("scheduledExchanges", "jao_core_scheduled_exchange",
                      TABLES["jao_core_scheduled_exchange"][0], rows)]

    def _ds_price_spread(self, start: datetime, end: datetime) -> List[Batch]:
        data = self._core("priceSpread", start, end, "priceSpread")
        rows = (parsers.parse_price_spread(data, "da", self.unresolved_columns)
                if data else [])
        return [Batch("priceSpread", "jao_core_price_spread",
                      TABLES["jao_core_price_spread"][0], rows)]

    def _ds_allocation_constraint(self, start: datetime, end: datetime) -> List[Batch]:
        data = self._core("allocationConstraint", start, end, "allocationConstraint")
        rows = parsers.parse_allocation_constraint(data, "da") if data else []
        return [Batch("allocationConstraint", "jao_core_allocation_constraint",
                      TABLES["jao_core_allocation_constraint"][0], rows)]

    def _ds_intraday_atc(self, start: datetime, end: datetime) -> List[Batch]:
        """ATC and NTC of one round go into the same row.

        If only one of the two fetches succeeds, only its column is upserted so
        the other value already in the table is never overwritten with NULL.
        """
        full_columns = TABLES["jao_core_intraday_atc"][0]
        batches: List[Batch] = []
        for calc_round, prefix in CAPACITY_ROUNDS.items():
            atc_data = self._core_id(prefix, "intradayAtc", start, end, "intradayAtc")
            ntc_data = self._core_id(prefix, "intradayNtc", start, end, "intradayNtc")
            if atc_data is None and ntc_data is None:
                continue
            atc = (parsers.parse_intraday_capacity(atc_data, self.unresolved_columns)
                   if atc_data is not None else None)
            ntc = (parsers.parse_intraday_capacity(ntc_data, self.unresolved_columns)
                   if ntc_data is not None else None)
            rows = parsers.merge_intraday_atc(calc_round, atc, ntc)
            if atc is not None and ntc is not None:
                columns = full_columns
            elif atc is not None:
                columns = full_columns[:5]  # ... calc_round, atc_mw
            else:
                columns = full_columns[:4] + ["ntc_mw"]
            batches.append(Batch("intradayAtc", "jao_core_intraday_atc", columns, rows))
        return batches

    def _ds_id_net_pos(self, start: datetime, end: datetime) -> List[Batch]:
        batches: List[Batch] = []
        for market, prefix in AUCTIONS.items():
            data = self._core_id(prefix, "netPos", start, end, "idNetPos")
            if data is None:
                continue
            batches.append(Batch("idNetPos", "jao_core_net_position",
                                 TABLES["jao_core_net_position"][0],
                                 parsers.parse_net_position(data, market)))
        return batches

    def _ds_id_scheduled_exchanges(self, start: datetime, end: datetime) -> List[Batch]:
        batches: List[Batch] = []
        for market, prefix in AUCTIONS.items():
            data = self._core_id(prefix, "scheduledExchanges", start, end,
                                 "idScheduledExchanges")
            if data is None:
                continue
            batches.append(Batch(
                "idScheduledExchanges", "jao_core_scheduled_exchange",
                TABLES["jao_core_scheduled_exchange"][0],
                parsers.parse_scheduled_exchange(data, market, self.unresolved_columns)))
        return batches

    def _ds_id_price_spread(self, start: datetime, end: datetime) -> List[Batch]:
        batches: List[Batch] = []
        for market, prefix in AUCTIONS.items():
            data = self._core_id(prefix, "priceSpread", start, end, "idPriceSpread")
            if data is None:
                continue
            batches.append(Batch(
                "idPriceSpread", "jao_core_price_spread",
                TABLES["jao_core_price_spread"][0],
                parsers.parse_price_spread(data, market, self.unresolved_columns)))
        return batches

    # ------------------------------------------------------------------
    # Window handling and upload
    # ------------------------------------------------------------------

    def _days(self) -> List[date]:
        """Prague delivery days to process."""
        today = datetime.now(PRAGUE_TZ).date()
        if self.is_backfill:
            end = self.end_date or today
            start = self.start_date or end - timedelta(days=7)
            if end < start:
                raise ValueError(f"end_date ({end}) must be >= start_date ({start})")
        else:
            start = today - timedelta(days=self.DEFAULT_DAYS_BACK)
            end = today + timedelta(days=self.DEFAULT_DAYS_FORWARD)
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]

    def _fetch_day(self, day: date) -> List[Batch]:
        start, end = day_start_utc(day), day_start_utc(day + timedelta(days=1))
        batches: List[Batch] = []
        for key in DATASET_KEYS:  # fixed order regardless of --datasets order
            if key not in self.datasets:
                continue
            if self.deadline_exceeded():
                break
            got = self._handlers[key](start, end)
            n = sum(len(b.rows) for b in got)
            self.logger.debug(f"  {day} {key}: {n} rows")
            batches.extend(got)
        return batches

    def _upload_day(self, day: date, batches: List[Batch]) -> int:
        """Upsert one day's batches (one bulk_upsert per table/column set)."""
        batches = [b for b in batches if b.rows]
        if not batches:
            return 0
        total = 0
        if self.dry_run:
            for b in batches:
                total += self.bulk_upsert(None, b.table, b.columns, b.rows,
                                          TABLES[b.table][1], skip_unchanged=True)
                self.track_country(b.dataset, len(b.rows))
            return total
        with self.database_connection() as conn:
            for b in batches:
                self.bulk_upsert(conn, b.table, b.columns, b.rows,
                                 TABLES[b.table][1], skip_unchanged=True)
                total += len(b.rows)
                self.track_country(b.dataset, len(b.rows))
        return total

    def run(self) -> bool:
        """Execute the JAO Core pipeline."""
        self.print_header()
        try:
            days = self._days()
        except ValueError as exc:
            self.logger.error(str(exc))
            return False

        self.client = JaoClient()
        self.logger.info(
            f"{self.RUNNER_NAME}: {days[0]} .. {days[-1]} ({len(days)} day(s)), "
            f"datasets: {', '.join(d for d in DATASET_KEYS if d in self.datasets)}"
        )
        total = 0
        try:
            for day in days:
                if self.deadline_exceeded():
                    break
                batches = self._fetch_day(day)
                try:
                    total += self._upload_day(day, batches)
                except Exception as exc:
                    self.logger.error(f"{self.RUNNER_NAME}: upload failed for {day}: {exc}")
                    if self.debug:
                        import traceback
                        traceback.print_exc()
                    self.fetch_failed += 1
                    continue

            if self.unresolved_columns:
                self.logger.warning(
                    f"{self.RUNNER_NAME}: skipped unrecognised border column(s): "
                    f"{', '.join(sorted(self.unresolved_columns))}"
                )
            self.logger.info(
                f"{self.format_summary(total)} "
                f"[requests={self.client.request_count}, ok={self.fetch_ok}, "
                f"failed={self.fetch_failed}]"
            )
            success = self.fetch_ok > 0 or self.fetch_failed == 0
            self.print_footer(success=success)
            return success
        except Exception as exc:
            self.logger.error(f"Pipeline failed: {exc}")
            if self.debug:
                import traceback
                traceback.print_exc()
            self.print_footer(success=False)
            return False

    # ------------------------------------------------------------------
    # CLI
    # ------------------------------------------------------------------

    @classmethod
    def create_argument_parser(cls) -> argparse.ArgumentParser:
        parser = super().create_argument_parser()
        parser.add_argument(
            "--datasets",
            type=str,
            default=None,
            metavar="KEY[,KEY...]",
            help=f"Comma-separated subset of datasets (default: all). Keys: {', '.join(DATASET_KEYS)}",
        )
        return parser

    @classmethod
    def main(cls) -> None:
        parser = cls.create_argument_parser()
        args = parser.parse_args()
        try:
            start_date = cls.parse_date(args.start)
            end_date = cls.parse_date(args.end)
            datasets = [d.strip() for d in args.datasets.split(",") if d.strip()] if args.datasets else None
            runner = cls(
                datasets=datasets,
                debug=args.debug,
                dry_run=args.dry_run,
                start_date=start_date,
                end_date=end_date,
                max_runtime=args.max_runtime,
            )
        except ValueError as exc:
            print(f"Error: {exc}")
            sys.exit(1)
        sys.exit(0 if runner.run() else 1)


if __name__ == "__main__":
    JaoCoreRunner.main()
