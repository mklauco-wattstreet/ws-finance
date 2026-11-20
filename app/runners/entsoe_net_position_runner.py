#!/usr/bin/env python3
"""
ENTSO-E Net Position Runner: zone net positions and congestion costs.

Datasets (``--datasets``, comma separated; default = all but see ``costs``):

    np_da        A25/B09, contract_MarketAgreement.Type=A01   -> net_position_da_mw
    np_intraday  A25/B09, contract_MarketAgreement.Type=A07   -> net_position_intraday_mw
    sched_np     A09/B09 (one request, A01 and A05 series)    -> sched_net_position_da_mw
                                                                 sched_net_position_total_mw
    costs        A92 (monthly costs of congestion management) -> entsoe_congestion_costs

Net position sign convention for all four columns: positive = net export of the
zone, negative = net import (see entsoe/net_position_parser.py for how it is
read from the in/out domains).

Every dataset upserts ONLY its own column(s) into entsoe_net_positions, so the
datasets never null each other out. Net positions are always re-fetched for the
whole window (yesterday .. tomorrow in default mode) with skip_unchanged, because
they are published incrementally and can be revised.

Congestion costs are monthly and change rarely. In default (no --start/--end)
mode WITHOUT --datasets they are fetched only between 06:00 and 06:30 Prague
time (the first cron firing of the morning); ``--datasets costs`` always fetches
them. They always cover the full current year (ENTSO-E allows one year per
request). In backfill mode without --datasets they are fetched for every year the
range touches.

Zone list: ``NET_POSITION_AREAS`` below (CZ only for now); add tuples
``(area_id, eic, label, country_code)`` to extend.

Usage:
    python3 -m runners.entsoe_net_position_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_net_position_runner --start 2026-11-12 --end 2026-11-19
    python3 -m runners.entsoe_net_position_runner --datasets costs --dry-run
"""

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.query import fetch_document, fmt_period
from entsoe.net_position_parser import (
    parse_congestion_costs_xml,
    parse_net_position_xml,
    slot_fields,
)

# (area_id, eic, label, country_code)
NET_POSITION_AREAS: List[Tuple[int, str, str, str]] = [
    (1, "10YCZ-CEPS-----N", "CZ", "CZ"),
]

ALL_DATASETS = ("np_da", "np_intraday", "sched_np", "costs")

# Prague time window in which default (cron) runs also fetch congestion costs.
COSTS_WINDOW_HOUR = 6
COSTS_WINDOW_MAX_MINUTE = 30

NP_TABLE = "entsoe_net_positions"
NP_BASE_COLUMNS = [
    "delivery_datetime", "area_id", "country_code",
    "trade_date", "period", "time_interval",
]
NP_CONFLICT = ["delivery_datetime", "area_id", "country_code"]

COSTS_TABLE = "entsoe_congestion_costs"
COSTS_COLUMNS = ["period_start", "area_id", "cost_type", "period_end", "amount_eur"]
COSTS_CONFLICT = ["period_start", "area_id", "cost_type"]

# dataset -> (request parameters, {contract_MarketAgreement.type: target column})
NP_SPECS: Dict[str, Tuple[Dict[str, str], Dict[str, str]]] = {
    "np_da": (
        {"documentType": "A25", "businessType": "B09", "contract_MarketAgreement.Type": "A01"},
        {"A01": "net_position_da_mw"},
    ),
    "np_intraday": (
        {"documentType": "A25", "businessType": "B09", "contract_MarketAgreement.Type": "A07"},
        {"A07": "net_position_intraday_mw"},
    ),
    "sched_np": (
        {"documentType": "A09", "businessType": "B09"},
        {"A01": "sched_net_position_da_mw", "A05": "sched_net_position_total_mw"},
    ),
}


def _prague_midnight_utc(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()).replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)


class NetPositionRunner(BaseRunner):
    """Runner for zone net positions (A25/A09 B09) and congestion costs (A92)."""

    RUNNER_NAME = "ENTSO-E Net Position Runner"
    TABLE_NAME = NP_TABLE
    COLUMNS = NP_BASE_COLUMNS
    CONFLICT_COLUMNS = NP_CONFLICT

    # Net position requests are short; 7 days per chunk keeps responses small.
    MAX_CHUNK_DAYS = 7
    DEFAULT_DAYS_BACK = 1
    DEFAULT_DAYS_FORWARD = 1

    def __init__(self, datasets: Optional[List[str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.client: Optional[EntsoeClient] = None
        self.explicit_datasets = datasets is not None
        self.datasets = list(datasets) if datasets else list(ALL_DATASETS)
        self.failures: List[str] = []

    # ------------------------------------------------------------------ helpers
    def _fail(self, label: str, error: Exception) -> None:
        """Count a failed fetch/parse and keep going with the rest."""
        self.failures.append(label)
        self.record_area_failure(label, error)

    def _fetch(self, label: str, params: Dict[str, str]) -> Optional[List[str]]:
        """Fetch one document set; None on "no data" or failure (failure is counted)."""
        try:
            return fetch_document(self.client, params)
        except Exception as e:  # noqa: BLE001 - per-fetch isolation by design
            self._fail(label, e)
            return None

    def _costs_requested(self) -> bool:
        if "costs" not in self.datasets:
            return False
        if self.explicit_datasets or self.is_backfill:
            return True
        now = datetime.now(PRAGUE_TZ)
        return now.hour == COSTS_WINDOW_HOUR and now.minute < COSTS_WINDOW_MAX_MINUTE

    def _default_window_utc(self) -> Tuple[datetime, datetime]:
        today = datetime.now(PRAGUE_TZ).date()
        return (
            _prague_midnight_utc(today - timedelta(days=self.DEFAULT_DAYS_BACK)),
            _prague_midnight_utc(today + timedelta(days=self.DEFAULT_DAYS_FORWARD + 1)),
        )

    def _windows(self) -> List[Tuple[datetime, datetime]]:
        if self.is_backfill:
            return list(self.get_backfill_chunks())
        return [self._default_window_utc()]

    # ------------------------------------------------------------ net positions
    def _process_net_positions(
        self, conn, dataset: str, window: Tuple[datetime, datetime],
        area: Tuple[int, str, str, str],
    ) -> int:
        area_id, eic, label, country_code = area
        params, columns = NP_SPECS[dataset]
        start, end = window
        tag = f"{label}/{dataset}"
        query = {
            **params,
            "in_Domain": eic,
            "out_Domain": eic,
            "periodStart": fmt_period(start),
            "periodEnd": fmt_period(end),
        }
        self.logger.debug(f"  Fetching {tag} [{query['periodStart']} - {query['periodEnd']} UTC]")
        docs = self._fetch(tag, query)
        if docs is None:
            self.logger.info(f"{self.RUNNER_NAME}: no {dataset} data for {label} "
                             f"[{query['periodStart']} - {query['periodEnd']} UTC]")
            return 0

        # contract type -> slot -> signed MW (merged across documents)
        merged: Dict[Optional[str], Dict[datetime, float]] = {}
        try:
            for xml_text in docs:
                for contract, series in parse_net_position_xml(xml_text, eic).items():
                    merged.setdefault(contract, {}).update(series)
        except Exception as e:  # noqa: BLE001
            self._fail(tag, e)
            return 0

        total = 0
        for contract, column in columns.items():
            series = merged.get(contract)
            if not series:
                self.logger.debug(f"    {tag}: no {column} values in response")
                continue
            records = []
            for slot in sorted(series):
                trade_date, period, interval = slot_fields(slot)
                records.append((
                    slot, area_id, country_code, trade_date, period, interval,
                    round(series[slot], 3),
                ))
            self.logger.debug(f"    {tag}: {len(records)} slots -> {column}")
            self.bulk_upsert(
                conn, NP_TABLE, NP_BASE_COLUMNS + [column], records, NP_CONFLICT,
                update_columns=[column], skip_unchanged=True,
            )
            total += len(records)
        self.track_country(country_code, total)
        return total

    # ---------------------------------------------------------------- costs
    def _cost_year_windows(self) -> List[Tuple[datetime, datetime]]:
        """One request window per calendar year (Prague local), API max is one year."""
        if self.is_backfill:
            first = (self.start_date or date.today()).year
            last = (self.end_date or date.today()).year
        else:
            first = last = datetime.now(PRAGUE_TZ).year
        return [
            (_prague_midnight_utc(date(y, 1, 1)), _prague_midnight_utc(date(y + 1, 1, 1)))
            for y in range(first, last + 1)
        ]

    def _process_costs(self, conn, area: Tuple[int, str, str, str]) -> int:
        area_id, eic, label, country_code = area
        total = 0
        for start, end in self._cost_year_windows():
            tag = f"{label}/costs"
            query = {
                "documentType": "A92",
                "in_Domain": eic,
                "out_Domain": eic,
                "periodStart": fmt_period(start),
                "periodEnd": fmt_period(end),
            }
            self.logger.debug(f"  Fetching {tag} [{query['periodStart']} - {query['periodEnd']} UTC]")
            docs = self._fetch(tag, query)
            if docs is None:
                self.logger.info(f"{self.RUNNER_NAME}: no congestion costs for {label} "
                                 f"[{query['periodStart']} - {query['periodEnd']} UTC]")
                continue
            try:
                rows: Dict[Tuple[datetime, str], tuple] = {}
                for xml_text in docs:
                    for row in parse_congestion_costs_xml(xml_text):
                        rows[(row["period_start"], row["cost_type"])] = (
                            row["period_start"], area_id, row["cost_type"],
                            row["period_end"], round(row["amount_eur"], 3),
                        )
            except Exception as e:  # noqa: BLE001
                self._fail(tag, e)
                continue
            records = sorted(rows.values())
            self.logger.debug(f"    {tag}: {len(records)} monthly rows")
            if records:
                self.bulk_upsert(
                    conn, COSTS_TABLE, COSTS_COLUMNS, records, COSTS_CONFLICT,
                    skip_unchanged=True,
                )
            total += len(records)
        self.track_country(f"{country_code}_costs", total)
        return total

    # ------------------------------------------------------------------- run
    def run(self) -> bool:
        self.print_header()
        try:
            self.client = EntsoeClient()
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            return False

        total = 0
        try:
            np_datasets = [d for d in self.datasets if d in NP_SPECS]
            run_costs = self._costs_requested()
            self.logger.info(
                f"{self.RUNNER_NAME}: datasets={','.join(np_datasets + (['costs'] if run_costs else []))}"
                f"{' (dry-run)' if self.dry_run else ''}"
            )
            with self.database_connection() as conn:
                for window in (self._windows() if np_datasets else []):
                    for area in NET_POSITION_AREAS:
                        for dataset in np_datasets:
                            total += self._process_net_positions(conn, dataset, window, area)
                if run_costs:
                    for area in NET_POSITION_AREAS:
                        total += self._process_costs(conn, area)

            self.logger.info(self.format_summary(total))
            if self.failures:
                self.logger.warning(
                    f"{self.RUNNER_NAME}: {len(self.failures)} fetch(es) failed: "
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

    # ------------------------------------------------------------------- CLI
    @classmethod
    def create_argument_parser(cls) -> argparse.ArgumentParser:
        parser = super().create_argument_parser()
        parser.add_argument(
            "--datasets",
            type=str,
            default=None,
            metavar="LIST",
            help=f"Comma separated subset of: {','.join(ALL_DATASETS)} (default: all; "
                 f"costs only 06:00-06:30 Prague time in cron mode unless listed explicitly)",
        )
        return parser

    @classmethod
    def main(cls) -> None:
        parser = cls.create_argument_parser()
        args = parser.parse_args()
        try:
            start_date = cls.parse_date(args.start)
            end_date = cls.parse_date(args.end)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        datasets: Optional[List[str]] = None
        if args.datasets:
            datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
            unknown = [d for d in datasets if d not in ALL_DATASETS]
            if unknown:
                print(f"Error: unknown dataset(s) {unknown}; choose from {list(ALL_DATASETS)}")
                sys.exit(1)

        runner = cls(
            datasets=datasets,
            debug=args.debug,
            dry_run=args.dry_run,
            start_date=start_date,
            end_date=end_date,
            max_runtime=args.max_runtime,
        )
        sys.exit(0 if runner.run() else 1)


if __name__ == "__main__":
    NetPositionRunner.main()
