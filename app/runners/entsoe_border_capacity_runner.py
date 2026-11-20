#!/usr/bin/env python3
"""
ENTSO-E Border Capacity Runner.

Loads every cross-border capacity item for the active borders in
``entsoe_borders`` into ``entsoe_border_capacity`` (one directed row per source
interval; see docs/cross_border_plan.md, section 2.2).

Datasets (``--datasets``, comma list, default all):

    forecast          A61  capacity forecast week / month / year    -> ntc_forecast
    offered_longterm  A31  explicit auction, month / year           -> offered_explicit
    offered_intraday  A31  intraday idct / ida1 / ida2 / ida3       -> offered_intraday
    allocated         A26  businessType A29, daily contract         -> already_allocated
    nominated         A26  businessType B08                         -> nominated
    use               A25  businessType B05, month / year           -> use_allocated, auction_price
    revenue           A25  businessType B07, month / year           -> auction_revenue

Direction: ``from_area_id`` is the ENTSO-E ``out_Domain``, ``to_area_id`` the
``in_Domain``; both directions of every border are requested.

Windows:
    - no args:  intraday items today..tomorrow; day items yesterday..tomorrow;
                week / month / year items for the current and the next
                ISO week / calendar month / calendar year.
    - --start/--end:  the same item groups over that date range. Day-resolution
                items are chunked into 7-day requests; week / month / year items
                query every ISO week / calendar month / calendar year that
                overlaps the range (one request per unit).

Every (dataset, variant, border, direction, window) is one request. A failed
request is counted and reported in the run summary and does not stop the run;
"No matching data" is a normal empty result.

Usage:
    python3 -m runners.entsoe_border_capacity_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_border_capacity_runner --start 2026-11-12 --end 2026-11-19
    python3 -m runners.entsoe_border_capacity_runner --datasets forecast,use
"""

import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.query import fetch_document, fmt_period
from entsoe.borders import Border, load_active_borders, directions
from entsoe.border_capacity_parser import (
    ParsedPoint,
    ParsedSeries,
    merge_equal_runs,
    normalise_unit,
    parse_capacity_document,
)

DAY = "day"
WEEK = "week"
MONTH = "month"
YEAR = "year"
INTRADAY = "intraday"  # a day window, queried for today..tomorrow by default

ZONE_ONLY = ("bidding_zone",)
ALL_KINDS = ("bidding_zone", "control_area")

# --------------------------------------------------------------------------
# Static skip map: (dataset, border_code) pairs that are structurally empty.
#
# The explicit capacity auctions between CZ and Germany are run per German
# control area (TenneT, 50Hertz), never under the DE-LU bidding-zone code, so
# offered (explicit), already allocated, nominated, auction results and
# auction revenue return "No matching data" for CZ-DE_LU in every probe
# (2026-11-20, all windows and both directions). Skipping them saves
# 2 requests per dataset variant and window. Everything else that returned
# nothing in the probes is NOT skipped: an empty week forecast for PL/SK or an
# empty daily allocation for SK may simply be "not published yet".
# Remove an entry here if a probe ever shows data for it.
# --------------------------------------------------------------------------
SKIP_MAP = {
    ("offered_longterm", "CZ-DE_LU"),
    ("allocated", "CZ-DE_LU"),
    ("nominated", "CZ-DE_LU"),
    ("use", "CZ-DE_LU"),
    ("revenue", "CZ-DE_LU"),
}

# idct / ida1 / ida2 / ida3 -> (auction.Type, classification position)
INTRADAY_PRODUCTS = {
    "idct": ("A08", None),
    "ida1": ("A01", 1),
    "ida2": ("A01", 2),
    "ida3": ("A01", 3),
}


@dataclass(frozen=True)
class Variant:
    """One kind of request of a dataset (fixed parameters, one horizon)."""

    name: str                       # log label, e.g. "month"
    params: Tuple[Tuple[str, str], ...]
    window: str                     # DAY | WEEK | MONTH | YEAR
    horizon: str                    # value stored in entsoe_border_capacity.horizon


@dataclass(frozen=True)
class Dataset:
    name: str
    kinds: Tuple[str, ...]          # border kinds requested
    variants: Tuple[Variant, ...]


def _v(name, window, horizon, **params) -> Variant:
    return Variant(name, tuple(sorted(params.items())), window, horizon)


DATASETS: Dict[str, Dataset] = {
    "forecast": Dataset("forecast", ALL_KINDS, (
        _v("week", WEEK, "week", documentType="A61", **{"contract_MarketAgreement.Type": "A02"}),
        _v("month", MONTH, "month", documentType="A61", **{"contract_MarketAgreement.Type": "A03"}),
        _v("year", YEAR, "year", documentType="A61", **{"contract_MarketAgreement.Type": "A04"}),
    )),
    "offered_longterm": Dataset("offered_longterm", ALL_KINDS, (
        _v("month", MONTH, "month", documentType="A31", **{
            "auction.Type": "A02", "contract_MarketAgreement.Type": "A03"}),
        _v("year", YEAR, "year", documentType="A31", **{
            "auction.Type": "A02", "contract_MarketAgreement.Type": "A04"}),
    )),
    "offered_intraday": Dataset("offered_intraday", ZONE_ONLY, tuple(
        _v(product, INTRADAY, product, documentType="A31", **{
            "contract_MarketAgreement.Type": "A07",
            "auction.Type": auction_type,
            **({"classificationSequence_AttributeInstanceComponent.Position": str(position)}
               if position is not None else {}),
        })
        for product, (auction_type, position) in INTRADAY_PRODUCTS.items()
    )),
    "allocated": Dataset("allocated", ALL_KINDS, (
        _v("day", DAY, "day", documentType="A26", businessType="A29",
           **{"contract_MarketAgreement.Type": "A01"}),
    )),
    "nominated": Dataset("nominated", ALL_KINDS, (
        _v("day", DAY, "day", documentType="A26", businessType="B08"),
    )),
    "use": Dataset("use", ALL_KINDS, (
        _v("month", MONTH, "month", documentType="A25", businessType="B05",
           **{"contract_MarketAgreement.Type": "A03"}),
        _v("year", YEAR, "year", documentType="A25", businessType="B05",
           **{"contract_MarketAgreement.Type": "A04"}),
    )),
    "revenue": Dataset("revenue", ALL_KINDS, (
        _v("month", MONTH, "month", documentType="A25", businessType="B07",
           **{"contract_MarketAgreement.Type": "A03"}),
        _v("year", YEAR, "year", documentType="A25", businessType="B07",
           **{"contract_MarketAgreement.Type": "A04"}),
    )),
}
ALL_DATASETS = list(DATASETS.keys())

# Nominated capacity (A26 / B08) returns two TimeSeries per request, told apart
# only by contract_MarketAgreement.type: A01 = day-ahead (daily) nominations,
# A07 = intraday nominations. They are different quantities (the intraday
# nomination is not a total of the daily one), so both are stored; the
# intraday one is filed under horizon 'idct' (the table has no plain
# 'intraday' horizon).
NOMINATED_HORIZON_BY_CONTRACT = {"A01": "day", "A07": "idct"}


# --------------------------------------------------------------------------
# Windows
# --------------------------------------------------------------------------
def _local_midnight_utc(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time()).replace(
        tzinfo=PRAGUE_TZ
    ).astimezone(timezone.utc)


def _iso_week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _unit_windows(kind: str, first: date, last: date) -> Iterator[Tuple[datetime, datetime]]:
    """UTC bounds of every ISO week / calendar month / calendar year overlapping [first, last]."""
    if kind == WEEK:
        cur = _iso_week_start(first)
        while cur <= last:
            yield _local_midnight_utc(cur), _local_midnight_utc(cur + timedelta(days=7))
            cur += timedelta(days=7)
    elif kind == MONTH:
        cur = first.replace(day=1)
        while cur <= last:
            nxt = date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
            yield _local_midnight_utc(cur), _local_midnight_utc(nxt)
            cur = nxt
    elif kind == YEAR:
        for year in range(first.year, last.year + 1):
            yield _local_midnight_utc(date(year, 1, 1)), _local_midnight_utc(date(year + 1, 1, 1))
    else:
        raise ValueError(kind)


def _day_chunks(first: date, last: date, max_days: int = 7) -> Iterator[Tuple[datetime, datetime]]:
    """Prague-midnight-aligned UTC chunks of at most ``max_days`` covering [first, last]."""
    cur = first
    while cur <= last:
        nxt = min(cur + timedelta(days=max_days), last + timedelta(days=1))
        yield _local_midnight_utc(cur), _local_midnight_utc(nxt)
        cur = nxt


class BorderCapacityRunner(BaseRunner):
    """Runner for cross-border capacity items (A61, A31, A26, A25)."""

    RUNNER_NAME = "ENTSO-E Border Capacity Runner"

    TABLE_NAME = "entsoe_border_capacity"
    COLUMNS = [
        "period_start", "from_area_id", "to_area_id", "item", "horizon",
        "period_end", "resolution", "value", "unit",
    ]
    CONFLICT_COLUMNS = ["period_start", "from_area_id", "to_area_id", "item", "horizon"]

    # ENTSO-E answers a capacity request in about 5 seconds; a full run is a few
    # hundred of them. A few parallel requests keep a run short and stay far
    # below the 400 requests/minute token limit (4 workers ~ 50 requests/min).
    MAX_WORKERS = 4

    def __init__(self, datasets: Optional[List[str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.client: Optional[EntsoeClient] = None
        self.datasets = datasets or ALL_DATASETS
        self.requests_made = 0
        self.requests_empty = 0
        self.failures: List[str] = []
        self.rows_by_group: Counter = Counter()   # (dataset, item, horizon) -> rows

    # ------------------------------------------------------------- setup
    def _init_client(self) -> bool:
        try:
            self.client = EntsoeClient()
            return True
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            return False

    def _load_borders(self) -> List[Border]:
        with self.database_connection() as conn:
            try:
                return load_active_borders(conn)
            finally:
                # Read transaction must not stay open across the HTTP calls.
                conn.rollback()

    # ------------------------------------------------------------ windows
    def _date_range(self) -> Tuple[date, date]:
        """Inclusive Prague date range of the run (backfill or default)."""
        if self.is_backfill:
            end = self.end_date or date.today()
            start = self.start_date or end - timedelta(days=7)
            if end < start:
                raise ValueError(f"end_date ({end}) must be >= start_date ({start})")
            return start, end
        today = datetime.now(PRAGUE_TZ).date()
        return today, today

    def _windows(self, variant: Variant) -> List[Tuple[datetime, datetime]]:
        """Request windows (UTC) for one variant, per the module docstring."""
        if self.is_backfill:
            first, last = self._date_range()
        else:
            today = datetime.now(PRAGUE_TZ).date()
            if variant.window == INTRADAY:
                first, last = today, today + timedelta(days=1)
            elif variant.window == DAY:
                first, last = today - timedelta(days=1), today + timedelta(days=1)
            elif variant.window == WEEK:
                first, last = today, today + timedelta(days=7)          # this and next week
            elif variant.window == MONTH:
                nxt = date(today.year + (today.month == 12), today.month % 12 + 1, 1)
                first, last = today, nxt                                # this and next month
            else:
                first, last = today, date(today.year + 1, 1, 1)         # this and next year
        if variant.window in (INTRADAY, DAY):
            return list(_day_chunks(first, last))
        return list(_unit_windows(variant.window, first, last))

    # -------------------------------------------------------------- rows
    Row = Tuple[datetime, int, int, str, str, datetime, Optional[str], Optional[float], Optional[str]]

    @staticmethod
    def _row(point: ParsedPoint, from_id: int, to_id: int, item: str, horizon: str,
             value: Optional[float], unit: Optional[str]) -> "BorderCapacityRunner.Row":
        return (point.period_start, from_id, to_id, item, horizon,
                point.period_end, point.resolution, value, unit)

    @staticmethod
    def _long_term_points(series: ParsedSeries, horizon: str) -> List[ParsedPoint]:
        """Points of a week / month / year series; hourly forward-fill is merged.

        A31 / A25 long-term Periods are PT60M over a whole month or year with a
        Point only where the value changes (curveType A03); expanded to hourly
        that is 720 / 8760 identical rows per border direction, so equal
        neighbours are merged into one span. Daily (P1D) and monthly (P1M)
        positions are kept as they come.
        """
        if horizon in ("week", "month", "year") and series.points and \
                series.points[0].resolution.startswith("PT"):
            return merge_equal_runs(series.points)
        return series.points

    def _series_rows(
        self, dataset: str, variant: Variant, series: ParsedSeries, from_id: int, to_id: int
    ) -> List["BorderCapacityRunner.Row"]:
        """Turn one TimeSeries into table rows; the item depends on the dataset."""
        unit = normalise_unit(series.quantity_unit)
        horizon = variant.horizon
        rows: List[BorderCapacityRunner.Row] = []

        if dataset == "nominated":
            horizon = NOMINATED_HORIZON_BY_CONTRACT.get(series.contract_type or "")
            if horizon is None:
                self.logger.warning(
                    f"    nominated: unexpected contract type {series.contract_type!r}, series skipped"
                )
                return rows
            for p in series.points:
                rows.append(self._row(p, from_id, to_id, "nominated", horizon, p.quantity, unit))
            return rows

        points = self._long_term_points(series, horizon)

        if dataset == "forecast":
            for p in points:
                rows.append(self._row(p, from_id, to_id, "ntc_forecast", horizon, p.quantity, unit))
        elif dataset == "offered_longterm":
            for p in points:
                rows.append(self._row(p, from_id, to_id, "offered_explicit", horizon, p.quantity, unit))
        elif dataset == "offered_intraday":
            for p in points:
                rows.append(self._row(p, from_id, to_id, "offered_intraday", horizon, p.quantity, unit))
        elif dataset == "allocated":
            for p in points:
                rows.append(self._row(p, from_id, to_id, "already_allocated", horizon, p.quantity, unit))
        elif dataset == "use":
            # A25 / B05: one TimeSeries per explicit auction; the Point carries
            # the allocated quantity (MAW) and the clearing price (currency per
            # price unit, e.g. EUR / MWH). No requested quantity is published.
            price_unit = None
            if series.currency or series.price_unit:
                price_unit = "/".join(
                    part for part in (series.currency, normalise_unit(series.price_unit)) if part
                )[:10]
            for p in points:
                rows.append(self._row(p, from_id, to_id, "use_allocated", horizon, p.quantity, unit))
                if p.price is not None:
                    rows.append(self._row(p, from_id, to_id, "auction_price", horizon, p.price, price_unit))
        elif dataset == "revenue":
            # A25 / B07: Point carries price.amount only, in the series currency.
            for p in points:
                rows.append(self._row(p, from_id, to_id, "auction_revenue", horizon,
                                      p.price, normalise_unit(series.currency)))
        return rows

    @staticmethod
    def _latest_revision(series_list: List[ParsedSeries]) -> List[ParsedSeries]:
        """Keep, per interval, only the points of the latest-updated TimeSeries.

        idct is revised throughout the delivery day: one document can hold
        several TimeSeries for the same intervals, each with its own
        update_DateAndOrTime. Series without an update time (ida*) rank lowest,
        and the later series in the document wins a tie.
        """
        best: Dict[Tuple, Tuple[datetime, int]] = {}
        floor = datetime.min.replace(tzinfo=timezone.utc)
        for index, series in enumerate(series_list):
            rank = (series.updated_at or floor, index)
            for p in series.points:
                key = (series.in_eic, series.out_eic, p.period_start)
                if key not in best or rank > best[key]:
                    best[key] = rank
        result = []
        for index, series in enumerate(series_list):
            rank = (series.updated_at or floor, index)
            kept = [p for p in series.points
                    if best[(series.in_eic, series.out_eic, p.period_start)] == rank]
            result.append(ParsedSeries(**{**series.__dict__, "points": kept}))
        return result

    # ------------------------------------------------------------- fetch
    def _fetch_rows(
        self, dataset: Dataset, variant: Variant, border: Border,
        direction: Tuple[int, str, int, str], window: Tuple[datetime, datetime],
    ) -> Tuple[str, List["BorderCapacityRunner.Row"], Optional[str]]:
        """One request. Returns (status, rows, error); status is ok | empty | failed.

        Runs in worker threads, so it touches no shared counters; the caller
        tallies the results.
        """
        from_id, from_eic, to_id, to_eic = direction
        start, end = window
        label = (f"{dataset.name}/{variant.name} {border.border_code} "
                 f"{from_eic}->{to_eic} [{start:%Y-%m-%d %H:%M}, {end:%Y-%m-%d %H:%M})Z")
        params = dict(variant.params)
        params.update({
            "in_Domain": to_eic,
            "out_Domain": from_eic,
            "periodStart": fmt_period(start),
            "periodEnd": fmt_period(end),
        })
        try:
            documents = fetch_document(self.client, params)
            if documents is None:
                self.logger.debug(f"    no data: {label}")
                return "empty", [], None

            series_list: List[ParsedSeries] = []
            for xml in documents:
                series_list.extend(parse_capacity_document(xml))
            if dataset.name == "offered_intraday":
                series_list = self._latest_revision(series_list)

            rows: List[BorderCapacityRunner.Row] = []
            for series in series_list:
                # The series' own domains decide the direction; they must agree
                # with the request, otherwise the response is not what was asked.
                if series.out_eic != from_eic or series.in_eic != to_eic:
                    self.logger.warning(
                        f"    {label}: series domains out={series.out_eic} in={series.in_eic} "
                        f"differ from the request, series skipped"
                    )
                    continue
                rows.extend(self._series_rows(dataset.name, variant, series, from_id, to_id))
            self.logger.debug(f"    {len(rows)} rows: {label}")
            return "ok", rows, None
        except Exception as e:
            self.logger.warning(f"    Failed {label}: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return "failed", [], f"{label}: {e}"

    def _upsert(self, dataset: str, rows: List["BorderCapacityRunner.Row"]) -> int:
        """Upsert a batch (deduplicated on the primary key, last occurrence wins)."""
        if not rows:
            return 0
        unique: Dict[Tuple, BorderCapacityRunner.Row] = {}
        for row in rows:
            unique[(row[0], row[1], row[2], row[3], row[4])] = row
        records = list(unique.values())
        for row in records:
            self.rows_by_group[(dataset, row[3], row[4])] += 1
        if self.dry_run:
            self.logger.info(f"    DRY RUN - would upsert {len(records)} rows ({dataset})")
            return len(records)
        with self.database_connection() as conn:
            return self.bulk_upsert(
                conn, self.TABLE_NAME, self.COLUMNS, records, self.CONFLICT_COLUMNS,
                skip_unchanged=True,
            )

    # --------------------------------------------------------------- run
    def run(self) -> bool:
        self.print_header()
        if not self._init_client():
            return False

        try:
            borders = self._load_borders()
            if not borders:
                self.logger.error("No active borders in entsoe_borders")
                return False
            total_rows = 0
            for name in self.datasets:
                dataset = DATASETS[name]
                dataset_borders = [
                    b for b in borders
                    if b.kind in dataset.kinds and (name, b.border_code) not in SKIP_MAP
                ]
                for variant in dataset.variants:
                    for window in self._windows(variant):
                        if self.deadline_exceeded():
                            break
                        tasks = [
                            (border, direction)
                            for border in dataset_borders
                            for direction in directions(border)
                        ]
                        rows: List[BorderCapacityRunner.Row] = []
                        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as pool:
                            futures = []
                            for border, direction in tasks:
                                if self.deadline_exceeded():
                                    break
                                futures.append(pool.submit(
                                    self._fetch_rows, dataset, variant, border, direction, window
                                ))
                            for future in futures:
                                status, fetched, error = future.result()
                                self.requests_made += 1
                                if status == "empty":
                                    self.requests_empty += 1
                                elif status == "failed":
                                    self.failures.append(error)
                                rows.extend(fetched)
                        try:
                            total_rows += self._upsert(name, rows)
                        except Exception as e:
                            self.failures.append(f"{name}/{variant.name} upsert: {e}")
                            self.logger.warning(f"    Upsert failed for {name}/{variant.name}: {e}")

            self._log_summary(total_rows)
            self.print_footer(success=not self.failures)
            return not self.failures

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            self.print_footer(success=False)
            return False

    def _log_summary(self, total_rows: int) -> None:
        self.logger.info(
            f"{self.RUNNER_NAME}: {total_rows} rows, {self.requests_made} requests "
            f"({self.requests_empty} empty, {len(self.failures)} failed)"
        )
        for (dataset, item, horizon), count in sorted(self.rows_by_group.items()):
            self.logger.info(f"  {dataset:17s} {item:18s} {horizon:6s} {count}")
        if self.failures:
            self.logger.warning(
                f"{self.RUNNER_NAME}: {len(self.failures)} request(s) failed; first: {self.failures[0]}"
            )
            for failure in self.failures[1:20]:
                self.logger.warning(f"  failed: {failure}")

    # --------------------------------------------------------------- CLI
    @classmethod
    def create_argument_parser(cls):
        parser = super().create_argument_parser()
        parser.add_argument(
            "--datasets",
            type=str,
            default=None,
            help="Comma-separated subset of: " + ",".join(ALL_DATASETS) + " (default: all)",
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

        datasets = None
        if args.datasets:
            datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
            invalid = [d for d in datasets if d not in DATASETS]
            if invalid:
                print(f"Error: invalid --datasets entries {invalid}. Choose from {ALL_DATASETS}.")
                sys.exit(1)

        runner = cls(
            debug=args.debug,
            dry_run=args.dry_run,
            start_date=start_date,
            end_date=end_date,
            max_runtime=args.max_runtime,
            datasets=datasets,
        )
        sys.exit(0 if runner.run() else 1)


if __name__ == "__main__":
    BorderCapacityRunner.main()
