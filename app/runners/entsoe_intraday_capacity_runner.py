#!/usr/bin/env python3
"""
ENTSO-E Intraday Offered Transfer Capacity Runner (A31, 12.1.A/B).

Fetches CZ-border intraday transfer capacity for four products (idct = the
continuous intraday product, ida1/ida2/ida3 = implicit auction results) and
uploads wide-format rows to the partitioned entsoe_intraday_transfer_capacity
table. Each product/day requires 8 HTTP requests (4 neighbours x
import/export direction).

idct is revised continuously through the delivery day and is ALWAYS
re-fetched. ida1/ida2/ida3 are static once published, so before fetching this
runner checks the DB for an already-complete product/day and skips the 8
requests when nothing new can arrive (see _is_product_complete()). This is a
different rule from BaseRunner._run_with_availability_check(), which skips as
soon as ANY row exists - wrong here, since a day can be partially published.

Usage:
    python3 entsoe_intraday_capacity_runner.py [--debug] [--dry-run]
    python3 entsoe_intraday_capacity_runner.py --products idct,ida1
    python3 entsoe_intraday_capacity_runner.py --start 2026-08-01 --end 2026-08-07
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Iterator, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.capacity_parser import IntradayTransferCapacityParser
from entsoe.constants import (
    CZ_CAPACITY_BORDERS,
    INTRADAY_CAPACITY_PRODUCTS,
    ACTIVE_INTRADAY_CAPACITY_AREAS,
)

ALL_PRODUCTS = list(INTRADAY_CAPACITY_PRODUCTS.keys())  # ['idct', 'ida1', 'ida2', 'ida3']

# idct carries rolling revisions throughout the delivery day and must never
# be skipped by the completeness pre-check; ida1/ida2/ida3 are static once
# published and are eligible for the skip.
REVISABLE_PRODUCTS = {"idct"}


class IntradayCapacityRunner(BaseRunner):
    """Runner for ENTSO-E Intraday Offered Transfer Capacity data (A31)."""

    RUNNER_NAME = "ENTSO-E Intraday Transfer Capacity Runner"

    TABLE_NAME = "entsoe_intraday_transfer_capacity"
    COLUMNS = [
        "trade_date", "period", "time_interval", "delivery_datetime",
        "area_id", "country_code", "product",
        "cap_import_de_mw", "cap_export_de_mw",
        "cap_import_at_mw", "cap_export_at_mw",
        "cap_import_pl_mw", "cap_export_pl_mw",
        "cap_import_sk_mw", "cap_export_sk_mw",
        "cap_import_total_mw", "cap_export_total_mw",
        "published_at",
    ]
    CONFLICT_COLUMNS = ["trade_date", "time_interval", "area_id", "country_code", "product"]

    # How far back the ida1/ida2/ida3 default window reaches. Days that are
    # already complete cost one SELECT and no API call.
    IDA_LOOKBACK_DAYS = 10

    def __init__(self, products: Optional[List[str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.client = None
        self.products = products or ALL_PRODUCTS

    def _init_client(self) -> bool:
        self.logger.debug("Initializing ENTSO-E client...")
        try:
            self.client = EntsoeClient()
            self.logger.debug("Client initialized")
            return True
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            return False

    # ------------------------------------------------------------- windowing
    @staticmethod
    def _day_utc_bounds(day: date) -> Tuple[datetime, datetime]:
        """Prague-local midnight-to-midnight bounds for `day`, in UTC."""
        start_local = datetime.combine(day, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        end_local = start_local + timedelta(days=1)
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

    def _default_window(self) -> Tuple[datetime, datetime]:
        """Default fetch window (Prague local), converted to UTC.

        idct is only ever revised for the current delivery day, so it looks at
        today + tomorrow and nothing older. ida1/ida2/ida3 look back
        IDA_LOOKBACK_DAYS as well, so a day ENTSO-E published late still gets
        picked up: the per-product completeness SELECT skips the 8 HTTP calls
        for every day that is already whole, so the extra span costs one cheap
        query per day/product and no API traffic once the history is filled.
        """
        today = datetime.now(PRAGUE_TZ).date()
        lookback = 0 if set(self.products) <= REVISABLE_PRODUCTS else self.IDA_LOOKBACK_DAYS
        start_local = datetime.combine(
            today - timedelta(days=lookback), datetime.min.time()
        ).replace(tzinfo=PRAGUE_TZ)
        end_local = datetime.combine(
            today + timedelta(days=1), datetime.min.time()
        ).replace(tzinfo=PRAGUE_TZ) + timedelta(days=1)  # through end of tomorrow
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

    @staticmethod
    def _iter_local_days(period_start_utc: datetime, period_end_utc: datetime) -> Iterator[date]:
        """Enumerate Prague-local calendar days covered by [start, end).

        Both get_backfill_chunks() and _default_window() align their bounds to
        Prague-local midnight, so this simple wall-clock walk is exact.
        """
        cur_local = period_start_utc.astimezone(PRAGUE_TZ)
        end_local = period_end_utc.astimezone(PRAGUE_TZ)
        while cur_local < end_local:
            yield cur_local.date()
            cur_local += timedelta(days=1)

    # --------------------------------------------------------- completeness
    def _expected_interval_count(self, day: date, product: str) -> int:
        """Number of distinct 15-min MTUs `product` is expected to cover for `day`.

        Derived from the local day's actual length in minutes (92/96/100
        periods on DST switch days), never a hardcoded 96. ida3 only covers
        the second half of the delivery day (from 12:00 Prague local), so its
        expected count is half of the full day's interval count.
        """
        day_start_utc, day_end_utc = self._day_utc_bounds(day)
        day_minutes = int((day_end_utc - day_start_utc).total_seconds() / 60)
        full_day_intervals = day_minutes // 15
        if product == "ida3":
            return full_day_intervals // 2
        return full_day_intervals

    def _is_product_complete(self, conn, day: date, area_id: int, country_code: str, product: str) -> bool:
        """One cheap SELECT: is (day, area, product) already fully populated?

        Complete = the count of distinct time_interval rows already stored
        for (trade_date, area_id, country_code, product) reaches the expected
        interval count for that local day/product. For ida1/ida2 that is the
        full day; for ida3 it is the second-half-of-day count, so "count
        reaches the expected total" is equivalent to "every interval from
        ida3's first published position through end of day is present, no
        holes" - upserts are idempotent and ida3 never publishes outside its
        second-half window, so there is no way to reach the expected count
        with a gap in the middle.
        """
        expected = self._expected_interval_count(day, product)
        if expected <= 0:
            return True
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(DISTINCT time_interval) FROM {self.TABLE_NAME} "
                f"WHERE trade_date = %s AND area_id = %s AND country_code = %s AND product = %s",
                (day, area_id, country_code, product)
            )
            (count,) = cur.fetchone()
        return count >= expected

    # -------------------------------------------------------------- fetch
    def _fetch_product_borders(
        self, product: str, area_code: str, period_start: datetime, period_end: datetime
    ) -> List[Tuple[str, str, Optional[str]]]:
        """Fetch all 8 border-direction XML responses for one product/day.

        Returns:
            List of (border_key, direction, xml_or_None) tuples. A single
            border failing is logged and yields (key, direction, None); it
            must not abort the run.
        """
        auction_type, classification_sequence = INTRADAY_CAPACITY_PRODUCTS[product]
        results: List[Tuple[str, str, Optional[str]]] = []

        for border_key, neighbor_eic in CZ_CAPACITY_BORDERS.items():
            for direction, (in_domain, out_domain) in (
                ("import", (area_code, neighbor_eic)),
                ("export", (neighbor_eic, area_code)),
            ):
                try:
                    self.logger.debug(
                        f"    Fetching {product} {border_key}/{direction} "
                        f"(in={in_domain}, out={out_domain})..."
                    )
                    xml = self.client.fetch_intraday_offered_capacity(
                        period_start, period_end,
                        in_domain=in_domain, out_domain=out_domain,
                        auction_type=auction_type,
                        classification_sequence=classification_sequence,
                    )
                    if xml is None:
                        self.logger.info(
                            f"{self.RUNNER_NAME}: {product} {border_key}/{direction} not published yet"
                        )
                    else:
                        self.logger.debug(f"    {product} {border_key}/{direction}: received data")
                    results.append((border_key, direction, xml))
                except Exception as e:
                    self.logger.warning(f"    Failed {product} {border_key}/{direction}: {e}")
                    results.append((border_key, direction, None))

        return results

    def _process_product_day(
        self, conn, product: str, day: date,
        area_id: int, area_code: str, country_code: str
    ) -> int:
        """Fetch, parse, and upsert one product for a single local day."""
        day_start_utc, day_end_utc = self._day_utc_bounds(day)

        fetch_results = self._fetch_product_borders(product, area_code, day_start_utc, day_end_utc)

        parser = IntradayTransferCapacityParser(area_id=area_id, country_code=country_code)
        any_data = False
        for border_key, direction, xml in fetch_results:
            if xml is None:
                continue
            try:
                parser.parse_xml_content(xml, border_key, direction)
                any_data = True
            except Exception as e:
                self.logger.warning(f"    Failed to parse {product} {border_key}/{direction}: {e}")

        if not any_data:
            self.logger.info(f"{self.RUNNER_NAME}: no data for {product} {country_code} {day}")
            return 0

        records = parser.get_wide_format_data(product)
        if not records:
            return 0

        self.logger.debug(f"    {product} {day}: {len(records)} rows parsed")

        rows = [tuple(record[col] for col in self.COLUMNS) for record in records]

        if not self.dry_run:
            self.bulk_upsert(
                conn, self.TABLE_NAME, self.COLUMNS, rows, self.CONFLICT_COLUMNS,
                skip_unchanged=True,
            )
        else:
            self.logger.info(f"    DRY RUN - would upsert {len(rows)} rows for {product} {day}")

        self.track_country(country_code, len(rows))
        return len(rows)

    def _process_day(
        self, conn, day: date, area_id: int, area_code: str, country_code: str
    ) -> int:
        """Process every requested product for a single local day."""
        total = 0
        for product in self.products:
            if product not in REVISABLE_PRODUCTS:
                try:
                    if self._is_product_complete(conn, day, area_id, country_code, product):
                        self.logger.debug(f"  {product} {day}: already complete, skipping fetch")
                        continue
                except Exception as e:
                    # Completeness check failing must not block fetching.
                    self.logger.warning(f"  {product} {day}: completeness check failed ({e}), fetching anyway")

            try:
                total += self._process_product_day(conn, product, day, area_id, area_code, country_code)
            except Exception as e:
                self.logger.error(f"  {product} {day} failed: {e}")
                if self.debug:
                    import traceback
                    traceback.print_exc()

        return total

    # ---------------------------------------------------------------- run
    def run(self) -> bool:
        """Execute the intraday transfer capacity pipeline."""
        self.print_header()

        if not self._init_client():
            return False

        total_records = 0

        try:
            area_id, area_code, display_label, country_code = ACTIVE_INTRADAY_CAPACITY_AREAS[0]

            if self.is_backfill:
                windows = list(self.get_backfill_chunks())
            else:
                windows = [self._default_window()]

            with self.database_connection() as conn:
                for period_start, period_end in windows:
                    for day in self._iter_local_days(period_start, period_end):
                        total_records += self._process_day(conn, day, area_id, area_code, country_code)

            self.logger.debug("")
            self.logger.info(self.format_summary(total_records))
            self.print_footer(success=True)
            return True

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            self.print_footer(success=False)
            return False

    # --------------------------------------------------------------- CLI
    @classmethod
    def create_argument_parser(cls):
        parser = super().create_argument_parser()
        parser.add_argument(
            '--products',
            type=str,
            default=None,
            help=(
                "Comma-separated subset of idct,ida1,ida2,ida3 "
                "(default: all four)"
            ),
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

        products = None
        if args.products:
            products = [p.strip() for p in args.products.split(',') if p.strip()]
            invalid = [p for p in products if p not in INTRADAY_CAPACITY_PRODUCTS]
            if invalid:
                print(
                    f"Error: invalid --products entries {invalid}. "
                    f"Choose from {list(INTRADAY_CAPACITY_PRODUCTS.keys())}."
                )
                sys.exit(1)

        runner = cls(
            debug=args.debug,
            dry_run=args.dry_run,
            start_date=start_date,
            end_date=end_date,
            products=products,
        )
        success = runner.run()
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    IntradayCapacityRunner.main()
