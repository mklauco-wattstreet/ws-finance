#!/usr/bin/env python3
"""
ENTSO-E Congestion Income Runner (12.1.E, documentType=A25).

Fetches congestion income for active bidding zones (CZ only today) and
upserts into the partitioned entsoe_congestion_income table.

Congestion income for the current delivery day is published incrementally
and can be corrected after the fact, so this runner is designed to be
polled every 15 minutes and to always re-fetch its whole window rather than
skip areas/dates that already have some rows. skip_unchanged=True on the
upsert keeps re-polling from bumping updated_at on rows whose value has not
changed.

Usage:
    python3 -m runners.entsoe_congestion_income_runner [--debug] [--dry-run]
    python3 -m runners.entsoe_congestion_income_runner --start 2026-06-01 --end 2026-06-22
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.congestion_parser import CongestionIncomeParser
from entsoe.constants import ACTIVE_CONGESTION_INCOME_AREAS


class CongestionIncomeRunner(BaseRunner):
    """Runner for ENTSO-E Congestion Income data (A25, 12.1.E)."""

    RUNNER_NAME = "ENTSO-E Congestion Income Runner"

    # Table configuration - partitioned by country_code
    TABLE_NAME = "entsoe_congestion_income"
    COLUMNS = [
        "trade_date", "period", "time_interval", "delivery_datetime",
        "area_id", "country_code", "congestion_income_eur", "source_resolution"
    ]
    CONFLICT_COLUMNS = ["trade_date", "time_interval", "area_id", "country_code"]

    # Default (non-backfill) polling window: D-2 through D, Prague local.
    # 10 days, not 2: ENTSO-E sometimes publishes a delivery day late (or not at
    # all for a while), and a short window would leave that day permanently holed.
    # Cost is unchanged in practice - 11 days is 2 chunked requests per firing.
    DEFAULT_WINDOW_DAYS_BACK = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

    def _init_client(self) -> bool:
        """Initialize ENTSO-E client."""
        self.logger.debug("Initializing ENTSO-E client...")
        try:
            self.client = EntsoeClient()
            self.logger.debug("Client initialized")
            return True
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            return False

    def _save_xml_file(
        self, xml_content: str, period_start, period_end, country_code: str
    ) -> Path:
        """Save XML file to disk with area-specific naming."""
        start_str = period_start.strftime('%Y%m%d%H%M')
        end_str = period_end.strftime('%Y%m%d%H%M')

        xml_file = self.get_output_path(
            f'entsoe_congestion_income_{country_code.lower()}_{start_str}_{end_str}.xml',
            period_start
        )

        self.save_xml(xml_content, xml_file)
        return xml_file

    def _prepare_records(self, data: List[dict]) -> List[Tuple]:
        """Convert parsed row dicts into tuples matching COLUMNS order."""
        records = []
        for record in data:
            records.append((
                record['trade_date'],
                record['period'],
                record['time_interval'],
                record['delivery_datetime'],
                record['area_id'],
                record['country_code'],
                record.get('congestion_income_eur'),
                record.get('source_resolution'),
            ))
        return records

    def _process_area(
        self, period_start, period_end,
        area_id: int, area_code: str, display_label: str, country_code: str,
        conn=None
    ) -> int:
        """Fetch, parse and upsert congestion income for a single area/window."""
        self.logger.debug(
            f"  Fetching {display_label} (area_id={area_id}, country={country_code}) "
            f"[{period_start.strftime('%Y-%m-%d %H:%M')} - {period_end.strftime('%Y-%m-%d %H:%M')} UTC]..."
        )

        try:
            xml_content = self.client.fetch_congestion_income_for_domain(
                period_start, period_end, area_code
            )

            if xml_content is None:
                # "No matching data" acknowledgement - expected for windows
                # ENTSO-E has not published yet. Not an error.
                self.logger.info(
                    f"{self.RUNNER_NAME}: no congestion income published yet for "
                    f"{country_code} [{period_start.strftime('%Y-%m-%d %H:%M')} - "
                    f"{period_end.strftime('%Y-%m-%d %H:%M')} UTC]"
                )
                return 0

            self.logger.debug(f"    XML: {len(xml_content)} bytes")

            xml_file = self._save_xml_file(
                xml_content, period_start, period_end, country_code
            )

            parser = CongestionIncomeParser(area_id=area_id, country_code=country_code)
            data = parser.parse_xml_content(xml_content)

            if not data:
                self.logger.info(f"{self.RUNNER_NAME}: no rows parsed for {country_code}")
                return 0

            self.logger.debug(f"    Parsed {len(data)} rows")

            records = self._prepare_records(data)

            if not self.dry_run and conn:
                self.bulk_upsert(
                    conn,
                    self.TABLE_NAME,
                    self.COLUMNS,
                    records,
                    self.CONFLICT_COLUMNS,
                    skip_unchanged=True,
                )
            elif self.dry_run:
                self.logger.info(f"    DRY RUN - Would upsert {len(records)} records")

            self.logger.debug(f"    XML saved to {xml_file}")
            self.track_country(country_code, len(records))
            return len(records)

        except Exception as e:
            if self.is_data_unavailable_error(e):
                self.logger.info(
                    f"{self.RUNNER_NAME}: {country_code} not available for "
                    f"[{period_start.strftime('%Y-%m-%d %H:%M')} - "
                    f"{period_end.strftime('%Y-%m-%d %H:%M')} UTC]"
                )
            else:
                self.logger.error(f"  Failed {country_code}: {e}")
                if self.debug:
                    import traceback
                    traceback.print_exc()
            return 0

    def _process_chunk(self, period_start, period_end, conn=None) -> int:
        """Process a single time chunk for ALL active areas."""
        self.logger.debug(
            f"Processing: {period_start.strftime('%Y-%m-%d %H:%M')} "
            f"to {period_end.strftime('%Y-%m-%d %H:%M')} UTC"
        )

        total_records = 0
        for area_id, area_code, display_label, country_code in ACTIVE_CONGESTION_INCOME_AREAS:
            records = self._process_area(
                period_start, period_end,
                area_id, area_code, display_label, country_code,
                conn
            )
            total_records += records

        return total_records

    def _default_window_utc(self):
        """D-2 through D (Prague local, end-exclusive), converted to UTC."""
        today_prague = datetime.now(PRAGUE_TZ).date()
        window_start_date = today_prague - timedelta(days=self.DEFAULT_WINDOW_DAYS_BACK)
        window_end_date = today_prague + timedelta(days=1)  # end-exclusive -> through end of "today"

        start_prague = datetime.combine(window_start_date, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)
        end_prague = datetime.combine(window_end_date, datetime.min.time()).replace(tzinfo=PRAGUE_TZ)

        return start_prague.astimezone(timezone.utc), end_prague.astimezone(timezone.utc)

    def run(self) -> bool:
        """Execute the congestion income pipeline."""
        self.print_header()

        if not self._init_client():
            return False

        total_records = 0

        try:
            if self.is_backfill:
                self.logger.debug("")
                self.logger.debug(
                    f"Processing {len(ACTIVE_CONGESTION_INCOME_AREAS)} areas: "
                    f"{', '.join(label for _, _, label, _ in ACTIVE_CONGESTION_INCOME_AREAS)}"
                )
                with self.database_connection() as conn:
                    for period_start, period_end in self.get_backfill_chunks():
                        try:
                            records = self._process_chunk(period_start, period_end, conn)
                            total_records += records
                        except Exception as e:
                            self.logger.error(f"Chunk failed: {e}")
                            if self.debug:
                                import traceback
                                traceback.print_exc()
                            continue
            else:
                # Default polling window: D-2 through D (Prague local), always
                # re-fetched in full (data is revisable / partially published
                # intraday). Deliberately does NOT use
                # _run_with_availability_check(), which would skip a date as
                # soon as any row exists for it - wrong for revisable data.
                period_start, period_end = self._default_window_utc()
                with self.database_connection() as conn:
                    total_records = self._process_chunk(period_start, period_end, conn)

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


if __name__ == '__main__':
    CongestionIncomeRunner.main()
