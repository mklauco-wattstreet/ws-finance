#!/usr/bin/env python3
"""ENTSO-E Outages runner (A77 production unit unavailability).

Outages are EVENT-based, so this runner deviates from the period-grid runners:

  * Fetch is offset-paginated (ENTSO-E returns max 200 XML docs per ZIP; page
    in steps of 200 until a short/empty page). Time window may span up to 1 year.
  * Backfill uses the OUTAGE window (periodStart/periodEnd), clamped to the
    OUTAGE_BACKFILL_FLOOR (2026-03-01).
  * Cron (no args) uses the UPDATE window (PeriodStartUpdate/PeriodEndUpdate) so
    revisions to long-running outages are caught, not just brand-new outages.

Raw docs land losslessly in entsoe_outages (+ entsoe_outage_points). The realized
feature aggregates are then rebuilt in-DB for the affected window:
  entsoe_outages_15min  total OUT MW per 15-min MTU (active only, deduped per unit)
  entsoe_outages_60min  hourly mean (level) / max (peak,count), gated to full hours
"""
import io
import zipfile
from datetime import datetime, timedelta, timezone, date

from psycopg2 import extras

from runners.base_runner import BaseRunner, PRAGUE_TZ
from entsoe.client import EntsoeClient
from entsoe.constants import ACTIVE_OUTAGE_AREAS
from entsoe.outages_parser import parse_document

OUTAGE_BACKFILL_FLOOR = date(2026, 3, 1)
DOC_TYPE_A77 = "A77"   # production unit unavailability (CZ files PLANNED here)
DOC_TYPE_A80 = "A80"   # generation unit unavailability (CZ files FORCED here)
BUSINESS_FORCED = "A54"

# What we ingest per area, as (documentType, businessType, docStatus):
#   A77 (all)     -> planned production-unit outages
#   A80 + A54     -> forced generation-unit outages (forced doesn't exist under A77)
# docStatus=None  -> ENTSO-E default returns Active (A05) + Cancelled (A09)
# docStatus=A13   -> Withdrawn (NOT returned by default; must be queried explicitly)
# We deliberately do NOT ingest A80 A53: it overlaps A77 A53 and would double-count.
FETCH_SPECS = [
    (DOC_TYPE_A77, None, None),          # planned: active + cancelled
    (DOC_TYPE_A77, None, "A13"),         # planned: withdrawn
    (DOC_TYPE_A80, BUSINESS_FORCED, None),   # forced: active + cancelled
    (DOC_TYPE_A80, BUSINESS_FORCED, "A13"),  # forced: withdrawn
]

EVENT_COLUMNS = [
    "doc_mrid", "revision_number", "timeseries_mrid", "doc_type", "business_type",
    "doc_status", "process_type", "created_datetime", "area_id", "country_code",
    "biddingzone_domain", "production_resource_mrid", "production_resource_name",
    "location_name", "psr_type", "power_system_resource_mrid", "power_system_resource_name",
    "nominal_power_mw", "quantity_unit", "curve_type", "unavail_start", "unavail_end",
    "min_available_mw", "max_unavailable_mw", "reason_code", "reason_text",
]
EVENT_CONFLICT = ["doc_mrid", "revision_number", "timeseries_mrid", "country_code"]

POINT_COLUMNS = [
    "doc_mrid", "revision_number", "timeseries_mrid", "area_id", "country_code",
    "point_start", "point_end", "resolution", "position", "available_mw",
]


class OutagesRunner(BaseRunner):
    RUNNER_NAME = "ENTSO-E Outages Runner"
    TABLE_NAME = "entsoe_outages"
    COLUMNS = EVENT_COLUMNS
    CONFLICT_COLUMNS = EVENT_CONFLICT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = None

    # ------------------------------------------------------------------ fetch
    @staticmethod
    def _fmt(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M")

    def _fetch(self, bidding_zone, period_start, period_end, doc_type=DOC_TYPE_A77,
               business_type=None, doc_status=None, update=False):
        """Offset-paginated fetch; returns list of XML byte documents."""
        docs = []
        offset = 0
        bt = f"&BusinessType={business_type}" if business_type else ""
        ds = f"&DocStatus={doc_status}" if doc_status else ""
        while True:
            if update:
                tparams = f"PeriodStartUpdate={self._fmt(period_start)}&PeriodEndUpdate={self._fmt(period_end)}"
            else:
                tparams = f"periodStart={self._fmt(period_start)}&periodEnd={self._fmt(period_end)}"
            url = (f"{self.client.base_url}?securityToken={self.client.security_token}"
                   f"&documentType={doc_type}{bt}{ds}&BiddingZone_Domain={bidding_zone}"
                   f"&{tparams}&offset={offset}")
            resp = self.client.session.get(url, timeout=120)
            if resp.status_code != 200:
                # 200-cap should never trigger (offset always sent); log others.
                snippet = resp.content[:300].decode("utf-8", "replace")
                if "No matching data" in snippet:
                    break
                self.logger.warning(f"  fetch offset={offset} HTTP {resp.status_code}: {snippet[:160]}")
                break
            if resp.content[:2] != b"PK":  # non-ZIP = acknowledgement / no data
                break
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            batch = [zf.read(n) for n in zf.namelist()]
            docs.extend(batch)
            self.logger.debug(f"  offset={offset}: +{len(batch)} docs (total {len(docs)})")
            if len(batch) < 200:
                break
            offset += 200
        return docs

    # ------------------------------------------------------------------ upsert
    def _upsert_points(self, conn, records):
        """Curve points are immutable per (doc,rev,ts,position) -> DO NOTHING."""
        if not records or self.dry_run:
            if self.dry_run and records:
                self.logger.info(f"DRY RUN - would upsert {len(records)} point rows")
            return 0
        cols = ", ".join(POINT_COLUMNS)
        conflict = "doc_mrid, revision_number, timeseries_mrid, position, country_code"
        q = (f"INSERT INTO entsoe_outage_points ({cols}) VALUES %s "
             f"ON CONFLICT ({conflict}) DO NOTHING")
        with conn.cursor() as cur:
            extras.execute_values(cur, q, records, page_size=1000)
            conn.commit()
        return len(records)

    def _store_raw(self, conn, docs, area_id, country_code):
        """Parse docs, dedup by PK, upsert events + points."""
        events, points = {}, {}
        for xb in docs:
            try:
                evs, pts = parse_document(xb, area_id, country_code)
            except Exception as e:
                self.logger.warning(f"  parse error: {e}")
                continue
            for e in evs:
                events[(e["doc_mrid"], e["revision_number"], e["timeseries_mrid"], e["country_code"])] = e
            for p in pts:
                points[(p["doc_mrid"], p["revision_number"], p["timeseries_mrid"], p["position"], p["country_code"])] = p

        ev_rows = [tuple(e[c] for c in EVENT_COLUMNS) for e in events.values()]
        pt_rows = [tuple(p[c] for c in POINT_COLUMNS) for p in points.values()]
        self.bulk_upsert(conn, self.TABLE_NAME, EVENT_COLUMNS, ev_rows, EVENT_CONFLICT)
        self._upsert_points(conn, pt_rows)
        self.logger.info(f"  stored {len(ev_rows)} events, {len(pt_rows)} points")
        return len(ev_rows)

    # -------------------------------------------------------------- aggregate
    def _aggregate_15min(self, conn, win_start, win_end, area_id, cc):
        if self.dry_run:
            return
        sql = """
        INSERT INTO entsoe_outages_15min
          (trade_date, period, time_interval, area_id, country_code, delivery_datetime,
           total_out_mw, planned_out_mw, forced_out_mw, n_units,
           out_lignite_mw, out_hard_coal_mw, out_gas_mw, out_nuclear_mw, out_hydro_mw, out_other_mw)
        SELECT
          (s2.slot AT TIME ZONE 'Europe/Prague')::date,
          (extract(hour FROM s2.slot AT TIME ZONE 'Europe/Prague')::int * 4
             + extract(minute FROM s2.slot AT TIME ZONE 'Europe/Prague')::int / 15 + 1)::smallint,
          to_char(s2.slot AT TIME ZONE 'Europe/Prague','HH24:MI') || '-' ||
          to_char((s2.slot + interval '15 min') AT TIME ZONE 'Europe/Prague','HH24:MI'),
          %(area)s, %(cc)s, s2.slot,
          -- Only the LATEST version per unit counts, and only if it is still active
          -- (doc_status NULL=Active or A05). A09/A13 (cancelled/withdrawn) -> 0.
          COALESCE(sum(s2.un) FILTER (WHERE s2.active), 0),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND s2.bt = 'A53'), 0),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND s2.bt = 'A54'), 0),
          count(*) FILTER (WHERE s2.active AND s2.un IS NOT NULL),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND s2.psr = 'B02'), 0),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND s2.psr = 'B05'), 0),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND s2.psr = 'B04'), 0),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND s2.psr = 'B14'), 0),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND s2.psr IN ('B10','B11','B12')), 0),
          COALESCE(sum(s2.un) FILTER (WHERE s2.active AND (s2.psr IS NULL
                   OR s2.psr NOT IN ('B02','B05','B04','B14','B10','B11','B12'))), 0)
        FROM (
          SELECT DISTINCT ON (s.slot, COALESCE(o.power_system_resource_mrid, o.production_resource_mrid, o.location_name))
            s.slot AS slot, o.business_type AS bt, o.psr_type AS psr,
            (o.doc_status IS NULL OR o.doc_status = 'A05') AS active,
            CASE WHEN o.nominal_power_mw IS NOT NULL AND o.min_available_mw IS NOT NULL
                 THEN GREATEST(o.nominal_power_mw - o.min_available_mw, 0) END AS un
          FROM generate_series(%(start)s::timestamptz,
                               %(end)s::timestamptz - interval '15 min',
                               interval '15 min') AS s(slot)
          LEFT JOIN entsoe_outages o
            ON o.country_code = %(cc)s
           AND o.nominal_power_mw IS NOT NULL AND o.min_available_mw IS NOT NULL
           AND o.unavail_start < s.slot + interval '15 min'
           AND o.unavail_end   > s.slot
          -- pick the most recently published / highest-revision version per unit,
          -- INCLUDING cancellations, so a later A09 supersedes an earlier active row
          ORDER BY s.slot, COALESCE(o.power_system_resource_mrid, o.production_resource_mrid, o.location_name),
                   o.created_datetime DESC NULLS LAST, o.revision_number DESC,
                   GREATEST(o.nominal_power_mw - o.min_available_mw, 0) DESC
        ) s2
        GROUP BY s2.slot
        ON CONFLICT (trade_date, period, area_id, country_code) DO UPDATE SET
          time_interval = EXCLUDED.time_interval, delivery_datetime = EXCLUDED.delivery_datetime,
          total_out_mw = EXCLUDED.total_out_mw, planned_out_mw = EXCLUDED.planned_out_mw,
          forced_out_mw = EXCLUDED.forced_out_mw, n_units = EXCLUDED.n_units,
          out_lignite_mw = EXCLUDED.out_lignite_mw, out_hard_coal_mw = EXCLUDED.out_hard_coal_mw,
          out_gas_mw = EXCLUDED.out_gas_mw, out_nuclear_mw = EXCLUDED.out_nuclear_mw,
          out_hydro_mw = EXCLUDED.out_hydro_mw, out_other_mw = EXCLUDED.out_other_mw,
          updated_at = CURRENT_TIMESTAMP;
        """
        with conn.cursor() as cur:
            cur.execute(sql, {"area": area_id, "cc": cc, "start": win_start, "end": win_end})
            n = cur.rowcount
            conn.commit()
        self.logger.info(f"  aggregated {n} 15-min rows")

    def _aggregate_60min(self, conn, win_start, win_end, area_id, cc):
        if self.dry_run:
            return
        sql = """
        INSERT INTO entsoe_outages_60min
          (trade_date, time_interval, area_id, country_code, delivery_datetime,
           total_out_mw, total_out_mw_max, planned_out_mw, forced_out_mw, n_units_max,
           out_lignite_mw, out_hard_coal_mw, out_gas_mw, out_nuclear_mw, out_hydro_mw, out_other_mw)
        SELECT trade_date,
          to_char(h_local,'HH24:MI') || '-' || to_char(h_local + interval '1 hour','HH24:MI'),
          %(area)s, %(cc)s, (h_local AT TIME ZONE 'Europe/Prague'),
          avg(total_out_mw), max(total_out_mw), avg(planned_out_mw), avg(forced_out_mw), max(n_units),
          avg(out_lignite_mw), avg(out_hard_coal_mw), avg(out_gas_mw),
          avg(out_nuclear_mw), avg(out_hydro_mw), avg(out_other_mw)
        FROM (
          SELECT trade_date,
                 date_trunc('hour', delivery_datetime AT TIME ZONE 'Europe/Prague') AS h_local,
                 period, total_out_mw, planned_out_mw, forced_out_mw, n_units,
                 out_lignite_mw, out_hard_coal_mw, out_gas_mw, out_nuclear_mw, out_hydro_mw, out_other_mw
          FROM entsoe_outages_15min
          WHERE country_code = %(cc)s
            AND delivery_datetime >= %(start)s AND delivery_datetime < %(end)s
        ) b
        GROUP BY trade_date, h_local
        HAVING count(DISTINCT period) = 4
        ON CONFLICT (trade_date, time_interval, area_id, country_code) DO UPDATE SET
          delivery_datetime = EXCLUDED.delivery_datetime,
          total_out_mw = EXCLUDED.total_out_mw, total_out_mw_max = EXCLUDED.total_out_mw_max,
          planned_out_mw = EXCLUDED.planned_out_mw, forced_out_mw = EXCLUDED.forced_out_mw,
          n_units_max = EXCLUDED.n_units_max,
          out_lignite_mw = EXCLUDED.out_lignite_mw, out_hard_coal_mw = EXCLUDED.out_hard_coal_mw,
          out_gas_mw = EXCLUDED.out_gas_mw, out_nuclear_mw = EXCLUDED.out_nuclear_mw,
          out_hydro_mw = EXCLUDED.out_hydro_mw, out_other_mw = EXCLUDED.out_other_mw,
          updated_at = CURRENT_TIMESTAMP;
        """
        with conn.cursor() as cur:
            cur.execute(sql, {"area": area_id, "cc": cc, "start": win_start, "end": win_end})
            n = cur.rowcount
            conn.commit()
        self.logger.info(f"  aggregated {n} 60-min rows")

    # -------------------------------------------------------------------- run
    @staticmethod
    def _prague_midnight_utc(d: date) -> datetime:
        return datetime.combine(d, datetime.min.time()).replace(tzinfo=PRAGUE_TZ).astimezone(timezone.utc)

    def run(self) -> bool:
        try:
            self.client = EntsoeClient()
        except Exception as e:
            self.logger.error(f"client init failed: {e}")
            return False

        total = 0
        try:
            for area_id, bidding_zone, label, cc in ACTIVE_OUTAGE_AREAS:
                if self.is_backfill:
                    start = self.start_date or OUTAGE_BACKFILL_FLOOR
                    if start < OUTAGE_BACKFILL_FLOOR:
                        self.logger.debug(f"  clamping start {start} -> {OUTAGE_BACKFILL_FLOOR}")
                        start = OUTAGE_BACKFILL_FLOOR
                    end = self.end_date or datetime.now(PRAGUE_TZ).date()
                    fetch_start = self._prague_midnight_utc(start)
                    fetch_end = self._prague_midnight_utc(end + timedelta(days=1))
                    agg_start, agg_end = fetch_start, fetch_end
                    self.logger.info(f"{label}: backfill {start} -> {end} (outage window)")
                    docs = []
                    for doc_type, btype, dstatus in FETCH_SPECS:
                        d = self._fetch(bidding_zone, fetch_start, fetch_end,
                                        doc_type=doc_type, business_type=btype,
                                        doc_status=dstatus, update=False)
                        lbl = f"{doc_type}{('/'+btype) if btype else ''}{('/'+dstatus) if dstatus else ''}"
                        self.logger.info(f"  {lbl}: {len(d)} docs")
                        docs.extend(d)
                else:
                    # cron: poll recent UPDATE window for new + revised outages
                    now_utc = datetime.now(timezone.utc)
                    upd_start = now_utc - timedelta(hours=6)
                    # Forward horizon = decision horizon (D+1) + 1 day margin = D+2.
                    # Beyond that, planned outages aren't published yet, so rows would
                    # just be self-rewriting zeros. Backward 3d catches late revisions.
                    today = datetime.now(PRAGUE_TZ).date()
                    agg_start = self._prague_midnight_utc(today - timedelta(days=3))
                    agg_end = self._prague_midnight_utc(today + timedelta(days=3))
                    self.logger.info(f"{label}: cron update-window {upd_start:%Y-%m-%d %H:%M} -> now")
                    docs = []
                    for doc_type, btype, dstatus in FETCH_SPECS:
                        docs.extend(self._fetch(bidding_zone, upd_start, now_utc,
                                                doc_type=doc_type, business_type=btype,
                                                doc_status=dstatus, update=True))

                self.logger.info(f"{label}: fetched {len(docs)} documents")
                with self.database_connection() as conn:
                    total += self._store_raw(conn, docs, area_id, cc)
                    self._aggregate_15min(conn, agg_start, agg_end, area_id, cc)
                    self._aggregate_60min(conn, agg_start, agg_end, area_id, cc)
                self.track_country(cc, len(docs))

            self.logger.info(self.format_summary(total))
            return True
        except Exception as e:
            self.logger.error(f"pipeline failed: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return False


if __name__ == "__main__":
    OutagesRunner.main()
