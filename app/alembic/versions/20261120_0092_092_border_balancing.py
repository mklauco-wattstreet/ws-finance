"""Add cross-border balancing tables: per-border limits / exchanged energy, per-area exchange.

Revision ID: 092
Revises: 091
Create Date: 2026-11-20

Plan: docs/cross_border_plan.md (deferred item "cross-border balancing items on
ENTSO-E"; live check done 2026-11-20). Two tables, both 15-minute, both keyed on
directed area pairs / areas so level-1 borders are just more rows.

entsoe_border_balancing   per directed border, per balancing process
  limit_mw        balancing-energy exchange limit (documentType A31 / businessType
                  A26, processType A47 mFRR | A51 aFRR | A63 imbalance netting;
                  unit MAW = MW; compressed curve A03 forward-filled by the parser).
                  Direction: out_Domain (from) -> in_Domain (to). process
                  'mfrr' | 'afrr' | 'netting'.
  exchanged_mwh   netted / exchanged energy on the border (documentType A30,
                  businessType A45, processType A60 mFRR scheduled | A61 mFRR
                  direct | A63 netting | A51 aFRR; unit MWH per 15 min, stored as
                  published). Direction: connecting_Domain (from, the area that
                  delivers) -> acquiring_Domain (to, the area that receives).
                  process 'mfrr_sa' | 'mfrr_da' | 'netting' | 'afrr'.
  Rows of process 'mfrr' carry only limit_mw and rows of 'mfrr_sa' / 'mfrr_da'
  only exchanged_mwh (the limit is one figure for both mFRR products); rows of
  'netting' and 'afrr' can carry both, because the limit and the volume of one
  process share a key.

entsoe_area_balancing_exchange   per area, LIST-partitioned by country_code (CZ)
  documentType B17 (netted and exchanged volumes per area), businessType B09,
  Acquiring_Domain = Connecting_Domain = the area. The answer holds two
  TimeSeries, both against the European platform (IN-platform 10Y1001C--00119X for
  netting, MARI 10Y1001C--00085O for mFRR, PICASSO 10Y1001C--00090V for aFRR):
    acquiring_Domain = area        -> import_mwh (area receives energy)
    connecting_Domain = area       -> export_mwh (area delivers energy)
  Verified against ceps_export_import_svr_15min: CEPS value (MW) =
  (export_mwh - import_mwh) * 4, correlation 0.9997 netting / 0.94 mFRR / 1.000
  aFRR; CZ deficit intervals show negative CEPS aFRR = import.
  process 'netting' | 'mfrr_sa' | 'mfrr_da' | 'afrr'. Unit MWH per 15 min; the
  aFRR source is 4-second MWh points, summed per 15-minute interval (only
  complete intervals of 225 points). import/export are NULL when the document
  does not report that side for the interval (the documents are sparse).
  doc_status is the document docStatus (A01 intermediate, A02 final) where the
  source sends one (netting and aFRR do; mFRR does not).

Timestamps are TIMESTAMPTZ; trade_date / period / time_interval follow migration
090 (Prague-local trade date, period counted from local midnight, descriptive
only - the key is the UTC instant). created_at / updated_at are TIMESTAMPTZ
DEFAULT CURRENT_TIMESTAMP with no trigger (the upserts set updated_at).

No existing table is touched. Downgrade drops exactly these two tables (the CZ
partition goes with its parent).
"""

from alembic import op


revision = '092'
down_revision = '091'
branch_labels = None
depends_on = None


_AUDIT = """
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"""

_BORDER_PROCESSES = "'netting', 'afrr', 'mfrr', 'mfrr_sa', 'mfrr_da'"
_AREA_PROCESSES = "'netting', 'afrr', 'mfrr_sa', 'mfrr_da'"


def _grant(table: str) -> None:
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table} TO user_finance;")


def upgrade() -> None:
    # ------------------------------------------------------------------
    # entsoe_border_balancing - 15 min, directed, per process
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_border_balancing (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            from_area_id INTEGER NOT NULL,
            to_area_id INTEGER NOT NULL,
            process VARCHAR(12) NOT NULL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            limit_mw NUMERIC(12, 3),
            exchanged_mwh NUMERIC(14, 4),
            {_AUDIT.strip()},
            CONSTRAINT entsoe_border_balancing_pkey
                PRIMARY KEY (delivery_datetime, from_area_id, to_area_id, process),
            CONSTRAINT entsoe_border_balancing_from_fkey FOREIGN KEY (from_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_border_balancing_to_fkey FOREIGN KEY (to_area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_border_balancing_areas_differ_check CHECK (from_area_id <> to_area_id),
            CONSTRAINT entsoe_border_balancing_process_check CHECK (process IN ({_BORDER_PROCESSES}))
        );
    """)
    op.execute(
        "CREATE INDEX ix_entsoe_border_balancing_border_process_time "
        "ON entsoe_border_balancing (from_area_id, to_area_id, process, delivery_datetime);"
    )
    op.execute(
        "COMMENT ON TABLE entsoe_border_balancing IS "
        "'Directed 15-min cross-border balancing data from_area_id -> to_area_id. "
        "limit_mw (MW) = balancing exchange limit, ENTSO-E A31/A26 out_Domain -> in_Domain, "
        "process mfrr (A47) / afrr (A51) / netting (A63). exchanged_mwh (MWh per 15 min) = "
        "netted/exchanged energy, ENTSO-E A30/A45 connecting_Domain (delivers) -> acquiring_Domain "
        "(receives), process mfrr_sa scheduled (A60) / mfrr_da direct (A61) / netting (A63) / afrr (A51).';"
    )
    _grant("entsoe_border_balancing")

    # ------------------------------------------------------------------
    # entsoe_area_balancing_exchange - 15 min, per area, partitioned by country
    # ------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE entsoe_area_balancing_exchange (
            delivery_datetime TIMESTAMPTZ NOT NULL,
            area_id INTEGER NOT NULL,
            country_code VARCHAR(5) NOT NULL,
            process VARCHAR(12) NOT NULL,
            trade_date DATE NOT NULL,
            period INTEGER NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            import_mwh NUMERIC(14, 4),
            export_mwh NUMERIC(14, 4),
            doc_status VARCHAR(3),
            {_AUDIT.strip()},
            CONSTRAINT entsoe_area_balancing_exchange_pkey
                PRIMARY KEY (delivery_datetime, area_id, country_code, process),
            CONSTRAINT entsoe_area_balancing_exchange_area_fkey FOREIGN KEY (area_id) REFERENCES entsoe_areas (id),
            CONSTRAINT entsoe_area_balancing_exchange_process_check CHECK (process IN ({_AREA_PROCESSES}))
        ) PARTITION BY LIST (country_code);
    """)
    op.execute(
        "CREATE TABLE entsoe_area_balancing_exchange_cz "
        "PARTITION OF entsoe_area_balancing_exchange FOR VALUES IN ('CZ');"
    )
    op.execute(
        "CREATE INDEX ix_entsoe_area_balancing_exchange_country_trade_date "
        "ON entsoe_area_balancing_exchange (country_code, trade_date);"
    )
    op.execute(
        "COMMENT ON TABLE entsoe_area_balancing_exchange IS "
        "'15-min energy the area exchanges with the European balancing platforms (ENTSO-E B17/B09). "
        "import_mwh = TimeSeries with acquiring_Domain = area (area receives), export_mwh = "
        "connecting_Domain = area (area delivers); MWh per 15 min; NULL = side not reported. "
        "process netting (A63, IN platform) / mfrr_sa scheduled (A60, MARI) / mfrr_da direct (A61, MARI) / "
        "afrr (A51, PICASSO, 4-second points summed per 15 min). doc_status A01 intermediate / A02 final.';"
    )
    _grant("entsoe_area_balancing_exchange")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS entsoe_area_balancing_exchange CASCADE;")
    op.execute("DROP TABLE IF EXISTS entsoe_border_balancing CASCADE;")
