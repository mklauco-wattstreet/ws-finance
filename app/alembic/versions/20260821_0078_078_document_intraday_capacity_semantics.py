"""Document intraday transfer capacity semantics via table/column comments.

The `product` values ida1/ida2/ida3 name the SIDC intraday auction that the
transfer capacity was released to. They are an INPUT to that auction. The
auction's OUTPUT - clearing price and traded volume - is not published by
ENTSO-E at all; it comes from the exchange and lives in ote_prices_ida
(CZ) / okte_prices_ida (SK), keyed by ida_idx.

Comments only: no data or structural change. They surface in psql \\d+,
DataGrip, and any schema browser, so the distinction is visible to whoever
reads the table next instead of living only in ENTSOE_README.md.

Revision ID: 078
Revises: 077
Create Date: 2026-08-21
"""

from alembic import op


revision = '078'
down_revision = '077'
branch_labels = None
depends_on = None


_TABLE_COMMENT = (
    "ENTSO-E intraday OFFERED TRANSFER CAPACITY [12.1.A/B, documentType A31]. "
    "MW made available for cross-border trading on the CZ borders. "
    "This is an INPUT to intraday trading, not a market result: these documents "
    "contain no prices and no traded volumes. IDA auction clearing prices and "
    "volumes are not published by ENTSO-E - see finance.ote_prices_ida (CZ) and "
    "finance.okte_prices_ida (SK), keyed by ida_idx."
)

_PRODUCT_COMMENT = (
    "Which capacity release stage this row describes: "
    "'ida1'/'ida2'/'ida3' = capacity released TO the first/second/third SIDC "
    "intraday auction (auction.Type=A01, classificationSequence 1/2/3); "
    "'idct' = capacity available for SIDC continuous trading (auction.Type=A08), "
    "revised repeatedly during the delivery day. "
    "NOT the auction outcome - for IDA prices/volumes see ote_prices_ida.ida_idx."
)

_PUBLISHED_AT_COMMENT = (
    "update_DateAndOrTime of the source document. Populated for 'idct' only "
    "(continuously revised); NULL for ida1/ida2/ida3, which are static once published."
)

_CONGESTION_COMMENT = (
    "ENTSO-E congestion income [12.1.E, documentType A25 / businessType B10]. "
    "EUR per MTU attributed to the CZ bidding zone under Core flow-based implicit "
    "allocation. Queried per bidding zone (in_Domain = out_Domain), not per border. "
    "source_resolution='PT60M' marks rows expanded from a pre-2025-10-01 hourly "
    "value, split by 4 so the quarters sum back to the published hourly amount."
)


def upgrade() -> None:
    for table in ('entsoe_intraday_transfer_capacity',
                  'entsoe_intraday_transfer_capacity_60min'):
        op.execute(f"COMMENT ON TABLE {table} IS $cmt${_TABLE_COMMENT}$cmt$;")
        op.execute(f"COMMENT ON COLUMN {table}.product IS $cmt${_PRODUCT_COMMENT}$cmt$;")
        op.execute(f"COMMENT ON COLUMN {table}.published_at IS $cmt${_PUBLISHED_AT_COMMENT}$cmt$;")

    for table in ('entsoe_congestion_income',
                  'entsoe_congestion_income_60min'):
        op.execute(f"COMMENT ON TABLE {table} IS $cmt${_CONGESTION_COMMENT}$cmt$;")


def downgrade() -> None:
    for table in ('entsoe_intraday_transfer_capacity',
                  'entsoe_intraday_transfer_capacity_60min'):
        op.execute(f"COMMENT ON TABLE {table} IS NULL;")
        op.execute(f"COMMENT ON COLUMN {table}.product IS NULL;")
        op.execute(f"COMMENT ON COLUMN {table}.published_at IS NULL;")

    for table in ('entsoe_congestion_income',
                  'entsoe_congestion_income_60min'):
        op.execute(f"COMMENT ON TABLE {table} IS NULL;")
