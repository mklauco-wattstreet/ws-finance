"""Add ote_limit_snapshot (OTE SOAP "Stav financniho limitu", SFVOTLIMITS).

Revision ID: 086
Revises: 085
Create Date: 2026-11-10

Source: OTE SOAP (CS OTE web services), report request code 463, data message
SFVOTLIMITS code 464 (482 = current IM limit only). XSD:
D1.4.2_XSD_sablony_2026_10_26/XML/SFVOTLIMITS/SFVOTLIMITS.xsd. Loaded by
app/ote_soap/limits.py.

A snapshot of the participant's financial-security limits at the moment of the
request. One row per value OTE sends (19 values per snapshot today), flattened
from Participant > DataGroups@commodity > one of five groups:
  additional   AdditionalData   warningLimit, bonityCoef
  utilization  UtilizationData  per-market utilization (VDT, IDA, DT, DEV, ...)
  limits       LimitsData       fsLimit, dynamicLimit
  vat          VatData          unpaid VAT in the dynamic limit, per trade type
                                and month (vat_period); not sent today
  totals       TotalsData       limit, utilization, freeResources, limitIM, ...

snapshot_at is Limits@date and carries a UTC offset, so all three timestamps are
TIMESTAMPTZ. participant_ean / vat_period can be NULL, hence the unique key is
NULLS NOT DISTINCT (PostgreSQL 15+). Not partitioned.
"""

from alembic import op


revision = '086'
down_revision = '085'
branch_labels = None
depends_on = None

TABLE = 'ote_limit_snapshot'

COMMENTS = {
    None: ('Source: OTE SOAP (CS OTE web services) report 463 / data message 464 SFVOTLIMITS '
           '(Stav financniho limitu), loaded by app/ote_soap/limits.py. One row per value per '
           'snapshot. A snapshot is the state at the moment of the request and has no per-order '
           'or per-trade rows. Amounts in CZK; utilization values are negative as sent by OTE.'),
    'snapshot_at': 'SFVOTLIMITS/Limits@date: production timestamp of the snapshot (carries a UTC offset).',
    'message_at': 'SFVOTLIMITS@date-time: date and time of the message.',
    'message_id': 'SFVOTLIMITS@id: message identification.',
    'request_id': 'SFVOTLIMITS/Reference@id: id of our SFVOTREQ request; NULL if OTE sent the message unrequested.',
    'message_code': 'SFVOTLIMITS@message-code: 464 = financial status limit, 482 = current value of limit IM.',
    'report_title': 'Limits@title: report title.',
    'participant_ean': 'Participant@ean: EAN participant identification.',
    'participant_name': 'Participant@name: the participant\'s short name.',
    'participant_id': 'Participant@particId: the participant\'s unique id.',
    'participant_dic': 'Participant@dic: tax number; in the schema, not sent today.',
    'participant_eic': 'Participant@eic: EIC participant identification; in the schema, not sent today.',
    'participant_ic': 'Participant@ic: registration number; in the schema, not sent today.',
    'commodity': 'DataGroups@commodity: E = electricity, G = gas.',
    'data_group': ('Which XML group the value came from: additional (AdditionalData), '
                   'utilization (UtilizationData), limits (LimitsData), vat (VatData), '
                   'totals (TotalsData).'),
    'value_type': ('Data@type. additional: warningLimit, bonityCoef. utilization: market code '
                   '(VDT, IDA, DT, DEV, TDD, POP, ACT, RE, REVF, LIM_VDT, ...). limits: fsLimit, '
                   'dynamicLimit. vat: trade type (VD+, DT, PP1, ...). totals: limit, utilization, '
                   'bonityUtilization, freeResources, limitIM, utilizationIM, freeResourcesIM.'),
    'vat_period': 'Data@period (xs:gYearMonth, YYYY-MM); only for data_group = vat, otherwise NULL.',
    'value': 'Data@value: CZK amount, or a plain coefficient for bonityCoef. Utilization values are negative as sent by OTE.',
    'currency': 'Data@currency (CZK); NULL when OTE sends none (bonityCoef).',
}


def upgrade() -> None:
    op.execute(f"""
        CREATE TABLE {TABLE} (
            id BIGSERIAL,
            snapshot_at TIMESTAMPTZ NOT NULL,
            message_at TIMESTAMPTZ NOT NULL,
            message_id VARCHAR(35) NOT NULL,
            request_id VARCHAR(35),
            message_code VARCHAR(3) NOT NULL,
            report_title TEXT,
            participant_ean VARCHAR(20),
            participant_name TEXT,
            participant_id BIGINT,
            participant_dic VARCHAR(20),
            participant_eic VARCHAR(20),
            participant_ic VARCHAR(20),
            commodity VARCHAR(1) NOT NULL,
            data_group VARCHAR(12) NOT NULL,
            value_type VARCHAR(20) NOT NULL,
            vat_period VARCHAR(7),
            value NUMERIC(20, 6) NOT NULL,
            currency VARCHAR(3),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT {TABLE}_pkey PRIMARY KEY (id),
            CONSTRAINT {TABLE}_data_group_check
                CHECK (data_group IN ('additional', 'utilization', 'limits', 'vat', 'totals')),
            CONSTRAINT {TABLE}_value_key
                UNIQUE NULLS NOT DISTINCT (message_id, participant_ean, commodity, data_group, value_type, vat_period)
        );
    """)
    op.execute(
        f"CREATE INDEX ix_{TABLE}_snapshot_at ON {TABLE} (snapshot_at);"
    )
    op.execute(
        f"CREATE INDEX ix_{TABLE}_type_snapshot_at ON {TABLE} (data_group, value_type, snapshot_at);"
    )
    for column, text in COMMENTS.items():
        target = f"TABLE {TABLE}" if column is None else f"COLUMN {TABLE}.{column}"
        op.execute(f"COMMENT ON {target} IS '{text.replace(chr(39), chr(39) * 2)}';")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {TABLE} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {TABLE}_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
