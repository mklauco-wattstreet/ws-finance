"""Add ote_limit_snapshot (OTE SOAP "Stav financniho limitu", SFVOTLIMITS).

Revision ID: 086
Revises: 085
Create Date: 2026-11-10

Source: OTE SOAP (CS OTE web services), report request code 463, data message
SFVOTLIMITS code 464 (482 = current IM limit only). XSD:
D1.4.2_XSD_sablony_2026_10_26/XML/SFVOTLIMITS/SFVOTLIMITS.xsd. Loaded every 5
minutes by app/ote_soap/limits.py.

One row per snapshot (per participant and commodity), one column per number OTE
sends. A snapshot is the state of the participant's financial-security limits at
the moment of the request; there are no per-order or per-trade rows. Per-order
history stays in the Excel-fed tables ote_intraday_limit_utilization and
ote_financial_security_trend.

The 19 value columns are plain NUMERIC (no precision or scale): the value is
stored exactly as OTE sent it. They come from Participant > DataGroups@commodity >
TotalsData / LimitsData / UtilizationData / AdditionalData. Every <Data> value
that has no column of its own (bonityCoef, bonityUtilization, other utilization
market codes, every VatData row) and every value whose currency is not CZK goes
into other_values (JSONB array), so nothing OTE sends is dropped.

snapshot_at is Limits@date and carries a UTC offset, so all three timestamps are
TIMESTAMPTZ. participant_ean can be NULL, hence the unique key is NULLS NOT
DISTINCT (PostgreSQL 15+). Not partitioned.
"""

from alembic import op


revision = '086'
down_revision = '085'
branch_labels = None
depends_on = None

TABLE = 'ote_limit_snapshot'

COMMENTS = {
    None: ('Source: OTE SOAP (CS OTE web services) report 463 / data message 464 SFVOTLIMITS '
           '(Stav financniho limitu), loaded every 5 minutes by app/ote_soap/limits.py. One row per '
           'snapshot (per participant and commodity). A snapshot is the state at the moment of the '
           'request and has no per-order or per-trade rows; per-order history stays in '
           'ote_intraday_limit_utilization and ote_financial_security_trend (Excel-fed). Amounts in '
           'CZK stored exactly as OTE sent them; utilization values are negative as sent by OTE.'),
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
    'im_limit_czk': 'TotalsData / limitIM: limit for the intraday market (CZK).',
    'im_utilization_czk': 'TotalsData / utilizationIM: utilization of the intraday market limit (CZK, negative as sent by OTE).',
    'im_free_resources_czk': 'TotalsData / freeResourcesIM: free resources of the intraday market limit (CZK).',
    'total_limit_czk': 'TotalsData / limit: total limit of the financial security (CZK).',
    'total_utilization_czk': 'TotalsData / utilization: total utilization (CZK, negative as sent by OTE).',
    'free_resources_czk': 'TotalsData / freeResources: free resources (CZK).',
    'fs_limit_czk': 'LimitsData / fsLimit: financial security limit (CZK).',
    'dynamic_limit_czk': 'LimitsData / dynamicLimit: dynamic limit (CZK).',
    'utilization_vdt_czk': 'UtilizationData / VDT: utilization by the intraday market, electricity (CZK, negative as sent by OTE).',
    'utilization_ida_czk': 'UtilizationData / IDA: utilization by the intraday auction, electricity (CZK, negative as sent by OTE).',
    'utilization_dt_czk': 'UtilizationData / DT: utilization by the day-ahead market, electricity (CZK, negative as sent by OTE).',
    'utilization_dev_czk': 'UtilizationData / DEV: utilization by real imbalances, electricity (CZK, negative as sent by OTE).',
    'utilization_tdd_czk': 'UtilizationData / TDD: utilization by clearing, electricity (CZK, negative as sent by OTE).',
    'utilization_pop_czk': 'UtilizationData / POP: utilization by fees, electricity (CZK, negative as sent by OTE).',
    'utilization_act_czk': 'UtilizationData / ACT: utilization by regulating energy, electricity (CZK, negative as sent by OTE).',
    'utilization_re_czk': 'UtilizationData / RE: utilization by real imbalances, electricity (CZK, negative as sent by OTE).',
    'utilization_revf_czk': 'UtilizationData / REVF: risk exposure flex / excluded bids, day-ahead market (CZK, negative as sent by OTE).',
    'utilization_lim_vdt_czk': 'UtilizationData / LIM_VDT: utilization of the limit for the intraday market, electricity (CZK, negative as sent by OTE).',
    'warning_limit_czk': 'AdditionalData / warningLimit: warning limit (CZK).',
    'other_values': ('JSON array of every <Data> value that has no column of its own, so nothing OTE sends is '
                     'dropped: bonityCoef, bonityUtilization, other utilization market codes, every VatData row, '
                     'and any value whose currency is not CZK. Each item: data_group, value_type, vat_period '
                     '(or null), value (decimal as string), currency (or null). [] when there is nothing.'),
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
            im_limit_czk NUMERIC,
            im_utilization_czk NUMERIC,
            im_free_resources_czk NUMERIC,
            total_limit_czk NUMERIC,
            total_utilization_czk NUMERIC,
            free_resources_czk NUMERIC,
            fs_limit_czk NUMERIC,
            dynamic_limit_czk NUMERIC,
            utilization_vdt_czk NUMERIC,
            utilization_ida_czk NUMERIC,
            utilization_dt_czk NUMERIC,
            utilization_dev_czk NUMERIC,
            utilization_tdd_czk NUMERIC,
            utilization_pop_czk NUMERIC,
            utilization_act_czk NUMERIC,
            utilization_re_czk NUMERIC,
            utilization_revf_czk NUMERIC,
            utilization_lim_vdt_czk NUMERIC,
            warning_limit_czk NUMERIC,
            other_values JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT {TABLE}_pkey PRIMARY KEY (id),
            CONSTRAINT {TABLE}_message_key
                UNIQUE NULLS NOT DISTINCT (message_id, participant_ean, commodity)
        );
    """)
    op.execute(
        f"CREATE INDEX ix_{TABLE}_snapshot_at ON {TABLE} (snapshot_at DESC);"
    )
    for column, text in COMMENTS.items():
        target = f"TABLE {TABLE}" if column is None else f"COLUMN {TABLE}.{column}"
        op.execute(f"COMMENT ON {target} IS '{text.replace(chr(39), chr(39) * 2)}';")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {TABLE} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {TABLE}_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
