"""Add ote_exchange_rate (OTE SOAP SFVOTEXCHRATE) and ote_soap_message (message register).

Revision ID: 087
Revises: 086
Create Date: 2026-11-10

Two tables for the OTE SOAP (CS OTE web services) pipeline in app/ote_soap/.

ote_exchange_rate: report request 425, data message SFVOTEXCHRATE code 426
("Vypořádací kurz OTE", OTE settlement exchange rate, CZK per 1 EUR). XSD:
D1.4.2_XSD_sablony_2026_10_26/XML/SFVOTEXCHRATE/SFVOTEXCHRATE.xsd. Loaded daily by
app/ote_soap/exchange_rate.py. One row per date (valid_date); the loader upserts
with ON CONFLICT (valid_date). This is OTE's own settlement rate and differs from
the CNB fixing in cnb_exchange_rate. The three rate columns are plain NUMERIC (no
precision or scale): the value is stored exactly as OTE sent it. They are nullable
because OTE sends only OteFinalRate today.

ote_soap_message: a register with one row per message picked up from the OTE queue
that has no table of its own, so the database shows that OTE delivered something we
have no loader for yet (for example a report OTE sends on its own after settlement)
and where its raw XML file is. The message content is NOT stored here, only in the
file under downloads/ote_soap/<YYYY-MM-DD>/. Limit snapshots (464) and exchange
rates (426) loaded by their own runners get no row. Written by app/ote_soap/ (the
limit and exchange-rate runners and ote_soap.fetch --collect). No unique constraint
besides the primary key. Not partitioned.
"""

from alembic import op


revision = '087'
down_revision = '086'
branch_labels = None
depends_on = None

RATE_TABLE = 'ote_exchange_rate'
MESSAGE_TABLE = 'ote_soap_message'

RATE_COMMENTS = {
    None: ('Source: OTE SOAP (CS OTE web services) report 425 / data message 426 SFVOTEXCHRATE '
           '(Vyporadaci kurz OTE, OTE settlement exchange rate, CZK per 1 EUR), loaded daily by '
           'app/ote_soap/exchange_rate.py. One row per valid_date. This is OTE\'s own settlement rate '
           'and differs from the CNB fixing in cnb_exchange_rate. Rates stored exactly as OTE sent them.'),
    'valid_date': 'ExchRates/*@validDate: date the rate is valid for.',
    'ote_final_rate': 'ExchRates/OteFinalRate: the OTE final (settlement) rate for the day (CZK per 1 EUR); the only rate OTE sends today.',
    'ote_final_vdt_rate': 'ExchRates/OteFinalVdtRate: the OTE final rate for intraday market (IM, VDT) trades; NULL when not sent.',
    'ote_final_ida_rate': 'ExchRates/OteFinalIdaRate: the OTE final rate for the intraday auctions (IDA); NULL when not sent.',
    'message_id': 'SFVOTEXCHRATE@id: id of the message the row was last loaded from.',
    'request_id': 'SFVOTEXCHRATE/Reference@id: id of our SFVOTREQ request; NULL if OTE sent the message unrequested.',
    'message_code': 'SFVOTEXCHRATE@message-code: 426 = OTE settlement exchange rate.',
    'message_at': 'SFVOTEXCHRATE@date-time: date and time of the message.',
}

MESSAGE_COMMENTS = {
    None: ('Register of messages picked up from the OTE SOAP queue that have no table of their own: shows that OTE '
           'delivered something we have no loader for yet and where its raw XML file is. One row per message; '
           'the content is not stored here, only in the file under downloads/ote_soap/<YYYY-MM-DD>/. Limit '
           'snapshots (464) and exchange rates (426) loaded by their own runners get no row. Written by '
           'app/ote_soap/ (limit and exchange-rate runners, ote_soap.fetch --collect).'),
    'received_at': 'When we picked the message up from the OTE queue.',
    'message_type': 'Local name of the payload root element, e.g. SFVOTBILLING; NULL when the pickup carried only a RESPONSE.',
    'message_code': 'Payload @message-code, e.g. 401 or a gas code like GF2; NULL without payload.',
    'message_id': 'Payload @id: message identification.',
    'request_id': 'Payload Reference@id: id of the request it answers, if any.',
    'response_code': '@message-code of the accompanying RESPONSE element, if any.',
    'reason_code': 'RESPONSE/Reason@code, e.g. 9000 = ok, 9001 = no data.',
    'reason_text': 'RESPONSE/Reason text.',
    'file_name': 'Name of the raw XML file saved under downloads/ote_soap/<YYYY-MM-DD>/.',
}


def _apply_comments(table: str, comments: dict) -> None:
    for column, text in comments.items():
        target = f"TABLE {table}" if column is None else f"COLUMN {table}.{column}"
        op.execute(f"COMMENT ON {target} IS '{text.replace(chr(39), chr(39) * 2)}';")


def upgrade() -> None:
    op.execute(f"""
        CREATE TABLE {RATE_TABLE} (
            id BIGSERIAL,
            valid_date DATE NOT NULL,
            ote_final_rate NUMERIC,
            ote_final_vdt_rate NUMERIC,
            ote_final_ida_rate NUMERIC,
            message_id VARCHAR(35) NOT NULL,
            request_id VARCHAR(35),
            message_code VARCHAR(3) NOT NULL,
            message_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT {RATE_TABLE}_pkey PRIMARY KEY (id),
            CONSTRAINT {RATE_TABLE}_valid_date_key UNIQUE (valid_date)
        );
    """)
    _apply_comments(RATE_TABLE, RATE_COMMENTS)
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {RATE_TABLE} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {RATE_TABLE}_id_seq TO user_finance;")

    op.execute(f"""
        CREATE TABLE {MESSAGE_TABLE} (
            id BIGSERIAL,
            received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            message_type VARCHAR(40),
            message_code VARCHAR(3),
            message_id VARCHAR(35),
            request_id VARCHAR(35),
            response_code VARCHAR(3),
            reason_code VARCHAR(10),
            reason_text TEXT,
            file_name TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT {MESSAGE_TABLE}_pkey PRIMARY KEY (id)
        );
    """)
    op.execute(
        f"CREATE INDEX ix_{MESSAGE_TABLE}_received_at ON {MESSAGE_TABLE} (received_at DESC);"
    )
    op.execute(
        f"CREATE INDEX ix_{MESSAGE_TABLE}_code ON {MESSAGE_TABLE} (message_code);"
    )
    _apply_comments(MESSAGE_TABLE, MESSAGE_COMMENTS)
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {MESSAGE_TABLE} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {MESSAGE_TABLE}_id_seq TO user_finance;")


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {MESSAGE_TABLE} CASCADE;")
    op.execute(f"DROP TABLE IF EXISTS {RATE_TABLE} CASCADE;")
