"""Extend ote_soap_message to one row per OTE SOAP message, sent or received.

Revision ID: 089
Revises: 088
Create Date: 2026-11-12

ote_soap_message (migration 087) was a register of queue messages nobody asked for:
one row per message picked up from the OTE queue that has no table of its own. From
now on it holds ONE ROW PER MESSAGE, sent or received: every request we send to OTE
(report requests SFVOTREQ and queue pickups COMMONREQ 921) and every answer (receipts,
"queue empty" 922, data messages, the 424 limit warning, failed calls). The writer code
in app/ote_soap/ is changed in a later step; the current code inserts only the old
columns and keeps working, because every new column is nullable or has a default. The
message XML itself is still NOT stored in the table: it is in the file under
downloads/ote_soap/<YYYY-MM-DD>/ when file_name is set.

Additive changes only (nothing is dropped, renamed or retyped; grants are untouched,
the existing table grants cover the new columns):
- direction VARCHAR(3) NOT NULL DEFAULT 'in', check ote_soap_message_direction_check
  (in, out): 'out' = we sent it, 'in' = OTE sent it. The existing rows are all received
  messages, so the default is right for them.
- service VARCHAR(20): reportService or commonService, the endpoint of the HTTP call.
- reply_to_id BIGINT: for an 'in' row, id of the 'out' row of the same HTTP call. No
  foreign key.
- fetch_log_id BIGINT: id of the finance.fetch_log run that caused the message; NULL for
  the manual tool ote_soap.fetch. No foreign key.
- http_status INTEGER, return_code VARCHAR(10), reason_type VARCHAR(3),
  result_code VARCHAR(10): HTTP status, OTE RETURN_CODE, RESPONSE/Reason@type and
  RESPONSE/Reason@result-code of the answer.
- level VARCHAR(7) NOT NULL DEFAULT 'ok', check ote_soap_message_level_check
  (ok, warning, error), set by the writer.
- outcome VARCHAR(10), check ote_soap_message_outcome_check (delivered, no_data,
  timeout, failed): only on an 'out' report request that a runner waited for.
- error TEXT: error text when the call failed.
- file_name: NOT NULL dropped (queue pickup requests and "queue empty" answers have no
  file).
- four new indexes: ix_ote_soap_message_message_id, ix_ote_soap_message_request_id,
  ix_ote_soap_message_fetch_log_id and the partial ix_ote_soap_message_problem on
  (received_at DESC) WHERE level <> 'ok'. The two indexes from 087
  (ix_ote_soap_message_received_at, ix_ote_soap_message_code) are kept.
- COMMENT ON TABLE and on every new column and every column whose meaning changes.

Data update: existing rows with request_id IS NULL get level = 'warning', matching the
rule that a queue message nobody asked for is a warning. On production this is 3 of the
5 rows (the 424 limit warning and two unasked billing messages); the other 2 stay 'ok'.

Downgrade drops the four new indexes and the new columns (their check constraints go
with them). It does NOT restore NOT NULL on file_name, because rows without a file may
exist by then. The level data update needs no separate revert: the column is dropped.
"""

from alembic import op


revision = '089'
down_revision = '088'
branch_labels = None
depends_on = None

TABLE = 'ote_soap_message'

NEW_INDEXES = [
    f'ix_{TABLE}_message_id',
    f'ix_{TABLE}_request_id',
    f'ix_{TABLE}_fetch_log_id',
    f'ix_{TABLE}_problem',
]

NEW_COLUMNS = [
    'direction',
    'service',
    'reply_to_id',
    'fetch_log_id',
    'http_status',
    'return_code',
    'reason_type',
    'result_code',
    'level',
    'outcome',
    'error',
]

COMMENTS = {
    None: ('Register of every OTE SOAP (CS OTE web services) message, sent or received: one row per message. '
           'direction out = requests we send (report requests SFVOTREQ, queue pickups COMMONREQ 921); '
           'direction in = answers from OTE (receipts, queue empty 922, data messages, the 424 limit warning, '
           'failed calls). The message XML itself is NOT stored in the table: it is in the file under '
           'downloads/ote_soap/<YYYY-MM-DD>/ when file_name is set. Written by app/ote_soap/. No unique '
           'constraint besides the primary key.'),
    'received_at': 'When we sent the message (direction out) or received it (direction in).',
    'message_type': ('Local name of the message root element, e.g. SFVOTBILLING; for an out row the type of our '
                     'request, e.g. SFVOTREQ or COMMONREQ; NULL when an answer carried only a RESPONSE.'),
    'message_code': ('@message-code of the message, e.g. 401 or a gas code like GF2; for an out row the code of '
                     'our request, e.g. 463 or 921; NULL without one.'),
    'message_id': '@id: message identification; for an out row the id of our request.',
    'request_id': 'Reference@id: id of the request the message answers, if any.',
    'file_name': ('Name of the XML file saved under downloads/ote_soap/<YYYY-MM-DD>/; NULL when there is no file '
                  '(queue pickup requests, queue empty answers, failed calls).'),
    'direction': "out = we sent the message, in = OTE sent it.",
    'service': 'Endpoint the HTTP call went to: reportService or commonService.',
    'reply_to_id': 'For an in row: id of the out row of the same HTTP call. NULL for out rows. No foreign key.',
    'fetch_log_id': ('id of the finance.fetch_log run that caused the message; NULL for the manual tool '
                     'ote_soap.fetch. No foreign key.'),
    'http_status': 'HTTP status of the answer.',
    'return_code': 'OTE RETURN_CODE of the answer; 0 = accepted.',
    'reason_type': ('RESPONSE/Reason@type: A01 syntax error, A02 rejection by application, A03 acceptance without '
                    'reservations, A04 acceptance with reservations.'),
    'result_code': 'RESPONSE/Reason@result-code, e.g. S09000, K00004.',
    'level': ('ok, warning or error, set by the writer. error = no answer from OTE, HTTP error, SOAP Fault, '
              'non-XML answer, RETURN_CODE missing or not 0, Reason type A01/A02, or a report request that got '
              'no answer within the timeout; warning = the 424 limit warning, reason 9001 no data, or a queue '
              'message nobody asked for; ok = everything else.'),
    'outcome': ('delivered, no_data, timeout or failed: only on an out report request that a runner waited for; '
                'NULL otherwise (also NULL when the process died while waiting).'),
    'error': 'Error text when the call failed: exception text, SOAP Fault code and string, or the start of an HTTP error body.',
}


def _apply_comments(table: str, comments: dict) -> None:
    for column, text in comments.items():
        target = f"TABLE {table}" if column is None else f"COLUMN {table}.{column}"
        op.execute(f"COMMENT ON {target} IS '{text.replace(chr(39), chr(39) * 2)}';")


def upgrade() -> None:
    op.execute(f"""
        ALTER TABLE {TABLE}
            ADD COLUMN direction VARCHAR(3) NOT NULL DEFAULT 'in'
                CONSTRAINT {TABLE}_direction_check CHECK (direction IN ('in', 'out')),
            ADD COLUMN service VARCHAR(20),
            ADD COLUMN reply_to_id BIGINT,
            ADD COLUMN fetch_log_id BIGINT,
            ADD COLUMN http_status INTEGER,
            ADD COLUMN return_code VARCHAR(10),
            ADD COLUMN reason_type VARCHAR(3),
            ADD COLUMN result_code VARCHAR(10),
            ADD COLUMN level VARCHAR(7) NOT NULL DEFAULT 'ok'
                CONSTRAINT {TABLE}_level_check CHECK (level IN ('ok', 'warning', 'error')),
            ADD COLUMN outcome VARCHAR(10)
                CONSTRAINT {TABLE}_outcome_check CHECK (outcome IN ('delivered', 'no_data', 'timeout', 'failed')),
            ADD COLUMN error TEXT,
            ALTER COLUMN file_name DROP NOT NULL;
    """)
    op.execute(f"UPDATE {TABLE} SET level = 'warning' WHERE request_id IS NULL;")
    op.execute(f"CREATE INDEX ix_{TABLE}_message_id ON {TABLE} (message_id);")
    op.execute(f"CREATE INDEX ix_{TABLE}_request_id ON {TABLE} (request_id);")
    op.execute(f"CREATE INDEX ix_{TABLE}_fetch_log_id ON {TABLE} (fetch_log_id);")
    op.execute(
        f"CREATE INDEX ix_{TABLE}_problem ON {TABLE} (received_at DESC) WHERE level <> 'ok';"
    )
    _apply_comments(TABLE, COMMENTS)


def downgrade() -> None:
    for index in NEW_INDEXES:
        op.execute(f"DROP INDEX IF EXISTS {index};")
    # The check constraints are dropped together with their columns.
    op.execute(
        f"ALTER TABLE {TABLE} "
        + ", ".join(f"DROP COLUMN IF EXISTS {column}" for column in NEW_COLUMNS)
        + ";"
    )
    # file_name stays nullable: rows without a file (queue pickups, queue empty answers)
    # may exist by now, so restoring NOT NULL could fail. The level data update needs no
    # revert because the level column is dropped.
