"""
Sentry SDK initialization.

Import this module early in any entry point to enable error tracking.
Only activates when SENTRY_DSN environment variable is set (production).

Quota discipline
----------------
The pipelines are cron-driven: ~14 ENTSO-E runners fire four times an hour
across six countries. Anything that emits one Sentry event per (runner, run,
country) turns a single upstream outage into hundreds of billed errors, which
is exactly how the reserved-error budget was consumed. Two guards keep that
from happening again:

* ``traces_sample_rate`` defaults to 0. Tracing every HTTP call of every cron
  firing is what produced ~5M spans/month. Set SENTRY_TRACES_SAMPLE_RATE to a
  small float temporarily if you need to profile a specific run.
* ``before_send`` drops upstream-transport failures. ENTSO-E returning 599/527
  or timing out is not a code defect and is not actionable in the issue
  stream - use a cron/uptime monitor for that. Genuine bugs (parse errors, DB
  failures, unexpected exceptions) are unaffected.
"""

import os
import re

SENTRY_DSN = os.environ.get("SENTRY_DSN", "")

# Tracing is off by default; every cron firing would otherwise emit a
# transaction plus one span per outbound HTTP request.
try:
    TRACES_SAMPLE_RATE = float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0"))
except ValueError:
    TRACES_SAMPLE_RATE = 0.0

# Upstream is unreachable / erroring / slow. None of these are our bug.
_TRANSPORT_PATTERNS = (
    re.compile(r"\b(?:429|5\d\d)\s+(?:Server|Client)\s+Error", re.IGNORECASE),
    re.compile(r"status code (?:429|5\d\d)", re.IGNORECASE),
    re.compile(r"Max retries exceeded", re.IGNORECASE),
    re.compile(r"(?:Read|Connection) timed out", re.IGNORECASE),
    re.compile(r"\btimed out\b", re.IGNORECASE),
    re.compile(r"Connection (?:aborted|refused|reset)", re.IGNORECASE),
    re.compile(r"(?:Name or service not known|Temporary failure in name resolution)", re.IGNORECASE),
    re.compile(r"RemoteDisconnected|ProtocolError", re.IGNORECASE),
)

# Exception class names that are transport-level by construction. Matched by
# name so this module stays import-light (no requests/urllib3 import here).
_TRANSPORT_EXC_TYPES = {
    "ConnectionError",
    "ConnectTimeout",
    "ReadTimeout",
    "Timeout",
    "ChunkedEncodingError",
    "ProtocolError",
    "MaxRetryError",
    "NewConnectionError",
    "RemoteDisconnected",
}


# A transport pattern alone is not enough to discard an event: psycopg2 says
# "Connection refused" when pgbouncer is down, and that IS actionable (it is
# usually the wrong-compose-file network mistake). Require evidence that the
# failure happened against an outbound HTTP call before dropping anything.
_UPSTREAM_MARKER = re.compile(
    r"https?://|for url:|with url:|HTTPS?ConnectionPool", re.IGNORECASE
)


def _event_texts(event):
    """Yield the human-readable strings an event carries."""
    logentry = event.get("logentry") or {}
    for key in ("formatted", "message"):
        value = logentry.get(key)
        if isinstance(value, str):
            yield value
    message = event.get("message")
    if isinstance(message, str):
        yield message
    for value in (event.get("exception") or {}).get("values") or []:
        text = value.get("value")
        if isinstance(text, str):
            yield text


def _exception_types(event):
    for value in (event.get("exception") or {}).get("values") or []:
        exc_type = value.get("type")
        if isinstance(exc_type, str):
            yield exc_type


def _before_send(event, hint):
    """Drop upstream-transport failures; let everything else through.

    Returning None discards the event without touching breadcrumbs or the
    logs stream, so the failure is still visible when investigating a real
    issue - it just does not consume an error from the reserved budget.
    """
    try:
        texts = list(_event_texts(event))
        if not any(_UPSTREAM_MARKER.search(text) for text in texts):
            return event
        if any(t in _TRANSPORT_EXC_TYPES for t in _exception_types(event)):
            return None
        if any(p.search(text) for text in texts for p in _TRANSPORT_PATTERNS):
            return None
    except Exception:
        # Never let filtering suppress a real event through a bug of its own.
        return event
    return event


if SENTRY_DSN:
    import logging

    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        send_default_pii=True,
        enable_logs=True,
        traces_sample_rate=TRACES_SAMPLE_RATE,
        before_send=_before_send,
        integrations=[
            # Default behaviour, stated explicitly so it is obvious that every
            # logger.error() call site is a billable event: INFO and above
            # become breadcrumbs, ERROR and above become issues. Per-area
            # failures inside a runner loop must therefore log at WARNING -
            # see BaseRunner.record_area_failure.
            LoggingIntegration(
                level=logging.INFO,
                event_level=logging.ERROR,
            ),
        ],
    )
    sentry_sdk.set_tag("service", "ws-finance")


def set_module(name: str):
    """Set the module tag for Sentry events (no-op if Sentry is not active)."""
    if SENTRY_DSN:
        import sentry_sdk
        sentry_sdk.set_tag("module", name)
