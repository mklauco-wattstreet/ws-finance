"""
Sentry SDK initialization.

Import this module early in any entry point to enable error tracking.
Only activates when SENTRY_DSN environment variable is set (production).
"""

import os

SENTRY_DSN = os.environ.get("SENTRY_DSN", "")

if SENTRY_DSN:
    import sentry_sdk
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        send_default_pii=True,
        enable_logs=True,
        traces_sample_rate=1.0,
    )
    sentry_sdk.set_tag("service", "ws-finance")


def set_module(name: str):
    """Set the module tag for Sentry events (no-op if Sentry is not active)."""
    if SENTRY_DSN:
        import sentry_sdk
        sentry_sdk.set_tag("module", name)
