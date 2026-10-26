"""
OTE portal force-fetch API.

Lets trader-web trigger ote_portal_session.py on demand instead of waiting for
the next cron slot (10 2-22/2). Runs inside the entsoe-ote-data-uploader
container (started by entrypoint.sh), because the script needs this
container's Chromium profile, certificates and lock file.

Endpoints (all require the X-API-Token header == OTE_API_TOKEN):
    POST /ote/portal/refresh         {"reports": ["intraday_limit"], "dry_run": false}
        202 started | 409 a run already holds the lock | 429 cooldown
    GET  /ote/portal/refresh/status  {"state": "idle"|"running", "last_run": {...}}

The run is the same command cron uses, under the same flock, so an API run and
a cron run can never overlap: whichever starts second is refused (API: 409,
cron: flock -n skips the slot). Runs are tagged trigger=api in finance.fetch_log.

Usage (inside the container):
    python3 -m uvicorn api.ote_refresh_api:app --app-dir /app/scripts --host 0.0.0.0 --port 8080 --workers 1

Must run with ONE worker: run state lives in this process's memory.
"""

import sentry_init  # noqa: F401 - must be first to capture errors
import fcntl
import hmac
import logging
import math
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from ote_portal_session import ALL_REPORTS

sentry_init.set_module("ote_api")

LOCK_PATH = "/tmp/ote_portal_session.lock"
SCRIPT = "/app/scripts/ote_portal_session.py"
RUN_LOG = "/var/log/ote.log"
LOCK_BUSY_EXIT = 75          # flock -E: lock already held (distinct from a script failure)
MAX_RUN_S = 900              # hard kill: the script's own report timeout is 300 s per report
COOLDOWN_S = int(os.environ.get("OTE_API_COOLDOWN_S", "300"))
API_TOKEN = os.environ.get("OTE_API_TOKEN", "")

if not API_TOKEN:
    raise RuntimeError("OTE_API_TOKEN not configured - refusing to start the OTE refresh API")

logger = logging.getLogger("ote_api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

_state_lock = threading.Lock()
_proc: Optional[subprocess.Popen] = None     # set while an API-started run is alive
_last_run: Optional[dict] = None
_last_api_start: Optional[float] = None      # monotonic, for the cooldown


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _portal_lock_held() -> bool:
    """True if any process (cron or API run) holds the portal-session flock."""
    with open(LOCK_PATH, "a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(f, fcntl.LOCK_UN)
        return False


def require_token(x_api_token: str = Header(default="")):
    if not hmac.compare_digest(x_api_token.encode(), API_TOKEN.encode()):
        raise HTTPException(status_code=401, detail="unauthorized")


class RefreshRequest(BaseModel):
    reports: list[str] = Field(default_factory=lambda: ["intraday_limit"], min_length=1)
    dry_run: bool = False

    @field_validator("reports")
    @classmethod
    def known_reports(cls, v: list[str]) -> list[str]:
        unknown = [r for r in v if r not in ALL_REPORTS]
        if unknown:
            raise ValueError(f"unknown report(s): {', '.join(unknown)}; allowed: {', '.join(ALL_REPORTS)}")
        return list(dict.fromkeys(v))  # dedupe, keep order


app = FastAPI(dependencies=[Depends(require_token)], docs_url=None, redoc_url=None, openapi_url=None)


def _watch(proc: subprocess.Popen, run: dict, previous: Optional[dict]):
    """Wait for the run, kill it after MAX_RUN_S, record the outcome."""
    global _proc, _last_run, _last_api_start
    try:
        code = proc.wait(timeout=MAX_RUN_S)
    except subprocess.TimeoutExpired:
        logger.error(f"OTE refresh: run exceeded {MAX_RUN_S}s, killing")
        os.killpg(proc.pid, signal.SIGKILL)
        code = proc.wait()
    with _state_lock:
        _proc = None
        if code == LOCK_BUSY_EXIT:
            # Lost the race to a cron run between the pre-check and flock: not a run.
            logger.info("OTE refresh: lock taken by another run, nothing started")
            _last_run, _last_api_start = previous, None
            return
        run["finished_at"] = _now_iso()
        run["exit_code"] = code
    logger.info(f"OTE refresh: finished, exit code {code}")


@app.post("/ote/portal/refresh")
def refresh(req: Optional[RefreshRequest] = None):
    global _proc, _last_run, _last_api_start
    req = req or RefreshRequest()
    with _state_lock:
        if _proc is not None or _portal_lock_held():
            started_at = _last_run["started_at"] if _proc is not None else None
            return JSONResponse(status_code=409, content={"state": "running", "started_at": started_at})

        if _last_api_start is not None:
            remaining = COOLDOWN_S - (time.monotonic() - _last_api_start)
            if remaining > 0:
                return JSONResponse(status_code=429, content={
                    "retry_after_s": math.ceil(remaining),
                    "last_started_at": _last_run["started_at"] if _last_run else None,
                })

        cmd = ["flock", "-n", "-E", str(LOCK_BUSY_EXIT), LOCK_PATH,
               sys.executable, SCRIPT, "--reports", ",".join(req.reports)]
        if req.dry_run:
            cmd.append("--dry-run")
        with open(RUN_LOG, "a") as log:
            proc = subprocess.Popen(cmd, cwd="/app/scripts", stdout=log, stderr=subprocess.STDOUT,
                                    start_new_session=True, env={**os.environ, "FETCH_TRIGGER": "api"})

        previous = _last_run
        run = {"started_at": _now_iso(), "finished_at": None, "exit_code": None,
               "reports": req.reports, "dry_run": req.dry_run, "source": "api"}
        _proc, _last_run, _last_api_start = proc, run, time.monotonic()
        threading.Thread(target=_watch, args=(proc, run, previous), daemon=True).start()

    logger.info(f"OTE refresh: started pid {proc.pid} ({', '.join(req.reports)}{', dry run' if req.dry_run else ''})")
    return JSONResponse(status_code=202, content={"state": "running", "started_at": run["started_at"],
                                                  "reports": req.reports})


@app.get("/ote/portal/refresh/status")
def status():
    with _state_lock:
        if _proc is not None:
            return {"state": "running", "last_run": dict(_last_run)}
        if _portal_lock_held():
            return {"state": "running", "last_run": {
                "started_at": None, "finished_at": None, "exit_code": None,
                "reports": None, "dry_run": None, "source": "cron",
            }}
        return {"state": "idle", "last_run": dict(_last_run) if _last_run else None}
