"""
Shared logging setup for all 3 containers (mcs, synchronizer, janitor) —
prefixes every log line with a Taiwan-time (UTC+8) timestamp.

Taiwan does not observe DST, so a fixed UTC+8 offset is used rather than
zoneinfo — avoids depending on the tzdata package on the slim base image
(python:3.12.9-slim ships no IANA tz database by default).
"""
import logging
import re
from datetime import datetime, timedelta, timezone

TAIWAN_TZ = timezone(timedelta(hours=8), name="+08:00")

# Matches uvicorn's default access-log message, e.g.:
#   127.0.0.1:52134 - "GET /health HTTP/1.1" 200
_ACCESS_LOG_RE = re.compile(r'"(?:GET|POST) (/health|/metrics)[^"]*"\s+(\d{3})')


class TaiwanTimeFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=TAIWAN_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " +08:00"


# uvicorn installs its own dictConfig (with its own default formatter — no
# timestamp) on "uvicorn", "uvicorn.error", "uvicorn.access" *before*
# importing the ASGI app — which is what triggers this function. Those
# loggers also default to propagate=False, so simply configuring the root
# logger never reaches them; probe/access log lines (GET /health, GET
# /metrics, etc.) would otherwise stay timestamp-less forever.
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


class _SuppressHealthyProbeLogs(logging.Filter):
    """
    Drops uvicorn access-log lines for successful (2xx) GET /health and
    GET /metrics requests — these fire on every readiness/liveness probe
    and scrape interval and add no signal when they succeed. A failing
    probe (non-2xx) still logs normally, since that's exactly when you
    want the line.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        match = _ACCESS_LOG_RE.search(record.getMessage())
        if match is None:
            return True
        status = int(match.group(2))
        return not (200 <= status < 300)


def configure_logging(level: str) -> None:
    """
    Call once per process, before any other logging happens — configures
    the root logger so every module's logger (via logging.getLogger(__name__))
    inherits the Taiwan-time-prefixed format. Also re-homes uvicorn's own
    loggers onto the same handler so access/probe logs get timestamped too,
    and filters out successful /health and /metrics probe noise.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        TaiwanTimeFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]

    for name in _UVICORN_LOGGERS:
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True

    access_logger = logging.getLogger("uvicorn.access")
    access_logger.filters = []  # idempotent — avoid stacking duplicates on repeat calls
    access_logger.addFilter(_SuppressHealthyProbeLogs())
