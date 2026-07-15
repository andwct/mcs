"""
Shared logging setup for all 3 containers (mcs, synchronizer, janitor) —
prefixes every log line with a Taiwan-time (UTC+8) timestamp.

Taiwan does not observe DST, so a fixed UTC+8 offset is used rather than
zoneinfo — avoids depending on the tzdata package on the slim base image
(python:3.12.9-slim ships no IANA tz database by default).
"""
import logging
from datetime import datetime, timedelta, timezone

TAIWAN_TZ = timezone(timedelta(hours=8), name="+08:00")


class TaiwanTimeFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=TAIWAN_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " +08:00"


def configure_logging(level: str) -> None:
    """
    Call once per process, before any other logging happens — configures
    the root logger so every module's logger (via logging.getLogger(__name__))
    inherits the Taiwan-time-prefixed format.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        TaiwanTimeFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]
