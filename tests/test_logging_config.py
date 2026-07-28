"""Unit tests for core/logging_config.py — Taiwan-time (UTC+8) log prefix,
uvicorn logger re-homing, and healthy-probe noise suppression."""
import logging
import time
from datetime import datetime, timezone, timedelta

from core.logging_config import (
    TaiwanTimeFormatter,
    configure_logging,
    TAIWAN_TZ,
    _SuppressHealthyProbeLogs,
)


def _access_record(message: str) -> logging.LogRecord:
    return logging.LogRecord("uvicorn.access", logging.INFO, "", 0, message, None, None)


def test_taiwan_tz_is_fixed_utc_plus_8():
    assert TAIWAN_TZ.utcoffset(None) == timedelta(hours=8)


def test_format_time_converts_utc_epoch_to_taiwan_time():
    formatter = TaiwanTimeFormatter()
    record = logging.LogRecord("x", logging.INFO, "", 0, "msg", None, None)
    # 2026-01-01 00:00:00 UTC -> 2026-01-01 08:00:00 Taiwan time
    record.created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()

    formatted = formatter.formatTime(record)

    assert formatted.startswith("2026-01-01 08:00:00")
    assert formatted.endswith("+08:00")


def test_format_time_respects_custom_datefmt():
    formatter = TaiwanTimeFormatter()
    record = logging.LogRecord("x", logging.INFO, "", 0, "msg", None, None)
    record.created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()

    formatted = formatter.formatTime(record, datefmt="%Y/%m/%d %H:%M")

    assert formatted == "2026/01/01 08:00"


def test_configure_logging_prefixes_output_with_timestamp(capsys):
    configure_logging("INFO")
    logger = logging.getLogger("test_logger_taiwan")
    logger.info("hello world")

    captured = capsys.readouterr()
    line = captured.err or captured.out
    assert "hello world" in line
    assert "[INFO] test_logger_taiwan: hello world" in line
    # asctime prefix present — starts with a 4-digit year
    assert line[:4].isdigit()
    assert "+08:00" in line


def test_configure_logging_sets_level():
    configure_logging("WARNING")
    assert logging.getLogger().level == logging.WARNING
    configure_logging("INFO")  # restore for other tests


# ── uvicorn logger re-homing ──────────────────────────────────────────────────

def test_configure_logging_rehomes_uvicorn_loggers_to_propagate():
    # Simulate uvicorn's own dictConfig having already run (own handler,
    # propagate=False) before configure_logging() executes.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False

    configure_logging("INFO")

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        assert logger.propagate is True
        assert logger.handlers == []  # falls through to root's handler


def test_uvicorn_access_logs_get_taiwan_timestamp_end_to_end(capsys):
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False

    configure_logging("INFO")
    logging.getLogger("uvicorn.access").info(
        '127.0.0.1:52134 - "GET /mcs/model HTTP/1.1" 200'
    )

    line = capsys.readouterr().err or capsys.readouterr().out
    assert "+08:00" in line
    assert "GET /mcs/model" in line


# ── healthy probe log suppression ─────────────────────────────────────────────

def test_suppresses_successful_health_probe():
    f = _SuppressHealthyProbeLogs()
    record = _access_record('127.0.0.1:1 - "GET /health HTTP/1.1" 200')
    assert f.filter(record) is False


def test_suppresses_successful_metrics_scrape():
    f = _SuppressHealthyProbeLogs()
    record = _access_record('10.0.0.5:2 - "GET /metrics HTTP/1.1" 200')
    assert f.filter(record) is False


def test_keeps_failing_health_probe():
    f = _SuppressHealthyProbeLogs()
    record = _access_record('127.0.0.1:1 - "GET /health HTTP/1.1" 503')
    assert f.filter(record) is True


def test_keeps_unrelated_endpoint_logs():
    f = _SuppressHealthyProbeLogs()
    record = _access_record('127.0.0.1:1 - "POST /mcs/model HTTP/1.1" 200')
    assert f.filter(record) is True


def test_keeps_non_access_log_messages():
    f = _SuppressHealthyProbeLogs()
    record = _access_record("Application startup complete.")
    assert f.filter(record) is True


def test_configure_logging_installs_probe_filter_on_uvicorn_access():
    configure_logging("INFO")
    access_logger = logging.getLogger("uvicorn.access")
    assert any(isinstance(f, _SuppressHealthyProbeLogs) for f in access_logger.filters)
