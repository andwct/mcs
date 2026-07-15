"""Unit tests for core/logging_config.py — Taiwan-time (UTC+8) log prefix."""
import logging
import time
from datetime import datetime, timezone, timedelta

from core.logging_config import TaiwanTimeFormatter, configure_logging, TAIWAN_TZ


def test_taiwan_tz_is_fixed_utc_plus_8():
    assert TAIWAN_TZ.utcoffset(None) == timedelta(hours=8)


def test_format_time_converts_utc_epoch_to_taiwan_time():
    formatter = TaiwanTimeFormatter()
    record = logging.LogRecord("x", logging.INFO, "", 0, "msg", None, None)
    # 2026-01-01 00:00:00 UTC -> 2026-01-01 08:00:00 Taiwan time
    record.created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()

    formatted = formatter.formatTime(record)

    assert formatted.startswith("2026-01-01 08:00:00")
    assert formatted.endswith("CST")


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
    assert "CST" in line


def test_configure_logging_sets_level():
    configure_logging("WARNING")
    assert logging.getLogger().level == logging.WARNING
    configure_logging("INFO")  # restore for other tests
