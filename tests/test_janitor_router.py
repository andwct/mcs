"""Unit tests for apps/janitor/router.py — gate check, re-entrancy, health."""
import asyncio
from unittest.mock import patch, MagicMock

import pytest

import apps.janitor.router as jrouter


def _disk_usage(used, total):
    m = MagicMock()
    m.used, m.total, m.free = used, total, total - used
    return m


async def test_under_watermark_no_sweep():
    with patch(
        "apps.janitor.router.shutil.disk_usage", return_value=_disk_usage(50, 100)
    ), patch("apps.janitor.router.run_eviction_sweep") as sweep:
        await jrouter._do_eviction()
    sweep.assert_not_called()


async def test_over_watermark_runs_sweep():
    with patch(
        "apps.janitor.router.shutil.disk_usage", return_value=_disk_usage(95, 100)
    ), patch("apps.janitor.router.run_eviction_sweep") as sweep:
        await jrouter._do_eviction()
    sweep.assert_called_once()
    # sweep receives storage path, fab name and both watermarks
    args = sweep.call_args[0]
    assert 0 < args[3] < args[2] <= 1.0  # low < high


async def test_concurrent_trigger_skipped_while_sweep_running():
    release = asyncio.Event()

    def slow_sweep(*a, **kw):
        # runs in a thread via asyncio.to_thread — block until released
        import time
        while not release.is_set():
            time.sleep(0.01)

    with patch(
        "apps.janitor.router.shutil.disk_usage", return_value=_disk_usage(95, 100)
    ), patch("apps.janitor.router.run_eviction_sweep", side_effect=slow_sweep) as sweep:
        first = asyncio.create_task(jrouter._do_eviction())
        await asyncio.sleep(0.05)  # let first acquire the lock and start sweeping
        assert jrouter._eviction_lock.locked()

        await jrouter._do_eviction()  # second trigger while sweep in flight
        release.set()
        await first

    sweep.assert_called_once()  # second trigger was dropped


async def test_check_endpoint_returns_accepted():
    with patch("apps.janitor.router._do_eviction", return_value=None):
        result = await jrouter.check_and_evict()
    assert result == {"status": "accepted"}
    await asyncio.sleep(0)  # drain the created task


async def test_health_reports_eviction_state():
    assert await jrouter.health() == {"ready": True, "evicting": False}
    async with jrouter._eviction_lock:
        assert (await jrouter.health())["evicting"] is True
