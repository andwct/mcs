"""
Unit tests for apps/janitor/eviction.py

Uses tmp directories to simulate PVC layout — no real disk required.
shutil.disk_usage() is mocked to control watermark behavior.
"""
import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from apps.janitor.eviction import (
    is_over_high_watermark,
    run_eviction_sweep,
    _collect_candidates,
    _prune_empty_parents,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _write_file(path: Path, content: bytes = b"x" * 1024) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _disk_usage(used: int, total: int) -> MagicMock:
    m = MagicMock()
    m.used = used
    m.total = total
    m.free = total - used
    return m


# ── is_over_high_watermark ────────────────────────────────────────────────────

def test_is_over_high_watermark_true():
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(95, 100)):
        assert is_over_high_watermark("/mnt/mcs", 0.90) is True


def test_is_over_high_watermark_false():
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(80, 100)):
        assert is_over_high_watermark("/mnt/mcs", 0.90) is False


def test_is_over_high_watermark_exactly_at_boundary():
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(90, 100)):
        # 90/100 == 0.90, not strictly greater — should not trigger
        assert is_over_high_watermark("/mnt/mcs", 0.90) is False


# ── _collect_candidates ────────────────────────────────────────────────────────

def test_collect_candidates_skips_tmp(tmp_path):
    fab = tmp_path / "mcs"
    _write_file(fab / "MODEL" / "prod1" / "func1" / "uuid1" / "v1.0.0")
    _write_file(fab / "MODEL" / "prod1" / "func1" / "uuid1" / "v1.0.0.tmp")
    _write_file(fab / "KERNEL" / "prod1" / "func1" / "uuid2" / "v1.0.0")

    candidates = _collect_candidates(tmp_path, "mcs")
    paths = [str(p) for p, _ in candidates]
    assert not any(p.endswith(".tmp") for p in paths)
    assert len(candidates) == 2


def test_collect_candidates_all_artifact_types(tmp_path):
    fab = tmp_path / "mcs"
    _write_file(fab / "MODEL" / "p" / "f" / "m1" / "v1")
    _write_file(fab / "KERNEL" / "p" / "f" / "k1" / "v1")
    _write_file(fab / "PACKAGE" / "p" / "f" / "v1")

    candidates = _collect_candidates(tmp_path, "mcs")
    assert len(candidates) == 3


def test_collect_candidates_empty_storage(tmp_path):
    candidates = _collect_candidates(tmp_path, "mcs")
    assert candidates == []


# ── _prune_empty_parents ──────────────────────────────────────────────────────

def test_prune_empty_parents_removes_empty_dirs(tmp_path):
    file_path = tmp_path / "mcs" / "MODEL" / "prod" / "func" / "uuid" / "v1"
    file_path.parent.mkdir(parents=True)
    file_path.touch()
    file_path.unlink()

    _prune_empty_parents(file_path, tmp_path)
    assert not (tmp_path / "mcs" / "MODEL" / "prod" / "func" / "uuid").exists()


def test_prune_empty_parents_stops_at_non_empty(tmp_path):
    base = tmp_path / "mcs" / "MODEL" / "prod" / "func"
    (base / "uuid1" / "v1").mkdir(parents=True)
    sibling = base / "uuid2" / "v1"
    sibling.mkdir(parents=True)
    sibling.touch()

    _prune_empty_parents(base / "uuid1" / "v1" / "file", tmp_path)
    # uuid1 pruned, uuid2 still present → func dir should survive
    assert (base / "uuid2").exists()


# ── run_eviction_sweep ────────────────────────────────────────────────────────

def test_eviction_deletes_least_recently_accessed_first(tmp_path):
    fab = tmp_path / "mcs"
    old_file = fab / "MODEL" / "p" / "f" / "uuid1" / "v1"
    new_file = fab / "MODEL" / "p" / "f" / "uuid2" / "v1"
    _write_file(old_file, b"x" * 1024)
    _write_file(new_file, b"x" * 1024)

    # Make old_file's atime older (mtime irrelevant to eviction order)
    old_time = time.time() - 3600
    os.utime(old_file, (old_time, old_file.stat().st_mtime))

    # Start at 95% used, low watermark at 75% — need to free ~20% (200 bytes of 1000)
    # Each file is 1024 bytes; freeing 1 file should be enough
    total = 1000
    used = 950
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(used, total)):
        run_eviction_sweep(str(tmp_path), "mcs", high_watermark=0.90, low_watermark=0.75)

    assert not old_file.exists()
    assert new_file.exists()


def test_eviction_skips_tmp_files(tmp_path):
    fab = tmp_path / "mcs"
    tmp_file = fab / "MODEL" / "p" / "f" / "uuid1" / "v1.tmp"
    _write_file(tmp_file, b"x" * 2048)

    total = 100
    used = 95
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(used, total)):
        run_eviction_sweep(str(tmp_path), "mcs", high_watermark=0.90, low_watermark=0.75)

    assert tmp_file.exists()


def test_eviction_logs_warning_when_cannot_reach_low_watermark(tmp_path, caplog):
    # No files on PVC — cannot free anything
    total = 100
    used = 95
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(used, total)):
        import logging
        with caplog.at_level(logging.WARNING, logger="apps.janitor.eviction"):
            run_eviction_sweep(str(tmp_path), "mcs", high_watermark=0.90, low_watermark=0.75)

    assert any("LOW_WATERMARK" in r.message for r in caplog.records)


def test_eviction_stops_at_low_watermark(tmp_path):
    fab = tmp_path / "mcs"
    files = []
    for i in range(5):
        f = fab / "MODEL" / "p" / "f" / f"uuid{i}" / "v1"
        _write_file(f, b"x" * 100)
        t = time.time() - (5 - i) * 100  # older atime first
        os.utime(f, (t, f.stat().st_mtime))
        files.append(f)

    # 95% used, low=75%, total=1000 → need to free 200 bytes → 2 files of 100 bytes each
    total = 1000
    used = 950
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(used, total)):
        run_eviction_sweep(str(tmp_path), "mcs", high_watermark=0.90, low_watermark=0.75)

    deleted = [f for f in files if not f.exists()]
    remaining = [f for f in files if f.exists()]
    assert len(deleted) == 2  # only freed enough to reach 75%
    assert len(remaining) == 3


# ── LRU touch (os.utime integration) ─────────────────────────────────────────

def test_utime_touch_updates_atime_only(tmp_path):
    f = tmp_path / "artifact"
    f.write_bytes(b"data")

    old_time = time.time() - 3600
    os.utime(f, (old_time, old_time))
    assert f.stat().st_atime == pytest.approx(old_time, abs=1)

    # Simulates mcs-serving cache-hit: bump atime, preserve mtime
    os.utime(f, (time.time(), f.stat().st_mtime))
    assert f.stat().st_atime == pytest.approx(time.time(), abs=2)
    assert f.stat().st_mtime == pytest.approx(old_time, abs=1)  # mtime untouched
