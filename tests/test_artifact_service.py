"""Unit tests for core/artifact_service.py path/write/trigger helpers."""
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest

from core.artifact_service import (
    artifact_dest_path,
    write_atomic,
    trigger_janitor_check,
    _post_janitor_check,
)
from core.models.nats_messages import ArtifactType


# ── artifact_dest_path ────────────────────────────────────────────────────────

def test_dest_path_model_includes_id():
    p = artifact_dest_path(ArtifactType.MODEL, "prod", "func", "uuid1", "v1.0.0")
    assert str(p).endswith("MODEL/prod/func/uuid1/v1.0.0")


def test_dest_path_kernel_includes_id():
    p = artifact_dest_path(ArtifactType.KERNEL, "prod", "func", "uuidK", "v2")
    assert str(p).endswith("KERNEL/prod/func/uuidK/v2")


def test_dest_path_package_omits_id():
    p = artifact_dest_path(ArtifactType.PACKAGE, "prod", "func", None, "v1.0.0")
    assert str(p).endswith("PACKAGE/prod/func/v1.0.0")
    assert "None" not in str(p)


# ── write_atomic ──────────────────────────────────────────────────────────────

def test_write_atomic_creates_parents_and_writes(tmp_path):
    dest = tmp_path / "a" / "b" / "file"
    write_atomic(dest, b"content")
    assert dest.read_bytes() == b"content"


def test_write_atomic_no_tmp_left_on_success(tmp_path):
    dest = tmp_path / "file"
    write_atomic(dest, b"x")
    assert not list(tmp_path.glob("*.tmp"))


def test_write_atomic_cleans_tmp_on_failure(tmp_path):
    dest = tmp_path / "file"
    with patch.object(Path, "rename", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            write_atomic(dest, b"x")
    assert not list(tmp_path.glob("*.tmp"))
    assert not dest.exists()


# ── janitor trigger ───────────────────────────────────────────────────────────

async def test_post_janitor_check_hits_localhost_port():
    client = AsyncMock()
    client.__aenter__.return_value = client
    with patch("httpx.AsyncClient", return_value=client):
        await _post_janitor_check()
    url = client.post.call_args[0][0]
    assert url.startswith("http://localhost:")
    assert url.endswith("/janitor/check")


async def test_post_janitor_check_swallows_errors():
    with patch("httpx.AsyncClient", side_effect=ConnectionError("janitor down")):
        await _post_janitor_check()  # must not raise


async def test_trigger_janitor_check_is_fire_and_forget():
    with patch("core.artifact_service._post_janitor_check", new=AsyncMock()) as post:
        import asyncio
        await trigger_janitor_check()
        await asyncio.sleep(0)  # let the created task run
        post.assert_awaited_once()
