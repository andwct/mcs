"""
Unit tests for apps/mcs/router.py cache-serving internals:
_try_serve_cached (hit / orphan / corrupt), _stream_decrypted,
_delete_cache_entry, and the atime LRU touch.
"""
import asyncio
import os
import time
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet

import core.utils.encryption as enc
from core.utils.encryption import encrypt_partial, generate_fernet_key, meta_path, MB
from core.models.nats_messages import ArtifactType

from apps.mcs.router import (
    _try_serve_cached,
    _delete_cache_entry,
    _stream_decrypted,
    get_active_pats,
)

CHUNK = 65536


@pytest.fixture(autouse=True)
def fixed_key(monkeypatch):
    fernet = Fernet(generate_fernet_key("test-key"))
    monkeypatch.setattr(enc, "_get_fernet", lambda: fernet)
    # router imported _get_fernet by name — patch there too
    monkeypatch.setattr("apps.mcs.router._get_fernet", lambda: fernet)
    yield fernet


def _collect(response) -> bytes:
    async def drain():
        return b"".join([chunk async for chunk in response.body_iterator])
    return asyncio.run(drain())


def _write_encrypted(dest, plaintext: bytes):
    stored, meta = encrypt_partial(plaintext)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(stored)
    meta_path(dest).write_bytes(meta.model_dump_json().encode())


# ── cache miss / absent ───────────────────────────────────────────────────────

def test_absent_file_returns_none(tmp_path):
    assert _try_serve_cached(tmp_path / "nope", ArtifactType.MODEL, CHUNK) is None


# ── PACKAGE — plaintext passthrough ───────────────────────────────────────────

def test_package_hit_streams_plaintext(tmp_path):
    dest = tmp_path / "v1"
    dest.write_bytes(b"package-bytes")
    response = _try_serve_cached(dest, ArtifactType.PACKAGE, CHUNK)
    assert response is not None
    assert _collect(response) == b"package-bytes"


def test_package_needs_no_meta(tmp_path):
    dest = tmp_path / "v1"
    dest.write_bytes(b"x")
    assert _try_serve_cached(dest, ArtifactType.PACKAGE, CHUNK) is not None
    assert dest.exists()


# ── MODEL/KERNEL — decrypt on serve ───────────────────────────────────────────

@pytest.mark.parametrize("size", [3 * MB, 1000, 0])
def test_model_hit_serves_original_plaintext(tmp_path, size):
    plaintext = os.urandom(size)
    dest = tmp_path / "v1"
    _write_encrypted(dest, plaintext)
    response = _try_serve_cached(dest, ArtifactType.MODEL, CHUNK)
    assert response is not None
    assert _collect(response) == plaintext
    assert dest.exists()  # serving must not consume the cache entry


def test_kernel_hit_serves_original_plaintext(tmp_path):
    plaintext = os.urandom(5 * MB)
    dest = tmp_path / "v1"
    _write_encrypted(dest, plaintext)
    response = _try_serve_cached(dest, ArtifactType.KERNEL, CHUNK)
    assert _collect(response) == plaintext


def test_hit_touches_atime_not_mtime(tmp_path):
    dest = tmp_path / "v1"
    _write_encrypted(dest, b"data")
    old = time.time() - 3600
    os.utime(dest, (old, old))

    _try_serve_cached(dest, ArtifactType.MODEL, CHUNK)

    st = dest.stat()
    assert st.st_atime == pytest.approx(time.time(), abs=2)
    assert st.st_mtime == pytest.approx(old, abs=1)  # write time preserved


# ── orphan / corrupt entry cleanup ────────────────────────────────────────────

def test_model_without_meta_is_deleted_and_miss(tmp_path):
    dest = tmp_path / "v1"
    dest.write_bytes(b"orphan-artifact")
    assert _try_serve_cached(dest, ArtifactType.MODEL, CHUNK) is None
    assert not dest.exists()


def test_unparseable_meta_deletes_both_and_miss(tmp_path):
    dest = tmp_path / "v1"
    dest.write_bytes(b"bytes")
    meta_path(dest).write_bytes(b"{not json")
    assert _try_serve_cached(dest, ArtifactType.MODEL, CHUNK) is None
    assert not dest.exists()
    assert not meta_path(dest).exists()


def test_truncated_artifact_deletes_both_and_miss(tmp_path):
    plaintext = os.urandom(1000)
    dest = tmp_path / "v1"
    _write_encrypted(dest, plaintext)
    dest.write_bytes(dest.read_bytes()[:-10])  # truncate → size mismatch
    assert _try_serve_cached(dest, ArtifactType.MODEL, CHUNK) is None
    assert not dest.exists()
    assert not meta_path(dest).exists()


def test_wrong_key_deletes_both_and_miss(tmp_path, monkeypatch):
    plaintext = os.urandom(1000)
    dest = tmp_path / "v1"
    _write_encrypted(dest, plaintext)

    other = Fernet(generate_fernet_key("rotated-key"))
    monkeypatch.setattr("apps.mcs.router._get_fernet", lambda: other)

    assert _try_serve_cached(dest, ArtifactType.MODEL, CHUNK) is None
    assert not dest.exists()
    assert not meta_path(dest).exists()


# ── helpers ───────────────────────────────────────────────────────────────────

def test_delete_cache_entry_removes_both(tmp_path):
    dest = tmp_path / "v1"
    dest.write_bytes(b"x")
    meta_path(dest).write_bytes(b"{}")
    _delete_cache_entry(dest)
    assert not dest.exists() and not meta_path(dest).exists()


def test_delete_cache_entry_tolerates_missing_files(tmp_path):
    _delete_cache_entry(tmp_path / "never-existed")  # must not raise


def test_stream_decrypted_mixed_segments(tmp_path):
    dest = tmp_path / "f"
    dest.write_bytes(b"AAAA" + b"IGNORED" + b"BBBB")
    segments = [
        ("file", 0, 4),
        ("mem", b"decrypted-middle"),
        ("file", 11, 4),
    ]

    async def drain():
        return b"".join([c async for c in _stream_decrypted(dest, segments, 3)])

    out = asyncio.run(drain())
    assert out == b"AAAA" + b"decrypted-middle" + b"BBBB"


# ── get_active_pats — EdgeService envelope contract ──────────────────────────
# get_active_pats() does `from core.redis.pat_list import get_pat_list as
# redis_get_pat_list` inside the function body, so the patch target is the
# original module attribute, not an apps.mcs.router name.

async def test_active_pats_returns_full_envelope_not_bare_list():
    from unittest.mock import AsyncMock

    with patch("core.redis.pat_list.get_pat_list", new=AsyncMock(return_value=["6", "5"])):
        result = await get_active_pats(function_id="func_id", _=None)

    assert isinstance(result, dict)
    assert result == {
        "status_code": "0",
        "message": "Get successfully",
        "content": ["6", "5"],
    }


async def test_active_pats_missing_returns_404():
    from unittest.mock import AsyncMock
    from fastapi import HTTPException

    with patch("core.redis.pat_list.get_pat_list", new=AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as e:
            await get_active_pats(function_id="func_id", _=None)
    assert e.value.status_code == 404
