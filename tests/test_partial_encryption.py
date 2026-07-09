"""
Unit tests for core/utils/encryption.py and the .meta-aware behavior in
core/artifact_service.py and apps/janitor/eviction.py.

The Fernet key is injected by monkeypatching _get_fernet's lru_cache —
no Vault mount required.
"""
import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from cryptography.fernet import Fernet

import core.utils.encryption as enc
from core.utils.encryption import (
    ArtifactDecryptionError,
    encrypt_partial,
    decrypt_partial,
    generate_fernet_key,
    load_meta,
    meta_path,
    LARGE_FILE_THRESHOLD,
    SMALL_HEADER_SIZE,
    LARGE_HEADER_SIZE,
    MB,
)
from core.models.encryption_models import ArtifactMeta


@pytest.fixture(autouse=True)
def fixed_key(monkeypatch):
    """Bypass Vault key loading with a deterministic test key."""
    fernet = Fernet(generate_fernet_key("test-raw-key"))
    monkeypatch.setattr(enc, "_get_fernet", lambda: fernet)
    yield fernet


# ── generate_fernet_key ───────────────────────────────────────────────────────

def test_generate_fernet_key_is_valid_and_deterministic():
    key1 = generate_fernet_key("some raw vault string")
    key2 = generate_fernet_key("some raw vault string")
    assert key1 == key2
    Fernet(key1)  # must not raise — valid 32-byte urlsafe-b64 key


def test_generate_fernet_key_differs_by_input():
    assert generate_fernet_key("a") != generate_fernet_key("b")


# ── encrypt/decrypt round-trip ────────────────────────────────────────────────

@pytest.mark.parametrize("size,expected_chunks", [
    (3 * MB, 3),                      # > 2MB → head/middle/tail
    (2 * MB, 2),                      # exactly 2MB → small scheme (strict >)
    (1000, 2),                        # small file
    (64, 2),                          # exactly header size → empty encrypted chunk
    (10, 2),                          # tiny file
    (0, 2),                           # empty file
])
def test_round_trip(size, expected_chunks):
    plaintext = os.urandom(size)
    stored, meta = encrypt_partial(plaintext)
    assert len(meta.chunks) == expected_chunks
    assert meta.plaintext_size == size
    assert meta.stored_size == len(stored)
    assert decrypt_partial(stored, meta) == plaintext


def test_large_file_layout():
    plaintext = os.urandom(5 * MB)
    stored, meta = encrypt_partial(plaintext)
    head, middle, tail = meta.chunks
    assert (head.encrypted, middle.encrypted, tail.encrypted) == (False, True, False)
    assert head.plaintext_length == LARGE_HEADER_SIZE
    assert middle.plaintext_length == 1 * MB
    assert middle.stored_length > middle.plaintext_length  # Fernet inflation
    # plaintext head/tail stored verbatim at their offsets
    assert stored[:LARGE_HEADER_SIZE] == plaintext[:LARGE_HEADER_SIZE]
    assert stored[-tail.stored_length:] == plaintext[-tail.plaintext_length:]
    # middle is NOT stored as plaintext
    assert plaintext[LARGE_HEADER_SIZE:2 * MB] not in stored


def test_small_file_layout():
    plaintext = os.urandom(1000)
    stored, meta = encrypt_partial(plaintext)
    head, rest = meta.chunks
    assert (head.encrypted, rest.encrypted) == (False, True)
    assert head.stored_length == SMALL_HEADER_SIZE
    assert stored[:SMALL_HEADER_SIZE] == plaintext[:SMALL_HEADER_SIZE]
    assert plaintext[SMALL_HEADER_SIZE:] not in stored[SMALL_HEADER_SIZE:]


# ── decrypt failure paths ─────────────────────────────────────────────────────

def test_decrypt_wrong_key_raises(fixed_key, monkeypatch):
    plaintext = os.urandom(1000)
    stored, meta = encrypt_partial(plaintext)
    other = Fernet(generate_fernet_key("different-key"))
    monkeypatch.setattr(enc, "_get_fernet", lambda: other)
    with pytest.raises(ArtifactDecryptionError):
        decrypt_partial(stored, meta)


def test_decrypt_corrupted_token_raises():
    plaintext = os.urandom(1000)
    stored, meta = encrypt_partial(plaintext)
    corrupted = stored[:SMALL_HEADER_SIZE] + b"garbage" + stored[SMALL_HEADER_SIZE + 7:]
    with pytest.raises(ArtifactDecryptionError):
        decrypt_partial(corrupted, meta)


def test_decrypt_size_mismatch_raises():
    plaintext = os.urandom(1000)
    stored, meta = encrypt_partial(plaintext)
    with pytest.raises(ArtifactDecryptionError, match="size mismatch"):
        decrypt_partial(stored + b"extra", meta)


def test_decrypt_unknown_algorithm_raises():
    plaintext = os.urandom(1000)
    stored, meta = encrypt_partial(plaintext)
    meta.algorithm = "rot13"
    with pytest.raises(ArtifactDecryptionError, match="algorithm"):
        decrypt_partial(stored, meta)


# ── .meta sidecar I/O ─────────────────────────────────────────────────────────

def test_meta_path():
    dest = Path("/mnt/mcs/mcs/MODEL/p/f/uuid/v1.0.0")
    assert meta_path(dest) == Path("/mnt/mcs/mcs/MODEL/p/f/uuid/v1.0.0.meta")


def test_load_meta_round_trip(tmp_path):
    dest = tmp_path / "v1.0.0"
    stored, meta = encrypt_partial(os.urandom(1000))
    dest.write_bytes(stored)
    meta_path(dest).write_bytes(meta.model_dump_json().encode())
    assert load_meta(dest) == meta


def test_load_meta_unparseable_raises(tmp_path):
    dest = tmp_path / "v1.0.0"
    meta_path(dest).write_bytes(b"not json{")
    with pytest.raises(ArtifactDecryptionError):
        load_meta(dest)


# ── write_artifact / is_cached ────────────────────────────────────────────────

def test_write_artifact_model_writes_meta(tmp_path):
    from core.artifact_service import write_artifact, is_cached
    from core.models.nats_messages import ArtifactType

    dest = tmp_path / "MODEL" / "v1.0.0"
    plaintext = os.urandom(3 * MB)
    write_artifact(dest, plaintext, ArtifactType.MODEL)

    assert dest.exists()
    assert meta_path(dest).exists()
    assert is_cached(dest, ArtifactType.MODEL)
    # stored bytes decrypt back to the original
    assert decrypt_partial(dest.read_bytes(), load_meta(dest)) == plaintext


def test_write_artifact_package_stays_plaintext(tmp_path):
    from core.artifact_service import write_artifact, is_cached
    from core.models.nats_messages import ArtifactType

    dest = tmp_path / "PACKAGE" / "v1.0.0"
    plaintext = os.urandom(3 * MB)
    write_artifact(dest, plaintext, ArtifactType.PACKAGE)

    assert dest.read_bytes() == plaintext
    assert not meta_path(dest).exists()
    assert is_cached(dest, ArtifactType.PACKAGE)


def test_is_cached_model_without_meta_is_miss(tmp_path):
    from core.artifact_service import is_cached
    from core.models.nats_messages import ArtifactType

    dest = tmp_path / "v1.0.0"
    dest.write_bytes(b"orphan")
    assert not is_cached(dest, ArtifactType.MODEL)


# ── janitor .meta handling ────────────────────────────────────────────────────

def _disk_usage(used: int, total: int) -> MagicMock:
    m = MagicMock()
    m.used, m.total, m.free = used, total, total - used
    return m


def test_janitor_excludes_meta_from_candidates(tmp_path):
    from apps.janitor.eviction import _collect_candidates

    fab = tmp_path / "mcs"
    artifact = fab / "MODEL" / "p" / "f" / "uuid" / "v1"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"x" * 100)
    (fab / "MODEL" / "p" / "f" / "uuid" / "v1.meta").write_bytes(b"{}")

    candidates = _collect_candidates(tmp_path, "mcs")
    assert [p for p, _ in candidates] == [artifact]


def test_janitor_deletes_orphaned_meta(tmp_path):
    from apps.janitor.eviction import _collect_candidates

    fab = tmp_path / "mcs"
    orphan = fab / "MODEL" / "p" / "f" / "uuid" / "v1.meta"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"{}")

    _collect_candidates(tmp_path, "mcs")
    assert not orphan.exists()


def test_janitor_evicts_meta_with_artifact(tmp_path):
    from apps.janitor.eviction import run_eviction_sweep

    fab = tmp_path / "mcs"
    artifact = fab / "MODEL" / "p" / "f" / "uuid" / "v1"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"x" * 100)
    sidecar = artifact.parent / "v1.meta"
    sidecar.write_bytes(b"{}")

    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=_disk_usage(95, 100)):
        run_eviction_sweep(str(tmp_path), "mcs", high_watermark=0.90, low_watermark=0.75)

    assert not artifact.exists()
    assert not sidecar.exists()
