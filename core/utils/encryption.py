"""
Partial encryption at rest for MODEL/KERNEL artifacts.

Scheme (see documents/partial-encryption.md):
- Files > 2MB:  [0–1MB plaintext][1–2MB Fernet-encrypted][2MB–EOF plaintext]
- Files ≤ 2MB:  [0–64B plaintext][64B–EOF Fernet-encrypted]
- PACKAGE artifacts are never passed through this module.

Chunk thresholds are fixed constants — changing them breaks decryption of
already-cached artifacts. Bump ArtifactMeta.meta_version instead if the
layout ever needs to change.
"""
import base64
import hashlib
import logging
from functools import lru_cache
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from core.config.settings import get_settings
from core.models.encryption_models import ArtifactMeta, ChunkEntry

logger = logging.getLogger(__name__)

MB = 1024 * 1024
LARGE_FILE_THRESHOLD = 2 * MB   # strict >: exactly 2MB uses the small scheme
LARGE_HEADER_SIZE = 1 * MB      # plaintext head for large files
LARGE_ENCRYPTED_SIZE = 1 * MB   # encrypted middle chunk for large files
SMALL_HEADER_SIZE = 64          # plaintext head for small files


class ArtifactDecryptionError(Exception):
    """Cache entry cannot be decrypted — treat as corrupt, delete, re-fetch."""


def generate_fernet_key(raw: str) -> bytes:
    """Derive a valid Fernet key from the raw Vault-provided string."""
    digest = hashlib.sha256(raw.encode()).digest()
    return base64.urlsafe_b64encode(digest)


@lru_cache
def _get_fernet() -> Fernet:
    """
    Load the raw key string from the Vault mount and derive the Fernet key.
    Cached for the process lifetime. Raises RuntimeError if the secret file
    is missing or empty — encryption is mandatory for MODEL/KERNEL.
    """
    settings = get_settings()
    key_file = Path(settings.SECRET_MOUNT_PATH) / settings.ENCRYPTION_KEY_FILE
    if not key_file.exists():
        raise RuntimeError(
            f"Encryption key file not found: {key_file}. Ensure "
            f"'{settings.ENCRYPTION_KEY_FILE}' exists in the Vault path and "
            f"is mounted at {settings.SECRET_MOUNT_PATH}."
        )
    raw = key_file.read_text().strip()
    if not raw:
        raise RuntimeError(f"Encryption key file is empty: {key_file}")
    return Fernet(generate_fernet_key(raw))


def meta_path(dest: Path) -> Path:
    """Sidecar path for an artifact — {version}.meta next to {version}."""
    return dest.parent / (dest.name + ".meta")


def encrypt_partial(plaintext: bytes) -> tuple[bytes, ArtifactMeta]:
    """
    Partially encrypt an artifact for storage. Returns (stored_bytes, meta).
    stored_bytes is the concatenation of all chunks' on-disk bytes.
    """
    fernet = _get_fernet()
    size = len(plaintext)

    if size > LARGE_FILE_THRESHOLD:
        head = plaintext[:LARGE_HEADER_SIZE]
        middle = plaintext[LARGE_HEADER_SIZE:LARGE_HEADER_SIZE + LARGE_ENCRYPTED_SIZE]
        tail = plaintext[LARGE_HEADER_SIZE + LARGE_ENCRYPTED_SIZE:]
        token = fernet.encrypt(middle)
        chunks = [
            ChunkEntry(encrypted=False, stored_length=len(head), plaintext_length=len(head)),
            ChunkEntry(encrypted=True, stored_length=len(token), plaintext_length=len(middle)),
            ChunkEntry(encrypted=False, stored_length=len(tail), plaintext_length=len(tail)),
        ]
        stored = head + token + tail
    else:
        head = plaintext[:SMALL_HEADER_SIZE]
        rest = plaintext[SMALL_HEADER_SIZE:]
        token = fernet.encrypt(rest)  # empty rest still produces a valid token
        chunks = [
            ChunkEntry(encrypted=False, stored_length=len(head), plaintext_length=len(head)),
            ChunkEntry(encrypted=True, stored_length=len(token), plaintext_length=len(rest)),
        ]
        stored = head + token

    meta = ArtifactMeta(plaintext_size=size, chunks=chunks)
    return stored, meta


def decrypt_partial(stored: bytes, meta: ArtifactMeta) -> bytes:
    """
    Reverse encrypt_partial(). Raises ArtifactDecryptionError on any
    inconsistency — callers must treat the cache entry as corrupt.
    """
    if meta.algorithm != "fernet":
        raise ArtifactDecryptionError(f"Unknown algorithm: {meta.algorithm}")
    if meta.stored_size != len(stored):
        raise ArtifactDecryptionError(
            f"Stored size mismatch: meta says {meta.stored_size}, "
            f"file has {len(stored)} bytes"
        )

    fernet = _get_fernet()
    out = []
    offset = 0
    for chunk in meta.chunks:
        piece = stored[offset:offset + chunk.stored_length]
        offset += chunk.stored_length
        if chunk.encrypted:
            try:
                piece = fernet.decrypt(piece)
            except InvalidToken as e:
                raise ArtifactDecryptionError(
                    "Fernet decryption failed (corrupt data or wrong key)"
                ) from e
            if len(piece) != chunk.plaintext_length:
                raise ArtifactDecryptionError(
                    f"Decrypted chunk length {len(piece)} != "
                    f"expected {chunk.plaintext_length}"
                )
        out.append(piece)

    plaintext = b"".join(out)
    if len(plaintext) != meta.plaintext_size:
        raise ArtifactDecryptionError(
            f"Decrypted size {len(plaintext)} != expected {meta.plaintext_size}"
        )
    return plaintext


def load_meta(dest: Path) -> ArtifactMeta:
    """Parse the .meta sidecar. Raises ArtifactDecryptionError if unreadable."""
    try:
        return ArtifactMeta.model_validate_json(meta_path(dest).read_bytes())
    except ArtifactDecryptionError:
        raise
    except Exception as e:
        raise ArtifactDecryptionError(f"Unreadable .meta for {dest}: {e}") from e
