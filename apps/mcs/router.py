"""
mcs-serving API — drop-in replacement for EdgeService's serving endpoints.
Model Service calls these instead of siteMC/EdgeService directly.

Artifact endpoints (model/kernel/package): cache-aware streaming —
PVC hit serves locally, PVC miss falls back to siteMC and populates
PVC for next time (single-flight locked per artifact+version).

Meta endpoints (model_list/kernel_list/package_list/active_pats):
read directly from Redis (populated by synchronizer) — no siteMC fallback.
"""
import asyncio
import logging
import os
import time
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBasicCredentials

from core.config.settings import get_settings
from core.models.api_models import ModelRequestModel, KernelRequestModel, PackageRequestModel
from core.models.nats_messages import ArtifactType
from core.artifact_service import (
    fetch_artifact_bytes,
    artifact_dest_path,
    write_artifact,
    trigger_janitor_check,
)
from core.utils.encryption import (
    ArtifactDecryptionError,
    load_meta,
    meta_path,
    _get_fernet,
)
from cryptography.fernet import InvalidToken
from apps.synchronizer.state import get_product_by_func_id
from apps.mcs.auth import security, verify_credentials_path, verify_credentials_for_function_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mcs")

# Single-flight locks — prevent duplicate siteMC downloads for the same
# artifact+version when concurrent requests arrive during a cache miss.
_download_locks: dict[str, asyncio.Lock] = {}


def _get_lock(key: str) -> asyncio.Lock:
    if key not in _download_locks:
        _download_locks[key] = asyncio.Lock()
    return _download_locks[key]


async def _read_file_chunks(path: Path, chunk_size: int):
    """Generator: read a file from PVC in chunks for StreamingResponse."""
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            yield chunk


def _delete_cache_entry(dest: Path) -> None:
    """Remove artifact and .meta sidecar (either may be absent)."""
    for path in (dest, meta_path(dest)):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


async def _stream_decrypted(dest: Path, segments: list, chunk_size: int):
    """
    Generator: stream a partially-encrypted artifact as plaintext.
    segments: ("file", offset, length) → read plaintext range from disk;
              ("mem", bytes)           → already-decrypted chunk from memory.
    """
    with open(dest, "rb") as f:
        for seg in segments:
            if seg[0] == "mem":
                data = seg[1]
                for i in range(0, len(data), chunk_size):
                    yield data[i : i + chunk_size]
            else:
                _, offset, length = seg
                f.seek(offset)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(chunk_size, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk


def _touch_atime(dest: Path) -> None:
    """Update atime only (mtime preserved) — janitor evicts LRU by atime."""
    try:
        os.utime(dest, (time.time(), dest.stat().st_mtime))
    except OSError:
        pass


def _try_serve_cached(
    dest: Path,
    artifact_type: ArtifactType,
    chunk_size: int,
) -> StreamingResponse | None:
    """
    Serve from PVC if a valid cache entry exists, else return None.
    Orphaned (artifact without .meta) or corrupt (undecryptable) MODEL/KERNEL
    entries are deleted so the caller falls through to the siteMC re-fetch.
    """
    if not dest.exists():
        return None

    headers = {"Content-Disposition": "attachment; filename=test.zip"}

    if artifact_type == ArtifactType.PACKAGE:
        logger.info(f"Cache hit: {dest}")
        _touch_atime(dest)
        return StreamingResponse(
            _read_file_chunks(dest, chunk_size),
            media_type="application/octet-stream",
            headers=headers,
        )

    # MODEL/KERNEL — require .meta sidecar and decryptable content
    if not meta_path(dest).exists():
        logger.warning(f"Orphaned artifact without .meta: {dest} — deleting, cache miss")
        _delete_cache_entry(dest)
        return None

    try:
        meta = load_meta(dest)
        if meta.algorithm != "fernet":
            raise ArtifactDecryptionError(f"Unknown algorithm: {meta.algorithm}")
        if meta.stored_size != dest.stat().st_size:
            raise ArtifactDecryptionError(
                f"Stored size mismatch: meta says {meta.stored_size}, "
                f"file has {dest.stat().st_size} bytes"
            )
        # Decrypt encrypted chunks up front (≤ ~1.4MB each by design);
        # plaintext chunks stream straight from disk.
        fernet = _get_fernet()
        segments = []
        offset = 0
        with open(dest, "rb") as f:
            for chunk in meta.chunks:
                if chunk.encrypted:
                    f.seek(offset)
                    token = f.read(chunk.stored_length)
                    try:
                        plain = fernet.decrypt(token)
                    except InvalidToken as e:
                        raise ArtifactDecryptionError(
                            "Fernet decryption failed (corrupt data or wrong key)"
                        ) from e
                    if len(plain) != chunk.plaintext_length:
                        raise ArtifactDecryptionError(
                            f"Decrypted chunk length {len(plain)} != "
                            f"expected {chunk.plaintext_length}"
                        )
                    segments.append(("mem", plain))
                else:
                    segments.append(("file", offset, chunk.stored_length))
                offset += chunk.stored_length
    except ArtifactDecryptionError as e:
        logger.error(f"Corrupt cache entry: {dest} — deleting, cache miss. {e}")
        _delete_cache_entry(dest)
        return None

    logger.info(f"Cache hit (decrypting {len([s for s in segments if s[0] == 'mem'])} chunk(s)): {dest}")
    _touch_atime(dest)
    return StreamingResponse(
        _stream_decrypted(dest, segments, chunk_size),
        media_type="application/octet-stream",
        headers=headers,
    )


async def _serve_artifact(
    artifact_type: ArtifactType,
    product_id: str,
    function_id: str,
    artifact_id: str,
    version: str,
    **download_kwargs,
) -> StreamingResponse:
    settings = get_settings()
    dest = artifact_dest_path(artifact_type, product_id, function_id, artifact_id, version)

    response = _try_serve_cached(dest, artifact_type, settings.DOWNLOAD_CHUNK_SIZE)
    if response is not None:
        return response

    # Cache miss — single-flight lock per artifact path
    lock_key = f"{artifact_type.value}:{artifact_id or function_id}:{version}"
    async with _get_lock(lock_key):
        # Re-check — another request may have completed the download
        # while we were waiting for the lock
        response = _try_serve_cached(dest, artifact_type, settings.DOWNLOAD_CHUNK_SIZE)
        if response is not None:
            logger.info(f"Cache hit after lock wait: {dest}")
            return response

        logger.info(f"Cache miss: {dest} — falling back to siteMC")
        try:
            product = get_product_by_func_id(function_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="function_id not found")

        try:
            content = await fetch_artifact_bytes(
                func_id=function_id,
                product_id=product_id,
                artifact_type=artifact_type,
                version=version,
                account=product.MODEL_CENTER_ACCOUNT,
                password=product.MODEL_CENTER_PASSWORD,
                **download_kwargs,
            )
        except Exception as e:
            logger.error(f"Artifact fetch failed: {e}")
            raise HTTPException(status_code=502, detail=f"Failed to fetch artifact from siteMC: {e}")

        # Write-through: PVC copy is encrypted (MODEL/KERNEL) or plaintext
        # (PACKAGE); the client always receives plaintext from memory.
        try:
            write_artifact(dest, content, artifact_type)
            await trigger_janitor_check()
        except Exception as e:
            # Serving takes priority over caching — log and stream anyway
            logger.error(f"Write-through cache failed for {dest}: {e}")

        async def _stream_plaintext():
            for i in range(0, len(content), settings.DOWNLOAD_CHUNK_SIZE):
                yield content[i : i + settings.DOWNLOAD_CHUNK_SIZE]

        return StreamingResponse(
            _stream_plaintext(),
            media_type="application/octet-stream",
            headers={"Content-Disposition": "attachment; filename=test.zip"},
        )


# ── Artifact endpoints ──────────────────────────────────────────────────────

@router.post("/model")
async def get_model(
    body: ModelRequestModel,
    credentials: HTTPBasicCredentials = Depends(security),
) -> StreamingResponse:
    verify_credentials_for_function_id(body.function_id, credentials)
    return await _serve_artifact(
        ArtifactType.MODEL,
        body.product_id,
        body.function_id,
        body.model_id,
        body.model_version,
        model_id=body.model_id,
    )


@router.post("/kernel")
async def get_kernel(
    body: KernelRequestModel,
    credentials: HTTPBasicCredentials = Depends(security),
) -> StreamingResponse:
    verify_credentials_for_function_id(body.function_id, credentials)
    return await _serve_artifact(
        ArtifactType.KERNEL,
        body.product_id,
        body.function_id,
        body.kernel_id,
        body.kernel_version,
        kernel_id=body.kernel_id,
    )


@router.post("/package")
async def get_package(
    body: PackageRequestModel,
    credentials: HTTPBasicCredentials = Depends(security),
) -> StreamingResponse:
    verify_credentials_for_function_id(body.function_id, credentials)
    return await _serve_artifact(
        ArtifactType.PACKAGE,
        body.product_id,
        body.function_id,
        None,
        body.package_version,
    )


# ── Meta endpoints (Redis only, no siteMC fallback) ─────────────────────────

@router.get("/model_list/{function_id}")
async def get_model_list(
    function_id: str,
    _: None = Depends(verify_credentials_path),
) -> dict:
    """
    Mirrors EdgeService's exact response shape — Model Service does
    r.json()["content"]["online"], so the response must be wrapped in the
    full siteMC envelope, not just {"online", "shadow", "headers"}.
    """
    from core.redis.model_list import get_model_list as redis_get_model_list

    try:
        model_map = await redis_get_model_list(function_id)
        if model_map is None:
            raise FileNotFoundError(f"model_list not found for function_id={function_id}")
        return {
            "status_code": "0",
            "message": "Get successfully",
            "content": {
                "online": list(model_map.values()),
                "shadow": [],
                "headers": {},
            },
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_model_list: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/kernel_list/{function_id}")
async def get_kernel_list(
    function_id: str,
    _: None = Depends(verify_credentials_path),
) -> dict:
    from core.redis.kernel_list import get_kernel_list as redis_get_kernel_list

    try:
        record = await redis_get_kernel_list(function_id)
        if record is None:
            raise FileNotFoundError(f"kernel_list not found for function_id={function_id}")
        return record
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_kernel_list: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/package_list/{function_id}")
async def get_package_list(
    function_id: str,
    _: None = Depends(verify_credentials_path),
) -> dict:
    from core.redis.package_list import get_package_list as redis_get_package_list

    try:
        record = await redis_get_package_list(function_id)
        if record is None:
            raise FileNotFoundError(f"package_list not found for function_id={function_id}")
        return record
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_package_list: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/active_pats/{function_id}")
async def get_active_pats(
    function_id: str,
    _: None = Depends(verify_credentials_path),
) -> dict:
    """
    Mirrors EdgeService's exact response shape — Model Service expects the
    full siteMC envelope, not a bare JSON array:
    {"status_code": "...", "message": "Get successfully", "content": [...]}
    Redis stores this full envelope verbatim (see core/redis/pat_list.py /
    core/http/meta_client.py::fetch_pat_list) — served through unchanged.
    """
    from core.redis.pat_list import get_pat_list as redis_get_pat_list

    try:
        record = await redis_get_pat_list(function_id)
        if record is None:
            raise FileNotFoundError(f"pat_list not found for function_id={function_id}")
        return record
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_active_pats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
