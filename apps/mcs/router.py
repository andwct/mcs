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
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBasicCredentials

from core.config.settings import get_settings
from core.models.api_models import ModelRequestModel, KernelRequestModel, PackageRequestModel
from core.models.nats_messages import ArtifactType
from core.artifact_service import fetch_artifact_bytes, artifact_dest_path
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


async def _tee_and_cache(content: bytes, dest: Path, chunk_size: int):
    """
    Generator: yields chunks of already-downloaded content to the client
    while writing the same content to PVC (atomic tmp -> rename).
    Used on cache miss after content has been fully downloaded+decrypted.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    try:
        with open(tmp, "wb") as f:
            for i in range(0, len(content), chunk_size):
                chunk = content[i : i + chunk_size]
                f.write(chunk)
                yield chunk
        tmp.rename(dest)
        logger.info(f"Artifact cached: {dest}")
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


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

    if dest.exists():
        logger.info(f"Cache hit: {dest}")
        return StreamingResponse(
            _read_file_chunks(dest, settings.DOWNLOAD_CHUNK_SIZE),
            media_type="application/octet-stream",
            headers={"Content-Disposition": "attachment; filename=test.zip"},
        )

    # Cache miss — single-flight lock per artifact path
    lock_key = f"{artifact_type.value}:{artifact_id or function_id}:{version}"
    async with _get_lock(lock_key):
        # Re-check — another request may have completed the download
        # while we were waiting for the lock
        if dest.exists():
            logger.info(f"Cache hit after lock wait: {dest}")
            return StreamingResponse(
                _read_file_chunks(dest, settings.DOWNLOAD_CHUNK_SIZE),
                media_type="application/octet-stream",
                headers={"Content-Disposition": "attachment; filename=test.zip"},
            )

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

        return StreamingResponse(
            _tee_and_cache(content, dest, settings.DOWNLOAD_CHUNK_SIZE),
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
    from core.redis.model_list import get_model_list as redis_get_model_list

    try:
        model_map = await redis_get_model_list(function_id)
        if model_map is None:
            raise FileNotFoundError(f"model_list not found for function_id={function_id}")
        return {
            "online": list(model_map.values()),
            "shadow": [],
            "headers": {},
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
) -> list:
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
