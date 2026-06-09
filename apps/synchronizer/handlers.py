import asyncio
import logging
from pathlib import Path
from nats.aio.msg import Msg
from core.models.nats_messages import ArtifactMessage, MetadataMessage, ArtifactType
from core.redis.model_list import set_model_list
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Per-key lock: deduplicates concurrent artifact fetches for same (func_id, version) on this pod
_artifact_locks: dict[str, asyncio.Lock] = {}


async def handle_artifact_message(msg: Msg) -> None:
    try:
        payload = ArtifactMessage.model_validate_json(msg.data)
    except Exception as e:
        logger.error(f"Failed to parse ArtifactMessage: {e} raw={msg.data}")
        await msg.ack()   # ack bad message — don't redeliver unparseable payload
        return

    if payload.artifact_type != ArtifactType.MODEL:
        logger.debug(f"Ignoring artifact_type={payload.artifact_type}")
        await msg.ack()
        return

    func_id = payload.function_id
    version = payload.deployed_version
    lock_key = f"{func_id}:{version}"

    if lock_key not in _artifact_locks:
        _artifact_locks[lock_key] = asyncio.Lock()

    async with _artifact_locks[lock_key]:
        try:
            await _fetch_and_store(func_id, version)
            await msg.ack()
        except Exception as e:
            logger.error(f"Artifact fetch failed func_id={func_id} version={version}: {e}")
            await msg.nak()


async def _fetch_and_store(func_id: str, version: str) -> None:
    from core.http.artifact_client import fetch_model_file

    dest = Path(settings.STORAGE_PATH) / func_id / version / "model.bin"
    if dest.exists():
        logger.info(f"Artifact already cached: {dest} — skipping")
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    try:
        await fetch_model_file(func_id, version, tmp)
        tmp.rename(dest)
        logger.info(f"Artifact stored: {dest}")
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


async def handle_metadata_message(msg: Msg) -> None:
    try:
        payload = MetadataMessage.model_validate_json(msg.data)
    except Exception as e:
        logger.error(f"Failed to parse MetadataMessage: {e} raw={msg.data}")
        await msg.ack()
        return

    if payload.artifact_type != ArtifactType.MODEL_LIST:
        logger.debug(f"Ignoring artifact_type={payload.artifact_type}")
        await msg.ack()
        return

    func_id = payload.function_id
    model_map = {str(r.modelId): r.model_dump() for r in payload.online}

    try:
        await set_model_list(func_id, model_map)
        await msg.ack()
        logger.info(f"model_list updated in Redis: func_id={func_id} models={len(model_map)}")
    except Exception as e:
        logger.error(f"Redis write failed func_id={func_id}: {e}")
        await msg.nak()
