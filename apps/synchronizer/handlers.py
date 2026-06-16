import asyncio
import logging
from pathlib import Path
from nats.aio.msg import Msg
from core.models.nats_messages import ArtifactMessage, MetadataMessage, ArtifactType, MetaType
from core.redis.model_list import set_model, model_list_exists
from core.redis.kernel_list import set_kernel_list
from core.redis.package_list import set_package_list
from core.redis.pat_list import set_pat_list
from core.http.meta_client import (
    fetch_model_list,
    fetch_kernel_list,
    fetch_package_list,
    fetch_pat_list,
)
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

_artifact_locks: dict[str, asyncio.Lock] = {}


# ── Artifact handler ────────────────────────────────────────────────────────

async def handle_artifact_message(msg: Msg) -> None:
    try:
        payload = ArtifactMessage.model_validate_json(msg.data)
    except Exception as e:
        logger.error(f"Failed to parse ArtifactMessage: {e} raw={msg.data}")
        await msg.ack()
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
    settings = get_settings()

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


# ── Metadata handler ────────────────────────────────────────────────────────

async def handle_metadata_message(msg: Msg) -> None:
    try:
        payload = MetadataMessage.model_validate_json(msg.data)
    except Exception as e:
        logger.error(f"Failed to parse MetadataMessage: {e} raw={msg.data}")
        await msg.ack()
        return

    func_id = payload.function_id
    product_id = payload.product_id

    # Use global credentials — same across all products
    settings = get_settings()
    account = settings.MODEL_CENTER_ACCOUNT
    password = settings.MODEL_CENTER_PASSWORD

    dispatch = {
        MetaType.MODEL_LIST:   _handle_model_list,
        MetaType.KERNEL_LIST:  _handle_kernel_list,
        MetaType.PACKAGE_LIST: _handle_package_list,
        MetaType.PAT_LIST:     _handle_pat_list,
    }

    handler = dispatch.get(payload.meta_type)
    if handler is None:
        logger.warning(f"Unknown meta_type={payload.meta_type} — ignoring")
        await msg.ack()
        return

    try:
        await handler(func_id, product_id, account, password, payload)
        await msg.ack()
    except Exception as e:
        logger.error(
            f"Metadata handler failed meta_type={payload.meta_type} "
            f"func_id={func_id}: {e}"
        )
        await msg.nak()


async def _handle_model_list(
    func_id: str,
    product_id: str,
    account: str,
    password: str,
    payload: MetadataMessage,
) -> None:
    """
    Fetch full model list for function_id, extract the updated model record
    from the 'online' list (identified by payload.model_id), and update
    just that one field in Redis. Fine-grained O(1) update.
    """
    model_id = payload.model_id
    if not model_id:
        logger.error(f"model_id missing in model_list update for func_id={func_id}")
        return

    content = await fetch_model_list(func_id, product_id, account, password)

    # content = {"online": [...], "shadow": [...], "headers": {...}}
    online = content.get("online", [])
    record = _extract_model_record(online, model_id)
    if record is None:
        logger.warning(
            f"model_id={model_id} not found in online list "
            f"for func_id={func_id} — model may be in shadow or deleted"
        )
        return

    await set_model(func_id, model_id, record)
    logger.info(f"model_list updated: func_id={func_id} model_id={model_id}")


async def _handle_kernel_list(
    func_id: str,
    product_id: str,
    account: str,
    password: str,
    payload: MetadataMessage,
) -> None:
    content = await fetch_kernel_list(func_id, product_id, account, password)
    await set_kernel_list(func_id, content)
    logger.info(f"kernel_list updated: func_id={func_id}")


async def _handle_package_list(
    func_id: str,
    product_id: str,
    account: str,
    password: str,
    payload: MetadataMessage,
) -> None:
    content = await fetch_package_list(func_id, product_id, account, password)
    await set_package_list(func_id, content)
    logger.info(f"package_list updated: func_id={func_id}")


async def _handle_pat_list(
    func_id: str,
    product_id: str,
    account: str,
    password: str,
    payload: MetadataMessage,
) -> None:
    content = await fetch_pat_list(func_id, product_id, account, password)
    await set_pat_list(func_id, content)
    logger.info(f"pat_list updated: func_id={func_id}")


def _extract_model_record(online: list, model_id: str) -> dict | None:
    """Find a model record by modelId from the 'online' list."""
    for record in online:
        if str(record.get("modelId", "")) == model_id:
            return record
    return None
