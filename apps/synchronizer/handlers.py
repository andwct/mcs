import logging
from pathlib import Path
from nats.aio.msg import Msg
from core.models.nats_messages import ArtifactMessage, MetadataMessage, ArtifactType, MetaType
from core.redis.model_list import set_model
from core.redis.kernel_list import set_kernel_list
from core.redis.package_list import set_package_list
from core.redis.pat_list import set_pat_list
from core.http.meta_client import (
    fetch_model_list,
    fetch_kernel_list,
    fetch_package_list,
    fetch_pat_list,
)
from apps.synchronizer.state import get_product_by_func_id, get_product_by_id
from core.config.settings import get_settings

logger = logging.getLogger(__name__)




# ── Artifact handler ────────────────────────────────────────────────────────

async def handle_artifact_message(msg: Msg) -> None:
    try:
        payload = ArtifactMessage.model_validate_json(msg.data)
    except Exception as e:
        logger.error(f"Failed to parse ArtifactMessage: {e} raw={msg.data}")
        await msg.ack()
        return

    func_id = payload.function_id
    product_id = payload.product_id
    artifact_type = payload.artifact_type
    version = payload.deployed_version

    # Credentials from productConfig — product-level identity
    try:
        product = get_product_by_func_id(func_id)
    except KeyError as e:
        logger.error(f"Unknown function_id={func_id}: {e}")
        await msg.ack()
        return

    account = product.MODEL_CENTER_ACCOUNT
    password = product.MODEL_CENTER_PASSWORD

    try:
        await _handle_artifact(
            func_id, product_id, artifact_type, version, account, password,
            model_id=payload.model_id,
            kernel_id=payload.kernel_id,
            package_id=payload.package_id,
        )
        await msg.ack()
    except Exception as e:
        logger.error(
            f"Artifact download failed artifact_type={artifact_type} "
            f"func_id={func_id} version={version}: {e}"
        )
        await msg.nak()


async def _handle_artifact(
    func_id: str,
    product_id: str,
    artifact_type: ArtifactType,
    version: str,
    account: str,
    password: str,
    model_id: str | None = None,
    kernel_id: str | None = None,
    package_id: str | None = None,
) -> None:
    """
    Route to correct downloader based on artifact_type.
    All IDs come directly from the ArtifactMessage — no Redis lookup.
    Artifact stream and metadata stream are fully independent.
    Atomic write pattern (tmp → rename) prevents partial writes.
    """
    from core.http.artifact_downloader import (
        download_model,
        download_kernel,
        download_package,
    )
    settings = get_settings()

    if artifact_type == ArtifactType.MODEL:
        if not model_id:
            raise RuntimeError(
                f"model_id missing in ArtifactMessage for func_id={func_id} "
                f"version={version} — model_id is required for artifact_type=model"
            )
        dest_dir = Path(settings.STORAGE_PATH) / func_id / "model" / model_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "MODEL_FILE.bin"
        if dest.exists():
            logger.info(f"Model already cached: {dest} — skipping")
            return
        tmp = dest.with_suffix(".tmp")
        try:
            await download_model(func_id, product_id, model_id, version, account, password, tmp)
            tmp.rename(dest)
            logger.info(f"Model artifact stored: {dest}")
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    elif artifact_type == ArtifactType.KERNEL:
        if not kernel_id:
            raise RuntimeError(
                f"kernel_id missing in ArtifactMessage for func_id={func_id} "
                f"version={version} — kernel_id is required for artifact_type=kernel"
            )
        dest_dir = Path(settings.STORAGE_PATH) / func_id / "kernel" / kernel_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "MODEL_FILE.bin"
        if dest.exists():
            logger.info(f"Kernel already cached: {dest} — skipping")
            return
        tmp = dest.with_suffix(".tmp")
        try:
            await download_kernel(func_id, product_id, kernel_id, version, account, password, tmp)
            tmp.rename(dest)
            logger.info(f"Kernel artifact stored: {dest}")
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    elif artifact_type == ArtifactType.PACKAGE:
        if not package_id:
            raise RuntimeError(
                f"package_id missing in ArtifactMessage for func_id={func_id} "
                f"version={version} — package_id is required for artifact_type=package"
            )
        dest_dir = Path(settings.STORAGE_PATH) / func_id / "package" / package_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "MODEL_FILE.bin"
        if dest.exists():
            logger.info(f"Package already cached: {dest} — skipping")
            return
        tmp = dest.with_suffix(".tmp")
        try:
            await download_package(func_id, product_id, version, account, password, tmp)
            tmp.rename(dest)
            logger.info(f"Package artifact stored: {dest}")
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    else:
        logger.warning(f"Unknown artifact_type={artifact_type} — ignoring")


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

    # Credentials from productConfig — product-level identity
    try:
        product = get_product_by_id(product_id)
    except KeyError as e:
        logger.error(f"Unknown product_id={product_id}: {e}")
        await msg.ack()
        return

    account = product.MODEL_CENTER_ACCOUNT
    password = product.MODEL_CENTER_PASSWORD

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
    model_id = payload.model_id
    if not model_id:
        logger.error(f"model_id missing in model_list update for func_id={func_id}")
        return

    content = await fetch_model_list(func_id, product_id, account, password)
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
