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
    model_id = payload.model_id
    kernel_id = payload.kernel_id
    package_id = payload.package_id

    try:
        product = get_product_by_func_id(func_id)
    except KeyError as e:
        logger.error(f"Unknown function_id={func_id}: {e}")
        await msg.ack()
        return

    try:
        await _download_artifact(
            func_id=func_id,
            product_id=product_id,
            artifact_type=artifact_type,
            version=version,
            model_id=model_id,
            kernel_id=kernel_id,
            package_id=package_id,
            account=product.MODEL_CENTER_ACCOUNT,
            password=product.MODEL_CENTER_PASSWORD,
        )
        await msg.ack()
    except Exception as e:
        logger.error(
            f"Artifact download failed artifact_type={artifact_type} "
            f"func_id={func_id} version={version}: {e}"
        )
        await msg.nak()


async def _download_artifact(
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
    from uuid import uuid1
    from core.http.site_authorization import SiteAuthorizationService
    from core.http.site_artifact_service import SiteArtifactCacheService
    from core.utils.security import SecurityModelServiceDataTunnel
    from core.models.artifact_models import ArtifactItem, ModelSyncModel, KernelModel, PackageModel
    from Cryptodome.Random import get_random_bytes
    settings = get_settings()

    dummy_uid = str(uuid1())
    auth_svc = SiteAuthorizationService()
    artifact_svc = SiteArtifactCacheService()

    # Step 1: Get one-time access token
    auth_item = ArtifactItem(
        product_id=product_id,
        function_id=func_id,
        ARTIFACT_TYPE=artifact_type.value,
        dummy_uid=dummy_uid,
    )
    access_token = auth_svc.get_one_time_access_token(auth_item, action="DOWNLOAD")
    if not access_token:
        raise RuntimeError(f"Failed to get access token for func_id={func_id}")
    logger.info(f"Got access token: func_id={func_id} artifact_type={artifact_type}")

    if artifact_type == ArtifactType.MODEL:
        if not model_id:
            raise RuntimeError(f"model_id required for artifact_type=model")

        # Step 2: Generate RSA key pair
        tunnel = SecurityModelServiceDataTunnel(aes_key=get_random_bytes(32))
        private_key, public_key = tunnel.generate_rsa_key()

        # Step 3: Build item + download
        item = ModelSyncModel(
            model_id=model_id,
            function_id=func_id,
            product_id=product_id,
            model_version=version,
            access_token=access_token,
            dummy_uid=dummy_uid,
            account=account,
        )
        response = await artifact_svc.get_model_from_artifact_service(item, pub_key=public_key)
        if response is None or response.status_code != 200:
            raise RuntimeError(f"Model download failed: status={getattr(response, 'status_code', None)}")

        # Step 4: Decrypt RSA+AES-CBC tunnel
        plaintext = tunnel.decrypt_rsa_aes_tunnel(response, private_key)
        logger.info(f"Model decrypted: func_id={func_id} model_id={model_id} size={len(plaintext)}")

        # Step 5: Write to PVC (atomic)
        dest_dir = Path(settings.STORAGE_PATH) / func_id / "model" / model_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "MODEL_FILE.bin"
        if dest.exists():
            logger.info(f"Model already cached: {dest} — skipping")
            return
        tmp = dest.with_suffix(".tmp")
        try:
            tmp.write_bytes(plaintext)
            tmp.rename(dest)
            logger.info(f"Model stored: {dest}")
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    elif artifact_type == ArtifactType.KERNEL:
        if not kernel_id:
            raise RuntimeError(f"kernel_id required for artifact_type=kernel")

        item = KernelModel(
            product_id=product_id,
            function_id=func_id,
            kernel_id=kernel_id,
            kernel_version=version,
            access_token=access_token,
            dummy_uid=dummy_uid,
        )
        response = await artifact_svc.get_kernel_from_artifact_service(item)
        if response is None or response.status_code != 200:
            raise RuntimeError(f"Kernel download failed: status={getattr(response, 'status_code', None)}")

        dest_dir = Path(settings.STORAGE_PATH) / func_id / "kernel" / kernel_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "MODEL_FILE.bin"
        if dest.exists():
            logger.info(f"Kernel already cached: {dest} — skipping")
            return
        tmp = dest.with_suffix(".tmp")
        try:
            tmp.write_bytes(response.content)
            tmp.rename(dest)
            logger.info(f"Kernel stored: {dest}")
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    elif artifact_type == ArtifactType.PACKAGE:
        if not package_id:
            raise RuntimeError(f"package_id required for artifact_type=package")

        item = PackageModel(
            product_id=product_id,
            function_id=func_id,
            package_id=package_id,
            package_version=version,
            access_token=access_token,
            dummy_uid=dummy_uid,
        )
        response = await artifact_svc.get_package_from_artifact_service(item)
        if response is None or response.status_code != 200:
            raise RuntimeError(f"Package download failed: status={getattr(response, 'status_code', None)}")

        dest_dir = Path(settings.STORAGE_PATH) / func_id / "package" / package_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "MODEL_FILE.bin"
        if dest.exists():
            logger.info(f"Package already cached: {dest} — skipping")
            return
        tmp = dest.with_suffix(".tmp")
        try:
            tmp.write_bytes(response.content)
            tmp.rename(dest)
            logger.info(f"Package stored: {dest}")
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    else:
        logger.warning(f"Unknown artifact_type={artifact_type} — ignoring")


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
