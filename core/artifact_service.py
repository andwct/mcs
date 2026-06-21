"""
Shared artifact download + decrypt logic — used by both:
- apps/synchronizer/handlers.py (pre-warm PVC on NATS artifact message)
- apps/mcs/router.py (on-demand fallback during serving, cache miss)

Single source of truth for the 3-step siteMC download pipeline:
  1. SiteAuthorizationService.get_one_time_access_token()
  2. SiteArtifactCacheService.get_{model,kernel,package}_from_artifact_service()
  3. Decrypt (RSA+AES-CBC tunnel for model, AES for kernel/package)
"""
import logging
from pathlib import Path
from uuid import uuid1

from core.config.settings import get_settings
from core.models.nats_messages import ArtifactType

logger = logging.getLogger(__name__)


def artifact_dest_path(
    artifact_type: ArtifactType,
    product_id: str,
    func_id: str,
    artifact_id: str,
    version: str,
) -> Path:
    """
    PVC path: {STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{function_ID}/{id}/{version}
    See documents/synchronizer-artifact.md issue #33 for design rationale.
    """
    settings = get_settings()
    return (
        Path(settings.STORAGE_PATH)
        / settings.FAB_NAME
        / artifact_type.value
        / product_id
        / func_id
        / artifact_id
        / version
    )


async def fetch_artifact_bytes(
    func_id: str,
    product_id: str,
    artifact_type: ArtifactType,
    version: str,
    account: str,
    password: str,
    model_id: str | None = None,
    kernel_id: str | None = None,
    package_id: str | None = None,
) -> bytes:
    """
    Download and decrypt an artifact from siteMC. Returns plaintext bytes.
    Does NOT write to disk — caller decides (synchronizer writes directly,
    mcs-serving tee-streams to client while writing).

    Raises RuntimeError on any failure in the pipeline.
    """
    from core.http.site_authorization import SiteAuthorizationService
    from core.http.site_artifact_service import SiteArtifactCacheService
    from core.utils.security import SecurityModelServiceDataTunnel
    from core.models.artifact_models import ArtifactItem, ModelSyncModel, KernelModel, PackageModel
    from Cryptodome.Random import get_random_bytes

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
            raise RuntimeError("model_id required for artifact_type=MODEL")

        # Step 2: Generate RSA key pair + AES session key
        tunnel = SecurityModelServiceDataTunnel(aes_key=get_random_bytes(32))
        private_key, public_key = tunnel.generate_rsa_key()

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
            raise RuntimeError(
                f"Model download failed: status={getattr(response, 'status_code', None)}"
            )

        # Step 3: Decrypt RSA+AES-CBC tunnel
        plaintext = tunnel.decrypt_rsa_aes_tunnel(response, private_key)
        logger.info(
            f"Model decrypted: func_id={func_id} model_id={model_id} size={len(plaintext)}"
        )
        return plaintext

    elif artifact_type == ArtifactType.KERNEL:
        if not kernel_id:
            raise RuntimeError("kernel_id required for artifact_type=KERNEL")

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
            raise RuntimeError(
                f"Kernel download failed: status={getattr(response, 'status_code', None)}"
            )
        logger.info(f"Kernel downloaded: func_id={func_id} kernel_id={kernel_id}")
        return response.content

    elif artifact_type == ArtifactType.PACKAGE:
        if not package_id:
            raise RuntimeError("package_id required for artifact_type=PACKAGE")

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
            raise RuntimeError(
                f"Package download failed: status={getattr(response, 'status_code', None)}"
            )
        logger.info(f"Package downloaded: func_id={func_id} package_id={package_id}")
        return response.content

    else:
        raise ValueError(f"Unknown artifact_type={artifact_type}")


def write_atomic(dest: Path, content: bytes) -> None:
    """Write content to dest using tmp file + atomic rename."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    try:
        tmp.write_bytes(content)
        tmp.rename(dest)
        logger.info(f"Artifact stored: {dest}")
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
