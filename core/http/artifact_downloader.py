"""
Artifact download pipeline for MCS synchronizer.

Wires together SiteAuthorizationService, SiteArtifactCacheService,
and SecurityModelServiceDataTunnel/SecurityObjectStore to download
model, kernel and package artifacts from siteMC.

Adapts EdgeService's synchronous flow to async MCS architecture —
the EdgeService classes are called in a thread pool executor to avoid
blocking the asyncio event loop during network I/O.
"""
import asyncio
import logging
from pathlib import Path
from uuid import uuid1

from core.models.artifact_models import (
    ArtifactItem,
    ModelSyncModel,
    KernelModel,
    PackageModel,
)
from core.config.settings import get_settings

logger = logging.getLogger(__name__)


def _get_random_bytes(n: int) -> bytes:
    """Generate n cryptographically random bytes."""
    from Crypto.Random import get_random_bytes
    return get_random_bytes(n)


async def download_model(
    function_id: str,
    product_id: str,
    model_id: str,
    model_version: str,
    account: str,
    password: str,
    dest: Path,
) -> None:
    """
    Download and decrypt a model artifact from siteArtifactCacheService.

    Flow:
    1. Get one-time access token from siteAuthorizationService
    2. Generate RSA key pair + 32-byte AES session key
    3. POST to artifact-cache-service with RSA public key
    4. Decrypt RSA+AES-CBC tunnel → plaintext model bytes
    5. Write to dest (caller handles atomic rename)
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        _download_model_sync,
        function_id, product_id, model_id, model_version,
        account, password, dest,
    )


def _download_model_sync(
    function_id: str,
    product_id: str,
    model_id: str,
    model_version: str,
    account: str,
    password: str,
    dest: Path,
) -> None:
    from core.http.site_authorization import SiteAuthorizationService
    from core.http.site_artifact_service import SiteArtifactCacheService
    from core.security import SecurityModelServiceDataTunnel

    dummy_uid = str(uuid1())

    # 1. Get one-time access token
    auth = SiteAuthorizationService()
    auth_item = ArtifactItem(
        product_id=product_id,
        function_id=function_id,
        ARTIFACT_TYPE="model",
        dummy_uid=dummy_uid,
    )
    access_token = auth.get_one_time_access_token(auth_item, action="DOWNLOAD")
    logger.info(f"Got access token for model: function_id={function_id} model_id={model_id}")

    # 2. Generate RSA key pair + 32-byte random AES session key
    tunnel = SecurityModelServiceDataTunnel(aes_key=_get_random_bytes(32))
    private_key, public_key = tunnel.generate_rsa_key()

    # 3. Build download item
    item = ModelSyncModel(
        model_id=model_id,
        function_id=function_id,
        product_id=product_id,
        model_version=model_version,
        access_token=access_token,
        dummy_uid=dummy_uid,
        account=account,
    )

    # 4. Download via RSA+AES-CBC tunnel
    svc = SiteArtifactCacheService()
    response = svc.get_model_from_artifact_service(item, pub_key=public_key)
    response.raise_for_status()

    # 5. Decrypt tunnel → plaintext bytes
    plaintext = tunnel.decrypt_rsa_aes_tunnel(response, private_key)
    logger.info(f"Model decrypted: function_id={function_id} model_id={model_id} size={len(plaintext)}")

    # 6. Write to dest
    dest.write_bytes(plaintext)
    logger.info(f"Model written: {dest}")


async def download_kernel(
    function_id: str,
    product_id: str,
    kernel_id: str,
    kernel_version: str,
    account: str,
    password: str,
    dest: Path,
) -> None:
    """Download and decrypt a kernel artifact."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        _download_kernel_sync,
        function_id, product_id, kernel_id, kernel_version,
        account, password, dest,
    )


def _download_kernel_sync(
    function_id: str,
    product_id: str,
    kernel_id: str,
    kernel_version: str,
    account: str,
    password: str,
    dest: Path,
) -> None:
    from core.http.site_authorization import SiteAuthorizationService
    from core.http.site_artifact_service import SiteArtifactCacheService
    from core.security import SecurityObjectStore

    dummy_uid = str(uuid1())

    # 1. Get one-time access token + artifact key
    auth = SiteAuthorizationService()
    auth_item = ArtifactItem(
        product_id=product_id,
        function_id=function_id,
        ARTIFACT_TYPE="kernel",
        dummy_uid=dummy_uid,
    )
    access_token = auth.get_one_time_access_token(auth_item, action="DOWNLOAD")
    artifact_key = auth.get_artifact_key(function_id, "kernel", access_token, action="DOWNLOAD")
    logger.info(f"Got access token + artifact key for kernel: function_id={function_id}")

    # 2. Build download item
    item = KernelModel(
        product_id=product_id,
        function_id=function_id,
        kernel_id=kernel_id,
        kernel_version=kernel_version,
        access_token=access_token,
        dummy_uid=dummy_uid,
    )

    # 3. Download
    svc = SiteArtifactCacheService()
    response = svc.get_decrypt_kernel_from_artifact_service(item)
    response.raise_for_status()

    # 4. Decrypt
    security = SecurityObjectStore(function_id, "kernel", access_token)
    plaintext = security.decrypt_object(response.content, artifact_key)
    logger.info(f"Kernel decrypted: function_id={function_id} kernel_id={kernel_id} size={len(plaintext)}")

    # 5. Write to dest
    dest.write_bytes(plaintext)
    logger.info(f"Kernel written: {dest}")


async def download_package(
    function_id: str,
    product_id: str,
    package_version: str,
    account: str,
    password: str,
    dest: Path,
) -> None:
    """Download and decrypt a package artifact."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        _download_package_sync,
        function_id, product_id, package_version,
        account, password, dest,
    )


def _download_package_sync(
    function_id: str,
    product_id: str,
    package_version: str,
    account: str,
    password: str,
    dest: Path,
) -> None:
    from core.http.site_authorization import SiteAuthorizationService
    from core.http.site_artifact_service import SiteArtifactCacheService
    from core.security import SecurityObjectStore

    dummy_uid = str(uuid1())

    # 1. Get one-time access token + artifact key
    auth = SiteAuthorizationService()
    auth_item = ArtifactItem(
        product_id=product_id,
        function_id=function_id,
        ARTIFACT_TYPE="package",
        dummy_uid=dummy_uid,
    )
    access_token = auth.get_one_time_access_token(auth_item, action="DOWNLOAD")
    artifact_key = auth.get_artifact_key(function_id, "package", access_token, action="DOWNLOAD")
    logger.info(f"Got access token + artifact key for package: function_id={function_id}")

    # 2. Build download item
    item = PackageModel(
        product_id=product_id,
        function_id=function_id,
        package_version=package_version,
        access_token=access_token,
        dummy_uid=dummy_uid,
    )

    # 3. Download
    svc = SiteArtifactCacheService()
    response = svc.get_package_from_artifact_service(item)
    response.raise_for_status()

    # 4. Decrypt
    security = SecurityObjectStore(function_id, "package", access_token)
    plaintext = security.decrypt_object(response.content, artifact_key)
    logger.info(f"Package decrypted: function_id={function_id} size={len(plaintext)}")

    # 5. Write to dest
    dest.write_bytes(plaintext)
    logger.info(f"Package written: {dest}")
