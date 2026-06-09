import logging
from pathlib import Path
import httpx
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def _auth() -> tuple[str, str]:
    return (settings.MODEL_CENTER_ACCOUNT, settings.MODEL_CENTER_PASSWORD)


async def fetch_model_file(func_id: str, version: str, dest: Path) -> None:
    """
    Stream model file from siteMC ArtifactCacheService into dest (temp file).
    Caller is responsible for atomic rename after success.

    URL: {SITE_ARTIFACT_SERVICE_URL}/v1/models/{func_id}/versions/{version}/file
    Auth: HTTP Basic Auth (MODEL_CENTER_ACCOUNT / MODEL_CENTER_PASSWORD)
    """
    url = (
        f"{settings.SITE_ARTIFACT_SERVICE_URL}"
        f"/v1/models/{func_id}/versions/{version}/file"
    )
    logger.info(f"Fetching artifact: {url}")

    async with httpx.AsyncClient(
        auth=_auth(),
        timeout=settings.NATS_ACK_WAIT_ARTIFACT_SECONDS,
    ) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            with open(dest, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    f.write(chunk)

    logger.info(f"Artifact fetch complete → {dest}")
