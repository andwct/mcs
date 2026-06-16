import logging
import httpx
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

_FIXED_PARAMS = {
    "inline_": "true",
    "source": "cache_service",
}


def _auth(account: str, password: str) -> tuple[str, str]:
    return (account, password)


def _params(function_id: str, product_id: str) -> dict:
    return {
        "functionId": function_id,
        "productId": product_id,
        **_FIXED_PARAMS,
    }


async def _get_content(
    url: str,
    function_id: str,
    product_id: str,
    account: str,
    password: str,
    stream: bool = False,
) -> dict | list:
    """
    Generic GET helper — fetches from siteMC meta cache service,
    extracts and returns response["content"].
    stream=True for streaming responses (kernel_list, package_list).
    """
    settings = get_settings()
    timeout = httpx.Timeout(settings.META_CACHE_REQUEST_TIMEOUT_SECONDS)

    async with httpx.AsyncClient(
        auth=_auth(account, password),
        timeout=timeout,
    ) as client:
        if stream:
            chunks = []
            async with client.stream(
                "GET", url, params=_params(function_id, product_id)
            ) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    chunks.append(chunk)
            import json
            data = json.loads(b"".join(chunks))
        else:
            response = await client.get(
                url, params=_params(function_id, product_id)
            )
            response.raise_for_status()
            data = response.json()

    content = data.get("content")
    if content is None:
        raise ValueError(f"No 'content' field in response from {url}")
    return content


async def fetch_model_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> dict:
    """
    GET /meta-cache/model_list/{function_id}
    Returns content: raw model list payload (dict keyed by modelId or list).
    """
    settings = get_settings()
    url = f"{settings.SITE_META_CACHE_SERVICE_URL}/meta-cache/model_list/{function_id}"
    logger.info(f"Fetching model_list: function_id={function_id}")
    return await _get_content(url, function_id, product_id, account, password)


async def fetch_kernel_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> dict:
    """
    GET /meta-cache/DATA_EXPORT/kernel-version/{function_id}
    Returns content: {"kernelId": "...", "kernelVersion": "..."}
    Response is streaming.
    """
    settings = get_settings()
    url = (
        f"{settings.SITE_META_CACHE_SERVICE_URL}"
        f"/meta-cache/DATA_EXPORT/kernel-version/{function_id}"
    )
    logger.info(f"Fetching kernel_list: function_id={function_id}")
    return await _get_content(url, function_id, product_id, account, password, stream=True)


async def fetch_package_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> dict:
    """
    GET /meta-cache/DATA_EXPORT/package-version/{function_id}
    Returns content: {"packageId": "...", "packageVersion": "..."}
    Response is streaming.
    """
    settings = get_settings()
    url = (
        f"{settings.SITE_META_CACHE_SERVICE_URL}"
        f"/meta-cache/DATA_EXPORT/package-version/{function_id}"
    )
    logger.info(f"Fetching package_list: function_id={function_id}")
    return await _get_content(url, function_id, product_id, account, password, stream=True)


async def fetch_pat_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> list:
    """
    GET /meta-cache/pats/{function_id}
    Returns content: ["1", "2", "3"] (list of PAT strings)
    """
    settings = get_settings()
    url = f"{settings.SITE_META_CACHE_SERVICE_URL}/meta-cache/pats/{function_id}"
    logger.info(f"Fetching pat_list: function_id={function_id}")
    return await _get_content(url, function_id, product_id, account, password)
