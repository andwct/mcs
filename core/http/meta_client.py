import logging
from uuid import uuid4
import httpx
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

_FIXED_PARAMS = {
    "inline": "true",        # no trailing underscore — matches EdgeService
    "source": "cache_service",
}


def _headers() -> dict:
    """
    Request headers aligned with EdgeService.
    X-dummy-UID: unique request ID per call — required by siteMC API gateway.
    """
    return {"X-dummy-UID": str(uuid4())}


def _params(function_id: str, product_id: str) -> dict:
    return {
        "functionId": function_id,
        "productId": product_id,
        **_FIXED_PARAMS,
    }


async def _get(
    url: str,
    function_id: str,
    product_id: str,
    account: str,
    password: str,
    stream: bool = False,
) -> dict | list:
    """
    Generic GET helper — fetches from siteMC meta cache service.

    Response body IS the content directly (no wrapper envelope).
    Aligned with EdgeService which uses response.content directly.

    stream=True for streaming responses (kernel_list, package_list).
    """
    settings = get_settings()
    timeout = httpx.Timeout(settings.META_CACHE_REQUEST_TIMEOUT_SECONDS)

    async with httpx.AsyncClient(
        auth=(account, password),
        timeout=timeout,
        headers=_headers(),
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
            return json.loads(b"".join(chunks))
        else:
            response = await client.get(
                url, params=_params(function_id, product_id)
            )
            response.raise_for_status()
            return response.json()


async def fetch_model_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> dict:
    """
    GET /meta-cache/model_list/{function_id}

    siteMC wraps the response in an envelope:
    {"status_code": "...", "message": "...", "content": {"online": [...], "shadow": [...], "headers": {...}}}

    Returns content only: {"online": [...], "shadow": [...], "headers": {...}}
    Other endpoints (kernel_list, package_list, pat_list) return raw response
    directly with no envelope — only model_list has this wrapper.
    """
    settings = get_settings()
    url = f"{settings.SITE_META_CACHE_SERVICE_URL}/meta-cache/model_list/{function_id}"
    logger.info(f"Fetching model_list: function_id={function_id}")
    data = await _get(url, function_id, product_id, account, password)
    content = data.get("content")
    if content is None:
        raise ValueError(
            f"model_list response missing 'content' field. Got keys: {list(data.keys())}"
        )
    return content


async def fetch_kernel_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> dict:
    """
    GET /meta-cache/DATA_EXPORT/kernel-version/{function_id}
    Returns raw kernel record directly:
    {"kernelId": "...", "kernelVersion": "..."}
    Response is streaming.
    """
    settings = get_settings()
    url = (
        f"{settings.SITE_META_CACHE_SERVICE_URL}"
        f"/meta-cache/DATA_EXPORT/kernel-version/{function_id}"
    )
    logger.info(f"Fetching kernel_list: function_id={function_id}")
    return await _get(url, function_id, product_id, account, password, stream=True)


async def fetch_package_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> dict:
    """
    GET /meta-cache/DATA_EXPORT/package-version/{function_id}
    Returns raw package record directly:
    {"packageId": "...", "packageVersion": "..."}
    Response is streaming.
    """
    settings = get_settings()
    url = (
        f"{settings.SITE_META_CACHE_SERVICE_URL}"
        f"/meta-cache/DATA_EXPORT/package-version/{function_id}"
    )
    logger.info(f"Fetching package_list: function_id={function_id}")
    return await _get(url, function_id, product_id, account, password, stream=True)


async def fetch_pat_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> list:
    """
    GET /meta-cache/pats/{function_id}

    siteMC wraps this response in the same envelope as model_list:
    {"status_code": "...", "message": "...", "content": ["1", "2", "3"]}
    Returns content only.
    """
    settings = get_settings()
    url = f"{settings.SITE_META_CACHE_SERVICE_URL}/meta-cache/pats/{function_id}"
    logger.info(f"Fetching pat_list: function_id={function_id}")
    data = await _get(url, function_id, product_id, account, password)
    content = data.get("content")
    if content is None:
        raise ValueError(
            f"pat_list response missing 'content' field. Got keys: {list(data.keys())}"
        )
    return content
