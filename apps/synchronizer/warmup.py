"""
Initial Redis warm-up — fetch all four meta types from siteMC for every
configured function_id and write to Redis before NATS consumers are created.

Deduplication: checks Redis before fetching — if a key already exists
(populated by another pod), skips the fetch. StatefulSet pods start in
sequence (0 → 1 → 2), so pod-0 typically populates Redis first and
pod-1/2 skip cleanly.

Failure policy: any fetch failure raises RuntimeError — pod restarts.
Redis must be fully populated before consumers start processing NATS messages.
"""
import logging
from core.models.product import ProductConfig
from core.config.settings import get_settings
from core.http.meta_client import (
    fetch_model_list,
    fetch_kernel_list,
    fetch_package_list,
    fetch_pat_list,
)
from core.redis.model_list import set_model_list, model_list_exists
from core.redis.kernel_list import set_kernel_list, kernel_list_exists
from core.redis.package_list import set_package_list, package_list_exists
from core.redis.pat_list import set_pat_list, pat_list_exists

logger = logging.getLogger(__name__)


async def warm_up_redis(products: list[ProductConfig]) -> None:
    """
    For each (product, function_id), fetch all four meta types from siteMC
    and write to Redis — only if not already populated.

    Called in lifespan.py BEFORE NATS consumers are created.
    Raises RuntimeError on any failure → pod restarts.
    """
    settings = get_settings()
    account = settings.MODEL_CENTER_ACCOUNT
    password = settings.MODEL_CENTER_PASSWORD

    for product in products:
        for func_id in product.FUNCTION_LIST:
            product_id = product.PRODUCT_ID
            await _warm_up_model_list(func_id, product_id, account, password)
            await _warm_up_kernel_list(func_id, product_id, account, password)
            await _warm_up_package_list(func_id, product_id, account, password)
            await _warm_up_pat_list(func_id, product_id, account, password)

    logger.info("Redis warm-up complete — all meta lists populated")


async def _warm_up_model_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await model_list_exists(function_id):
        logger.info(f"model_list already in Redis for function_id={function_id} — skipping")
        return
    try:
        logger.info(f"Fetching model_list from siteMC: function_id={function_id}")
        content = await fetch_model_list(function_id, product_id, account, password)
        model_map = _extract_online_models(content)
        await set_model_list(function_id, model_map)
        logger.info(
            f"Warm-up model_list OK: function_id={function_id} "
            f"models={len(model_map)}"
        )
    except Exception as e:
        msg = (
            f"WARM-UP FAILED: could not fetch model_list from siteMC "
            f"for function_id={function_id}: {e}"
        )
        logger.error(msg)
        raise RuntimeError(msg) from e


async def _warm_up_kernel_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await kernel_list_exists(function_id):
        logger.info(f"kernel_list already in Redis for function_id={function_id} — skipping")
        return
    try:
        logger.info(f"Fetching kernel_list from siteMC: function_id={function_id}")
        content = await fetch_kernel_list(function_id, product_id, account, password)
        await set_kernel_list(function_id, content)
        logger.info(f"Warm-up kernel_list OK: function_id={function_id}")
    except Exception as e:
        msg = (
            f"WARM-UP FAILED: could not fetch kernel_list from siteMC "
            f"for function_id={function_id}: {e}"
        )
        logger.error(msg)
        raise RuntimeError(msg) from e


async def _warm_up_package_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await package_list_exists(function_id):
        logger.info(f"package_list already in Redis for function_id={function_id} — skipping")
        return
    try:
        logger.info(f"Fetching package_list from siteMC: function_id={function_id}")
        content = await fetch_package_list(function_id, product_id, account, password)
        await set_package_list(function_id, content)
        logger.info(f"Warm-up package_list OK: function_id={function_id}")
    except Exception as e:
        msg = (
            f"WARM-UP FAILED: could not fetch package_list from siteMC "
            f"for function_id={function_id}: {e}"
        )
        logger.error(msg)
        raise RuntimeError(msg) from e


async def _warm_up_pat_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await pat_list_exists(function_id):
        logger.info(f"pat_list already in Redis for function_id={function_id} — skipping")
        return
    try:
        logger.info(f"Fetching pat_list from siteMC: function_id={function_id}")
        content = await fetch_pat_list(function_id, product_id, account, password)
        await set_pat_list(function_id, content)
        logger.info(f"Warm-up pat_list OK: function_id={function_id}")
    except Exception as e:
        msg = (
            f"WARM-UP FAILED: could not fetch pat_list from siteMC "
            f"for function_id={function_id}: {e}"
        )
        logger.error(msg)
        raise RuntimeError(msg) from e


def _extract_online_models(content: dict) -> dict:
    """
    Extract online model records from siteMC model_list response content.

    content shape:
    {
        "online": [{"modelId": "...", "modelName": "...", ...}, ...],
        "shadow": [...],
        "headers": {...}
    }

    Returns {modelId: record} dict from the "online" list only.
    Shadow models are ignored — MCS serves only active (online) models.
    """
    online = content.get("online")
    if online is None:
        raise ValueError(
            f"model_list response missing 'online' field. "
            f"Got keys: {list(content.keys())}"
        )
    if not isinstance(online, list):
        raise ValueError(
            f"model_list 'online' field expected list, got {type(online)}"
        )

    result = {}
    for record in online:
        model_id = str(record.get("modelId", ""))
        if not model_id:
            logger.warning(f"Skipping model record with missing modelId: {record}")
            continue
        result[model_id] = record

    return result
