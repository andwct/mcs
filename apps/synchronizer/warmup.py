"""
Initial Redis warm-up — fetch all four meta types from siteMC for every
configured function_id and write to Redis before NATS consumers are created.

Deduplication: checks Redis before fetching — if a key already exists
(populated by another pod), skips the fetch. This prevents all 3 pods from
hammering siteMC simultaneously on startup while still ensuring Redis is
fully populated before the first NATS message is processed.
"""
import logging
from core.models.product import ProductConfig
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
    """
    for product in products:
        for func_id in product.FUNCTION_LIST:
            account = product.MODEL_CENTER_ACCOUNT
            password = product.MODEL_CENTER_PASSWORD
            product_id = product.PRODUCT_ID

            await _warm_up_model_list(func_id, product_id, account, password)
            await _warm_up_kernel_list(func_id, product_id, account, password)
            await _warm_up_package_list(func_id, product_id, account, password)
            await _warm_up_pat_list(func_id, product_id, account, password)

    logger.info("Redis warm-up complete")


async def _warm_up_model_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await model_list_exists(function_id):
        logger.info(f"model_list already in Redis for {function_id} — skipping")
        return
    try:
        content = await fetch_model_list(function_id, product_id, account, password)
        # content is expected to be a dict or list of model records
        # normalise to {modelId: record} dict for Redis storage
        model_map = _normalise_model_list(content)
        await set_model_list(function_id, model_map)
        logger.info(
            f"Warm-up model_list: function_id={function_id} "
            f"models={len(model_map)}"
        )
    except Exception as e:
        logger.error(f"Warm-up model_list failed for {function_id}: {e}")


async def _warm_up_kernel_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await kernel_list_exists(function_id):
        logger.info(f"kernel_list already in Redis for {function_id} — skipping")
        return
    try:
        content = await fetch_kernel_list(function_id, product_id, account, password)
        await set_kernel_list(function_id, content)
        logger.info(f"Warm-up kernel_list: function_id={function_id}")
    except Exception as e:
        logger.error(f"Warm-up kernel_list failed for {function_id}: {e}")


async def _warm_up_package_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await package_list_exists(function_id):
        logger.info(f"package_list already in Redis for {function_id} — skipping")
        return
    try:
        content = await fetch_package_list(function_id, product_id, account, password)
        await set_package_list(function_id, content)
        logger.info(f"Warm-up package_list: function_id={function_id}")
    except Exception as e:
        logger.error(f"Warm-up package_list failed for {function_id}: {e}")


async def _warm_up_pat_list(
    function_id: str,
    product_id: str,
    account: str,
    password: str,
) -> None:
    if await pat_list_exists(function_id):
        logger.info(f"pat_list already in Redis for {function_id} — skipping")
        return
    try:
        content = await fetch_pat_list(function_id, product_id, account, password)
        await set_pat_list(function_id, content)
        logger.info(f"Warm-up pat_list: function_id={function_id}")
    except Exception as e:
        logger.error(f"Warm-up pat_list failed for {function_id}: {e}")


def _normalise_model_list(content) -> dict:
    """
    Normalise siteMC model list response content to {modelId: record} dict.
    content may be:
      - dict {modelId: record}  → use as-is
      - list [record, ...]      → key by record["modelId"]
    """
    if isinstance(content, dict):
        return content
    if isinstance(content, list):
        result = {}
        for record in content:
            model_id = str(record.get("modelId", ""))
            if model_id:
                result[model_id] = record
        return result
    raise ValueError(f"Unexpected model_list content type: {type(content)}")
