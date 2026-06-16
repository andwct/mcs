import json
import logging
from core.redis.client import get_client
from core.config.settings import get_settings

logger = logging.getLogger(__name__)


async def get_package_list(function_id: str) -> dict | None:
    """HGET mcs:package_list <function_id> → parsed dict or None."""
    settings = get_settings()
    client = await get_client()
    raw = await client.hget(settings.REDIS_PACKAGE_LIST_KEY, function_id)
    if raw is None:
        return None
    return json.loads(raw)


async def set_package_list(function_id: str, content: dict) -> None:
    """HSET mcs:package_list <function_id> <json>"""
    settings = get_settings()
    client = await get_client()
    await client.hset(
        settings.REDIS_PACKAGE_LIST_KEY,
        function_id,
        json.dumps(content),
    )
    logger.info(f"Redis package_list updated: function_id={function_id}")


async def package_list_exists(function_id: str) -> bool:
    """Check if package_list is already populated for this function_id."""
    settings = get_settings()
    client = await get_client()
    return await client.hexists(settings.REDIS_PACKAGE_LIST_KEY, function_id)
