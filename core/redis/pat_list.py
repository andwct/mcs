import json
import logging
from core.redis.client import get_client
from core.config.settings import get_settings

logger = logging.getLogger(__name__)


async def get_pat_list(function_id: str) -> list | None:
    """HGET mcs:pat_list <function_id> → parsed list or None."""
    settings = get_settings()
    client = await get_client()
    raw = await client.hget(settings.REDIS_PAT_LIST_KEY, function_id)
    if raw is None:
        return None
    return json.loads(raw)


async def set_pat_list(function_id: str, content: list) -> None:
    """
    HSET mcs:pat_list <function_id> <json>
    Stores content as-is from siteMC response — model service uses
    same API contract so shape must not change.
    """
    settings = get_settings()
    client = await get_client()
    await client.hset(
        settings.REDIS_PAT_LIST_KEY,
        function_id,
        json.dumps(content),
    )
    logger.info(f"Redis pat_list updated: function_id={function_id}")


async def pat_list_exists(function_id: str) -> bool:
    """Check if pat_list is already populated for this function_id."""
    settings = get_settings()
    client = await get_client()
    return await client.hexists(settings.REDIS_PAT_LIST_KEY, function_id)
