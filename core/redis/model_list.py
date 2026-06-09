import json
import logging
from redis.exceptions import RedisError
from core.redis.client import get_client
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def get_model_list(function_id: str) -> dict | None:
    """HGET mcs:model_list <function_id> → parsed dict or None."""
    client = await get_client()
    raw = await client.hget(settings.REDIS_MODEL_LIST_KEY, function_id)
    if raw is None:
        return None
    return json.loads(raw)


async def set_model_list(function_id: str, model_map: dict) -> None:
    """HSET mcs:model_list <function_id> <json>"""
    client = await get_client()
    await client.hset(
        settings.REDIS_MODEL_LIST_KEY,
        function_id,
        json.dumps(model_map),
    )
    logger.info(f"Redis model_list updated: function_id={function_id} count={len(model_map)}")


async def get_all_active_model_ids() -> set[str]:
    """Return all modelIds across all function_ids — used by janitor."""
    client = await get_client()
    all_fields = await client.hgetall(settings.REDIS_MODEL_LIST_KEY)
    model_ids: set[str] = set()
    for value in all_fields.values():
        model_map = json.loads(value)
        model_ids.update(model_map.keys())
    return model_ids
