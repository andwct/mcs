import json
import logging
from redis.exceptions import RedisError
from core.redis.client import get_client
from core.config.settings import get_settings

logger = logging.getLogger(__name__)


def _model_list_key(function_id: str) -> str:
    """
    Per-function hash key: mcs:model_list:{function_id}
    Each field = modelId, value = JSON model record.
    Fine-grained — updating one model touches only one hash field.
    """
    settings = get_settings()
    return f"{settings.REDIS_MODEL_LIST_KEY_PREFIX}:{function_id}"


async def get_model_list(function_id: str) -> dict | None:
    """
    HGETALL mcs:model_list:{function_id}
    Returns {modelId: record_dict} or None if not found.
    """
    client = await get_client()
    raw = await client.hgetall(_model_list_key(function_id))
    if not raw:
        return None
    return {model_id: json.loads(record) for model_id, record in raw.items()}


async def set_model(function_id: str, model_id: str, record: dict) -> None:
    """
    HSET mcs:model_list:{function_id} <model_id> <json>
    Updates a single model record — O(1), does not affect other models.
    Used for both initial warm-up (per model) and incremental updates.
    """
    client = await get_client()
    await client.hset(
        _model_list_key(function_id),
        model_id,
        json.dumps(record),
    )
    logger.info(f"Redis model updated: function_id={function_id} model_id={model_id}")


async def set_model_list(function_id: str, model_map: dict) -> None:
    """
    Bulk write: HSET mcs:model_list:{function_id} for all models.
    Used during initial warm-up to populate all models at once.
    model_map: {modelId: record_dict}
    """
    if not model_map:
        return
    client = await get_client()
    mapping = {model_id: json.dumps(record) for model_id, record in model_map.items()}
    await client.hset(_model_list_key(function_id), mapping=mapping)
    logger.info(
        f"Redis model_list bulk updated: function_id={function_id} "
        f"count={len(model_map)}"
    )


async def model_list_exists(function_id: str) -> bool:
    """Check if model_list is already populated for this function_id."""
    client = await get_client()
    return await client.exists(_model_list_key(function_id)) > 0


async def get_all_active_model_ids() -> set[str]:
    """Return all modelIds across all function_ids — used by janitor."""
    settings = get_settings()
    client = await get_client()
    # Scan for all mcs:model_list:* keys
    model_ids: set[str] = set()
    async for key in client.scan_iter(
        f"{settings.REDIS_MODEL_LIST_KEY_PREFIX}:*"
    ):
        fields = await client.hkeys(key)
        model_ids.update(fields)
    return model_ids
