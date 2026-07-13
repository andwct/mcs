import json
import logging
from core.redis.client import get_client
from core.config.settings import get_settings

logger = logging.getLogger(__name__)


async def get_pat_list(function_id: str) -> dict | None:
    """
    HGET mcs:pat_list <function_id> → parsed envelope dict or None.
    Value is the FULL siteMC envelope {"status_code","message","content"},
    not just the content list — see core/http/meta_client.py::fetch_pat_list.
    """
    settings = get_settings()
    client = await get_client()
    raw = await client.hget(settings.REDIS_PAT_LIST_KEY, function_id)
    if raw is None:
        return None
    return json.loads(raw)


async def set_pat_list(function_id: str, envelope: dict) -> None:
    """
    HSET mcs:pat_list <function_id> <json>
    Stores the full siteMC envelope as-is (status_code, message, content) —
    apps/mcs/router.py::get_active_pats re-serves it verbatim to Model
    Service, matching EdgeService's exact response contract.
    """
    settings = get_settings()
    client = await get_client()
    await client.hset(
        settings.REDIS_PAT_LIST_KEY,
        function_id,
        json.dumps(envelope),
    )
    logger.info(f"Redis pat_list updated: function_id={function_id}")


async def pat_list_exists(function_id: str) -> bool:
    """Check if pat_list is already populated for this function_id."""
    settings = get_settings()
    client = await get_client()
    return await client.hexists(settings.REDIS_PAT_LIST_KEY, function_id)
