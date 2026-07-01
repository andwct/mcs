import asyncio
import logging

from fastapi import APIRouter

from apps.janitor.eviction import is_over_high_watermark, run_eviction_sweep
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
router = APIRouter()

_eviction_lock = asyncio.Lock()


@router.post("/janitor/check", status_code=202)
async def check_and_evict() -> dict:
    asyncio.create_task(_do_eviction())
    return {"status": "accepted"}


async def _do_eviction() -> None:
    if _eviction_lock.locked():
        return  # sweep already in progress — it will drive to LOW_WATERMARK
    async with _eviction_lock:
        settings = get_settings()
        if not is_over_high_watermark(settings.STORAGE_PATH, settings.JANITOR_HIGH_WATERMARK):
            return
        logger.info(
            f"High watermark ({settings.JANITOR_HIGH_WATERMARK:.0%}) exceeded "
            f"— starting eviction sweep"
        )
        await asyncio.to_thread(
            run_eviction_sweep,
            settings.STORAGE_PATH,
            settings.FAB_NAME,
            settings.JANITOR_HIGH_WATERMARK,
            settings.JANITOR_LOW_WATERMARK,
        )


@router.get("/health")
async def health() -> dict:
    return {"ready": True, "evicting": _eviction_lock.locked()}
