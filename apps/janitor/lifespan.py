import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from core.config.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(level=settings.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    logger.info(
        f"Janitor started: "
        f"HIGH_WATERMARK={settings.JANITOR_HIGH_WATERMARK:.0%} "
        f"LOW_WATERMARK={settings.JANITOR_LOW_WATERMARK:.0%} "
        f"STORAGE_PATH={settings.STORAGE_PATH} "
        f"PORT={settings.JANITOR_PORT}"
    )
    yield
    logger.info("Janitor shutdown")
