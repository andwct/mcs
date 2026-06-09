import logging
from fastapi import FastAPI
from core.config.settings import get_settings
from apps.synchronizer.lifespan import lifespan
from apps.synchronizer.router import router

settings = get_settings()
logging.basicConfig(level=settings.LOG_LEVEL)

app = FastAPI(title="MCS Synchronizer", lifespan=lifespan)
app.include_router(router)
