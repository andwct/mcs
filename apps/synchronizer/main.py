# TEMPORARY (rollback to v1.0.13 values.yaml shape): must run before any
# other import that calls get_settings() at module level, so NATS_URL,
# REDIS_SENTINEL_*, etc. from the mounted one.properties land in os.environ
# before pydantic-settings reads it.
from core.k8s.bootstrap import bootstrap_env_from_one_properties
bootstrap_env_from_one_properties()

import logging
from fastapi import FastAPI
from core.config.settings import get_settings
from apps.synchronizer.lifespan import lifespan
from apps.synchronizer.router import router

settings = get_settings()
logging.basicConfig(level=settings.LOG_LEVEL)

app = FastAPI(title="MCS Synchronizer", lifespan=lifespan)
app.include_router(router)
