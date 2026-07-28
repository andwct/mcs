# bootstrap_env_from_one_properties() MUST be called before any import
# that triggers get_settings() — pydantic-settings reads os.environ once
# at first construction (@lru_cache). By running bootstrap here first,
# NATS_URL, REDIS_SENTINEL_*, etc. from one.properties are in os.environ
# before any module-level code runs get_settings().
from core.k8s.bootstrap import bootstrap_env_from_one_properties
bootstrap_env_from_one_properties()

from fastapi import FastAPI
from core.config.settings import get_settings
from core.logging_config import configure_logging
from core.metrics_endpoint import mount_metrics
from apps.synchronizer.lifespan import lifespan
from apps.synchronizer.router import router

settings = get_settings()
configure_logging(settings.LOG_LEVEL)

app = FastAPI(title="MCS Synchronizer", lifespan=lifespan)
app.include_router(router)
mount_metrics(app)
