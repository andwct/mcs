# bootstrap_env_from_one_properties() MUST be called before any import
# that triggers get_settings() — see apps/synchronizer/main.py for full
# rationale (pydantic-settings @lru_cache reads os.environ on first call).
from core.k8s.bootstrap import bootstrap_env_from_one_properties
bootstrap_env_from_one_properties()

from fastapi import FastAPI
from core.config.settings import get_settings
from core.logging_config import configure_logging
from apps.mcs.lifespan import lifespan
from apps.mcs.router import router

settings = get_settings()
configure_logging(settings.LOG_LEVEL)

app = FastAPI(title="MCS Serving", lifespan=lifespan)
app.include_router(router)


@app.get("/health")
async def health():
    from apps.mcs.lifespan import is_ready
    return {"ready": is_ready()}
