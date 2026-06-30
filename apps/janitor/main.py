# bootstrap_env_from_one_properties() MUST be called before any import
# that triggers get_settings() — see apps/synchronizer/main.py for full
# rationale.
from core.k8s.bootstrap import bootstrap_env_from_one_properties
bootstrap_env_from_one_properties()

import logging
from fastapi import FastAPI
from core.config.settings import get_settings

settings = get_settings()
logging.basicConfig(level=settings.LOG_LEVEL)

# STUB: janitor (PVC eviction / cache cleanup) not yet implemented.
# This minimal app exists only so the container starts cleanly and
# passes health checks — does not yet perform any eviction logic.
app = FastAPI(title="MCS Janitor")


@app.get("/health")
async def health():
    return {"ready": True, "status": "stub — janitor not yet implemented"}
