# bootstrap_env_from_one_properties() MUST be called before any import
# that triggers get_settings() — see apps/synchronizer/main.py for rationale.
from core.k8s.bootstrap import bootstrap_env_from_one_properties
bootstrap_env_from_one_properties()

from fastapi import FastAPI
from apps.janitor.lifespan import lifespan
from apps.janitor.router import router

app = FastAPI(title="MCS Janitor", lifespan=lifespan)
app.include_router(router)
