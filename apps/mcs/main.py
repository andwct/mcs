# bootstrap_env_from_one_properties() MUST be called before any import
# that triggers get_settings() — see apps/synchronizer/main.py for full
# rationale (pydantic-settings @lru_cache reads os.environ on first call).
from core.k8s.bootstrap import bootstrap_env_from_one_properties
bootstrap_env_from_one_properties()

from fastapi import FastAPI, Request
from core.config.settings import get_settings
from core.logging_config import configure_logging
from core.metrics_endpoint import mount_metrics
from core.metrics import MCS_REQUESTS_TOTAL
from apps.mcs.lifespan import lifespan
from apps.mcs.router import router

settings = get_settings()
configure_logging(settings.LOG_LEVEL)

app = FastAPI(title="MCS Serving", lifespan=lifespan)
app.include_router(router)
mount_metrics(app)


@app.middleware("http")
async def _record_request_metrics(request: Request, call_next):
    response = await call_next(request)
    # request.url.path (not the raw path) collapses path params via the
    # matched route template where available, falling back to raw path.
    route = request.scope.get("route")
    endpoint = route.path if route is not None else request.url.path
    MCS_REQUESTS_TOTAL.labels(endpoint=endpoint, status=response.status_code).inc()
    return response


@app.get("/health")
async def health():
    from apps.mcs.lifespan import is_ready
    return {"ready": is_ready()}
