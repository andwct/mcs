import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from core.nats.client import connect as nats_connect, close as nats_close, verify_stream
from core.redis.client import connect as redis_connect, close as redis_close
from core.k8s.pod import get_pod_name, get_statefulset_name
from core.k8s.configmap import load_product_configs, get_all_function_subjects
from core.config.settings import get_settings
from apps.synchronizer.consumers import (
    ensure_artifact_consumer,
    ensure_metadata_consumer,
)
from apps.synchronizer.fetch_loop import start_fetch_loops, cancel_fetch_loops

logger = logging.getLogger(__name__)

_ready: bool = False


def is_ready() -> bool:
    return _ready


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    logger.info("Synchronizer starting up")

    settings = get_settings()

    # 1. Load ConfigMap
    products = load_product_configs()
    func_subjects = get_all_function_subjects(products)
    logger.info(
        f"Loaded {len(products)} products, "
        f"{len(func_subjects)} subjects: "
        f"{[s for (_, _, _, s) in func_subjects]}"
    )

    # 2. Connect NATS
    _, js = await nats_connect()

    # 3. Verify streams — RuntimeError → pod restarts
    await verify_stream(js, settings.NATS_ARTIFACT_STREAM)
    await verify_stream(js, settings.NATS_METADATA_STREAM)

    # 4. Connect Redis Sentinel
    await redis_connect()

    # 5. Resolve pod name + StatefulSet name
    pod_name = get_pod_name()
    statefulset_name = get_statefulset_name()
    logger.info(f"Running as pod: {pod_name} (StatefulSet: {statefulset_name})")

    # 6. Per func_id: create artifact pull consumer (broadcast — own consumer per pod)
    for product_id, func_id, sanitized_name, subject in func_subjects:
        await ensure_artifact_consumer(js, pod_name, func_id, subject)

    # 7. Per func_id: create metadata pull consumer (queue-group — shared across pods)
    for product_id, func_id, sanitized_name, subject in func_subjects:
        await ensure_metadata_consumer(js, statefulset_name, func_id, subject)

    # 8. Start fetch loops for BOTH streams — one artifact + one metadata task per func_id
    #    Unified pull pattern: backpressure-aware, easy reconnect, consistent codebase
    await start_fetch_loops(js, pod_name, statefulset_name, func_subjects)

    _ready = True
    logger.info("Synchronizer ready")

    yield

    # Shutdown
    logger.info("Synchronizer shutting down")
    _ready = False
    await cancel_fetch_loops()
    await nats_close()
    await redis_close()
