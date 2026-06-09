import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from core.nats.client import connect as nats_connect, close as nats_close, verify_stream
from core.redis.client import connect as redis_connect, close as redis_close
from core.k8s.pod import get_pod_name
from core.k8s.configmap import load_product_configs, get_all_function_subjects
from core.config.settings import get_settings
from apps.synchronizer.consumers import (
    ensure_artifact_consumer,
    ensure_metadata_consumer,
    artifact_deliver_subject,
)
from apps.synchronizer.handlers import handle_artifact_message
from apps.synchronizer.fetch_loop import start_fetch_loops, cancel_fetch_loops

logger = logging.getLogger(__name__)
settings = get_settings()

_ready: bool = False


def is_ready() -> bool:
    return _ready


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    logger.info("Synchronizer starting up")

    # 1. Load ConfigMap — raises FileNotFoundError → pod restarts
    products = load_product_configs()
    # Now returns (product_id, func_id, sanitized_name, subject)
    # sanitized_name sourced directly from FUNC_NAME_MAPPING, never from subject string
    func_subjects = get_all_function_subjects(products)
    logger.info(f"Loaded {len(products)} products, {len(func_subjects)} function subjects")

    # 2. Connect NATS
    nc, js = await nats_connect()

    # 3. Verify streams — RuntimeError → pod restarts
    await verify_stream(js, settings.NATS_ARTIFACT_STREAM)
    await verify_stream(js, settings.NATS_METADATA_STREAM)

    # 4. Connect Redis Sentinel — RuntimeError on ping fail → pod restarts
    await redis_connect()

    # 5. Resolve pod name from HOSTNAME env var (K8s downward API)
    pod_name = get_pod_name()
    logger.info(f"Running as pod: {pod_name}")

    # 6. Per func_id: create consumers + subscribe to artifact deliver subject
    for product_id, func_id, sanitized_name, subject in func_subjects:

        # Artifact push consumer — unique per (pod, func_id), broadcast fan-out
        await ensure_artifact_consumer(js, pod_name, func_id, subject)
        deliver_subj = artifact_deliver_subject(pod_name, func_id)
        await nc.subscribe(deliver_subj, cb=handle_artifact_message)
        logger.info(f"Subscribed to artifact deliver: {deliver_subj}")

        # Metadata pull consumer — shared durable, queue group semantics via fetch()
        await ensure_metadata_consumer(js, func_id, sanitized_name, subject)

    # 7. Start one fetch loop asyncio.Task per func_id for metadata pull consumer
    await start_fetch_loops(js, func_subjects)

    _ready = True
    logger.info("Synchronizer ready")

    yield

    # Shutdown
    logger.info("Synchronizer shutting down")
    _ready = False
    await cancel_fetch_loops()
    await nats_close()
    await redis_close()
