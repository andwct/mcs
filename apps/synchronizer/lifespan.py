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
    artifact_consumer_name,
)
from apps.synchronizer.handlers import handle_artifact_message
from apps.synchronizer.fetch_loop import start_fetch_loop, cancel_fetch_loop

logger = logging.getLogger(__name__)

_ready: bool = False


def is_ready() -> bool:
    return _ready


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    logger.info("Synchronizer starting up")

    settings = get_settings()

    # 1. Load ConfigMap — raises FileNotFoundError → pod restarts
    products = load_product_configs()
    func_subjects = get_all_function_subjects(products)
    subjects = [s for (_, _, _, s) in func_subjects]
    logger.info(f"Loaded {len(products)} products, {len(subjects)} subjects: {subjects}")

    # 2. Connect NATS
    _, js = await nats_connect()

    # 3. Verify streams — RuntimeError → pod restarts
    await verify_stream(js, settings.NATS_ARTIFACT_STREAM)
    await verify_stream(js, settings.NATS_METADATA_STREAM)

    # 4. Connect Redis Sentinel — RuntimeError on ping fail → pod restarts
    await redis_connect()

    # 5. Resolve pod name + StatefulSet name from HOSTNAME env var
    pod_name = get_pod_name()
    statefulset_name = get_statefulset_name()
    logger.info(f"Running as pod: {pod_name} (StatefulSet: {statefulset_name})")

    # 6. Artifact push consumer — ONE per pod, broadcast fan-out
    await ensure_artifact_consumer(js, pod_name, subjects)
    consumer_name = artifact_consumer_name(pod_name)
    consumer_info = await js.consumer_info(
        settings.NATS_ARTIFACT_STREAM,
        consumer_name,
    )
    await js.subscribe_bind(
        stream=settings.NATS_ARTIFACT_STREAM,
        config=consumer_info.config,
        consumer=consumer_name,
        cb=handle_artifact_message,
        manual_ack=True,
    )
    logger.info(f"Subscribed to artifact push consumer: {consumer_name}")

    # 7. Metadata pull consumer — ONE shared across all pods, queue-group via fetch()
    await ensure_metadata_consumer(js, statefulset_name, subjects)

    # 8. Start metadata fetch loop
    await start_fetch_loop(js, statefulset_name)

    _ready = True
    logger.info("Synchronizer ready")

    yield

    # Shutdown
    logger.info("Synchronizer shutting down")
    _ready = False
    await cancel_fetch_loop()
    await nats_close()
    await redis_close()
