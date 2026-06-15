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
    artifact_deliver_subject,
)
from apps.synchronizer.handlers import handle_artifact_message
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

    # 3. Verify streams exist — RuntimeError → pod restarts
    await verify_stream(js, settings.NATS_ARTIFACT_STREAM)
    await verify_stream(js, settings.NATS_METADATA_STREAM)

    # 4. Connect Redis Sentinel
    await redis_connect()

    # 5. Resolve pod name + StatefulSet name
    pod_name = get_pod_name()
    statefulset_name = get_statefulset_name()
    logger.info(f"Running as pod: {pod_name} (StatefulSet: {statefulset_name})")

    # 6. Per func_id: create artifact push consumer + subscribe
    #    One consumer per (pod, func_id) — filter_subject (singular),
    #    compatible with NATS 2.9.x
    for product_id, func_id, sanitized_name, subject in func_subjects:
        await ensure_artifact_consumer(js, pod_name, func_id, subject)

        consumer_name = artifact_consumer_name(pod_name, func_id)
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

    # 7. Per func_id: create metadata pull consumer
    #    One consumer per (statefulset, func_id) — shared across 3 pods
    for product_id, func_id, sanitized_name, subject in func_subjects:
        await ensure_metadata_consumer(js, statefulset_name, func_id, subject)

    # 8. Start one fetch loop task per func_id
    await start_fetch_loops(js, statefulset_name, func_subjects)

    _ready = True
    logger.info("Synchronizer ready")

    yield

    # Shutdown
    logger.info("Synchronizer shutting down")
    _ready = False
    await cancel_fetch_loops()
    await nats_close()
    await redis_close()
