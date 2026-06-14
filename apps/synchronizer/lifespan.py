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
    # (product_id, func_id, sanitized_name, subject) per configured function.
    # sanitized_name sourced directly from FUNCTION_NAME_MAPPING, never from
    # splitting the subject string.
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

    # 5. Resolve pod name + StatefulSet name from HOSTNAME env var (K8s downward API)
    pod_name = get_pod_name()
    statefulset_name = get_statefulset_name()
    logger.info(f"Running as pod: {pod_name} (StatefulSet: {statefulset_name})")

    # 6. Artifact push consumer — ONE per pod, listening across ALL configured
    #    subjects (filter_subjects). Broadcast fan-out: every pod creates its
    #    own consumer + deliver_subject.
    await ensure_artifact_consumer(js, pod_name, subjects)

    # Bind to the push consumer using subscribe_bind() — this correctly
    # attaches to the JetStream consumer so msg.ack()/msg.nak() work.
    # manual_ack=True because handle_artifact_message calls msg.ack()/msg.nak()
    # itself — without this, subscribe_bind wraps cb with auto-ack which would
    # double-ack and prevent manual nak on failure.
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

    # 7. Metadata pull consumer — ONE shared across all 3 pods of this
    #    deployment, listening across ALL configured subjects
    #    (filter_subjects). Queue-group semantics via fetch().
    await ensure_metadata_consumer(js, statefulset_name, subjects)

    # 8. Start the metadata fetch loop (single asyncio.Task for this deployment's
    #    shared consumer)
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
