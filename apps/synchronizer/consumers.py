import logging
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, AckPolicy, DeliverPolicy, ReplayPolicy
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def artifact_consumer_name(pod_name: str, func_id: str) -> str:
    return f"artifact-sync-{pod_name}-{func_id}"


def artifact_deliver_subject(pod_name: str, func_id: str) -> str:
    return f"artifact-sync-{pod_name}-{func_id}.deliver"


def metadata_consumer_name(func_id: str, sanitized_name: str) -> str:
    return f"metadata-sync-{func_id}-{sanitized_name}"


async def ensure_artifact_consumer(
    js: JetStreamContext,
    pod_name: str,
    func_id: str,
    subject: str,
) -> None:
    """
    Create push consumer for artifact broadcast.
    Each (pod, func_id) pair gets its own consumer + unique deliver_subject.
    Idempotent — safe to call if consumer already exists.
    """
    name = artifact_consumer_name(pod_name, func_id)
    deliver_subj = artifact_deliver_subject(pod_name, func_id)
    try:
        await js.add_consumer(
            settings.NATS_ARTIFACT_STREAM,
            ConsumerConfig(
                durable_name=name,
                filter_subject=subject,
                deliver_subject=deliver_subj,
                deliver_policy=DeliverPolicy.NEW,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=settings.NATS_ACK_WAIT_ARTIFACT_SECONDS,
            ),
        )
        logger.info(f"Artifact push consumer ready: {name} → {deliver_subj}")
    except Exception as e:
        logger.info(f"Artifact consumer '{name}' already exists or updated: {e}")


async def ensure_metadata_consumer(
    js: JetStreamContext,
    func_id: str,
    sanitized_name: str,
    subject: str,
) -> None:
    """
    Create pull consumer for metadata queue group.
    One consumer per func_id, shared across all pods.
    Idempotent — safe to call if consumer already exists.
    """
    name = metadata_consumer_name(func_id, sanitized_name)
    try:
        await js.add_consumer(
            settings.NATS_METADATA_STREAM,
            ConsumerConfig(
                durable_name=name,
                filter_subject=subject,
                deliver_policy=DeliverPolicy.NEW,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=settings.NATS_ACK_WAIT_METADATA_SECONDS,
                replay_policy=ReplayPolicy.INSTANT,
            ),
        )
        logger.info(f"Metadata pull consumer ready: {name}")
    except Exception as e:
        logger.info(f"Metadata consumer '{name}' already exists or updated: {e}")
