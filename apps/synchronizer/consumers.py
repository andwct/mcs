import logging
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, AckPolicy, DeliverPolicy, ReplayPolicy
from nats.js.errors import APIError
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

JS_ERR_CONSUMER_NAME_EXISTS = 10013


def _is_already_exists_error(e: APIError) -> bool:
    if e.err_code == JS_ERR_CONSUMER_NAME_EXISTS:
        return True
    if e.description and "already" in e.description.lower():
        return True
    return False


def artifact_consumer_name(pod_name: str, func_id: str) -> str:
    """One push consumer per (pod, func_id) — unique across pods and functions."""
    return f"artifact-sync-{pod_name}-{func_id}"


def artifact_deliver_subject(pod_name: str, func_id: str) -> str:
    return f"artifact-sync-{pod_name}-{func_id}.deliver"


def metadata_consumer_name(statefulset_name: str, func_id: str) -> str:
    """One pull consumer per (statefulset, func_id) — shared across 3 pods."""
    return f"metadata-sync-{statefulset_name}-{func_id}"


async def ensure_artifact_consumer(
    js: JetStreamContext,
    pod_name: str,
    func_id: str,
    subject: str,
) -> None:
    """
    Create push consumer for artifact broadcast per (pod, func_id).

    Uses filter_subject (singular) — compatible with NATS 2.9.x.
    Each pod creates its own consumer per func_id with a unique
    deliver_subject — broadcast fan-out across pods, isolated per func_id.
    """
    settings = get_settings()
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
        logger.info(
            f"Artifact push consumer created: {name} "
            f"filter_subject={subject} deliver={deliver_subj}"
        )
    except APIError as e:
        if _is_already_exists_error(e):
            logger.info(
                f"Artifact push consumer already exists: {name} (reusing). "
                f"err_code={e.err_code} description={e.description}"
            )
        else:
            logger.error(
                f"Failed to create artifact push consumer '{name}': "
                f"err_code={e.err_code} description={e.description}"
            )
            raise RuntimeError(
                f"NATS add_consumer failed for '{name}': "
                f"err_code={e.err_code} description={e.description}"
            ) from e
    except Exception as e:
        logger.error(f"Unexpected error creating artifact consumer '{name}': {e}")
        raise RuntimeError(f"NATS add_consumer failed for '{name}': {e}") from e


async def ensure_metadata_consumer(
    js: JetStreamContext,
    statefulset_name: str,
    func_id: str,
    subject: str,
) -> None:
    """
    Create pull consumer for metadata per func_id, shared across all pods.

    Uses filter_subject (singular) — compatible with NATS 2.9.x.
    All 3 pods call add_consumer() with the same name — first creates,
    others reuse. All 3 fetch() from this consumer giving queue-group
    semantics per func_id.
    """
    settings = get_settings()
    name = metadata_consumer_name(statefulset_name, func_id)
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
        logger.info(
            f"Metadata pull consumer created: {name} filter_subject={subject}"
        )
    except APIError as e:
        if _is_already_exists_error(e):
            logger.info(
                f"Metadata pull consumer already exists: {name} (reusing, shared "
                f"across pods). err_code={e.err_code} description={e.description}"
            )
        else:
            logger.error(
                f"Failed to create metadata pull consumer '{name}': "
                f"err_code={e.err_code} description={e.description}"
            )
            raise RuntimeError(
                f"NATS add_consumer failed for '{name}': "
                f"err_code={e.err_code} description={e.description}"
            ) from e
    except Exception as e:
        logger.error(f"Unexpected error creating metadata consumer '{name}': {e}")
        raise RuntimeError(f"NATS add_consumer failed for '{name}': {e}") from e
