import logging
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, AckPolicy, DeliverPolicy, ReplayPolicy
from nats.js.errors import APIError
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# JetStream API error code for "consumer name already in use"
# (nats-server jsapi_errors.go: JSConsumerNameExistErr).
# This is the one we're confident about. As a fallback (in case the
# server returns a different/older code for the same condition), we also
# match on description text containing "already" — logged either way so
# any unexpected err_code is visible and can be added here explicitly.
JS_ERR_CONSUMER_NAME_EXISTS = 10013


def _is_already_exists_error(e: APIError) -> bool:
    if e.err_code == JS_ERR_CONSUMER_NAME_EXISTS:
        return True
    if e.description and "already" in e.description.lower():
        return True
    return False


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

    Idempotent: if the consumer already exists (same or different config),
    logs and continues — NATS durable consumers persist across pod restarts
    and this is expected on every startup. Any OTHER error (permission
    denied, stream not found, etc.) is a real failure and is re-raised as
    RuntimeError so the pod restarts per the no-silent-failures policy.
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
        logger.info(f"Artifact push consumer created: {name} -> {deliver_subj}")
    except APIError as e:
        if _is_already_exists_error(e):
            logger.info(
                f"Artifact push consumer already exists: {name} (reusing). "
                f"err_code={e.err_code} description={e.description}"
            )
        else:
            logger.error(
                f"Failed to create artifact push consumer '{name}' on "
                f"stream '{settings.NATS_ARTIFACT_STREAM}': "
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
    func_id: str,
    sanitized_name: str,
    subject: str,
) -> None:
    """
    Create pull consumer for metadata queue group.
    One consumer per func_id, shared across all pods — every pod calls
    add_consumer() with the SAME durable name + config on startup; only
    the first pod actually creates it, the other two get
    "consumer already exists" and reuse it. All 3 pods then fetch() from
    this single durable consumer, giving queue-group semantics.

    Any error other than "already exists" is a real failure and is
    re-raised as RuntimeError so the pod restarts.
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
        logger.info(f"Metadata pull consumer created: {name}")
    except APIError as e:
        if _is_already_exists_error(e):
            logger.info(
                f"Metadata pull consumer already exists: {name} (reusing, shared "
                f"across pods). err_code={e.err_code} description={e.description}"
            )
        else:
            logger.error(
                f"Failed to create metadata pull consumer '{name}' on "
                f"stream '{settings.NATS_METADATA_STREAM}': "
                f"err_code={e.err_code} description={e.description}"
            )
            raise RuntimeError(
                f"NATS add_consumer failed for '{name}': "
                f"err_code={e.err_code} description={e.description}"
            ) from e
    except Exception as e:
        logger.error(f"Unexpected error creating metadata consumer '{name}': {e}")
        raise RuntimeError(f"NATS add_consumer failed for '{name}': {e}") from e
