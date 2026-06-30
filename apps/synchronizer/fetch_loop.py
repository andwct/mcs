import asyncio
import logging
from nats.js.client import JetStreamContext
from nats.errors import TimeoutError as NatsTimeoutError
from apps.synchronizer.handlers import handle_artifact_message, handle_metadata_message
from apps.synchronizer.consumers import artifact_consumer_name, metadata_consumer_name
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

# All fetch loop tasks — cleared on shutdown
_fetch_tasks: list[asyncio.Task] = []


async def start_fetch_loops(
    js: JetStreamContext,
    pod_name: str,
    statefulset_name: str,
    func_subjects: list[tuple[str, str, str, str]],
) -> None:
    """
    Launch fetch loop tasks for both streams per func_id.
    func_subjects: (product_id, func_id, artifact_subject, metadata_subject)
    """
    _fetch_tasks.clear()

    for product_id, func_id, artifact_subject, metadata_subject in func_subjects:
        # Artifact pull loop — per pod, per func_id (broadcast)
        artifact_name = artifact_consumer_name(pod_name, func_id)
        artifact_task = asyncio.create_task(
            _fetch_loop(
                js=js,
                consumer_name=artifact_name,
                stream_key="NATS_ARTIFACT_STREAM",
                handler=handle_artifact_message,
                func_id=func_id,
            ),
            name=f"artifact-fetch-{func_id}",
        )
        _fetch_tasks.append(artifact_task)
        logger.info(f"Artifact fetch loop started: func_id={func_id} consumer={artifact_name}")

        # Metadata pull loop — per statefulset, per func_id (queue-group)
        metadata_name = metadata_consumer_name(statefulset_name, func_id)
        metadata_task = asyncio.create_task(
            _fetch_loop(
                js=js,
                consumer_name=metadata_name,
                stream_key="NATS_METADATA_STREAM",
                handler=handle_metadata_message,
                func_id=func_id,
            ),
            name=f"metadata-fetch-{func_id}",
        )
        _fetch_tasks.append(metadata_task)
        logger.info(f"Metadata fetch loop started: func_id={func_id} consumer={metadata_name}")


async def _fetch_loop(
    js: JetStreamContext,
    consumer_name: str,
    stream_key: str,
    handler,
    func_id: str,
) -> None:
    """
    Continuously fetch from a pull consumer.

    Server-side long-poll (timeout=5.0s) — no busy spin.
    Pod fetches only when ready — natural backpressure.
    On cancel: exits cleanly.
    On error: logs and retries after 2s.
    """
    settings = get_settings()
    stream_name = getattr(settings, stream_key)
    logger.info(f"Fetch loop running: consumer={consumer_name} stream={stream_name}")

    psub = await js.pull_subscribe_bind(
        consumer=consumer_name,
        stream=stream_name,
    )

    while True:
        try:
            msgs = await psub.fetch(batch=1, timeout=5.0)
            for msg in msgs:
                try:
                    await handler(msg)
                except Exception as e:
                    logger.error(
                        f"Handler error consumer={consumer_name} "
                        f"func_id={func_id}: {e}"
                    )
                    await msg.nak()
        except NatsTimeoutError:
            # Normal idle state — no message arrived within the 5s long-poll
            # window. Expected behavior on every quiet consumer, not an
            # error. Silently continue to the next fetch() call.
            continue
        except asyncio.CancelledError:
            logger.info(f"Fetch loop cancelled: consumer={consumer_name}")
            break
        except Exception as e:
            logger.error(
                f"Fetch loop error consumer={consumer_name}: {e} — retrying in 2s"
            )
            await asyncio.sleep(2)


async def cancel_fetch_loops() -> None:
    for task in _fetch_tasks:
        task.cancel()
    await asyncio.gather(*_fetch_tasks, return_exceptions=True)
    _fetch_tasks.clear()
    logger.info("All fetch loops cancelled")
