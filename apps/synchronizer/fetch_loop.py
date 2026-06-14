import asyncio
import logging
from nats.js.client import JetStreamContext
from apps.synchronizer.handlers import handle_metadata_message
from apps.synchronizer.consumers import metadata_consumer_name
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Single fetch loop task for this deployment's shared metadata pull consumer.
# Cleared on shutdown to prevent accumulation across restarts.
_fetch_task: asyncio.Task | None = None


async def start_fetch_loop(js: JetStreamContext, statefulset_name: str) -> None:
    """
    Launch the single asyncio.Task that long-polls this deployment's shared
    metadata pull consumer (filter_subjects covers all configured func_ids).
    """
    global _fetch_task
    consumer_name = metadata_consumer_name(statefulset_name)
    _fetch_task = asyncio.create_task(
        _fetch_loop(js, consumer_name),
        name=f"fetch-loop-{consumer_name}",
    )
    logger.info(f"Fetch loop started: consumer={consumer_name}")


async def _fetch_loop(js: JetStreamContext, consumer_name: str) -> None:
    """
    Continuously fetch from the shared metadata pull consumer.

    Uses server-side long-polling: consumer.fetch(batch=1, timeout=5.0) blocks
    up to 5 seconds server-side waiting for a message before returning empty.
    The coroutine simply awaits — no busy spin, event loop stays free for
    artifact push callbacks and other tasks.

    All 3 pods of this deployment run this same loop against the same
    durable consumer name — NATS delivers each message to exactly one
    fetching pod (queue-group semantics for pull consumers). On nak() or
    ack wait timeout, NATS redelivers to whichever pod fetches next.
    """
    logger.info(f"Fetch loop running: consumer={consumer_name}")

    # Bind to the pre-created durable pull consumer.
    # pull_subscribe_bind() attaches to an EXISTING durable consumer by name
    # without trying to create a new one — consumer must already exist
    # (created by ensure_metadata_consumer() in lifespan.py step 7).
    psub = await js.pull_subscribe_bind(
        consumer=consumer_name,
        stream=settings.NATS_METADATA_STREAM,
    )

    while True:
        try:
            msgs = await psub.fetch(batch=1, timeout=5.0)
            for msg in msgs:
                try:
                    await handle_metadata_message(msg)
                except Exception as e:
                    logger.error(f"Handler error consumer={consumer_name}: {e}")
                    await msg.nak()
        except asyncio.CancelledError:
            logger.info(f"Fetch loop cancelled: consumer={consumer_name}")
            break
        except Exception as e:
            logger.error(f"Fetch loop error consumer={consumer_name}: {e} — retrying in 2s")
            await asyncio.sleep(2)


async def cancel_fetch_loop() -> None:
    global _fetch_task
    if _fetch_task:
        _fetch_task.cancel()
        await asyncio.gather(_fetch_task, return_exceptions=True)
        _fetch_task = None
    logger.info("Fetch loop cancelled")
