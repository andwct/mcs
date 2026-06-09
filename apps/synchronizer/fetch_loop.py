import asyncio
import logging
from nats.js.client import JetStreamContext
from apps.synchronizer.handlers import handle_metadata_message
from apps.synchronizer.consumers import metadata_consumer_name
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Task registry — cleared on shutdown to prevent accumulation across restarts
_fetch_tasks: list[asyncio.Task] = []


async def start_fetch_loops(
    js: JetStreamContext,
    func_subjects: list[tuple[str, str, str, str]],  # (product_id, func_id, sanitized_name, subject)
) -> None:
    """
    Launch one asyncio.Task per func_id to long-poll the metadata pull consumer.
    sanitized_name comes directly from ProductConfig.FUNC_NAME_MAPPING — not derived
    from splitting the subject string.
    """
    # Clear any stale tasks from a previous lifespan (safety guard)
    _fetch_tasks.clear()

    for product_id, func_id, sanitized_name, subject in func_subjects:
        consumer_name = metadata_consumer_name(func_id, sanitized_name)
        task = asyncio.create_task(
            _fetch_loop(js, consumer_name, func_id),
            name=f"fetch-loop-{func_id}",
        )
        _fetch_tasks.append(task)
        logger.info(f"Fetch loop started: func_id={func_id} consumer={consumer_name}")


async def _fetch_loop(
    js: JetStreamContext,
    consumer_name: str,
    func_id: str,
) -> None:
    """
    Continuously fetch from a metadata pull consumer.

    Uses server-side long-polling: consumer.fetch(batch=1, timeout=5.0) blocks
    up to 5 seconds server-side waiting for a message before returning empty.
    The coroutine simply awaits — no busy spin, event loop stays free for
    artifact push callbacks and other tasks.

    On nak() or ack wait timeout, NATS redelivers to whichever pod fetches next.
    """
    logger.info(f"Fetch loop running: consumer={consumer_name} func_id={func_id}")

    # Bind to the pre-created durable pull consumer
    consumer = await js.consumer(
        settings.NATS_METADATA_STREAM,  # configurable — never hardcoded
        consumer_name,
    )

    while True:
        try:
            msgs = await consumer.fetch(batch=1, timeout=5.0)
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


async def cancel_fetch_loops() -> None:
    for task in _fetch_tasks:
        task.cancel()
    await asyncio.gather(*_fetch_tasks, return_exceptions=True)
    _fetch_tasks.clear()
    logger.info("All fetch loops cancelled")
