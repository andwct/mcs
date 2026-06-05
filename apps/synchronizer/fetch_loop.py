import asyncio
import logging
from nats.js.client import JetStreamContext
from apps.synchronizer.handlers import handle_metadata_message
from apps.synchronizer.consumers import metadata_consumer_name

logger = logging.getLogger(__name__)

# Registry of running fetch loop tasks — used for graceful shutdown
_fetch_tasks: list[asyncio.Task] = []


async def start_fetch_loops(
    js: JetStreamContext,
    func_subjects: list[tuple[str, str, str]],  # (product_id, func_id, subject)
) -> None:
    """
    Launch one asyncio.Task per func_id to continuously fetch from
    the metadata pull consumer for that func_id.
    """
    for product_id, func_id, subject in func_subjects:
        sanitized = subject.split("-", 1)[1]   # subject = "{func_id}-{sanitized}"
        consumer_name = metadata_consumer_name(func_id, sanitized)
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
    Continuously fetch from pull consumer.
    Uses server-side long-poll (timeout=5s) — no busy spin.
    On task crash, supervisor in lifespan.py restarts it.
    """
    logger.info(f"Fetch loop running: {consumer_name}")
    consumer = await js.consumer(
        # stream name resolved from consumer_name via NATS
        # nats-py >= 2.7: js.consumer(stream, durable)
        "MLOP-MCS-METADATA",
        consumer_name,
    )

    while True:
        try:
            msgs = await consumer.fetch(batch=1, timeout=5.0)
            for msg in msgs:
                try:
                    await handle_metadata_message(msg)
                except Exception as e:
                    logger.error(f"Handler error in fetch loop {consumer_name}: {e}")
                    await msg.nak()
        except asyncio.CancelledError:
            logger.info(f"Fetch loop cancelled: {consumer_name}")
            break
        except Exception as e:
            logger.error(f"Fetch loop error {consumer_name}: {e} — retrying in 2s")
            await asyncio.sleep(2)


async def cancel_fetch_loops() -> None:
    for task in _fetch_tasks:
        task.cancel()
    await asyncio.gather(*_fetch_tasks, return_exceptions=True)
    _fetch_tasks.clear()
    logger.info("All fetch loops cancelled")
