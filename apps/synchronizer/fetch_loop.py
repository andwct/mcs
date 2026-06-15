import asyncio
import logging
from nats.js.client import JetStreamContext
from apps.synchronizer.handlers import handle_metadata_message
from apps.synchronizer.consumers import metadata_consumer_name
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

# One fetch task per func_id — cleared on shutdown
_fetch_tasks: list[asyncio.Task] = []


async def start_fetch_loops(
    js: JetStreamContext,
    statefulset_name: str,
    func_subjects: list[tuple[str, str, str, str]],
) -> None:
    """
    Launch one asyncio.Task per func_id to long-poll its metadata
    pull consumer. Each task is independent — one func_id's backlog
    cannot block another's.
    """
    _fetch_tasks.clear()
    for product_id, func_id, sanitized_name, subject in func_subjects:
        consumer_name = metadata_consumer_name(statefulset_name, func_id)
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
    Continuously fetch from a metadata pull consumer for one func_id.

    Server-side long-poll (timeout=5.0s) — no busy spin.
    All 3 pods run this loop against the same durable consumer name —
    NATS delivers each message to exactly one pod (queue-group semantics).
    On nak() or ack wait timeout, NATS redelivers to next pod that fetches.
    """
    settings = get_settings()
    logger.info(f"Fetch loop running: consumer={consumer_name} func_id={func_id}")

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
