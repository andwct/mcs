import asyncio
import logging
from nats.js.client import JetStreamContext
from apps.synchronizer.handlers import handle_metadata_message
from apps.synchronizer.consumers import metadata_consumer_name
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

_fetch_task: asyncio.Task | None = None


async def start_fetch_loop(js: JetStreamContext, statefulset_name: str) -> None:
    global _fetch_task
    consumer_name = metadata_consumer_name(statefulset_name)
    _fetch_task = asyncio.create_task(
        _fetch_loop(js, consumer_name),
        name=f"fetch-loop-{consumer_name}",
    )
    logger.info(f"Fetch loop started: consumer={consumer_name}")


async def _fetch_loop(js: JetStreamContext, consumer_name: str) -> None:
    settings = get_settings()
    logger.info(f"Fetch loop running: consumer={consumer_name}")

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
