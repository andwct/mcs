import logging
import nats
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_nc: NATS | None = None
_js: JetStreamContext | None = None


async def connect() -> tuple[NATS, JetStreamContext]:
    global _nc, _js
    if _nc and _nc.is_connected:
        return _nc, _js

    logger.info(f"Connecting to NATS at {settings.NATS_URL}")
    _nc = await nats.connect(
        servers=settings.NATS_URL,
        credentials=settings.NATS_CREDS_FILE,
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
        error_cb=_on_error,
        disconnected_cb=_on_disconnected,
        reconnected_cb=_on_reconnected,
    )
    _js = _nc.jetstream()
    logger.info("NATS connected")
    return _nc, _js


async def close() -> None:
    global _nc, _js
    if _nc and _nc.is_connected:
        await _nc.drain()
        logger.info("NATS connection drained and closed")
    _nc = None
    _js = None


async def verify_stream(js: JetStreamContext, stream_name: str) -> None:
    """Verify stream exists — raises RuntimeError if not found."""
    try:
        await js.find_stream(stream_name)
        logger.info(f"Stream verified: {stream_name}")
    except Exception as e:
        raise RuntimeError(f"Required NATS stream '{stream_name}' not found: {e}")


async def _on_error(e: Exception) -> None:
    logger.error(f"NATS error: {e}")


async def _on_disconnected() -> None:
    logger.warning("NATS disconnected — reconnecting")


async def _on_reconnected() -> None:
    logger.info("NATS reconnected")
