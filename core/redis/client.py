import logging
from redis.asyncio import Redis, ConnectionPool
from redis.exceptions import RedisError
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_pool: ConnectionPool | None = None
_client: Redis | None = None


async def connect() -> Redis:
    global _pool, _client
    if _client:
        return _client
    _pool = ConnectionPool.from_url(
        settings.REDIS_URL,
        password=settings.REDIS_PASSWORD or None,
        decode_responses=True,
        max_connections=20,
    )
    _client = Redis(connection_pool=_pool)
    await _client.ping()
    logger.info("Redis connected")
    return _client


async def get_client() -> Redis:
    if _client is None:
        raise RuntimeError("Redis not connected — call connect() first")
    return _client


async def close() -> None:
    global _client, _pool
    if _client:
        await _client.aclose()
        _client = None
    if _pool:
        await _pool.aclose()
        _pool = None
    logger.info("Redis connection closed")


async def ping() -> bool:
    try:
        client = await get_client()
        return await client.ping()
    except RedisError as e:
        logger.error(f"Redis ping failed: {e}")
        return False
