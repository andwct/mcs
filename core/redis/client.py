import logging
from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import RedisError
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

_client: Redis | None = None


async def connect() -> Redis:
    global _client
    if _client:
        return _client

    settings = get_settings()
    sentinel_hosts = [
        (host.strip(), int(settings.REDIS_SENTINEL_PORT))
        for host in settings.REDIS_SENTINEL_HOSTS.split(",")
    ]

    sentinel = Sentinel(
        sentinel_hosts,
        sentinel_kwargs={
            "password": settings.REDIS_PASSWORD or None,
            "socket_timeout": 5.0,
        },
        password=settings.REDIS_PASSWORD or None,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
    )

    _client = sentinel.master_for(
        settings.REDIS_SENTINEL_MASTER_NAME,
        db=0,
    )

    await _client.ping()
    logger.info(
        f"Redis Sentinel connected: master={settings.REDIS_SENTINEL_MASTER_NAME} "
        f"sentinels={sentinel_hosts}"
    )
    return _client


async def get_client() -> Redis:
    if _client is None:
        raise RuntimeError("Redis not connected — call connect() first")
    return _client


async def close() -> None:
    global _client
    if _client:
        await _client.aclose()
        _client = None
    logger.info("Redis connection closed")


async def ping() -> bool:
    try:
        client = await get_client()
        return await client.ping()
    except RedisError as e:
        logger.error(f"Redis ping failed: {e}")
        return False
