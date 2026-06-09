import logging
from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import RedisError
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Single Sentinel-aware master client with built-in connection pool.
# redis-py manages the pool internally — callers just use _client directly.
_client: Redis | None = None


async def connect() -> Redis:
    """
    Connect to Redis via Sentinel for HA.

    Bitnami Redis Sentinel (chart 18.2.0, Redis 7.2.4) exposes sentinels at:
      <release-name>-redis-headless:<REDIS_SENTINEL_PORT>

    The Sentinel client discovers the current master automatically and
    maintains an internal connection pool (max_connections configurable).
    On master failover, redis-py re-discovers the new master transparently.
    """
    global _client
    if _client:
        return _client

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

    # master_for() returns a Redis client backed by a connection pool
    # that always points at the current Sentinel-elected master.
    _client = sentinel.master_for(
        settings.REDIS_SENTINEL_MASTER_NAME,
        db=0,
    )

    # Verify connection is live — raises on failure → pod restarts
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
