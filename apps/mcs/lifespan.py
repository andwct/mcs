"""
mcs-serving startup sequence — pure HTTP server, no NATS involvement.

Loads productConfig (for auth + siteMC fallback credentials), initialises
shared product state, and connects to Redis Sentinel (for meta serving).
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from core.redis.client import connect as redis_connect, close as redis_close
from core.k8s.configmap import load_product_configs
from apps.synchronizer.state import init_product_state

logger = logging.getLogger(__name__)

_ready: bool = False


def is_ready() -> bool:
    return _ready


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    logger.info("mcs-serving starting up")

    # 1. Load ConfigMap (productConfig — for auth + siteMC credentials)
    products = load_product_configs()
    logger.info(f"Loaded {len(products)} products")

    # 2. Initialise product state — O(1) lookup by product_id/function_id
    init_product_state(products)

    # 3. Connect Redis Sentinel — for meta serving (model_list, kernel_list, etc.)
    await redis_connect()

    _ready = True
    logger.info("mcs-serving ready")

    yield

    # Shutdown
    logger.info("mcs-serving shutting down")
    _ready = False
    await redis_close()
