"""
Module-level product state — loaded once at startup, shared across handlers.

Provides O(1) lookup of ProductConfig by product_id or function_id,
so handlers can get MODEL_CENTER_ACCOUNT/PASSWORD for siteMC HTTP calls
without re-reading ConfigMap files on every message.
"""
import logging
from core.models.product import ProductConfig

logger = logging.getLogger(__name__)

_product_map: dict[str, ProductConfig] = {}     # product_id  -> ProductConfig
_func_product_map: dict[str, ProductConfig] = {} # function_id -> ProductConfig


def init_product_state(products: list[ProductConfig]) -> None:
    """Called once in lifespan.py after loading productConfig."""
    _product_map.clear()
    _func_product_map.clear()
    for product in products:
        _product_map[product.PRODUCT_ID] = product
        for func_id in product.FUNCTION_LIST:
            _func_product_map[func_id] = product
    logger.info(
        f"Product state initialised: {len(_product_map)} products, "
        f"{len(_func_product_map)} functions"
    )


def get_product_by_id(product_id: str) -> ProductConfig:
    product = _product_map.get(product_id)
    if product is None:
        raise KeyError(f"Product not found: product_id={product_id}")
    return product


def get_all_products() -> list:
    """Return all loaded ProductConfig objects — used by config.PRODUCTS adapter."""
    return list(_product_map.values())
    product = _func_product_map.get(function_id)
    if product is None:
        raise KeyError(f"Product not found for function_id={function_id}")
    return product
