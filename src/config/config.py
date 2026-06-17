"""
MCS adapter for EdgeService's src.config.config module.

EdgeService classes reference config.SITE_AUTHORIZATION_URL,
config.SITE_ARTIFACT_SERVICE_URL, config.PRODUCTS etc.
This module bridges those references to MCS settings and productConfig state.
"""
from core.config.settings import get_settings


class _Config:
    """Lazy proxy — reads from MCS settings and state at access time (after bootstrap)."""

    @property
    def SITE_AUTHORIZATION_URL(self) -> str:
        return get_settings().SITE_AUTHORIZATION_URL

    @property
    def SITE_ARTIFACT_SERVICE_URL(self) -> str:
        return get_settings().SITE_ARTIFACT_SERVICE_URL

    @property
    def PRODUCTS(self) -> dict:
        """
        Return products in EdgeService's expected shape:
        {
            product_id: {
                "FUNCTION_LIST": [func_id, ...],
                "MODEL_CENTER_ACCOUNT": "...",
                "MODEL_CENTER_PASSWORD": "...",
            }
        }

        EdgeService uses this in get_artifact_key() to:
        1. Find product_id by iterating and checking if function_id in FUNCTION_LIST
        2. Look up MODEL_CENTER_ACCOUNT and MODEL_CENTER_PASSWORD

        Reads from MCS state (initialised in lifespan.py via init_product_state()).
        """
        from apps.synchronizer.state import get_all_products
        products = {}
        for product in get_all_products():
            products[product.PRODUCT_ID] = {
                "FUNCTION_LIST": product.FUNCTION_LIST,
                "MODEL_CENTER_ACCOUNT": product.MODEL_CENTER_ACCOUNT,
                "MODEL_CENTER_PASSWORD": product.MODEL_CENTER_PASSWORD,
            }
        return products


config = _Config()
