"""
MCS adapter for EdgeService's src.config.config module.
Bridges config.SITE_*_URL and config.PRODUCTS to MCS settings/state.
"""
from core.config.settings import get_settings


class _Config:
    @property
    def SITE_AUTHORIZATION_URL(self) -> str:
        return get_settings().SITE_AUTHORIZATION_URL

    @property
    def SITE_ARTIFACT_SERVICE_URL(self) -> str:
        return get_settings().SITE_ARTIFACT_SERVICE_URL

    @property
    def PRODUCTS(self) -> dict:
        """
        Returns products in EdgeService shape:
        {product_id: {FUNCTION_LIST: [...], MODEL_CENTER_ACCOUNT: ..., MODEL_CENTER_PASSWORD: ...}}
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
