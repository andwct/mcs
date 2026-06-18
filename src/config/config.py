"""
MCS adapter for EdgeService's src.config.config module.
Bridges config.SITE_*_URL and config.PRODUCTS to MCS settings/state.

Supports both EdgeService import styles:
  from src.config import config        → uses this module directly
  from src.config.config import config → uses the _Config instance
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
        from apps.synchronizer.state import get_all_products
        products = {}
        for product in get_all_products():
            products[product.PRODUCT_ID] = {
                "FUNCTION_LIST": product.FUNCTION_LIST,
                "MODEL_CENTER_ACCOUNT": product.MODEL_CENTER_ACCOUNT,
                "MODEL_CENTER_PASSWORD": product.MODEL_CENTER_PASSWORD,
            }
        return products


# Instance — used when: from src.config.config import config
config = _Config()


# Module-level proxy attributes — used when: from src.config import config
# then config.SITE_AUTHORIZATION_URL etc. resolve to the module attribute
def __getattr__(name: str):
    """
    Module-level __getattr__ — called when attribute not found on module.
    Delegates to the _Config instance so both import styles work:
      from src.config import config → config is this module
      config.SITE_AUTHORIZATION_URL → calls this __getattr__
    """
    return getattr(_config_instance, name)


_config_instance = _Config()
