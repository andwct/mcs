"""
Makes 'from src.config import config' work by exposing the config module.
EdgeService uses: from src.config import config
then: config.SITE_AUTHORIZATION_URL, config.PRODUCTS etc.
"""
from src.config import config
