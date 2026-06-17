"""
MCS adapter for EdgeService's src.config.config module.

EdgeService classes reference config.SITE_AUTHORIZATION_URL,
config.SITE_ARTIFACT_SERVICE_URL, config.PRODUCTS etc.
This module bridges those references to MCS settings.
"""
from core.config.settings import get_settings


class _Config:
    """Lazy proxy — reads from MCS settings at access time (after bootstrap)."""

    @property
    def SITE_AUTHORIZATION_URL(self) -> str:
        return get_settings().SITE_AUTHORIZATION_URL

    @property
    def SITE_ARTIFACT_SERVICE_URL(self) -> str:
        return get_settings().SITE_ARTIFACT_SERVICE_URL

    @property
    def PRODUCTS(self) -> list:
        # MCS validates products via productConfig — return empty list
        # to bypass EdgeService's product whitelist check
        return []


config = _Config()
