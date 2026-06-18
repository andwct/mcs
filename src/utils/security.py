"""
MCS adapter for EdgeService's src.utils.security module.
Exports both correct and typo'd class names from core.security.
"""
from core.security import SecurityModelServiceDataTunnel, SecurityObjectStore

# Typo alias used in site_artifact_service.py import
SecurityModelServceDataTunnel = SecurityModelServiceDataTunnel

__all__ = [
    "SecurityModelServiceDataTunnel",
    "SecurityModelServceDataTunnel",  # EdgeService typo
    "SecurityObjectStore",
]
