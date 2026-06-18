"""
MCS adapter for EdgeService's src.utils.security module.
Re-exports security classes from core.security (copied from EdgeService).
"""
from core.security import SecurityModelServiceDataTunnel, SecurityObjectStore
__all__ = ["SecurityModelServiceDataTunnel", "SecurityObjectStore"]
