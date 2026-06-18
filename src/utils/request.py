"""
Alias for src.utils.reqeust (preserving EdgeService typo in filename).
EdgeService imports use correct spelling 'request' — this file bridges both.
"""
from src.utils.reqeust import RetrySession, RetrySessionAsync
__all__ = ["RetrySession", "RetrySessionAsync"]
