"""
MCS adapter for EdgeService's src.utils.request module.
Provides RetrySession and RetrySessionAsync compatible with EdgeService's usage.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RetrySession(requests.Session):
    """
    Sync requests session with automatic retry — mirrors EdgeService RetrySession.
    Used by site_authorization.py and site_artifact_service.py (sync methods).
    """
    def __init__(self, retries=3, backoff_factor=0.3,
                 status_forcelist=(500, 502, 503, 504)):
        super().__init__()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("http://", adapter)
        self.mount("https://", adapter)


# RetrySessionAsync alias — site_artifact_service.py imports this
# but MCS runs sync EdgeService code in run_in_executor so sync is fine
RetrySessionAsync = RetrySession
