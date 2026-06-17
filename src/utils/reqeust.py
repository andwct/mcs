"""MCS adapter for EdgeService's src.utils.request module."""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RetrySession(requests.Session):
    def __init__(self, retries=3, backoff_factor=0.3,
                 status_forcelist=(500, 502, 503, 504)):
        super().__init__()
        retry = Retry(total=retries, read=retries, connect=retries,
                      backoff_factor=backoff_factor,
                      status_forcelist=status_forcelist)
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("http://", adapter)
        self.mount("https://", adapter)


RetrySessionAsync = RetrySession
