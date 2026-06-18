"""
MCS adapter for EdgeService's src.utils.request module.

RetrySession: sync requests.Session with retry — used by site_authorization.py
RetrySessionAsync: async aiohttp.ClientSession wrapper — used by site_artifact_service.py
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RetrySession(requests.Session):
    """
    Sync requests session with automatic retry.
    Used by site_authorization.py (sync methods).
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


class RetrySessionAsync:
    """
    Async aiohttp session wrapper.
    Used by site_artifact_service.py (async methods via _post_data).
    Mirrors aiohttp.ClientSession interface.
    """
    def __init__(self):
        self._session = None

    async def _get_session(self):
        import aiohttp
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def post(self, url, **kwargs):
        session = await self._get_session()
        return await session.post(url, **kwargs)

    async def get(self, url, **kwargs):
        session = await self._get_session()
        return await session.get(url, **kwargs)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
