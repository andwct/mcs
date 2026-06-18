import asyncio
import requests
from requests.adapters import HTTPAdapter, Retry
from requests.sessions import Session


class RetrySessionAsync:
    def __init__(self, max_retries=3, backoff_factor=0.1, status_forcelist=None):
        self.session = Session()
        self.retries = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist or [500, 502, 503, 504],
        )
        self.session.mount("http://", HTTPAdapter(max_retries=self.retries))
        self.session.mount("https://", HTTPAdapter(max_retries=self.retries))
        self.session.verify = False

    async def get(self, url, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.session.get(url, **kwargs))

    async def post(self, url, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.session.post(url, **kwargs))

    async def delete(self, url, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.session.delete(url, **kwargs))


class RetrySession:
    def __init__(self, max_retries=3, backoff_factor=0.1, status_forcelist=None):
        self.session = Session()
        self.retries = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist or [500, 502, 503, 504],
        )
        self.session.mount("http://", HTTPAdapter(max_retries=self.retries))
        self.session.mount("https://", HTTPAdapter(max_retries=self.retries))
        self.session.verify = False

    def get(self, url, **kwargs):
        return self.session.get(url, **kwargs)

    def post(self, url, **kwargs):
        return self.session.post(url, **kwargs)

    def delete(self, url, **kwargs):
        return self.session.delete(url, **kwargs)
