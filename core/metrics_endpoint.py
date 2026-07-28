"""Mounts GET /metrics on a FastAPI app using prometheus_client's default registry."""
from fastapi import FastAPI
from prometheus_client import make_asgi_app


def mount_metrics(app: FastAPI) -> None:
    app.mount("/metrics", make_asgi_app())
