"""
Unit tests for Prometheus metrics — /metrics endpoint exposition and that
key counters/histograms/gauges actually increment on the real code paths.
"""
import os
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import generate_latest

from core.metrics_endpoint import mount_metrics
from core.metrics import (
    MCS_CACHE_HITS_TOTAL,
    MCS_CACHE_MISSES_TOTAL,
    JANITOR_DISK_USAGE_RATIO,
    JANITOR_FILES_EVICTED_TOTAL,
    JANITOR_BYTES_FREED_TOTAL,
    JANITOR_EVICTION_SWEEPS_TOTAL,
    SYNC_NATS_MESSAGES_TOTAL,
)


def _metric_value(metric, **labels) -> float:
    for sample in metric.collect()[0].samples:
        if sample.name.endswith("_total") or sample.name == metric._name:
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                return sample.value
    return 0.0


# ── /metrics endpoint exposition ──────────────────────────────────────────────

def test_metrics_endpoint_exposes_prometheus_text_format():
    app = FastAPI()
    mount_metrics(app)
    client = TestClient(app)

    resp = client.get("/metrics")

    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]


def test_metrics_endpoint_reflects_incremented_counter():
    app = FastAPI()
    mount_metrics(app)
    client = TestClient(app)

    MCS_CACHE_HITS_TOTAL.labels(artifact_type="MODEL").inc()

    resp = client.get("/metrics")
    assert 'mcs_cache_hits_total{artifact_type="MODEL"}' in resp.text


# ── mcs-serving cache hit/miss counters (real code path) ──────────────────────

def test_cache_hit_increments_counter(tmp_path):
    from apps.mcs.router import _try_serve_cached
    from core.models.nats_messages import ArtifactType

    before = _metric_value(MCS_CACHE_HITS_TOTAL, artifact_type="PACKAGE")

    dest = tmp_path / "v1"
    dest.write_bytes(b"package-bytes")
    _try_serve_cached(dest, ArtifactType.PACKAGE, 65536)

    after = _metric_value(MCS_CACHE_HITS_TOTAL, artifact_type="PACKAGE")
    assert after == before + 1


def test_cache_miss_increments_counter_over_http(monkeypatch):
    import apps.mcs.router as router_module
    from apps.synchronizer.state import init_product_state
    from core.models.product import ProductConfig

    init_product_state([ProductConfig(
        PRODUCT_ID="p1", PRODUCT_NAME="ABC",
        MODEL_CENTER_ACCOUNT="acct", MODEL_CENTER_PASSWORD="secret",
        FUNCTION_LIST=["funcID_123"], FUNCTION_NAME_MAPPING={},
    )])

    app = FastAPI()
    app.include_router(router_module.router)
    client = TestClient(app)

    before = _metric_value(MCS_CACHE_MISSES_TOTAL, artifact_type="PACKAGE")

    monkeypatch.setattr(router_module, "_try_serve_cached", lambda *a, **kw: None)
    monkeypatch.setattr(
        router_module, "fetch_artifact_bytes", AsyncMock(return_value=b"pkg-bytes")
    )
    monkeypatch.setattr(router_module, "write_artifact", lambda *a, **kw: None)
    monkeypatch.setattr(router_module, "trigger_janitor_check", AsyncMock())

    body = {"product_id": "p1", "function_id": "funcID_123", "package_version": "1"}
    resp = client.post("/mcs/package", json=body, auth=("acct", "secret"))
    assert resp.status_code == 200

    after = _metric_value(MCS_CACHE_MISSES_TOTAL, artifact_type="PACKAGE")
    assert after == before + 1

    init_product_state([])


# ── janitor eviction metrics (real code path) ──────────────────────────────────

def test_eviction_sweep_updates_metrics(tmp_path):
    from apps.janitor.eviction import run_eviction_sweep

    fab = tmp_path / "mcs"
    artifact = fab / "MODEL" / "p" / "f" / "uuid" / "v1"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"x" * 100)

    before_evicted = _metric_value(JANITOR_FILES_EVICTED_TOTAL)
    before_freed = _metric_value(JANITOR_BYTES_FREED_TOTAL)
    before_completed = _metric_value(JANITOR_EVICTION_SWEEPS_TOTAL, result="completed")

    usage = MagicMock(used=95, total=100, free=5)
    with patch("apps.janitor.eviction.shutil.disk_usage", return_value=usage):
        run_eviction_sweep(str(tmp_path), "mcs", high_watermark=0.90, low_watermark=0.75)

    assert _metric_value(JANITOR_FILES_EVICTED_TOTAL) == before_evicted + 1
    assert _metric_value(JANITOR_BYTES_FREED_TOTAL) >= before_freed + 100
    assert _metric_value(JANITOR_EVICTION_SWEEPS_TOTAL, result="completed") == before_completed + 1


def test_janitor_check_updates_disk_usage_gauge():
    import apps.janitor.router as jrouter
    import asyncio

    usage = MagicMock(used=42, total=100, free=58)
    with patch("apps.janitor.router.shutil.disk_usage", return_value=usage), \
         patch("apps.janitor.router.run_eviction_sweep"):
        asyncio.run(jrouter._do_eviction())

    assert JANITOR_DISK_USAGE_RATIO._value.get() == pytest.approx(0.42)


# ── synchronizer NATS ack/nak counters (real code path) ────────────────────────

async def test_artifact_ack_increments_counter():
    from apps.synchronizer.handlers import handle_artifact_message
    from apps.synchronizer.state import init_product_state
    from core.models.product import ProductConfig
    import json

    init_product_state([ProductConfig(
        PRODUCT_ID="p1", PRODUCT_NAME="ABC",
        MODEL_CENTER_ACCOUNT="a", MODEL_CENTER_PASSWORD="pw",
        FUNCTION_LIST=["funcID_123"], FUNCTION_NAME_MAPPING={},
    )])

    msg = AsyncMock()
    msg.data = json.dumps({
        "functionId": "funcID_123", "productId": "p1",
        "artifactType": "MODEL", "deployedVersion": "v1", "modelId": "m1",
    }).encode()

    before = _metric_value(SYNC_NATS_MESSAGES_TOTAL, stream="artifact", result="ack")

    with patch("apps.synchronizer.handlers._download_artifact", new=AsyncMock()):
        await handle_artifact_message(msg)

    after = _metric_value(SYNC_NATS_MESSAGES_TOTAL, stream="artifact", result="ack")
    assert after == before + 1

    init_product_state([])
