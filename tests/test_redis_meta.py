"""
Unit tests for core/redis/{model_list,kernel_list,package_list,pat_list}.py.
The Redis client is replaced with an AsyncMock — no real Redis needed.
"""
import json
from unittest.mock import AsyncMock, patch

import pytest

from core.redis import model_list, kernel_list, package_list, pat_list


@pytest.fixture
def client():
    c = AsyncMock()
    patches = [
        patch("core.redis.model_list.get_client", new=AsyncMock(return_value=c)),
        patch("core.redis.kernel_list.get_client", new=AsyncMock(return_value=c)),
        patch("core.redis.package_list.get_client", new=AsyncMock(return_value=c)),
        patch("core.redis.pat_list.get_client", new=AsyncMock(return_value=c)),
    ]
    for p in patches:
        p.start()
    yield c
    for p in patches:
        p.stop()


# ── model_list — per-function hash, field per modelId ─────────────────────────

async def test_model_list_key_is_per_function():
    assert model_list._model_list_key("funcID_123") == "mcs:model_list:funcID_123"


async def test_get_model_list_parses_records(client):
    client.hgetall.return_value = {
        "m1": json.dumps({"modelId": "m1", "modelName": "a"}),
        "m2": json.dumps({"modelId": "m2"}),
    }
    result = await model_list.get_model_list("funcID_123")
    assert result["m1"]["modelName"] == "a"
    assert set(result) == {"m1", "m2"}
    client.hgetall.assert_awaited_once_with("mcs:model_list:funcID_123")


async def test_get_model_list_empty_returns_none(client):
    client.hgetall.return_value = {}
    assert await model_list.get_model_list("funcID_123") is None


async def test_set_model_writes_single_field(client):
    await model_list.set_model("funcID_123", "m1", {"modelId": "m1"})
    key, field, value = client.hset.await_args[0]
    assert key == "mcs:model_list:funcID_123"
    assert field == "m1"
    assert json.loads(value) == {"modelId": "m1"}


async def test_set_model_list_bulk(client):
    await model_list.set_model_list("funcID_123", {"m1": {"a": 1}, "m2": {"b": 2}})
    kwargs = client.hset.await_args.kwargs
    assert set(kwargs["mapping"]) == {"m1", "m2"}


async def test_set_model_list_empty_is_noop(client):
    await model_list.set_model_list("funcID_123", {})
    client.hset.assert_not_awaited()


# ── kernel/package/pat — shared hash, field per function_id ───────────────────

async def test_kernel_list_round_trip(client):
    client.hget.return_value = json.dumps({"kernelId": "k1", "kernelVersion": "v1"})
    result = await kernel_list.get_kernel_list("funcID_123")
    assert result == {"kernelId": "k1", "kernelVersion": "v1"}
    client.hget.assert_awaited_once_with("mcs:kernel_list", "funcID_123")

    await kernel_list.set_kernel_list("funcID_123", {"kernelId": "k2"})
    key, field, value = client.hset.await_args[0]
    assert (key, field) == ("mcs:kernel_list", "funcID_123")
    assert json.loads(value) == {"kernelId": "k2"}


async def test_kernel_list_missing_returns_none(client):
    client.hget.return_value = None
    assert await kernel_list.get_kernel_list("funcID_x") is None


async def test_package_list_round_trip(client):
    client.hget.return_value = json.dumps({"packageId": "p1"})
    assert await package_list.get_package_list("f") == {"packageId": "p1"}
    client.hget.assert_awaited_once_with("mcs:package_list", "f")

    await package_list.set_package_list("f", {"packageId": "p2"})
    key, field, _ = client.hset.await_args[0]
    assert (key, field) == ("mcs:package_list", "f")


async def test_pat_list_round_trip(client):
    client.hget.return_value = json.dumps(["1", "2", "3"])
    assert await pat_list.get_pat_list("f") == ["1", "2", "3"]
    client.hget.assert_awaited_once_with("mcs:pat_list", "f")

    await pat_list.set_pat_list("f", ["4"])
    key, field, value = client.hset.await_args[0]
    assert (key, field) == ("mcs:pat_list", "f")
    assert json.loads(value) == ["4"]
