"""
Unit tests for core/http/meta_client.py — envelope handling per endpoint.

siteMC wraps model_list AND pat_list responses in
{"status_code", "message", "content": ...}; kernel_list/package_list are
raw (no envelope). model_list unwraps to content-only (Redis needs
per-modelId granularity); pat_list keeps the FULL envelope, since
EdgeService's /active_pats returns the envelope verbatim to Model Service
and Redis is the only place that response is ever cached.
"""
from unittest.mock import AsyncMock, patch

import pytest

from core.http import meta_client


async def test_fetch_pat_list_returns_full_envelope():
    envelope = {"status_code": "1029312", "message": "Get successfully", "content": ["6", "5"]}
    with patch.object(meta_client, "_get", new=AsyncMock(return_value=envelope)):
        result = await meta_client.fetch_pat_list("func_id", "prod_id", "acct", "pw")
    assert result == envelope  # NOT unwrapped — full envelope is cached and re-served as-is


async def test_fetch_pat_list_missing_content_raises():
    with patch.object(meta_client, "_get", new=AsyncMock(return_value={"status_code": "x"})):
        with pytest.raises(ValueError, match="content"):
            await meta_client.fetch_pat_list("func_id", "prod_id", "acct", "pw")


async def test_fetch_model_list_unwraps_envelope():
    envelope = {"status_code": "0", "message": "ok", "content": {"online": [], "shadow": []}}
    with patch.object(meta_client, "_get", new=AsyncMock(return_value=envelope)):
        result = await meta_client.fetch_model_list("func_id", "prod_id", "acct", "pw")
    assert result == {"online": [], "shadow": []}


async def test_fetch_model_list_missing_content_raises():
    with patch.object(meta_client, "_get", new=AsyncMock(return_value={"status_code": "0"})):
        with pytest.raises(ValueError, match="content"):
            await meta_client.fetch_model_list("func_id", "prod_id", "acct", "pw")


async def test_fetch_kernel_list_returns_raw_no_envelope():
    raw = {"kernelId": "k1", "kernelVersion": "v1"}
    with patch.object(meta_client, "_get", new=AsyncMock(return_value=raw)):
        result = await meta_client.fetch_kernel_list("func_id", "prod_id", "acct", "pw")
    assert result == raw


async def test_fetch_package_list_returns_raw_no_envelope():
    raw = {"packageId": "p1", "packageVersion": "v1"}
    with patch.object(meta_client, "_get", new=AsyncMock(return_value=raw)):
        result = await meta_client.fetch_package_list("func_id", "prod_id", "acct", "pw")
    assert result == raw
