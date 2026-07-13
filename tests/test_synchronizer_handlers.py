"""
Unit tests for apps/synchronizer/handlers.py — NATS message ack/nak
semantics and dispatch. NATS Msg and downstream calls are mocked.
"""
import json
from unittest.mock import AsyncMock, patch

import pytest

from apps.synchronizer.handlers import handle_artifact_message, handle_metadata_message
from apps.synchronizer.state import init_product_state
from core.models.product import ProductConfig


def _product(**overrides) -> ProductConfig:
    base = dict(
        PRODUCT_ID="productID_ABC",
        PRODUCT_NAME="ABC",
        MODEL_CENTER_ACCOUNT="acct",
        MODEL_CENTER_PASSWORD="pw",
        FUNCTION_LIST=["funcID_123"],
        FUNCTION_NAME_MAPPING={"funcID_123": "funcName"},
    )
    base.update(overrides)
    return ProductConfig(**base)


@pytest.fixture(autouse=True)
def product_state():
    init_product_state([_product()])
    yield
    init_product_state([])


def _msg(payload: dict) -> AsyncMock:
    msg = AsyncMock()
    msg.data = json.dumps(payload).encode()
    return msg


ARTIFACT_PAYLOAD = {
    "functionId": "funcID_123",
    "productId": "productID_ABC",
    "artifactType": "MODEL",
    "deployedVersion": "v1.0.0",
    "modelId": "uuid-1",
}


# ── handle_artifact_message ───────────────────────────────────────────────────

async def test_artifact_unparseable_message_is_acked():
    msg = AsyncMock()
    msg.data = b"not json"
    await handle_artifact_message(msg)
    msg.ack.assert_awaited_once()
    msg.nak.assert_not_awaited()


async def test_artifact_unknown_function_id_is_acked():
    msg = _msg({**ARTIFACT_PAYLOAD, "functionId": "funcID_unknown"})
    await handle_artifact_message(msg)
    msg.ack.assert_awaited_once()


async def test_artifact_success_downloads_and_acks():
    msg = _msg(ARTIFACT_PAYLOAD)
    with patch(
        "apps.synchronizer.handlers._download_artifact", new=AsyncMock()
    ) as dl:
        await handle_artifact_message(msg)
    dl.assert_awaited_once()
    kwargs = dl.await_args.kwargs
    assert kwargs["func_id"] == "funcID_123"
    assert kwargs["model_id"] == "uuid-1"
    assert kwargs["account"] == "acct"
    assert kwargs["password"] == "pw"
    msg.ack.assert_awaited_once()
    msg.nak.assert_not_awaited()


async def test_artifact_download_failure_naks_for_redelivery():
    msg = _msg(ARTIFACT_PAYLOAD)
    with patch(
        "apps.synchronizer.handlers._download_artifact",
        new=AsyncMock(side_effect=RuntimeError("siteMC down")),
    ):
        await handle_artifact_message(msg)
    msg.nak.assert_awaited_once()
    msg.ack.assert_not_awaited()


# ── _download_artifact id requirements ────────────────────────────────────────

async def test_download_model_without_id_raises(tmp_path):
    from apps.synchronizer.handlers import _download_artifact
    from core.models.nats_messages import ArtifactType

    with pytest.raises(RuntimeError, match="model_id"):
        await _download_artifact(
            func_id="f", product_id="p",
            artifact_type=ArtifactType.MODEL, version="v1",
            account="a", password="pw",
        )


async def test_download_kernel_without_id_raises():
    from apps.synchronizer.handlers import _download_artifact
    from core.models.nats_messages import ArtifactType

    with pytest.raises(RuntimeError, match="kernel_id"):
        await _download_artifact(
            func_id="f", product_id="p",
            artifact_type=ArtifactType.KERNEL, version="v1",
            account="a", password="pw",
        )


async def test_download_package_without_id_succeeds(tmp_path, monkeypatch):
    """PACKAGE needs no artifact id — path omits {id} segment (issue #38)."""
    from apps.synchronizer import handlers
    from core.models.nats_messages import ArtifactType
    import core.artifact_service as svc

    monkeypatch.setattr(
        svc, "fetch_artifact_bytes", AsyncMock(return_value=b"pkg-bytes")
    )
    monkeypatch.setattr(svc, "trigger_janitor_check", AsyncMock())
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path))
    from core.config.settings import get_settings
    get_settings.cache_clear()
    try:
        await handlers._download_artifact(
            func_id="f", product_id="p",
            artifact_type=ArtifactType.PACKAGE, version="v1",
            account="a", password="pw",
        )
        stored = tmp_path / "mcs" / "PACKAGE" / "p" / "f" / "v1"
        assert stored.read_bytes() == b"pkg-bytes"
    finally:
        get_settings.cache_clear()


async def test_download_skips_when_already_cached(tmp_path, monkeypatch):
    from apps.synchronizer import handlers
    from core.models.nats_messages import ArtifactType
    import core.artifact_service as svc

    fetch = AsyncMock()
    monkeypatch.setattr(svc, "fetch_artifact_bytes", fetch)
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path))
    from core.config.settings import get_settings
    get_settings.cache_clear()
    try:
        cached = tmp_path / "mcs" / "PACKAGE" / "p" / "f" / "v1"
        cached.parent.mkdir(parents=True)
        cached.write_bytes(b"already here")

        await handlers._download_artifact(
            func_id="f", product_id="p",
            artifact_type=ArtifactType.PACKAGE, version="v1",
            account="a", password="pw",
        )
        fetch.assert_not_awaited()
    finally:
        get_settings.cache_clear()


# ── handle_metadata_message ───────────────────────────────────────────────────

META_PAYLOAD = {
    "functionId": "funcID_123",
    "productId": "productID_ABC",
    "metaType": "model_list",
    "modelId": "uuid-1",
}


async def test_metadata_unparseable_message_is_acked():
    msg = AsyncMock()
    msg.data = b"{broken"
    await handle_metadata_message(msg)
    msg.ack.assert_awaited_once()


async def test_metadata_unknown_product_is_acked():
    msg = _msg({**META_PAYLOAD, "productId": "productID_unknown"})
    await handle_metadata_message(msg)
    msg.ack.assert_awaited_once()


async def test_metadata_model_list_updates_redis_and_acks():
    msg = _msg(META_PAYLOAD)
    record = {"modelId": "uuid-1", "modelName": "m"}
    with patch(
        "apps.synchronizer.handlers.fetch_model_list",
        new=AsyncMock(return_value={"online": [record], "shadow": []}),
    ), patch(
        "apps.synchronizer.handlers.set_model", new=AsyncMock()
    ) as set_model:
        await handle_metadata_message(msg)
    set_model.assert_awaited_once_with("funcID_123", "uuid-1", record)
    msg.ack.assert_awaited_once()


async def test_metadata_model_not_in_online_list_still_acks():
    msg = _msg(META_PAYLOAD)
    with patch(
        "apps.synchronizer.handlers.fetch_model_list",
        new=AsyncMock(return_value={"online": [], "shadow": []}),
    ), patch(
        "apps.synchronizer.handlers.set_model", new=AsyncMock()
    ) as set_model:
        await handle_metadata_message(msg)
    set_model.assert_not_awaited()
    msg.ack.assert_awaited_once()  # shadow/deleted model — not an error


@pytest.mark.parametrize("meta_type,fetch_fn,set_fn", [
    ("kernel_list", "fetch_kernel_list", "set_kernel_list"),
    ("package_list", "fetch_package_list", "set_package_list"),
    ("pat_list", "fetch_pat_list", "set_pat_list"),
])
async def test_metadata_simple_types_full_replace(meta_type, fetch_fn, set_fn):
    msg = _msg({**META_PAYLOAD, "metaType": meta_type, "modelId": None})
    content = {"some": "content"} if meta_type != "pat_list" else ["1", "2"]
    with patch(
        f"apps.synchronizer.handlers.{fetch_fn}",
        new=AsyncMock(return_value=content),
    ), patch(
        f"apps.synchronizer.handlers.{set_fn}", new=AsyncMock()
    ) as setter:
        await handle_metadata_message(msg)
    setter.assert_awaited_once_with("funcID_123", content)
    msg.ack.assert_awaited_once()


async def test_metadata_fetch_failure_naks_for_redelivery():
    msg = _msg(META_PAYLOAD)
    with patch(
        "apps.synchronizer.handlers.fetch_model_list",
        new=AsyncMock(side_effect=ConnectionError("siteMC unreachable")),
    ):
        await handle_metadata_message(msg)
    msg.nak.assert_awaited_once()
    msg.ack.assert_not_awaited()
