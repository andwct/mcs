"""Unit tests for apps/mcs/auth.py — HTTPBasic credential checks."""
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials

from apps.mcs.auth import _check, verify_credentials_for_function_id
from apps.synchronizer.state import init_product_state
from core.models.product import ProductConfig


@pytest.fixture(autouse=True)
def product_state():
    init_product_state([ProductConfig(
        PRODUCT_ID="p1",
        PRODUCT_NAME="ABC",
        MODEL_CENTER_ACCOUNT="acct",
        MODEL_CENTER_PASSWORD="secret",
        FUNCTION_LIST=["funcID_123"],
        FUNCTION_NAME_MAPPING={},
    )])
    yield
    init_product_state([])


def _creds(username, password):
    return HTTPBasicCredentials(username=username, password=password)


def test_valid_credentials_pass():
    _check("funcID_123", _creds("acct", "secret"))  # no exception


def test_unknown_function_id_404():
    with pytest.raises(HTTPException) as e:
        _check("funcID_missing", _creds("acct", "secret"))
    assert e.value.status_code == 404


def test_wrong_password_401():
    with pytest.raises(HTTPException) as e:
        _check("funcID_123", _creds("acct", "wrong"))
    assert e.value.status_code == 401
    assert e.value.headers["WWW-Authenticate"] == "Basic"


def test_wrong_username_401():
    with pytest.raises(HTTPException) as e:
        _check("funcID_123", _creds("intruder", "secret"))
    assert e.value.status_code == 401


def test_body_variant_delegates_to_check():
    verify_credentials_for_function_id("funcID_123", _creds("acct", "secret"))
    with pytest.raises(HTTPException):
        verify_credentials_for_function_id("funcID_123", _creds("acct", "bad"))
