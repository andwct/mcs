"""
Unit tests for core/models/api_models.py — mcs-serving request body schemas.

Regression coverage for a real integration bug: Model Service's actual
POST /mcs/package body never includes package_id (siteArtifactService does
not require it — see issue #38 / core/artifact_service.py PACKAGE branch),
but PackageRequestModel originally declared it as a required field, so
FastAPI rejected every real package download with
422 {"detail":[{"loc":["body","package_id"],"msg":"Field required"}]}.
"""
import pytest
from pydantic import ValidationError

from core.models.api_models import ModelRequestModel, KernelRequestModel, PackageRequestModel


def test_package_request_without_package_id_is_valid():
    """Exact body shape from the reported production 422."""
    body = PackageRequestModel(
        product_id="productID_ABC",
        function_id="funcID_123",
        package_version="19",
    )
    assert body.package_id is None
    assert body.package_version == "19"


def test_package_request_with_package_id_still_accepted():
    body = PackageRequestModel(
        product_id="p", function_id="f", package_id="pkg1", package_version="v1",
    )
    assert body.package_id == "pkg1"


def test_package_request_missing_required_fields_still_rejected():
    with pytest.raises(ValidationError):
        PackageRequestModel(product_id="p", function_id="f")  # no package_version


def test_model_request_still_requires_model_id():
    with pytest.raises(ValidationError):
        ModelRequestModel(product_id="p", function_id="f", model_version="v1")


def test_kernel_request_still_requires_kernel_id():
    with pytest.raises(ValidationError):
        KernelRequestModel(product_id="p", function_id="f", kernel_version="v1")
