"""Unit tests for NATS message schemas — camelCase wire format, enums."""
import pytest
from pydantic import ValidationError

from core.models.nats_messages import (
    ArtifactMessage,
    MetadataMessage,
    ArtifactType,
    MetaType,
)


def test_artifact_message_parses_camel_case():
    msg = ArtifactMessage.model_validate_json(b"""
    {
      "functionId": "funcID_123",
      "productId": "productID_ABC",
      "artifactType": "MODEL",
      "deployedVersion": "v1.0.0",
      "modelId": "uuid-model-1",
      "kernelId": null,
      "packageId": null
    }""")
    assert msg.function_id == "funcID_123"
    assert msg.product_id == "productID_ABC"
    assert msg.artifact_type == ArtifactType.MODEL
    assert msg.deployed_version == "v1.0.0"
    assert msg.model_id == "uuid-model-1"
    assert msg.kernel_id is None
    assert msg.package_id is None


def test_artifact_message_accepts_snake_case_too():
    # populate_by_name=True — Python-side construction uses snake_case
    msg = ArtifactMessage(
        function_id="f", product_id="p",
        artifact_type=ArtifactType.KERNEL, deployed_version="v1",
        kernel_id="k1",
    )
    assert msg.kernel_id == "k1"


def test_artifact_message_rejects_unknown_artifact_type():
    with pytest.raises(ValidationError):
        ArtifactMessage.model_validate_json(b"""
        {"functionId": "f", "productId": "p",
         "artifactType": "FIRMWARE", "deployedVersion": "v1"}""")


def test_artifact_message_id_fields_optional():
    msg = ArtifactMessage.model_validate_json(b"""
    {"functionId": "f", "productId": "p",
     "artifactType": "PACKAGE", "deployedVersion": "v1"}""")
    assert msg.model_id is None and msg.kernel_id is None and msg.package_id is None


def test_metadata_message_parses_camel_case():
    msg = MetadataMessage.model_validate_json(b"""
    {"functionId": "f", "productId": "p",
     "metaType": "model_list", "modelId": "m1"}""")
    assert msg.meta_type == MetaType.MODEL_LIST
    assert msg.model_id == "m1"


def test_metadata_message_model_id_optional_for_other_types():
    msg = MetadataMessage.model_validate_json(b"""
    {"functionId": "f", "productId": "p", "metaType": "pat_list"}""")
    assert msg.meta_type == MetaType.PAT_LIST
    assert msg.model_id is None


def test_meta_type_covers_all_four_lists():
    assert {m.value for m in MetaType} == {
        "model_list", "kernel_list", "package_list", "pat_list"
    }


def test_artifact_type_uppercase_values():
    assert {a.value for a in ArtifactType} == {"MODEL", "KERNEL", "PACKAGE"}
