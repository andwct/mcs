from enum import Enum
from uuid import UUID
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class ArtifactType(str, Enum):
    """artifact_type values on MLOP-MCS-ARTIFACT stream — uppercase to match siteMC enum."""
    KERNEL = "KERNEL"
    PACKAGE = "PACKAGE"
    MODEL = "MODEL"


class MetaType(str, Enum):
    """meta_type values on MLOP-MCS-METADATA stream."""
    MODEL_LIST = "model_list"
    KERNEL_LIST = "kernel_list"
    PACKAGE_LIST = "package_list"
    PAT_LIST = "pat_list"


class ArtifactMessage(BaseModel):
    """
    MLOP-MCS-ARTIFACT stream message schema.
    JSON payload uses camelCase keys (Java/JSON convention):
      functionId, productId, artifactType, deployedVersion,
      modelId, kernelId, packageId
    Python code accesses fields as snake_case (function_id etc.)
    via Pydantic's alias_generator.
    """
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,  # allow both snake_case and camelCase in Python
    )

    function_id: str
    product_id: str
    artifact_type: ArtifactType
    deployed_version: str
    model_id: str | None = None    # required when artifact_type == MODEL
    kernel_id: str | None = None   # required when artifact_type == KERNEL
    package_id: str | None = None  # not used — siteArtifactService does not require packageId


class MetadataMessage(BaseModel):
    """
    MLOP-MCS-METADATA stream message schema.
    JSON payload uses camelCase keys:
      functionId, productId, metaType, modelId
    Python code accesses fields as snake_case via Pydantic's alias_generator.

    modelId is only present when metaType == model_list — identifies
    which specific model was updated so MCS can parse it from the full
    model list response and update just that one field in Redis.
    """
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )

    function_id: str
    product_id: str
    meta_type: MetaType
    model_id: str | None = None  # only for meta_type == model_list


class ModelRecord(BaseModel):
    """Single model entry — only modelId is semantically used by MCS."""
    model_config = ConfigDict(extra="allow")
    modelId: UUID
