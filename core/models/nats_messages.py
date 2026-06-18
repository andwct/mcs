from enum import Enum
from uuid import UUID
from pydantic import BaseModel, ConfigDict


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
    """MLOP-MCS-ARTIFACT stream message schema."""
    function_id: str
    product_id: str
    artifact_type: ArtifactType
    deployed_version: str
    model_id: str | None = None    # required when artifact_type == model
    kernel_id: str | None = None   # required when artifact_type == kernel
    package_id: str | None = None  # required when artifact_type == package


class MetadataMessage(BaseModel):
    """
    MLOP-MCS-METADATA stream message schema.

    Notifies MCS that a meta list has changed. MCS uses function_id,
    product_id and meta_type to fetch the updated data from siteMC
    HTTP API and write it to Redis.

    model_id is only present when meta_type == model_list — identifies
    which specific model was updated so MCS can parse it from the full
    model list response and update just that one field in Redis.
    """
    function_id: str
    product_id: str
    meta_type: MetaType
    model_id: str | None = None  # only for meta_type == model_list


class ModelRecord(BaseModel):
    """Single model entry — only modelId is semantically used by MCS."""
    model_config = ConfigDict(extra="allow")
    modelId: UUID
