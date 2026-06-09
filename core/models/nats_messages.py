from enum import Enum
from uuid import UUID
from pydantic import BaseModel, ConfigDict


class ArtifactType(str, Enum):
    KERNEL = "kernel"
    PACKAGE = "package"
    MODEL = "model"
    KERNEL_LIST = "kernel_list"
    PACKAGE_LIST = "package_list"
    MODEL_LIST = "model_list"
    PAT_LIST = "pat_list"


class ArtifactMessage(BaseModel):
    function_id: str
    artifact_type: ArtifactType
    deployed_version: str


class ModelRecord(BaseModel):
    model_config = ConfigDict(extra="allow")
    modelId: UUID


class MetadataMessage(BaseModel):
    function_id: str
    artifact_type: ArtifactType
    online: list[ModelRecord]
