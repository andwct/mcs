"""
Pydantic request models for apps/mcs/ serving API endpoints.
Mirror EdgeService's request body shapes exactly — Model Service
sends these same payloads, no changes needed on client side.
"""
from pydantic import BaseModel


class ModelRequestModel(BaseModel):
    ARTIFACT_TYPE: str = "MODEL"
    product_id: str
    function_id: str
    model_id: str
    model_version: str


class KernelRequestModel(BaseModel):
    ARTIFACT_TYPE: str = "KERNEL"
    product_id: str
    function_id: str
    kernel_id: str
    kernel_version: str


class PackageRequestModel(BaseModel):
    ARTIFACT_TYPE: str = "PACKAGE"
    product_id: str
    function_id: str
    package_id: str
    package_version: str
