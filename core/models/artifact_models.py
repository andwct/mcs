"""
Data models for artifact download requests.
Mirror of EdgeService's ModelSyncModel, KernelModel, PackageModel
adapted for MCS (dataclasses instead of ORM models).
"""
from dataclasses import dataclass


@dataclass
class ArtifactItem:
    """
    Minimal item passed to SiteAuthorizationService.get_one_time_access_token().
    Matches EdgeService's base artifact item shape.
    """
    product_id: str
    function_id: str
    ARTIFACT_TYPE: str
    dummy_uid: str


@dataclass
class ModelSyncModel:
    """Passed to SiteArtifactCacheService.get_model_from_artifact_service()."""
    model_id: str
    function_id: str
    product_id: str
    model_version: str
    access_token: str
    dummy_uid: str
    account: str


@dataclass
class KernelModel:
    """Passed to SiteArtifactCacheService.get_decrypt_kernel_from_artifact_service()."""
    product_id: str
    function_id: str
    kernel_id: str
    kernel_version: str
    access_token: str
    dummy_uid: str


@dataclass
class PackageModel:
    """Passed to SiteArtifactCacheService.get_package_from_artifact_service()."""
    product_id: str
    function_id: str
    package_version: str
    access_token: str
    dummy_uid: str
