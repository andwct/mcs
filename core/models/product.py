from pydantic import BaseModel


class ProductConfig(BaseModel):
    PRODUCT_ID: str
    PRODUCT_NAME: str
    ENABLE_VAULT: bool = False
    VAULT_PATH: str = ""
    MODEL_CENTER_ACCOUNT: str = ""
    MODEL_CENTER_PASSWORD: str = ""
    FUNCTION_LIST: list[str]
    FUNCTION_NAME_MAPPING: dict[str, str]

    def get_sanitized_name(self, func_id: str) -> str:
        return self.FUNCTION_NAME_MAPPING[func_id]

    def get_subject_suffix(self, func_id: str) -> str:
        """
        Base subject suffix: {func_id}-{sanitized_name}
        Used as the token after the stream prefix.
        e.g. funcID_123-funcName_123
        """
        return f"{func_id}-{self.get_sanitized_name(func_id)}"

    def get_artifact_subject(self, func_id: str) -> str:
        """
        Full subject for MLOP-MCS-ARTIFACT stream.
        Must be a subset of stream's interest subject MLOP-MCS-ARTIFACT.>
        """
        return f"MLOP-MCS-ARTIFACT.{self.get_subject_suffix(func_id)}"

    def get_metadata_subject(self, func_id: str) -> str:
        """
        Full subject for MLOP-MCS-METADATA stream.
        Must be a subset of stream's interest subject MLOP-MCS-METADATA.>
        """
        return f"MLOP-MCS-METADATA.{self.get_subject_suffix(func_id)}"
