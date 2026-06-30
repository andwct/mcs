from pydantic import BaseModel, ConfigDict


class ProductConfig(BaseModel):
    model_config = ConfigDict(frozen=False)  # allow mutation for Vault injection

    PRODUCT_ID: str
    PRODUCT_NAME: str
    ENABLE_VAULT: bool = False
    MODEL_CENTER_VAULT_PATH: str = ""
    MODEL_CENTER_ACCOUNT: str = ""
    MODEL_CENTER_PASSWORD: str = ""  # populated from Vault at load time if ENABLE_VAULT=true
    FUNCTION_LIST: list[str]
    FUNCTION_NAME_MAPPING: dict[str, str]  # kept for reference, no longer used in subject construction
