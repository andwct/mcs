from pydantic import BaseModel


class ProductConfig(BaseModel):
    PRODUCT_ID: str
    PRODUCT_NAME: str
    ENABLE_VAULT: bool = False
    VAULT_PATH: str = ""
    MODEL_CENTER_ACCOUNT: str = ""
    MODEL_CENTER_PASSWORD: str = ""
    FUNCTION_LIST: list[str]
    FUNC_NAME_MAPPING: dict[str, str]

    def get_sanitized_name(self, func_id: str) -> str:
        return self.FUNC_NAME_MAPPING[func_id]

    def get_subject(self, func_id: str) -> str:
        return f"{func_id}-{self.get_sanitized_name(func_id)}"
