import json
import logging
from pathlib import Path
from core.models.product import ProductConfig
from core.config.settings import get_settings

logger = logging.getLogger(__name__)

_ENV_CONFIG_FILENAME = "one.properties"


def load_one_properties() -> dict[str, str]:
    """Parse one.properties (key=value) from ConfigMap mount path."""
    settings = get_settings()
    props_path = Path(settings.CONFIGMAP_MOUNT_PATH) / _ENV_CONFIG_FILENAME
    if not props_path.exists():
        raise FileNotFoundError(
            f"{_ENV_CONFIG_FILENAME} not found at {props_path}"
        )

    props: dict[str, str] = {}
    for line in props_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        props[key.strip()] = value.strip()

    logger.info(f"Loaded {_ENV_CONFIG_FILENAME}: {list(props.keys())}")
    return props


def _load_vault_password(product: ProductConfig) -> str:
    """
    Read MODEL_CENTER_PASSWORD from the Vault-mounted secret file.

    ricoberger VSO operator mounts each Vault key as a separate file
    under SECRET_MOUNT_PATH (/root/mcs-secret by default).

    File: {SECRET_MOUNT_PATH}/{PRODUCT_NAME}_MODEL_CENTER_PASSWORD
    e.g. /root/mcs-secret/ABC_MODEL_CENTER_PASSWORD

    Raises RuntimeError if file is missing or empty — password is required
    for siteMC Basic Auth.
    """
    settings = get_settings()
    secret_file = Path(settings.SECRET_MOUNT_PATH) / f"{product.PRODUCT_NAME}_MODEL_CENTER_PASSWORD"

    if not secret_file.exists():
        raise RuntimeError(
            f"Vault secret file not found for product '{product.PRODUCT_NAME}': "
            f"{secret_file}. Ensure '{product.PRODUCT_NAME}_MODEL_CENTER_PASSWORD' "
            f"exists at Vault path '{product.MODEL_CENTER_VAULT_PATH}' and is "
            f"mounted via VaultSecret at {settings.SECRET_MOUNT_PATH}."
        )

    password = secret_file.read_text().strip()
    if not password:
        raise RuntimeError(
            f"Vault secret file is empty for product '{product.PRODUCT_NAME}': "
            f"{secret_file}"
        )

    logger.info(
        f"Loaded MODEL_CENTER_PASSWORD from Vault for "
        f"product '{product.PRODUCT_NAME}': {secret_file}"
    )
    return password


def load_product_configs() -> list[ProductConfig]:
    """
    Scan ConfigMap mount for product JSON files and return ProductConfig list.
    All *.json files are treated as product configs (one.properties is not JSON
    so it is naturally excluded).

    If ENABLE_VAULT=true, MODEL_CENTER_PASSWORD is populated from the
    Vault-mounted secret file at /root/{VAULT_PATH_LAST_SEGMENT}/{PRODUCT_NAME}_MODEL_CENTER_PASSWORD.
    Raises RuntimeError if the secret file is missing or empty.
    """
    settings = get_settings()
    mount = Path(settings.CONFIGMAP_MOUNT_PATH)
    product_files = sorted(mount.glob("*.json"))

    if not product_files:
        raise FileNotFoundError(
            f"No product *.json files found in {mount}"
        )

    products: list[ProductConfig] = []
    for f in product_files:
        try:
            data = json.loads(f.read_text())
            product = ProductConfig.model_validate(data)

            # Vault replacement — same pattern as EdgeService
            if product.ENABLE_VAULT:
                product.MODEL_CENTER_PASSWORD = _load_vault_password(product)

            products.append(product)
            logger.info(
                f"Loaded product: {product.PRODUCT_ID} "
                f"functions={product.FUNCTION_LIST} "
                f"vault={'enabled' if product.ENABLE_VAULT else 'disabled'}"
            )
        except Exception as e:
            logger.error(f"Failed to parse or load product {f.name}: {e}")
            raise

    return products


def get_all_function_subjects(
    products: list[ProductConfig],
) -> list[tuple[str, str, str, str, str]]:
    """
    Return flat list of (product_id, func_id, sanitized_name,
    artifact_subject, metadata_subject).

    artifact_subject = "MLOP-MCS-ARTIFACT.{func_id}-{sanitized_name}"
    metadata_subject = "MLOP-MCS-METADATA.{func_id}-{sanitized_name}"

    Both are valid subsets of their stream's interest subject (*.>).
    sanitized_name comes directly from FUNCTION_NAME_MAPPING — never
    derived by splitting the subject string.
    """
    result = []
    for product in products:
        for func_id in product.FUNCTION_LIST:
            sanitized_name = product.get_sanitized_name(func_id)
            artifact_subject = product.get_artifact_subject(func_id)
            metadata_subject = product.get_metadata_subject(func_id)
            result.append((
                product.PRODUCT_ID,
                func_id,
                sanitized_name,
                artifact_subject,
                metadata_subject,
            ))
    return result
