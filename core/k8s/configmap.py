import json
import logging
from pathlib import Path
from core.models.product import ProductConfig
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def load_env_config() -> dict[str, str]:
    """
    Load envConfig.json from ConfigMap mount path.

    This is a flat { "KEY": "value" } JSON object — the same data used to
    populate the env-config ConfigMap (envFrom -> settings.py). It is also
    mounted as a file so non-env-var consumers (if any) can read it directly.
    """
    path = Path(settings.CONFIGMAP_MOUNT_PATH) / settings.CONFIGMAP_ENV_CONFIG_FILE
    if not path.exists():
        raise FileNotFoundError(f"{settings.CONFIGMAP_ENV_CONFIG_FILE} not found at {path}")

    config = json.loads(path.read_text())
    logger.info(f"Loaded {settings.CONFIGMAP_ENV_CONFIG_FILE}: {list(config.keys())}")
    return config


def load_product_configs() -> list[ProductConfig]:
    """
    Scan ConfigMap mount for product JSON files and return ProductConfig list.

    Product files are named "{ProductName}.json" (e.g. "ABC.json"). All
    *.json files in the mount path are treated as product configs, EXCEPT
    CONFIGMAP_ENV_CONFIG_FILE (envConfig.json), which holds flat env vars
    and is loaded separately via load_env_config().
    """
    mount = Path(settings.CONFIGMAP_MOUNT_PATH)
    product_files = sorted(
        f for f in mount.glob("*.json")
        if f.name != settings.CONFIGMAP_ENV_CONFIG_FILE
    )

    if not product_files:
        raise FileNotFoundError(f"No product *.json files found in {mount}")

    products: list[ProductConfig] = []
    for f in product_files:
        try:
            data = json.loads(f.read_text())
            product = ProductConfig.model_validate(data)
            products.append(product)
            logger.info(
                f"Loaded product: {product.PRODUCT_ID} "
                f"functions={product.FUNCTION_LIST}"
            )
        except Exception as e:
            logger.error(f"Failed to parse {f.name}: {e}")
            raise

    return products


def get_all_function_subjects(
    products: list[ProductConfig],
) -> list[tuple[str, str, str, str]]:
    """
    Return flat list of (product_id, func_id, sanitized_name, subject).

    sanitized_name comes directly from FUNCTION_NAME_MAPPING in
    {ProductName}.json — never derived by splitting the subject string.

    subject = "{func_id}-{sanitized_name}"
    """
    result = []
    for product in products:
        for func_id in product.FUNCTION_LIST:
            sanitized_name = product.get_sanitized_name(func_id)
            subject = product.get_subject(func_id)
            result.append((product.PRODUCT_ID, func_id, sanitized_name, subject))
    return result
