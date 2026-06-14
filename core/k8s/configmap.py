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


def load_product_configs() -> list[ProductConfig]:
    """
    Scan ConfigMap mount for product JSON files and return ProductConfig list.
    All *.json files are treated as product configs (one.properties is not JSON
    so it is naturally excluded).
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
    sanitized_name from FUNCTION_NAME_MAPPING — never split from subject string.
    subject = "{func_id}-{sanitized_name}"
    """
    result = []
    for product in products:
        for func_id in product.FUNCTION_LIST:
            sanitized_name = product.get_sanitized_name(func_id)
            subject = product.get_subject(func_id)
            result.append((product.PRODUCT_ID, func_id, sanitized_name, subject))
    return result
