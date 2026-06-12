import json
import logging
from pathlib import Path
from core.models.product import ProductConfig
from core.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def load_one_properties() -> dict[str, str]:
    """Parse one.properties (key=value) from ConfigMap mount path."""
    props_path = Path(settings.CONFIGMAP_MOUNT_PATH) / settings.CONFIGMAP_ONE_PROPERTIES
    if not props_path.exists():
        raise FileNotFoundError(f"one.properties not found at {props_path}")

    props: dict[str, str] = {}
    for line in props_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            props[key.strip()] = value.strip()

    logger.info(f"Loaded one.properties: {list(props.keys())}")
    return props


def load_product_configs() -> list[ProductConfig]:
    """
    Scan ConfigMap mount for product JSON files and return ProductConfig list.

    Product files are named "{ProductName}.json" (e.g. "ABC.json") and are
    distinguished from one.properties by file extension. All *.json files
    in the mount path are treated as product configs.
    """
    mount = Path(settings.CONFIGMAP_MOUNT_PATH)
    product_files = sorted(mount.glob("*.json"))

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

    sanitized_name comes directly from FUNC_NAME_MAPPING in product_*.json —
    never derived by splitting the subject string.

    subject = "{func_id}-{sanitized_name}"
    """
    result = []
    for product in products:
        for func_id in product.FUNCTION_LIST:
            sanitized_name = product.get_sanitized_name(func_id)
            subject = product.get_subject(func_id)
            result.append((product.PRODUCT_ID, func_id, sanitized_name, subject))
    return result
