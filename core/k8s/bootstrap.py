"""
TEMPORARY (rollback to v1.0.13 values.yaml shape).

Loads one.properties (key=value format) from the ConfigMap mount and sets
any keys not already present in os.environ — so settings.py picks up
NATS_URL, REDIS_SENTINEL_*, SITE_*_URL, etc. even though the v1.0.13
StatefulSet template doesn't inject them via envFrom.

MUST be called FIRST, before importing any core/* or apps/* module that
calls get_settings() at module level — pydantic Settings (@lru_cache)
reads os.environ once at first construction and caches the result.
"""
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_MOUNT_PATH = "/etc/config"
ONE_PROPERTIES_FILENAME = "one.properties"


def bootstrap_env_from_one_properties() -> None:
    mount_path = os.environ.get("CONFIGMAP_MOUNT_PATH", DEFAULT_MOUNT_PATH)
    props_path = Path(mount_path) / ONE_PROPERTIES_FILENAME

    if not props_path.exists():
        logger.warning(f"{props_path} not found — skipping env bootstrap from one.properties")
        return

    loaded = []
    for line in props_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key and key not in os.environ:
            os.environ[key] = value
            loaded.append(key)

    logger.info(f"Bootstrapped {len(loaded)} env vars from {props_path}: {loaded}")
