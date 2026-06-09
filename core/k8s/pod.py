import os
import logging

logger = logging.getLogger(__name__)


def get_pod_name() -> str:
    """
    Resolve pod name from HOSTNAME env var injected by Kubernetes downward API.
    StatefulSet pods are named deterministically: <statefulset-name>-<ordinal>
    e.g. mcs-0, mcs-1, mcs-2
    """
    pod_name = os.getenv("HOSTNAME")
    if pod_name:
        logger.info(f"Pod name resolved: {pod_name}")
        return pod_name
    logger.warning("HOSTNAME env var not set — falling back to 'unknown-pod'")
    return "unknown-pod"
