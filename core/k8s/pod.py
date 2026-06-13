import os
import logging

logger = logging.getLogger(__name__)


def get_pod_name() -> str:
    """
    Resolve pod name from HOSTNAME env var injected by Kubernetes downward API.
    StatefulSet pods are named deterministically: <statefulset-name>-<ordinal>
    e.g. mcs-statefulset-0, mcs-statefulset-1, mcs-statefulset-2
    """
    pod_name = os.getenv("HOSTNAME")
    if pod_name:
        logger.info(f"Pod name resolved: {pod_name}")
        return pod_name
    logger.warning("HOSTNAME env var not set — falling back to 'unknown-pod'")
    return "unknown-pod"


def get_statefulset_name() -> str:
    """
    Derive the StatefulSet name by stripping the trailing "-<ordinal>" from
    the pod name (e.g. "mcs-statefulset-0" -> "mcs-statefulset").

    Used as a deployment-unique prefix for resources that must be SHARED
    across all pods of one deployment (e.g. the metadata pull consumer),
    as opposed to per-pod resources (e.g. the artifact push consumer's
    deliver_subject), which use the full pod name.

    Falls back to the full pod name if it doesn't end in "-<digits>"
    (e.g. local dev where HOSTNAME isn't a StatefulSet pod name).
    """
    pod_name = get_pod_name()
    parts = pod_name.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    logger.warning(
        f"Pod name '{pod_name}' does not end in '-<ordinal>' — "
        f"using full pod name as StatefulSet name"
    )
    return pod_name
