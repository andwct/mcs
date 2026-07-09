import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

_ARTIFACT_TYPES = ("MODEL", "KERNEL", "PACKAGE")


def is_over_high_watermark(storage_path: str, high_watermark: float) -> bool:
    usage = shutil.disk_usage(storage_path)
    return (usage.used / usage.total) > high_watermark


def _collect_candidates(storage_path: Path, fab_name: str) -> list[tuple[Path, os.stat_result]]:
    """Walk MODEL/KERNEL/PACKAGE dirs, return (path, stat) for all non-.tmp files."""
    candidates = []
    base = storage_path / fab_name
    for artifact_type in _ARTIFACT_TYPES:
        type_dir = base / artifact_type
        if not type_dir.exists():
            continue
        for root, _, files in os.walk(type_dir):
            for name in files:
                if name.endswith(".tmp"):
                    continue
                path = Path(root) / name
                try:
                    candidates.append((path, path.stat()))
                except FileNotFoundError:
                    pass  # deleted between walk and stat — skip
    return candidates


def _prune_empty_parents(file_path: Path, storage_root: Path) -> None:
    """Remove empty ancestor directories up to (not including) storage_root."""
    parent = file_path.parent
    while parent != storage_root and parent != storage_root.parent:
        try:
            parent.rmdir()  # no-op if not empty
            parent = parent.parent
        except OSError:
            break


def run_eviction_sweep(
    storage_path: str,
    fab_name: str,
    high_watermark: float,
    low_watermark: float,
) -> None:
    root = Path(storage_path)
    usage = shutil.disk_usage(storage_path)
    if usage.total == 0:
        return

    candidates = _collect_candidates(root, fab_name)
    candidates.sort(key=lambda x: x[1].st_atime)  # least recently accessed first (LRU)

    freed_bytes = 0
    for file_path, stat in candidates:
        current_used = usage.used - freed_bytes
        if (current_used / usage.total) <= low_watermark:
            break
        try:
            freed_bytes += stat.st_size
            os.remove(file_path)
            _prune_empty_parents(file_path, root)
            logger.info(
                f"Evicted: {file_path} "
                f"size={stat.st_size} atime={stat.st_atime:.0f}"
            )
        except FileNotFoundError:
            pass  # already gone — count size anyway since it freed space

    final_used = usage.used - freed_bytes
    if (final_used / usage.total) > low_watermark:
        logger.warning(
            f"Eviction exhausted all candidates but usage "
            f"{final_used / usage.total:.1%} still exceeds "
            f"LOW_WATERMARK {low_watermark:.1%}. "
            f"Consider setting JANITOR_LOW_WATERMARK to a higher value."
        )
    else:
        logger.info(
            f"Eviction complete: freed {freed_bytes} bytes, "
            f"estimated usage now {final_used / usage.total:.1%}"
        )
