"""
Shared Prometheus metric definitions for all 3 containers (mcs, synchronizer,
janitor). Each container is a separate process, so metrics registered here
only exist in whichever process imports them — no cross-container leakage.

Exposed via GET /metrics in each app (see apps/*/main.py) using
prometheus_client's default global REGISTRY.
"""
from prometheus_client import Counter, Gauge, Histogram

# ── mcs (serving) ────────────────────────────────────────────────────────────

MCS_REQUESTS_TOTAL = Counter(
    "mcs_requests_total",
    "Total mcs-serving API requests",
    ["endpoint", "status"],
)

MCS_CACHE_HITS_TOTAL = Counter(
    "mcs_cache_hits_total",
    "Artifact requests served from PVC cache",
    ["artifact_type"],
)

MCS_CACHE_MISSES_TOTAL = Counter(
    "mcs_cache_misses_total",
    "Artifact requests that fell back to siteMC",
    ["artifact_type"],
)

MCS_SITEMC_FETCH_DURATION_SECONDS = Histogram(
    "mcs_sitemc_fetch_duration_seconds",
    "Time spent fetching an artifact from siteMC on cache miss",
    ["artifact_type"],
)

MCS_CACHE_CORRUPT_TOTAL = Counter(
    "mcs_cache_corrupt_total",
    "Cache entries deleted due to being orphaned or undecryptable",
    ["artifact_type", "reason"],  # reason: orphaned_meta | decrypt_failure
)

# ── synchronizer ─────────────────────────────────────────────────────────────

SYNC_NATS_MESSAGES_TOTAL = Counter(
    "synchronizer_nats_messages_total",
    "NATS messages processed by the synchronizer",
    ["stream", "result"],  # stream: artifact | metadata; result: ack | nak
)

SYNC_ARTIFACT_DOWNLOAD_DURATION_SECONDS = Histogram(
    "synchronizer_artifact_download_duration_seconds",
    "Time to download+decrypt+store one artifact",
    ["artifact_type"],
)

SYNC_ARTIFACT_DOWNLOAD_FAILURES_TOTAL = Counter(
    "synchronizer_artifact_download_failures_total",
    "Artifact downloads that failed and were NAKed",
    ["artifact_type"],
)

SYNC_METADATA_UPDATES_TOTAL = Counter(
    "synchronizer_metadata_updates_total",
    "Metadata updates applied to Redis",
    ["meta_type"],
)

SYNC_REDIS_WARMUP_DURATION_SECONDS = Histogram(
    "synchronizer_redis_warmup_duration_seconds",
    "Time to complete initial Redis warm-up on startup",
)

# ── janitor ──────────────────────────────────────────────────────────────────

JANITOR_DISK_USAGE_RATIO = Gauge(
    "janitor_disk_usage_ratio",
    "Current PVC usage as a fraction of total capacity (0.0-1.0)",
)

JANITOR_EVICTION_SWEEPS_TOTAL = Counter(
    "janitor_eviction_sweeps_total",
    "Eviction sweeps run",
    ["result"],  # result: completed | exhausted (didn't reach LOW_WATERMARK)
)

JANITOR_FILES_EVICTED_TOTAL = Counter(
    "janitor_files_evicted_total",
    "Files deleted by eviction sweeps",
)

JANITOR_BYTES_FREED_TOTAL = Counter(
    "janitor_bytes_freed_total",
    "Bytes freed by eviction sweeps",
)

JANITOR_EVICTION_DURATION_SECONDS = Histogram(
    "janitor_eviction_duration_seconds",
    "Time to complete one eviction sweep",
)
