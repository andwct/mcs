from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── NATS ──────────────────────────────────────────────────────────────
    NATS_URL: str = "nats://localhost:4222"
    NATS_CREDS_FILE: str = "/vault/secrets/nats.creds"
    # Stream names — must match streams pre-created in siteMC NATS.
    # Configured via helm values.yaml → ConfigMap → env vars.
    NATS_ARTIFACT_STREAM: str = "MLOP-MCS-ARTIFACT"
    NATS_METADATA_STREAM: str = "MLOP-MCS-METADATA"
    NATS_ACK_WAIT_ARTIFACT_SECONDS: int = 300
    NATS_ACK_WAIT_METADATA_SECONDS: int = 30

    # ── Redis Sentinel (Bitnami chart 18.2.0 / Redis 7.2.4) ───────────────
    # Comma-separated list of sentinel host:port pairs.
    # Bitnami headless service: <release>-redis-headless
    REDIS_SENTINEL_HOSTS: str = "localhost"
    REDIS_SENTINEL_PORT: int = 26379
    REDIS_SENTINEL_MASTER_NAME: str = "mymaster"
    REDIS_PASSWORD: str = ""
    # Redis hash keys for meta lists.
    # model_list uses per-function hash: mcs:model_list:{function_id}
    # Others use a shared hash: key=field(function_id), value=JSON content
    REDIS_MODEL_LIST_KEY_PREFIX: str = "mcs:model_list"
    REDIS_KERNEL_LIST_KEY: str = "mcs:kernel_list"
    REDIS_PACKAGE_LIST_KEY: str = "mcs:package_list"
    REDIS_PAT_LIST_KEY: str = "mcs:pat_list"

    # ── Vault / Secrets ───────────────────────────────────────────────────
    # Path where ricoberger VSO operator mounts Vault secret files.
    # Matches the mountPath defined in Helm chart volumeMounts.
    SECRET_MOUNT_PATH: str = "/root/mcs-secret"

    CONFIGMAP_MOUNT_PATH: str = "/etc/config"

    # ── Storage ───────────────────────────────────────────────────────────
    STORAGE_PATH: str = "/mnt/models"

    # ── siteMC HTTP ───────────────────────────────────────────────────────
    SITE_AUTHORIZATION_URL: str = ""
    SITE_ARTIFACT_SERVICE_URL: str = ""
    SITE_META_CACHE_SERVICE_URL: str = ""
    # Separate timeouts: artifact fetches can be slow (large model files),
    # meta cache fetches are lightweight JSON responses.
    META_CACHE_REQUEST_TIMEOUT_SECONDS: int = 30

    # ── App ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    STAGE_NAME: str = "SIT"
    APP_NAME: str = "mcs"

    # ── Janitor ───────────────────────────────────────────────────────────
    JANITOR_INTERVAL_SECONDS: int = 300
    JANITOR_HIGH_WATERMARK: float = 0.90
    JANITOR_LOW_WATERMARK: float = 0.75


@lru_cache
def get_settings() -> Settings:
    return Settings()
