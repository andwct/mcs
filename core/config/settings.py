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
    REDIS_MODEL_LIST_KEY: str = "mcs:model_list"

    # ── ConfigMap ─────────────────────────────────────────────────────────
    CONFIGMAP_MOUNT_PATH: str = "/etc/config"
    CONFIGMAP_ONE_PROPERTIES: str = "one.properties"

    # ── Storage ───────────────────────────────────────────────────────────
    STORAGE_PATH: str = "/mnt/models"

    # ── siteMC HTTP ───────────────────────────────────────────────────────
    SITE_ARTIFACT_SERVICE_URL: str = ""
    SITE_META_CACHE_SERVICE_URL: str = ""

    # ── Auth ──────────────────────────────────────────────────────────────
    MODEL_CENTER_ACCOUNT: str = ""
    MODEL_CENTER_PASSWORD: str = ""

    # ── App ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
