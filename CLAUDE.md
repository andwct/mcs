# MCS — Model Caching Service

## Overview

MCS is a Kubernetes StatefulSet service that acts as a **CDN-pattern cache** between Model Service and siteMC (site Model Center). It sits between them, caching ML model artifacts and metadata locally so Model Service can be served faster with lower latency and reduced load on siteMC.

**Repo:** https://github.com/andwct/mcs (private, periodically made public for pulls)
**Enterprise registry:** `tcrtst.fart.dummy.com`
**K8s namespace:** `mlop-site-model-center`
**StatefulSet:** 3 pods × 3 containers (`mcs`, `synchronizer`, `janitor`) — one Docker image, different entrypoints

---

## Architecture

```
Model Center NATS
      ↓
  MetaUpdater (Java) — bridges MC NATS → siteMC NATS
      ↓
siteMC NATS (MLOP-MCS-ARTIFACT / MLOP-MCS-METADATA streams)
      ↓
MCS Synchronizer — consumes NATS, populates Redis + PVC
      ↓
Redis Sentinel ← model_list / kernel_list / package_list / pat_list
PVC (ReadWriteOnce per pod) ← model / kernel / package files
      ↑
MCS Serving — serves Model Service requests (cache-hit from PVC/Redis, cache-miss fallback to siteMC)
      ↑
Model Service
```

---

## Three-Tier Hierarchy

```
Model Center → siteMC (siteModelCenter) → MCS
```

---

## Completed Branches

### `feature/synchronizer-NATS` → merged to `main` (PR #17)
- NATS JetStream connection to siteMC NATS
- Pull consumers per `func_id`:
  - Artifact: `artifact-sync-{pod_name}-{func_id}` (broadcast — all 3 pods)
  - Metadata: `metadata-sync-{statefulset_name}-{func_id}` (queue group — one pod)
- Fetch loops running for both streams

### `feature/synchronizer-redis` → merged to `main` (PR #23)
- Redis warm-up on startup before consumers created (blocks on failure → pod restarts)
- 4 meta types in Redis:
  - `mcs:model_list:{function_id}` — hash, field=`modelId`, value=JSON model record
  - `mcs:kernel_list` — hash, field=`function_id`
  - `mcs:package_list` — hash, field=`function_id`
  - `mcs:pat_list` — hash, field=`function_id`
- Incremental updates via NATS metadata messages
- Vault password loading: `/root/mcs-secret/{PRODUCT_NAME}_MODEL_CENTER_PASSWORD`

### `feature/synchronizer-artifact` (in progress, latest tag `v0.1.9-synchronizer-artifact`)
- Full artifact download pipeline (Phase 1 — verified working end-to-end)
- 3-step auth flow: `SiteAuthorizationService` → `SiteArtifactCacheService` → decrypt
- Model: RSA+AES-CBC tunnel (`SecurityModelServiceDataTunnel`)
- Kernel/Package: AES decrypt (`SecurityObjectStore`)
- Files stored at: `{STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{function_ID}/{id}/{version}`
  - e.g. `/mnt/mcs/mcs/MODEL/productID_ABC/funcID_123/modelUUID/v1.0.0`
- **Phase 2 (partial AES-GCM encryption at rest) — NOT IMPLEMENTED**
  - Model and kernel files currently stored as plaintext
  - Will need `.meta` segment map file alongside each artifact
  - Package files are NOT encrypted (no `.meta` needed)

### `feature/mcs-serving` (in progress, latest tag `v0.1.4-mcs-serving`)
- 7 FastAPI endpoints mirroring EdgeService exactly (drop-in replacement):
  - `POST /mcs/model` — cache-aware model streaming
  - `POST /mcs/kernel` — cache-aware kernel streaming
  - `POST /mcs/package` — cache-aware package streaming
  - `GET /mcs/model_list/{function_id}` — from Redis
  - `GET /mcs/kernel_list/{function_id}` — from Redis
  - `GET /mcs/package_list/{function_id}` — from Redis
  - `GET /mcs/active_pats/{function_id}` — from Redis
- HTTPBasic auth validated against `productConfig` `MODEL_CENTER_ACCOUNT`/`PASSWORD`
- Cache-hit: StreamingResponse from PVC
- Cache-miss: fallback to siteMC, tee-stream to client + write to PVC, single-flight lock
- Helm chart templates fully rewritten to match `values.yaml` structure
- Per-pod VirtualServices: `mcs-mcs-replica-{0,1,2}.{baseDomain}/docs`

---

## Key Design Decisions

### NATS Message Schemas

**`MLOP-MCS-ARTIFACT` stream**
Subject: `MLOP-MCS-ARTIFACT.{functionId}`

```json
{
  "functionId": "funcID_123",
  "productId": "productID_ABC",
  "artifactType": "MODEL",
  "deployedVersion": "v1.0.0",
  "modelId": "uuid-model-1",
  "kernelId": null,
  "packageId": null
}
```

**`MLOP-MCS-METADATA` stream**
Subject: `MLOP-MCS-METADATA.{functionId}`

```json
{
  "functionId": "funcID_123",
  "productId": "productID_ABC",
  "metaType": "model_list",
  "modelId": "uuid-model-1"
}
```

Note: Fields use **camelCase** in JSON (Java/JSON convention). Pydantic models use `alias_generator=to_camel` — Python code accesses fields as `snake_case`.

### Credentials
- `MODEL_CENTER_ACCOUNT`/`PASSWORD` are **product-level** (from `{Product}.json` productConfig), not global settings
- `MODEL_CENTER_PASSWORD` loaded from Vault-mounted file at startup via `core/k8s/configmap.py`

### PVC Layout
```
{STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{functionID}/{id}/{version}
```
e.g. `/mnt/mcs/mcs/MODEL/productID_ABC/funcID_123/modelUUID/v1.0.0`

Each pod has its own `ReadWriteOnce` PVC — `mcs-statefulset-storage-mcs-statefulset-{0,1,2}`

### Consumer Names (no function_name)
- `artifact-sync-{pod_name}-{func_id}`
- `metadata-sync-{statefulset_name}-{func_id}`

### EdgeService Integration
- `core/http/site_authorization.py` — pasted from EdgeService
- `core/http/site_artifact_service.py` — pasted from EdgeService
- `core/utils/security.py` — pasted from EdgeService (one change: `Crypto.Random` → `Cryptodome.Random`)
- `core/utils/reqeust.py` — written by MCS, mirrors EdgeService's `RetrySession`/`RetrySessionAsync`
- `settings.PRODUCTS` property delegates to `state.get_all_products()` for EdgeService compat

---

## MetaUpdater (Java — separate service, in design)

Bridge between Model Center NATS and siteMC NATS. Design doc: `documents/meta-updater.md`

**MC NATS incoming message** (`MCS-UPDATE` stream, subject `MCS-UPDATE.{functionId}`):
```json
{
  "updateType": "ARTIFACT" | "METADATA",
  "functionId": "funcID_123",
  "functionName": "funcName_123",
  "productId": "productID_ABC",
  "artifactType": "MODEL",
  "deployedVersion": "v1.0.0",
  "modelId": "uuid",
  "kernelId": null,
  "packageId": null,
  "metaType": "model_list"
}
```

Transformation: routes to `MLOP-MCS-ARTIFACT.{functionId}` or `MLOP-MCS-METADATA.{functionId}` based on `updateType`.

**New work needed in MetaUpdater:** JetStream publish logic to siteMC NATS (subscribe logic already exists).

---

## Key File Locations

```
apps/
  synchronizer/main.py, lifespan.py, consumers.py, fetch_loop.py, handlers.py, state.py, warmup.py, router.py
  mcs/main.py, lifespan.py, router.py, auth.py
  janitor/main.py (stub — not yet implemented)
core/
  config/settings.py
  models/nats_messages.py, product.py, artifact_models.py, api_models.py
  k8s/bootstrap.py, configmap.py, pod.py
  nats/client.py
  redis/client.py, model_list.py, kernel_list.py, package_list.py, pat_list.py
  http/site_authorization.py (EdgeService), site_artifact_service.py (EdgeService), meta_client.py
  utils/security.py (EdgeService), reqeust.py
  artifact_service.py
helm/mcs/
  values.yaml
  templates/sts.yaml, cm.yaml, service.yaml, vaultsecret.yaml, virtualservice.yaml, virtualservice-replica.yaml, _helpers.tpl
documents/
  synchronizer-NATS.md, synchronizer-redis.md, synchronizer-artifact.md, mcs-serving.md, meta-updater.md
Build/Dockerfile
requirements.txt
CLAUDE.md (this file)
```

---

## Pending / Next Steps

### Immediate (deployment stabilization)
- [ ] Fix 503 on `mcs-mcs-replica-0.{baseDomain}/docs` — mcs container not responding
  - Check `kubectl logs mcs-statefulset-0 -c mcs` for startup errors
  - Verify all 3 containers show `READY` (`kubectl get pod mcs-statefulset-0`)
- [ ] Test all 7 mcs-serving endpoints via Swagger UI once pod is healthy
- [ ] Verify Redis meta serving (model_list / kernel_list / package_list / active_pats)

### Phase 2 — Partial encryption at rest (separate branch)
- [ ] Copy partial AES-GCM encryption code from EdgeService `security.py`
- [ ] Generate `{version}.meta` segment map file on write (model + kernel only)
- [ ] Apply partial encryption after download before writing to PVC
- [ ] Decrypt on read in `mcs-serving` cache-hit path (before streaming to Model Service)
- [ ] Package artifacts exempt — no `.meta` file

### MetaUpdater (Java)
- [ ] Implement JetStream publish logic to siteMC NATS
- [ ] Two outgoing connections: MC NATS (subscribe) + siteMC NATS (publish)
- [ ] Transform `MCS-UPDATE` → `MLOP-MCS-ARTIFACT.{functionId}` or `MLOP-MCS-METADATA.{functionId}`
- [ ] Separate siteMC NATS credentials for MetaUpdater

### Janitor (separate branch)
- [ ] Implement PVC eviction logic (currently a stub)
- [ ] High/low watermark based eviction (`JANITOR_HIGH_WATERMARK`, `JANITOR_LOW_WATERMARK`)

### PRs to merge
- [ ] `feature/synchronizer-artifact` → `main` (after deployment verified)
- [ ] `feature/mcs-serving` → `main` (after Swagger UI testing passes)

---

## Settings Reference

Key `one.properties` settings:
```
NATS_URL, NATS_CREDS_FILE, NATS_ARTIFACT_STREAM=MLOP-MCS-ARTIFACT, NATS_METADATA_STREAM=MLOP-MCS-METADATA
REDIS_SENTINEL_HOSTS, REDIS_SENTINEL_PORT=26379, REDIS_SENTINEL_MASTER_NAME=mymaster
STORAGE_PATH=/mnt/mcs, FAB_NAME=mcs, STAGE_NAME=SIT
SITE_AUTHORIZATION_URL, SITE_ARTIFACT_SERVICE_URL, SITE_META_CACHE_SERVICE_URL
SECRET_MOUNT_PATH=/root/mcs-secret
DOWNLOAD_CHUNK_SIZE=65536
```

Key `{Product}.json` productConfig settings:
```
PRODUCT_ID, PRODUCT_NAME, ENABLE_VAULT, MODEL_CENTER_VAULT_PATH
MODEL_CENTER_ACCOUNT, MODEL_CENTER_PASSWORD (empty — loaded from Vault)
FUNCTION_LIST, FUNCTION_NAME_MAPPING
```
