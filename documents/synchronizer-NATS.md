# Synchronizer NATS — Design & Task Tracker

> Branch: `feature/synchronizer-NATS`
> Latest tag: `v0.3.7-synchronizer-nats`
> Status: ✅ Connected to siteMC NATS in SIT, consumers created, fetch loops running
> Goal: synchronizer connects to siteMC NATS, creates durable consumers for
> artifact broadcast and metadata queue-group, processes messages to update
> Redis and fetch model artifacts.

---

## Final Design (v0.3.7)

### Consumer Pattern — Pull for Both Streams

Both `MLOP-MCS-ARTIFACT` and `MLOP-MCS-METADATA` use **pull consumers with
fetch loops**. Push consumers were considered and rejected in favour of pull
for consistency, backpressure, and simpler reconnect logic.

| | Artifact | Metadata |
|---|---|---|
| **Stream** | `MLOP-MCS-ARTIFACT` | `MLOP-MCS-METADATA` |
| **Stream interest subject** | `MLOP-MCS-ARTIFACT.>` | `MLOP-MCS-METADATA.>` |
| **Consumer type** | Pull | Pull |
| **Scope** | Per (pod, func_id) | Per (statefulset, func_id) |
| **Consumer name** | `artifact-sync-{pod_name}-{func_id}` | `metadata-sync-{statefulset_name}-{func_id}` |
| **filter_subject** | `MLOP-MCS-ARTIFACT.{func_id}-{sanitized_name}` | `MLOP-MCS-METADATA.{func_id}-{sanitized_name}` |
| **Semantics** | Broadcast — own consumer per pod | Queue-group — shared consumer across 3 pods |
| **Fetch loop** | One `asyncio.Task` per func_id | One `asyncio.Task` per func_id |
| **Handler** | `handle_artifact_message()` | `handle_metadata_message()` |

**Subject format:** `filter_subject` must be a subset of the stream's interest
subject. Both streams were created with `{STREAM_NAME}.>` as their interest
subject, so all consumer filter subjects must be prefixed with the stream name:
- `MLOP-MCS-ARTIFACT.{func_id}-{sanitized_name}` — valid subset of `MLOP-MCS-ARTIFACT.>`
- `MLOP-MCS-METADATA.{func_id}-{sanitized_name}` — valid subset of `MLOP-MCS-METADATA.>`

When siteMC publishes messages to these streams, it must publish to subjects
matching these patterns.

### Why Pull for Both

- **Consistent codebase** — one pattern instead of push+pull mix
- **Backpressure** — pod fetches only when ready; push can overwhelm during
  large model file downloads
- **Simpler reconnect** — `pull_subscribe_bind()` only, no subscription state
- **Already proven** — fetch loop working in SIT for metadata stream

### Why `filter_subject` (singular, not plural)

`filter_subjects` (list) requires NATS server **2.10+**. siteMC NATS runs
**2.9.20-alpine** which silently ignores `filter_subjects`. Using
`filter_subject` (singular) with one consumer per func_id is the correct
approach for NATS 2.9.x and provides proper subject isolation across
multiple MCS deployments sharing the same streams.

### Consumer Naming

```
Artifact (per pod, per func_id):
  artifact-sync-mcs-statefulset-0-funcID_123  filter: MLOP-MCS-ARTIFACT.funcID_123-funcName_123
  artifact-sync-mcs-statefulset-0-funcID_456  filter: MLOP-MCS-ARTIFACT.funcID_456-funcName_456
  artifact-sync-mcs-statefulset-1-funcID_123  filter: MLOP-MCS-ARTIFACT.funcID_123-funcName_123
  ...

Metadata (per statefulset, per func_id):
  metadata-sync-mcs-statefulset-funcID_123  filter: MLOP-MCS-METADATA.funcID_123-funcName_123
  metadata-sync-mcs-statefulset-funcID_456  filter: MLOP-MCS-METADATA.funcID_456-funcName_456
```

`pod_name` = `HOSTNAME` env var (K8s downward API) e.g. `mcs-statefulset-0`
`statefulset_name` = `pod_name` with `-{ordinal}` stripped e.g. `mcs-statefulset`

### Fetch Loop Per func_id

```
Per pod, per func_id:
  asyncio.Task: artifact-fetch-{func_id}  → psub.fetch(batch=1, timeout=5s)
  asyncio.Task: metadata-fetch-{func_id}  → psub.fetch(batch=1, timeout=5s)

2 func_ids → 4 total asyncio.Tasks per pod
```

Server-side long-poll — no busy spin. Independent per func_id — one func_id's
backlog cannot block another.

### Startup Sequence

```
1. Load one.properties + {Product}.json from /etc/config
2. Connect NATS (nats.creds from Vault)
3. Verify MLOP-MCS-ARTIFACT stream exists  → RuntimeError if missing
4. Verify MLOP-MCS-METADATA stream exists  → RuntimeError if missing
5. Connect Redis Sentinel
6. Resolve pod_name (HOSTNAME) + statefulset_name (strip ordinal)
7. Per func_id: ensure_artifact_consumer() — pull, per (pod, func_id)
8. Per func_id: ensure_metadata_consumer() — pull, per (statefulset, func_id)
9. start_fetch_loops() — 2 tasks per func_id (artifact + metadata)
10. /health returns 200 — synchronizer ready
```

---

## NATS User Permissions

MCS user: `mcs-{statefulset_name}-synchronizer`
Operator: `mlp` | Account: `mlop`

Required permissions:

```bash
--allow-pub '$JS.API.INFO'
--allow-pub '$JS.API.STREAM.INFO.MLOP-MCS-ARTIFACT'
--allow-pub '$JS.API.STREAM.INFO.MLOP-MCS-METADATA'
--allow-pub '$JS.API.CONSUMER.CREATE.MLOP-MCS-ARTIFACT.>'
--allow-pub '$JS.API.CONSUMER.CREATE.MLOP-MCS-METADATA.>'
--allow-pub '$JS.API.CONSUMER.DURABLE.CREATE.MLOP-MCS-ARTIFACT.>'
--allow-pub '$JS.API.CONSUMER.DURABLE.CREATE.MLOP-MCS-METADATA.>'
--allow-pub '$JS.API.CONSUMER.INFO.MLOP-MCS-ARTIFACT.>'
--allow-pub '$JS.API.CONSUMER.INFO.MLOP-MCS-METADATA.>'
--allow-pub '$JS.API.CONSUMER.MSG.NEXT.MLOP-MCS-ARTIFACT.>'
--allow-pub '$JS.API.CONSUMER.MSG.NEXT.MLOP-MCS-METADATA.>'
--allow-pub '$JS.API.CONSUMER.DELETE.MLOP-MCS-ARTIFACT.>'
--allow-sub '_INBOX.>'
```

Note: `--allow-sub 'artifact-sync-*.deliver'` is **NOT needed** — artifact
stream uses pull consumers (no deliver_subject).

Full script: `documents/generate-mcs-creds.sh`

---

## Bugs Fixed on This Branch

| Issue | Description | Fix |
|---|---|---|
| #4 | `credentials=` → `user_credentials=` | `core/nats/client.py` |
| #5 | `js.consumer()` doesn't exist | `pull_subscribe_bind()` in `fetch_loop.py` |
| #6 | `js.find_stream()` doesn't exist | `stream_info()` in `core/nats/client.py` |
| #7 | `nc.subscribe()` wrong for JetStream | `js.subscribe_bind(manual_ack=True)` → later removed entirely (pull) |
| #11 | Missing `nkeys` dependency | Added to `requirements.txt` |
| #12 | `get_settings()` fires before bootstrap | Moved inside functions in 10 files |
| #13 | `core/k8s/bootstrap.py` missing | New file added |
| #15 | `filter_subjects` not supported on NATS 2.9.20 | Reverted to `filter_subject` per func_id |
| #16 | One consumer per func_id for isolation | Implemented per-func_id consumers |
| #18 | Push consumer replaced with pull | Unified pull pattern for both streams |
| #19 | `filter_subject` not a subset of stream interest subject | Prefixed subjects with stream name: `MLOP-MCS-ARTIFACT.{func_id}-{sanitized_name}` |

---

## Files Changed on This Branch (vs main)

### Replace in enterprise codebase
| File | Key change |
|---|---|
| `apps/synchronizer/consumers.py` | Pull consumers, `filter_subject` per func_id with stream prefix |
| `apps/synchronizer/fetch_loop.py` | Both artifact + metadata fetch loops, 5-tuple unpack |
| `apps/synchronizer/handlers.py` | `get_settings()` inside functions |
| `apps/synchronizer/lifespan.py` | Pull-only, 5-tuple unpack, stream-specific subjects |
| `apps/synchronizer/main.py` | Bootstrap called first |
| `core/config/settings.py` | Added `JANITOR_*`, `STAGE_NAME`, `APP_NAME` |
| `core/http/artifact_client.py` | `get_settings()` inside functions |
| `core/k8s/configmap.py` | `*.json` glob, returns 5-tuple with artifact+metadata subjects |
| `core/k8s/pod.py` | Added `get_statefulset_name()` |
| `core/models/product.py` | Added `get_artifact_subject()`, `get_metadata_subject()` |
| `core/nats/client.py` | `user_credentials=`, `stream_info()` |
| `core/redis/client.py` | Sentinel client, `get_settings()` inside functions |
| `core/redis/model_list.py` | `get_settings()` inside functions |
| `helm/mcs/values.yaml` | Fixed `appModule: apps.X.main:app` |
| `requirements.txt` | Added `nkeys==0.2.1` |

### Add to enterprise codebase (new files)
| File | What it does |
|---|---|
| `core/k8s/bootstrap.py` | Loads `one.properties` into `os.environ` before `get_settings()` |
| `documents/generate-mcs-creds.sh` | `nsc` script for generating `nats.creds` |

---

## Outstanding Action Items

- [ ] Delete old consumers from NATS (created without `filter_subject`):
  ```bash
  nats consumer rm MLOP-MCS-ARTIFACT artifact-sync-mcs-statefulset-0
  nats consumer rm MLOP-MCS-ARTIFACT artifact-sync-mcs-statefulset-1
  nats consumer rm MLOP-MCS-ARTIFACT artifact-sync-mcs-statefulset-2
  nats consumer rm MLOP-MCS-METADATA metadata-sync-mcs-statefulset
  ```
- [ ] Update NATS user — add `$JS.API.CONSUMER.MSG.NEXT.MLOP-MCS-ARTIFACT.>`,
  remove `--allow-sub artifact-sync-*.deliver` (no longer needed):
  ```bash
  nsc edit user mcs-mcs-statefulset-synchronizer \
    --allow-pub '$JS.API.CONSUMER.MSG.NEXT.MLOP-MCS-ARTIFACT.>'
  nsc generate creds -a mlop -n mcs-mcs-statefulset-synchronizer > nats.creds
  # Update Vault + restart pod
  ```
- [ ] Rebuild image from `v0.3.7-synchronizer-nats` and redeploy
- [ ] Verify new consumers created in NATS:
  ```bash
  nats consumer ls MLOP-MCS-ARTIFACT --creds nats.creds --server <NATS_URL>
  nats consumer ls MLOP-MCS-METADATA --creds nats.creds --server <NATS_URL>
  ```
- [ ] Test artifact message:
  ```bash
  nats pub MLOP-MCS-ARTIFACT.{func_id}-{sanitized_name} \
    '{"function_id":"...","artifact_type":"model","deployed_version":"v1.0.0"}' \
    --creds nats.creds --server <NATS_URL>
  ```
- [ ] Test metadata message:
  ```bash
  nats pub MLOP-MCS-METADATA.{func_id}-{sanitized_name} \
    '{"function_id":"...","artifact_type":"model_list","online":[...]}' \
    --creds nats.creds --server <NATS_URL>
  ```
  Verify Redis updated:
  ```bash
  redis-cli -h <redis-host> HGET mcs:model_list <func_id>
  ```
- [ ] Merge `feature/synchronizer-NATS` → `main` via PR #17
