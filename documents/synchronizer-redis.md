# Synchronizer Redis — Design Document

> Branch: `feature/synchronizer-redis`
> Status: In progress
> Depends on: `feature/synchronizer-NATS` (merged into `main`)

---

## Where We Are

`feature/synchronizer-NATS` delivered:
- NATS connection to siteMC with `nats.creds`
- Durable pull consumers per func_id for both streams
- Fetch loops running, messages being received

**This branch:** metadata handling — initial Redis warm-up on startup and
incremental updates from NATS metadata messages for all four meta types.

---

## Scope

1. **Initial warm-up** — on synchronizer startup, fetch all four meta types
   from siteMC HTTP API for every configured `function_id` and write to Redis,
   before NATS consumers are created.
2. **Incremental updates** — handle `MLOP-MCS-METADATA` messages, dispatch
   by `meta_type`, fetch from siteMC HTTP API, update Redis.

---

## MetadataMessage Schema

```json
{
  "function_id": "funcID_123",
  "product_id": "productID_ABC",
  "meta_type": "model_list"
}
```

`meta_type` dispatches to which Redis store to update:
- `model_list` → `mcs:model_list:<function_id>`
- `kernel_list` → `mcs:kernel_list`
- `package_list` → `mcs:package_list`
- `pat_list` → `mcs:pat_list`

Note: `meta_type` is a new field on the metadata stream message (distinct
from `artifact_type` on the artifact stream).

For `model_list` updates, the message additionally carries:
```json
{
  "function_id": "funcID_123",
  "product_id": "productID_ABC",
  "meta_type": "model_list",
  "model_id": "uuid-of-updated-model"
}
```

---

## Redis Data Model

### model_list — hash per function_id, field per modelId

```
Key:   mcs:model_list:<function_id>
Field: <modelId>
Value: JSON of one model record {modelId, modelName, version, ...}
```

Operations:
- **Initial warm-up:** fetch full list → `HSET mcs:model_list:<function_id> <modelId> <json>` for each model
- **Incremental update:** siteMC notifies `model_id` changed → fetch full list for `function_id` → parse out `model_id` record → `HSET mcs:model_list:<function_id> <model_id> <json>` (one field update)

Why fine-grained: model updates arrive at `modelId` granularity. Updating
one field is O(1) and does not affect other models in the same function.

### kernel_list — single record per function_id

```
Key:   mcs:kernel_list
Field: <function_id>
Value: JSON {"kernelId": "...", "kernelVersion": "..."}
```

One kernel per function — full replace on update.

### package_list — single record per function_id

```
Key:   mcs:package_list
Field: <function_id>
Value: JSON {"packageId": "...", "packageVersion": "..."}
```

One package per function — full replace on update.

### pat_list — single record per function_id

```
Key:   mcs:pat_list
Field: <function_id>
Value: JSON ["1", "2", "3"]   (content array as-is from siteMC response)
```

MCS stores and returns the exact `content` from siteMC response — model
service uses the same siteMC API contract, so the response shape must not change.

---

## siteMC HTTP API Interfaces

All requests use Basic Auth from `productConfig`:
- `MODEL_CENTER_ACCOUNT` / `MODEL_CENTER_PASSWORD`

Query params (fixed): `functionId=<func_id>&productId=<product_id>&inline_=true&source=cache_service`

| meta_type | Endpoint | Response content |
|---|---|---|
| `model_list` | `GET {SITE_META_CACHE_SERVICE_URL}/meta-cache/model_list/{function_id}` | Raw model list payload (dict keyed by modelId) |
| `kernel_list` | `GET {SITE_META_CACHE_SERVICE_URL}/meta-cache/DATA_EXPORT/kernel-version/{function_id}` | `{"kernelId": "", "kernelVersion": ""}` (streaming) |
| `package_list` | `GET {SITE_META_CACHE_SERVICE_URL}/meta-cache/DATA_EXPORT/package-version/{function_id}` | `{"packageId": "", "packageVersion": ""}` (streaming) |
| `pat_list` | `GET {SITE_META_CACHE_SERVICE_URL}/meta-cache/pats/{function_id}` | `["1", "2", "3"]` |

All responses wrapped in: `{"statusCode": "...", "message": "...", "content": <data>}`
MCS extracts and stores `content` only.

---

## Startup Sequence (updated)

```
1.  Load one.properties + {Product}.json from /etc/config
2.  Connect NATS
3.  Verify streams exist
4.  Connect Redis Sentinel
5.  Resolve pod_name + statefulset_name
6.  ── INITIAL REDIS WARM-UP ──────────────────────────────────────────
    For each (function_id, product_id) in productConfig:
      a. Fetch model_list  → write to mcs:model_list:<function_id>   (per modelId)
      b. Fetch kernel_list → write to mcs:kernel_list  (field=function_id)
      c. Fetch package_list→ write to mcs:package_list (field=function_id)
      d. Fetch pat_list    → write to mcs:pat_list     (field=function_id)
    ─────────────────────────────────────────────────────────────────────
7.  Create artifact pull consumers (per pod, per func_id)
8.  Create metadata pull consumers (per statefulset, per func_id)
9.  Start fetch loops (artifact + metadata, per func_id)
10. /health returns 200
```

Warm-up (step 6) happens **before** consumers are created — ensures Redis
is fully populated before any NATS update messages are processed.

---

## Metadata Handler Dispatch

```python
dispatch = {
    "model_list":   handle_model_list_update,
    "kernel_list":  handle_kernel_list_update,
    "package_list": handle_package_list_update,
    "pat_list":     handle_pat_list_update,
}
```

### model_list update flow
```
msg.meta_type == "model_list"
  → extract function_id, product_id, model_id from msg
  → GET /meta-cache/model_list/{function_id}  (full list)
  → parse response["content"] to find record where modelId == model_id
  → HSET mcs:model_list:<function_id>  <model_id>  <json of that record>
  → msg.ack()
```

### kernel_list / package_list / pat_list update flow
```
msg.meta_type == "kernel_list"
  → extract function_id, product_id from msg
  → GET /meta-cache/DATA_EXPORT/kernel-version/{function_id}
  → extract response["content"]
  → HSET mcs:kernel_list  <function_id>  json(content)
  → msg.ack()
```

---

## NATS Message Models (updated)

```python
class MetaType(str, Enum):
    MODEL_LIST   = "model_list"
    KERNEL_LIST  = "kernel_list"
    PACKAGE_LIST = "package_list"
    PAT_LIST     = "pat_list"

class MetadataMessage(BaseModel):
    function_id: str
    product_id:  str
    meta_type:   MetaType
    model_id:    str | None = None  # only for meta_type == model_list
```

---

## Files to Create / Modify

### New files
| File | Purpose |
|---|---|
| `core/redis/kernel_list.py` | `get/set_kernel_list()` |
| `core/redis/package_list.py` | `get/set_package_list()` |
| `core/redis/pat_list.py` | `get/set_pat_list()` |
| `core/http/meta_client.py` | HTTP clients for all 4 siteMC meta endpoints |
| `apps/synchronizer/warmup.py` | Initial Redis warm-up logic |

### Modified files
| File | Change |
|---|---|
| `core/models/nats_messages.py` | Replace `MetadataMessage` with new schema (`meta_type`, `product_id`, `model_id`) |
| `core/config/settings.py` | Add `REDIS_KERNEL_LIST_KEY`, `REDIS_PACKAGE_LIST_KEY`, `REDIS_PAT_LIST_KEY` |
| `core/redis/model_list.py` | Change key from `mcs:model_list` (flat hash) to `mcs:model_list:<function_id>` (per-function hash) |
| `apps/synchronizer/handlers.py` | Extend `handle_metadata_message()` to dispatch all four types |
| `apps/synchronizer/lifespan.py` | Add warm-up step before consumer creation |
| `helm/mcs/values.yaml` | Add new Redis key env vars to `envConfig` |

---

## Task List

- [ ] Update `MetadataMessage` schema (`meta_type`, `product_id`, `model_id`)
- [ ] Add `REDIS_KERNEL_LIST_KEY`, `REDIS_PACKAGE_LIST_KEY`, `REDIS_PAT_LIST_KEY` to `settings.py` + `values.yaml`
- [ ] Update `core/redis/model_list.py` — change to per-function hash key
- [ ] Create `core/redis/kernel_list.py`, `package_list.py`, `pat_list.py`
- [ ] Create `core/http/meta_client.py` — all 4 siteMC meta HTTP clients
- [ ] Create `apps/synchronizer/warmup.py` — initial fetch + Redis write for all 4 types
- [ ] Update `apps/synchronizer/handlers.py` — dispatch by `meta_type`
- [ ] Update `apps/synchronizer/lifespan.py` — add warm-up before step 7
- [ ] Test: verify Redis populated on startup for all 4 types
- [ ] Test: publish NATS metadata messages and verify Redis updates correctly
