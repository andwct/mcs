# Synchronizer Redis — Design Document

> Branch: `feature/synchronizer-redis`
> Status: In progress
> Depends on: `feature/synchronizer-NATS` (merged into `main`)

---

## Where We Are

`feature/synchronizer-NATS` delivered:
- NATS connection to siteMC with `nats.creds`
- Durable pull consumers per func_id for both streams:
  - `MLOP-MCS-ARTIFACT` — broadcast (one consumer per pod per func_id)
  - `MLOP-MCS-METADATA` — queue-group (one shared consumer per statefulset per func_id)
- Fetch loops running, messages being received

**Current gap:** `handle_metadata_message()` in `handlers.py` only processes
`artifact_type == "model_list"` and ignores `kernel_list`, `package_list`,
`pat_list`. This branch completes the metadata Redis update for all four types.

---

## Scope of This Branch

Handle all four metadata artifact types from `MLOP-MCS-METADATA` stream and
store each in Redis Sentinel, keyed by `function_id`.

| artifact_type | Redis hash key | field | value |
|---|---|---|---|
| `model_list` | `mcs:model_list` | `function_id` | JSON `{modelId: {...}}` |
| `kernel_list` | `mcs:kernel_list` | `function_id` | JSON `{kernelId: {...}}` |
| `package_list` | `mcs:package_list` | `function_id` | JSON `{packageId: {...}}` |
| `pat_list` | `mcs:pat_list` | `function_id` | JSON `{patId: {...}}` |

Each is stored as a Redis **hash** — same pattern as `model_list`:
```
HSET mcs:<type>_list  <function_id>  <json>
HGET mcs:<type>_list  <function_id>
```

---

## Message Schema

The `MLOP-MCS-METADATA` stream carries one `MetadataMessage` shape for all
four types. `artifact_type` determines which list is being updated.

```python
class MetadataMessage(BaseModel):
    function_id: str
    artifact_type: ArtifactType   # model_list | kernel_list | package_list | pat_list
    online: list[...]             # list of records for that artifact type
```

### Record schemas

Each record in `online` follows the same flat structure as `ModelRecord` —
only the ID field name differs per type. All other fields are stored as-is
via `extra="allow"`.

| artifact_type | ID field | Redis key |
|---|---|---|
| `model_list` | `modelId` | `mcs:model_list` |
| `kernel_list` | `kernelId` | `mcs:kernel_list` |
| `package_list` | `packageId` | `mcs:package_list` |
| `pat_list` | `patId` | `mcs:pat_list` |

---

## Design Decisions

### 1. One generic record model vs four specific models

Since all four types share the same flat structure with `extra="allow"`,
and MCS only uses the ID field semantically (as the Redis hash field key),
we use a **single generic `ListRecord` model** with a configurable ID field
name, rather than four separate Pydantic models.

```python
class ListRecord(BaseModel):
    model_config = ConfigDict(extra="allow")
    # ID field varies by type — resolved dynamically
```

### 2. Redis key names — configurable via settings

All four Redis hash key names are configurable via `settings.py` / `one.properties`:

```
REDIS_MODEL_LIST_KEY   = mcs:model_list    (already exists)
REDIS_KERNEL_LIST_KEY  = mcs:kernel_list
REDIS_PACKAGE_LIST_KEY = mcs:package_list
REDIS_PAT_LIST_KEY     = mcs:pat_list
```

### 3. Handler dispatch

`handle_metadata_message()` dispatches to a type-specific handler based on
`artifact_type`:

```python
dispatch = {
    ArtifactType.MODEL_LIST:   handle_model_list,
    ArtifactType.KERNEL_LIST:  handle_kernel_list,
    ArtifactType.PACKAGE_LIST: handle_package_list,
    ArtifactType.PAT_LIST:     handle_pat_list,
}
```

### 4. Idempotency

Redis `HSET` is idempotent — writing the same data twice has no side effects.
No additional version-checking needed at this stage.

---

## Files to Create / Modify

### New files
| File | Purpose |
|---|---|
| `core/redis/kernel_list.py` | `get/set_kernel_list()` helpers |
| `core/redis/package_list.py` | `get/set_package_list()` helpers |
| `core/redis/pat_list.py` | `get/set_pat_list()` helpers |

### Modified files
| File | Change |
|---|---|
| `core/models/nats_messages.py` | Add `KernelRecord`, `PackageRecord`, `PatRecord` (or generic `ListRecord`) |
| `core/config/settings.py` | Add `REDIS_KERNEL_LIST_KEY`, `REDIS_PACKAGE_LIST_KEY`, `REDIS_PAT_LIST_KEY` |
| `apps/synchronizer/handlers.py` | Extend `handle_metadata_message()` to dispatch all four types |
| `helm/mcs/values.yaml` | Add new Redis key env vars to `envConfig` |
| `documents/synchronizer-redis.md` | This file |

---

## Open Questions

1. **`kernelId`, `packageId`, `patId` field names** — need to confirm exact
   field names in the `online` records for each type. Are they consistent
   naming conventions (camelCase like `modelId`)?

2. **`pat_list` structure** — what does a PAT (Personal Access Token?) record
   look like? Does it have a unique ID field?

3. **Subject naming for non-model types** — the NATS consumers currently use
   `MLOP-MCS-METADATA.{func_id}-{sanitized_name}` as `filter_subject`. Since
   `kernel_list`, `package_list`, `pat_list` messages arrive on the same
   stream/subject, the existing consumers will receive them automatically —
   no new consumers needed. ✅

---

## Task List

- [ ] Confirm ID field names for kernel, package, pat records
- [ ] Add `REDIS_KERNEL_LIST_KEY`, `REDIS_PACKAGE_LIST_KEY`, `REDIS_PAT_LIST_KEY` to `settings.py`
- [ ] Add new Redis list helpers (`kernel_list.py`, `package_list.py`, `pat_list.py`)
- [ ] Extend `nats_messages.py` with generic/specific record models
- [ ] Extend `handle_metadata_message()` to dispatch all four types
- [ ] Update `values.yaml` with new Redis key env vars
- [ ] Test: publish test messages for each type, verify Redis updated correctly
