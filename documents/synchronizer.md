# Synchronizer — Design Document

> Status: In progress  
> Repo: `https://github.com/andwct/mcs`  
> Last updated: 2025-06

---

## 1. What Is the Synchronizer

The synchronizer is one of three containers running inside each MCS StatefulSet pod. It is
an event-driven NATS JetStream consumer that replaces the legacy ConfigMap-watch +
APScheduler polling architecture.

Its two responsibilities:

| Responsibility | Stream | Consumer type |
|---|---|---|
| Fetch model artifact files into pod PVC | `MLOP-MCS-ARTIFACT` | Push (broadcast) |
| Update `model_list` in Redis | `MLOP-MCS-METADATA` | Pull (queue group) |

The synchronizer is a **FastAPI app served by uvicorn**, exposing only a `/health` endpoint.
All real work happens in async background tasks started during FastAPI lifespan startup.

---

## 2. ConfigMap Structure

MCS reads product and function configuration from a Kubernetes ConfigMap mounted at startup.

**Namespace:** `mlop-site-model-center`

### `one.properties`
Global site configuration:
```properties
SITE_AUTHORIZATION_URL=
SITE_ARTIFACT_SERVICE_URL=
SITE_META_CACHE_SERVICE_URL=
STORAGE_PATH=
PRODUCTS_PATH=
ENABLE_VAULT=true
VAULT_PATH=
ENCRYPT_MODEL=true
APP_NAME=
```

### `product_X.json` (one per product)
```json
{
  "PRODUCT_ID": "product-a",
  "PRODUCT_NAME": "Product A",
  "ENABLE_VAULT": true,
  "VAULT_PATH": "secret/...",
  "MODEL_CENTER_ACCOUNT": "",
  "MODEL_CENTER_PASSWORD": "",
  "FUNCTION_LIST": ["FUNC_AAA", "FUNC_BBB"],
  "FUNC_NAME_MAPPING": {
    "FUNC_AAA": "AAA",
    "FUNC_BBB": "BBB"
  }
}
```

### Subject Derivation
For each `function_id` in `FUNCTION_LIST`, the NATS subject is:
```
{function_id}-{sanitized_function_name}

e.g. FUNC_AAA → "FUNC_AAA-AAA"
     FUNC_BBB → "FUNC_BBB-BBB"
```

Sanitized name is looked up from `FUNC_NAME_MAPPING[function_id]`.

---

## 3. Project Structure

```
mcs/
├── apps/
│   ├── synchronizer/
│   │   ├── main.py          # FastAPI app entrypoint (uvicorn)
│   │   ├── lifespan.py      # startup/shutdown: connect NATS, create consumers, start tasks
│   │   ├── consumers.py     # consumer creation logic (push + pull)
│   │   ├── handlers.py      # artifact_handler, metadata_handler
│   │   ├── fetch_loop.py    # pull consumer fetch loop (one task per function_id)
│   │   └── router.py        # /health endpoint
│   ├── mcs/
│   │   ├── main.py
│   │   └── router.py
│   └── janitor/
│       ├── main.py
│       ├── disk_monitor.py
│       └── eviction.py
│
├── core/                    # shared internal library — imported by all 3 apps
│   ├── nats/
│   │   └── client.py        # NATS connection lifecycle
│   ├── redis/
│   │   ├── client.py        # Redis connection lifecycle
│   │   └── model_list.py    # hget / hset helpers
│   ├── k8s/
│   │   ├── pod.py           # pod name resolution (HOSTNAME env var + fallback)
│   │   └── configmap.py     # ConfigMap loader: one.properties + product_X.json parsing
│   ├── config/
│   │   ├── settings.py      # pydantic-settings: all env vars
│   │   └── product.py       # ProductConfig Pydantic model
│   └── models/
│       ├── nats_messages.py # ArtifactMessage, MetadataMessage, ModelRecord
│       └── product.py       # ProductConfig, FunctionConfig
│
├── helm/mcs/
├── Build/
│   └── Dockerfile
├── tests/
│   ├── integration/kind/
│   └── unit/
├── documents/
│   └── synchronizer.md      # this file
└── requirements.txt
```

---

## 4. NATS Consumer Design

### 4.1 Artifact Stream — Push Consumer (Broadcast)

**Requirement:** Every MCS pod must receive every artifact message for every function_id.

**Design:**
- One push consumer per `(pod, function_id)` pair
- Each consumer has a unique `deliver_subject` — NATS pushes exclusively to that inbox
- No queue group — broadcast semantics

**Consumer naming:**
```
durable:          artifact-sync-{pod_name}-{func_id}
deliver_subject:  artifact-sync-{pod_name}-{func_id}.deliver
```

**Example** — 3 pods × 2 function_ids = 6 consumers:
```
artifact-sync-mcs-0-FUNC_AAA  →  deliver: artifact-sync-mcs-0-FUNC_AAA.deliver
artifact-sync-mcs-1-FUNC_AAA  →  deliver: artifact-sync-mcs-1-FUNC_AAA.deliver
artifact-sync-mcs-2-FUNC_AAA  →  deliver: artifact-sync-mcs-2-FUNC_AAA.deliver
artifact-sync-mcs-0-FUNC_BBB  →  deliver: artifact-sync-mcs-0-FUNC_BBB.deliver
artifact-sync-mcs-1-FUNC_BBB  →  deliver: artifact-sync-mcs-1-FUNC_BBB.deliver
artifact-sync-mcs-2-FUNC_BBB  →  deliver: artifact-sync-mcs-2-FUNC_BBB.deliver
```

**Consumer config:**
| Setting | Value |
|---|---|
| Durable | `artifact-sync-{pod_name}-{func_id}` |
| Deliver subject | `artifact-sync-{pod_name}-{func_id}.deliver` |
| Deliver policy | `new` |
| Ack policy | `explicit` |
| Ack wait | `300s` (5 min — artifact fetch can be slow) |
| Filter subject | `{func_id}-{sanitized_name}` |

---

### 4.2 Metadata Stream — Pull Consumer (Queue Group)

**Requirement:** Exactly one pod processes each metadata message — no duplicate Redis writes.

**Design:**
- One pull consumer per `function_id`, shared across all pods
- All pods call `fetch()` on the same durable consumer — NATS delivers to whichever pod fetches first
- On `nak()` or ack wait timeout, NATS redelivers to the next pod that fetches

**Consumer naming:**
```
durable:  metadata-sync-{func_id}-{sanitized_name}
```

**Example** — 2 function_ids = 2 pull consumers (shared across all 3 pods):
```
metadata-sync-FUNC_AAA-AAA
metadata-sync-FUNC_BBB-BBB
```

**Consumer config:**
| Setting | Value |
|---|---|
| Durable | `metadata-sync-{func_id}-{sanitized_name}` |
| Deliver policy | `new` |
| Ack policy | `explicit` |
| Ack wait | `30s` (Redis write only) |
| Replay policy | `instant` |
| Filter subject | `{func_id}-{sanitized_name}` |

---

## 5. Fetch Loop Design

### Why Not Busy-Poll?

Each pull consumer runs as an **`asyncio.Task`** calling `consumer.fetch(batch=1, timeout=5.0)`.
The `timeout` parameter causes the NATS server to hold the request for up to 5 seconds
waiting for a message before returning empty. This is **long-polling at the server side**
— the client coroutine simply `await`s, yielding the event loop to other tasks.

This means:
- No busy spin — coroutine sleeps while waiting
- No threads — pure `asyncio` concurrency
- N fetch loops (one per function_id) coexist with push consumer callbacks on the same event loop

### Fetch Loop Per Function ID

```
asyncio event loop
  ├── Task: fetch_loop(metadata_consumer_FUNC_AAA)   ← awaits fetch(), processes, acks, loops
  ├── Task: fetch_loop(metadata_consumer_FUNC_BBB)   ← same
  └── Push callbacks: artifact messages fire directly via NATS push deliver_subject
```

### Fetch Loop Logic (per function_id)

```python
while running:
    msgs = await consumer.fetch(batch=1, timeout=5.0)
    for msg in msgs:
        try:
            await handle_metadata_message(msg)
            await msg.ack()
        except Exception:
            await msg.nak()
    # loop immediately — if fetch returned empty, just re-fetch
```

### Race Condition Analysis

| Scenario | Risk | Mitigation |
|---|---|---|
| Two pods fetch same metadata message | ❌ Not possible | Pull consumer delivers each message to exactly one fetcher |
| Two pods create same pull consumer simultaneously | Safe | NATS durable consumer creation is idempotent — first wins, others get existing config |
| Concurrent artifact fetch for same model on same pod | ❌ Partial file | Per-key `asyncio.Lock` in artifact handler — single in-flight fetch |
| Artifact push message delivered twice (redelivery) | Safe | Check file exists before fetching; atomic tmp→rename write |

---

## 6. Startup Sequence

```
FastAPI lifespan startup
  │
  1. Load one.properties from mounted ConfigMap
  2. Load all product_X.json files → extract all function_ids + name mappings
  3. Connect to NATS (nats.creds file from Vault secret)
  4. Verify MLOP-MCS-ARTIFACT stream exists  → RuntimeError if missing
  5. Verify MLOP-MCS-METADATA stream exists  → RuntimeError if missing
  6. Connect to Redis
  7. Resolve pod_name from os.getenv("HOSTNAME")
  8. For each function_id across all products:
     a. subject = f"{func_id}-{sanitized_name}"
     b. Create/verify artifact push consumer (durable, unique deliver_subject)
     c. Subscribe to deliver_subject → artifact_handler callback
     d. Create/verify metadata pull consumer (durable, shared)
     e. Launch asyncio.Task: fetch_loop(metadata_consumer, func_id)
  9. /health returns 200 — pod is ready
```

**Failure behaviour:**
- NATS connection fails → `RuntimeError` → uvicorn exits → pod restarts
- Stream missing → `RuntimeError` → pod restarts
- Redis connection fails → `RuntimeError` → pod restarts
- No silent failures — `/health` returns 503 until fully ready

---

## 7. HTTP Endpoints (synchronizer)

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Returns 200 when NATS connected, streams verified, consumers created |

---

## 8. Dockerfile

Single image, three entrypoints. Located at `Build/Dockerfile`.

```dockerfile
FROM python:3.12.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/ ./apps/
COPY core/ ./core/

ENV PYTHONPATH=/app

# Default CMD — overridden per container in helm StatefulSet
CMD ["uvicorn", "apps.mcs.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

Container commands in StatefulSet (port defined in `values.yaml`):
```yaml
synchronizer: ["uvicorn", "apps.synchronizer.main:app", "--host", "0.0.0.0", "--port", "8081"]
mcs:          ["uvicorn", "apps.mcs.main:app",          "--host", "0.0.0.0", "--port", "8080"]
janitor:      ["python",  "apps/janitor/main.py"]
```

---

## 9. Error Handling

| Failure | Behaviour |
|---|---|
| NATS connection fails | `RuntimeError` raised → pod restarts |
| Stream not found | `RuntimeError` raised → pod restarts |
| Consumer creation fails | `RuntimeError` raised → pod restarts |
| Redis connection fails | `RuntimeError` raised → pod restarts |
| Artifact fetch fails | `msg.nak()` → NATS redelivers after ack wait |
| Metadata Redis write fails | `msg.nak()` → NATS redelivers to next pod that fetches |
| Fetch loop task crashes | Task exception logged → task restarted by supervisor |

---

## 10. Open Items

| # | Item |
|---|---|
| 1 | Confirm ack wait values (300s artifact, 30s metadata) with platform team |
| 2 | Confirm NATS creds file mount path (`/vault/secrets/nats.creds`) |
| 3 | Confirm ConfigMap mount path in pod |
| 4 | Integration test: Kind cluster + NATS + Redis Sentinel setup |
| 5 | Consumer cleanup on pod shutdown (drain vs unsubscribe) |
