# MCS Serving — Design Document

> Branch: `feature/mcs-serving` (based on `feature/synchronizer-artifact`)
> Status: Design phase
> Container: `apps/mcs/` (currently a stub)

---

## Where We Are

- `feature/synchronizer-NATS` — NATS connection, consumer creation (merged to `main`)
- `feature/synchronizer-redis` — metadata Redis warm-up + incremental updates (merged to `main`)
- `feature/synchronizer-artifact` — artifact download pipeline, Phase 1 working
  (plaintext PVC write, Phase 2 partial encryption not yet implemented)

**This branch:** implement `mcs-serving` — the API container that Model Service
calls instead of calling siteMC/EdgeService directly. Drop-in replacement:
same request/response contracts as EdgeService, but MCS sits in between as a
CDN-style caching layer.

---

## Scope

Mirror EdgeService's 7 serving endpoints exactly (same paths, same request
bodies, same response shapes) so Model Service requires **zero changes**.

| Endpoint | Method | Purpose |
|---|---|---|
| `/mcs/model` | POST | Stream model file (cache-aware) |
| `/mcs/kernel` | POST | Stream kernel file (cache-aware) |
| `/mcs/package` | POST | Stream package file (cache-aware) |
| `/mcs/model_list/{function_id}` | GET | Return model list from Redis |
| `/mcs/kernel_list/{function_id}` | GET | Return kernel record from Redis |
| `/mcs/package_list/{function_id}` | GET | Return package record from Redis |
| `/mcs/active_pats/{function_id}` | GET | Return PAT list from Redis |

---

## Auth

All endpoints use `HTTPBasicCredentials` (FastAPI `HTTPBasic`), validated
against `MODEL_CENTER_ACCOUNT`/`MODEL_CENTER_PASSWORD` from `productConfig`
(looked up via `function_id` → `state.get_product_by_func_id()`).

Implemented as a **FastAPI dependency** (not inline calls in every handler):

```python
async def verify_credentials(
    function_id: str,
    credentials: HTTPBasicCredentials = Depends(security),
) -> None:
    try:
        product = get_product_by_func_id(function_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="function_id not found")

    correct = (
        credentials.username == product.MODEL_CENTER_ACCOUNT
        and credentials.password == product.MODEL_CENTER_PASSWORD
    )
    if not correct:
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
```

Used as: `_: None = Depends(verify_credentials)` on every route.

For POST endpoints (model/kernel/package), `function_id` comes from the
**request body**, not a path param — dependency needs adjusting per route
(see implementation section).

---

## Artifact Serving — `/mcs/model`, `/mcs/kernel`, `/mcs/package`

### Request body

```python
class ModelModel(BaseModel):
    ARTIFACT_TYPE: str = "MODEL"
    product_id: str
    function_id: str
    model_id: str
    model_version: str

class KernelModel(BaseModel):
    ARTIFACT_TYPE: str = "KERNEL"
    product_id: str
    function_id: str
    kernel_id: str
    kernel_version: str

class PackageModel(BaseModel):
    ARTIFACT_TYPE: str = "PACKAGE"
    product_id: str
    function_id: str
    package_id: str
    package_version: str
```

(Reuses `core/models/artifact_models.py` shapes — already defined for synchronizer.)

### Response

```python
StreamingResponse(
    generate(...),
    media_type="application/octet-stream",
    headers={"Content-Disposition": "attachment; filename=test.zip"},
)
```

Matches EdgeService exactly — Model Service sees no difference.

### Cache-aware flow (CDN pattern)

```
POST /mcs/model {product_id, function_id, model_id, model_version}
  │
  ▼
Check PVC: {STORAGE_PATH}/{FAB_NAME}/MODEL/{product_id}/{function_id}/{model_id}/{model_version}
  │
  ├── EXISTS (cache hit)
  │     └── StreamingResponse reading local file, same headers
  │
  └── NOT EXISTS (cache miss)
        └── Fallback to siteMC:
              1. SiteAuthorizationService.get_one_time_access_token()
              2. SiteArtifactCacheService.get_model_from_artifact_service()
              3. Decrypt (RSA+AES-CBC tunnel for model, AES for kernel/package)
              4. TEE pattern: stream decrypted chunks to client AND
                 write same chunks to PVC (atomic tmp→rename) concurrently
              5. Client receives data with minimal added latency
              6. PVC now has the file cached for next request
```

### Tee streaming implementation approach

```python
async def generate_and_cache(plaintext: bytes, dest: Path, chunk_size: int):
    """
    Yields chunks to the StreamingResponse while writing the same
    chunks to a temp file. On completion, atomic rename to dest.
    On any error mid-stream, temp file is removed (no partial cache).
    """
    tmp = dest.with_suffix(".tmp")
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(tmp, "wb") as f:
            for i in range(0, len(plaintext), chunk_size):
                chunk = plaintext[i:i + chunk_size]
                f.write(chunk)
                yield chunk
        tmp.rename(dest)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
```

Note: this reads the full decrypted artifact into memory first (since
`decrypt_rsa_aes_tunnel`/`decrypt_object` return complete bytes, not a
stream) — then chunks it out for both client response and disk write.
True end-to-end streaming (decrypt-as-you-receive) would require
restructuring the EdgeService decrypt methods; out of scope for this phase.

### Reuses synchronizer's download logic

The actual siteMC download + decrypt steps are **identical** to what
`apps/synchronizer/handlers.py` already does for cache pre-warming. To avoid
duplicating this logic, extract the shared download+decrypt steps into a
reusable function (e.g. `core/artifact_service.py`) called by both:
- `apps/synchronizer/handlers.py` (`_download_artifact`) — pre-warm on NATS message
- `apps/mcs/handlers.py` (new) — on-demand fallback during serving

---

## Meta Serving — `/mcs/model_list`, `/mcs/kernel_list`, `/mcs/package_list`, `/mcs/active_pats`

All four read directly from Redis (populated by synchronizer's warm-up +
incremental updates) — **no siteMC fallback**, no `CustomMessageResponse`
wrapper. Plain dict/list response, FastAPI handles JSON serialization.

### `GET /mcs/model_list/{function_id}`

```python
@router.get("/{function_id}")
async def get_model_list(
    function_id: str,
    _: None = Depends(verify_credentials),
) -> dict:
    model_map = await get_model_list_from_redis(function_id)  # core/redis/model_list.py
    if model_map is None:
        raise HTTPException(status_code=404, detail="model_list not found")
    return {
        "online": list(model_map.values()),
        "shadow": [],  # not stored in Redis — always empty, no live siteMC fetch
        "headers": {},
    }
```

### `GET /mcs/kernel_list/{function_id}`

```python
@router.get("/{function_id}")
async def get_kernel_list(
    function_id: str,
    _: None = Depends(verify_credentials),
) -> dict:
    record = await get_kernel_list_from_redis(function_id)  # core/redis/kernel_list.py
    if record is None:
        raise HTTPException(status_code=404, detail="kernel_list not found")
    return record
```

### `GET /mcs/package_list/{function_id}` — same pattern as kernel

### `GET /mcs/active_pats/{function_id}`

```python
@router.get("/{function_id}")
async def get_active_pats(
    function_id: str,
    _: None = Depends(verify_credentials),
) -> list:
    record = await get_pat_list_from_redis(function_id)  # core/redis/pat_list.py
    if record is None:
        raise HTTPException(status_code=404, detail="pat_list not found")
    return record
```

### Exception handling (per your direction — no CustomMessageResponse)

| Exception | HTTP Status |
|---|---|
| `FileNotFoundError` / Redis key not found | 404 |
| `ValueError` (bad input) | 422 |
| Unexpected exception | 500, logged |

---

## Files to Create

| File | Purpose |
|---|---|
| `apps/mcs/main.py` | FastAPI app entrypoint (currently stub) |
| `apps/mcs/router.py` | All 7 route definitions |
| `apps/mcs/auth.py` | `verify_credentials` dependency |
| `apps/mcs/lifespan.py` | Startup: load productConfig, init state, connect Redis |
| `core/artifact_service.py` | Shared download+decrypt logic (used by both synchronizer and mcs-serving) |
| `documents/mcs-serving.md` | This file |

## Files to Modify

| File | Change |
|---|---|
| `apps/synchronizer/handlers.py` | Refactor `_download_artifact` to use shared `core/artifact_service.py` |
| `helm/mcs/values.yaml` | `appModule` for `mcs` container already points to `apps.mcs.main:app` — verify |

---

## Open Questions

1. **`model_list` route — confirm pattern matches kernel/package** (you said "almost the same" for `ModelModel` request — does the route handler itself follow the identical try/except + Redis-read pattern?)
2. **Concurrent cache-miss requests** — if 2 requests arrive simultaneously for the same uncached model, do we want a lock to prevent duplicate siteMC downloads? Or is duplicate download acceptable (idempotent PVC write)?
3. **`apps/mcs/` container — separate consumer of NATS or pure HTTP server?** Confirmed: mcs-serving is pure HTTP, no NATS involvement — correct?
4. **Phase 2 (partial encryption) impact on serving** — once implemented, `mcs-serving` cache-hit path will need to **decrypt** the partially-encrypted file using `.meta` before streaming. Out of scope for this branch; tracked separately.

---

## Task List

- [ ] Confirm `model_list` route matches kernel/package pattern
- [ ] Extract shared download+decrypt logic into `core/artifact_service.py`
- [ ] Refactor `apps/synchronizer/handlers.py` to use shared logic
- [ ] Implement `apps/mcs/auth.py` — `verify_credentials` dependency
- [ ] Implement `apps/mcs/router.py` — all 7 endpoints
- [ ] Implement `apps/mcs/lifespan.py` — startup sequence
- [ ] Implement `apps/mcs/main.py` — FastAPI app
- [ ] Implement tee streaming for cache-miss artifact serving
- [ ] Test: cache hit (file on PVC) — verify correct StreamingResponse
- [ ] Test: cache miss — verify fallback + PVC write + correct response
- [ ] Test: all 4 meta endpoints against Redis
- [ ] Test: auth rejection (wrong credentials → 401)
- [ ] Test: 404 on missing function_id / missing list
