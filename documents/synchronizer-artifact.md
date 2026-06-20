# Synchronizer Artifact — Design Document

> Branch: `feature/synchronizer-artifact`
> Status: In progress
> Depends on: `feature/synchronizer-redis` (merged into `main`)

---

## Status (as of latest commit)

✅ **Phase 1 — Download pipeline: WORKING**
- NATS artifact message → siteAuthorizationService → siteArtifactCacheService → decrypt → PVC write
- Verified end-to-end: model file successfully downloaded, decrypted (RSA+AES-CBC tunnel),
  and written to PVC at the new path layout (see below)
- New PVC path layout implemented: `{STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{function_ID}/{id}/{version}`

❌ **Phase 2 — Partial encryption at rest: NOT IMPLEMENTED**
- Files currently written to PVC as **plaintext** — no `.meta` file generated
- No partial AES-GCM encryption applied after download
- This is a known, intentional gap — tracked separately, not yet started
- Required before production use: model and kernel files must be partially
  encrypted at rest per security requirements (package files are exempt —
  no `.meta` needed for package per design)

## Where We Are

`feature/synchronizer-redis` delivered:
- Redis warm-up on startup for all 4 meta types
- Incremental Redis updates from NATS metadata messages

`feature/synchronizer-NATS` delivered:
- Artifact pull consumer per `(pod, func_id)` on `MLOP-MCS-ARTIFACT`
- Fetch loop running — `handle_artifact_message()` called on each message
- Current `handle_artifact_message()` is a stub — no real implementation

**This branch:** implement the full artifact download pipeline when MCS
receives an `ArtifactMessage` from `MLOP-MCS-ARTIFACT`.

---

## Scope

### Phase 1 (this branch) — Download + Store (no partial encryption)
- Parse `ArtifactMessage` and route by `artifact_type`
- 3-step auth flow (one-time token → artifact key → download)
- RSA key generation + hybrid RSA+AES-CBC tunnel for model download
- Store raw downloaded file on PVC
- Copy `site_authorization.py`, `site_artifact_service.py`, `security.py`
  from EdgeService

### Phase 2 (separate branch) — Partial Encryption
- Apply partial AES-GCM encryption using `ENCRYPTION_KEY` on write
- Generate `.meta` file (segment map)
- Decrypt on read (for `mcs-serving`)

---

## ArtifactMessage Schema

```python
class ArtifactType(str, Enum):
    MODEL   = "model"
    KERNEL  = "kernel"
    PACKAGE = "package"

class ArtifactMessage(BaseModel):
    function_id:      str
    product_id:       str
    artifact_type:    ArtifactType
    deployed_version: str
```

---

## 3-Step Auth + Download Flow (per artifact)

```
Step 1: Get one-time access token
POST {SITE_AUTHORIZATION_URL}/authorization/access-token/one-time
  params:  functionId, productId, inline=true, source=cache_service
  payload: {account, functionId, artifactType, action: DOWNLOAD}
  auth:    Basic (MODEL_CENTER_ACCOUNT, MODEL_CENTER_PASSWORD)
  returns: accessToken

Step 2: Get artifact key (AES key for this artifact)
POST {SITE_AUTHORIZATION_URL}/authorization/artifact-key/artifact-key
  params:  functionId, productId, inline=true, source=cache_service
  payload: {functionId, artifactType, action: DOWNLOAD, accessToken}
  auth:    Basic (MODEL_CENTER_ACCOUNT, MODEL_CENTER_PASSWORD)
  returns: artifactKey (AES key)

Step 3: Download artifact via RSA+AES-CBC tunnel
  a. Generate RSA key pair (2048-bit)
  b. Encrypt artifactKey with artifact-cache-service RSA public key
     → encrypted_aes_key
  c. POST {SITE_ARTIFACT_SERVICE_URL}/artifact-cache-readonly/model/
       params:  modelId, functionId, productId, accessToken,
                inline=true, source=cache_service
       payload: {rsaKey (public), modelId, functionId, productId,
                 accessToken, modelVersion, encrypted_aes_key}
       auth:    Basic (MODEL_CENTER_ACCOUNT, MODEL_CENTER_PASSWORD)
  d. Response: encrypted_model_data + X-DUMMY-MODEL-ENC header
  e. Decrypt X-DUMMY-MODEL-ENC with MCS RSA private key → recovered aes_key
  f. Decrypt encrypted_model_data with aes_key (AES-CBC) → plaintext file
```

---

## On-Disk Layout (PVC)

```
{STORAGE_PATH}/{FAB_NAME}/         # e.g. /mnt/mcs/mcs
  MODEL/
    {productID}/
      {function_ID}/
        {model_id}/
          {model_version}        # plaintext in Phase 1, partially encrypted in Phase 2
          {model_version}.meta   # segment map — Phase 2 only, NOT YET IMPLEMENTED
  KERNEL/
    {productID}/
      {function_ID}/
        {kernel_id}/
          {kernel_version}
          {kernel_version}.meta  # Phase 2 only, NOT YET IMPLEMENTED
  PACKAGE/
    {productID}/
      {function_ID}/
        {package_id}/
          {package_version}      # no .meta — package not partially encrypted
```

See issue #33 for path layout design rationale.

---

## Pasting EdgeService Files — Only One Change Required

When pasting the 3 EdgeService files, **only `core/security.py` needs one line changed**.
`site_authorization.py` and `site_artifact_service.py` can be pasted completely unmodified
— the `src/` adapter layer handles all their imports transparently.

### `core/security.py` — one line only
```python
# REMOVE (pycryptodome — security flagged):
from Crypto.Random import get_random_bytes

# REPLACE WITH (pycryptodomex — safe):
from Cryptodome.Random import get_random_bytes
```

All other imports in `security.py` (`Cryptodome.*`) work as-is with `pycryptodomex`. ✅

---

## Files to Create / Modify

### New files
| File | Purpose |
|---|---|
| `core/http/site_authorization.py` | Copied from EdgeService |
| `core/http/site_artifact_service.py` | Copied from EdgeService |
| `core/security.py` | Copied from EdgeService |

### Modified files
| File | Change |
|---|---|
| `apps/synchronizer/handlers.py` | Replace stub `handle_artifact_message()` with full implementation |
| `core/http/artifact_client.py` | Replace stub with real download using site_artifact_service.py |
| `documents/synchronizer-artifact.md` | This file |

---

## Task List

### Phase 1
- [ ] Copy `site_authorization.py` from EdgeService into `core/http/`
- [ ] Copy `site_artifact_service.py` from EdgeService into `core/http/`
- [ ] Copy `security.py` (decrypt methods only) from EdgeService into `core/`
- [ ] Implement `handle_artifact_message()` — full dispatch for model/kernel/package
- [ ] Implement artifact download pipeline using copied files
- [ ] Write downloaded file to PVC at `{STORAGE_PATH}/{func_id}/{artifact_type}/{id}/MODEL_FILE.bin`
- [ ] Test: publish test `ArtifactMessage`, verify file appears on PVC
- [ ] Verify idempotency — if file already exists, skip download

### Phase 2 (separate branch) — ⚠️ NOT STARTED
- [ ] Copy partial AES-GCM encrypt/decrypt from EdgeService `security.py`
- [ ] Generate `{version}.meta` segment map file on write
- [ ] Apply partial encryption after download (model + kernel only — package excluded)
- [ ] Decrypt on read (for `mcs-serving`)
- [ ] Update file naming: `{model_version}` and `{model_version}.meta` per new path layout (#33)

---

## Open Questions

1. **Kernel + package download** — same 3-step flow as model? Same endpoint?
2. **`mcs-serving`** — not yet designed. For Phase 1, MCS just writes to PVC.
3. **Concurrency** — if 3 pods receive the same `ArtifactMessage` (broadcast),
   all 3 will attempt to download the same file. Need atomic write pattern
   (temp file + rename) to prevent corruption. Each pod writes to its own PVC
   (ReadWriteOnce) so no cross-pod conflict.
4. **Janitor** — old artifact versions on PVC not yet handled.
