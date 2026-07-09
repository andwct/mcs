# Partial Encryption — Design Document

> Branch: `feature/partial-encryption`
> Status: In progress
> Depends on: `feature/synchronizer-artifact`, `feature/mcs-serving` (both merged into `main`)

---

## Overview

Model and kernel artifact files stored on PVC are currently written as
**plaintext** (Phase 1). This branch implements **Phase 2**: partial
encryption at rest for MODEL and KERNEL artifacts, per security
requirements.

The scheme follows EdgeService's partial encryption structurally
(size-based chunk split, Fernet, `.meta` sidecar per artifact) but is an
independent MCS implementation — `.meta` files are **not** byte-compatible
with EdgeService's and never need to be (artifacts are always written and
read by MCS itself).

**PACKAGE artifacts are exempt** — no encryption, no `.meta` file.

---

## Encryption Scheme

### Algorithm — Fernet

[Fernet](https://cryptography.io/en/latest/fernet/) (from the `cryptography`
package): AES-128-CBC + HMAC-SHA256, base64url-encoded token. Chosen to
align with EdgeService's approved scheme.

Consequence of Fernet: **ciphertext is larger than plaintext** (~33%+
overhead from base64 + IV + HMAC + timestamp). The on-disk artifact file is
therefore larger than the original, and chunk boundaries in `.meta` store
the **on-disk (ciphertext) length** of each chunk so the reader can split
the file back into chunks without guessing.

### Size-based chunk split

Sizes use binary units: 1MB = 1,048,576 bytes (1024×1024). The 2MB
threshold is strict `>` — a file of exactly 2,097,152 bytes uses the
small-file scheme.

**Files > 2MB — middle-chunk encryption:**

| Chunk | Plaintext range | Stored as | Rationale |
|---|---|---|---|
| 0 | `0 – 1MB` | plaintext | preserves file header/structure |
| 1 | `1MB – 2MB` | **Fernet encrypted** | the "meat" of the file |
| 2 | `2MB – EOF` | plaintext | tail as-is |

**Files ≤ 2MB — header-only plaintext:**

| Chunk | Plaintext range | Stored as |
|---|---|---|
| 0 | `0 – 64B` | plaintext |
| 1 | `64B – EOF` | **Fernet encrypted** |

**Degenerate case (file ≤ 64B):** chunk 0 = entire file (plaintext),
chunk 1 = empty encrypted chunk. `.meta` is still written — structure stays
uniform, no special casing on the read path. (Real model/kernel artifacts
are never this small; this exists so tests and edge cases behave sanely.)

Rationale for partial (vs. full) encryption: large model files make full
encryption CPU-expensive on both write and serve; encrypting the meat of
the file renders it unusable without the key while keeping crypto cost
constant (at most 1MB encrypted per file, regardless of file size).

---

## Key Management

- **One global Fernet key** shared by MODEL and KERNEL artifacts across all
  products (not product-scoped).
- Vault provides a **raw string** in a file mounted at
  `{SECRET_MOUNT_PATH}/ENCRYPTION_KEY` (same VSO mount used for
  `{PRODUCT_NAME}_MODEL_CENTER_PASSWORD` files).
- Key derivation — `generate_fernet_key()`:

```python
import base64, hashlib

def generate_fernet_key(raw: str) -> bytes:
    digest = hashlib.sha256(raw.encode()).digest()   # 32 bytes
    return base64.urlsafe_b64encode(digest)          # valid Fernet key
```

- Loaded once at startup (mirrors `MODEL_CENTER_PASSWORD` loading in
  `core/k8s/configmap.py`). Missing/empty key file → startup failure (pod
  restart) for containers that need it (synchronizer, mcs).
- The key is never written to PVC, never logged, never present in `.meta`.
- All 3 pods mount the same Vault secret, so any pod can decrypt files
  regardless of which pod wrote them.

---

## File Layout

Every MODEL and KERNEL artifact version on PVC is represented by **two
files**:

```
{STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{funcID}/{id}/{version}        ← partially encrypted artifact
{STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{funcID}/{id}/{version}.meta   ← chunk map (JSON)
```

Example:
```
/mnt/mcs/mcs/MODEL/productID_ABC/funcID_123/modelUUID/v1.0.0
/mnt/mcs/mcs/MODEL/productID_ABC/funcID_123/modelUUID/v1.0.0.meta
```

PACKAGE artifacts are unchanged — plaintext, no `.meta`:
```
/mnt/mcs/mcs/PACKAGE/productID_ABC/funcID_123/v1.0.0
```

A MODEL/KERNEL `{version}` file is a valid cache entry **only if**
`{version}.meta` exists and parses. Missing/corrupt `.meta` → treat as
cache miss (re-fetch from siteMC) and delete the orphaned artifact file.

---

## `{version}.meta` Format

JSON sidecar, one per MODEL/KERNEL artifact. Stores chunk boundaries and
per-chunk encryption flags; the reader iterates chunks in order, decrypts
only those marked `"encrypted": true`, and joins the results.

**File > 2MB:**
```json
{
  "meta_version": 1,
  "algorithm": "fernet",
  "plaintext_size": 104857600,
  "chunks": [
    {"encrypted": false, "stored_length": 1048576,  "plaintext_length": 1048576},
    {"encrypted": true,  "stored_length": 1398200,  "plaintext_length": 1048576},
    {"encrypted": false, "stored_length": 102760448, "plaintext_length": 102760448}
  ]
}
```

**File ≤ 2MB:**
```json
{
  "meta_version": 1,
  "algorithm": "fernet",
  "plaintext_size": 1500000,
  "chunks": [
    {"encrypted": false, "stored_length": 64,      "plaintext_length": 64},
    {"encrypted": true,  "stored_length": 2000048, "plaintext_length": 1499936}
  ]
}
```

| Field | Meaning |
|---|---|
| `meta_version` | Schema version — future format changes don't break old files |
| `algorithm` | `"fernet"` — validated on read; unknown value → corrupt entry |
| `plaintext_size` | Original artifact size — integrity check after decrypt-join |
| `chunks` | Ordered list; on-disk file is the concatenation of all chunks' stored bytes |
| `chunks[].encrypted` | Whether this chunk's stored bytes are a Fernet token |
| `chunks[].stored_length` | Byte length of this chunk **on disk** (ciphertext length when encrypted) |
| `chunks[].plaintext_length` | Byte length after decryption (== stored_length when plaintext) |

`.meta` contains no secret material. Fernet tokens embed their own IV and
HMAC, so no nonce/tag fields are needed (unlike a raw AES-GCM design).

---

## Write Path (Encrypt)

Shared helper (new `core/utils/encryption.py`) used by both writers, called
after the existing decrypt-from-siteMC step, before writing to PVC:

```
encrypt_partial(plaintext: bytes) -> tuple[bytes, dict]   # (stored_bytes, meta)

1. If len(plaintext) > 2MB:
     chunks = [0:1MB], [1MB:2MB], [2MB:]
     encrypt chunk 1 with Fernet
   else:
     chunks = [0:64B], [64B:]
     encrypt chunk 1 with Fernet
2. stored_bytes = plaintext_chunk_0 + fernet_token + plaintext_tail(if any)
3. meta = chunk map as above
```

Callers (MODEL and KERNEL only; PACKAGE bypasses entirely):

- **Synchronizer pre-warm** (`apps/synchronizer/handlers.py` →
  `core/artifact_service.py`): after `fetch_artifact_bytes()`,
  `write_atomic(dest, stored_bytes)` then
  `write_atomic(dest_meta, meta_json)`.
- **mcs-serving cache-miss write-through** (`apps/mcs/router.py`
  `_tee_and_cache`): the client tee streams **plaintext** (unchanged
  contract with Model Service); the PVC write uses `encrypt_partial()` —
  same helper, same on-disk result as the synchronizer path.

Write ordering: artifact file first, `.meta` last — both via the existing
`write_atomic()` tmp+rename pattern. A crash between the two writes leaves
an artifact without `.meta`, which the read path already treats as a cache
miss (and cleans up). The janitor trigger (`trigger_janitor_check()`)
fires after both writes complete.

---

## Read Path (Decrypt) — mcs-serving cache-hit

`apps/mcs/router.py::_serve_artifact()`, MODEL and KERNEL only:

```
1. Cache-hit condition:
     PACKAGE:       dest.exists()
     MODEL/KERNEL:  dest.exists() AND dest_meta.exists()
   (missing .meta → delete orphan artifact file, treat as cache miss)
2. decrypt_partial(stored_bytes, meta) -> bytes:
     - split stored_bytes by chunks[].stored_length (validate sum == file size)
     - for each chunk: Fernet.decrypt() if encrypted else pass through
     - join; validate len == plaintext_size
3. Stream decrypted bytes to client (existing chunked StreamingResponse)
4. Failure at any step (bad token / InvalidToken, size mismatch, unparseable
   .meta) → log, delete BOTH files, fall back to siteMC. Never serve
   corrupted data.
```

At most 1MB per file is Fernet-decrypted, so decrypt cost is small and
constant. The plaintext chunks can still be streamed from disk; the
implementation may either (a) decrypt-then-stream the whole file from
memory, or (b) stream chunk-wise, decrypting only chunk 1 in memory —
(b) preserves the low-memory profile for multi-GB models and is the
preferred implementation since chunk boundaries are known from `.meta`.

The `os.utime()` LRU atime touch (feature/janitor) is unchanged and applies
to the artifact file only.

---

## Interaction with Janitor (eviction)

`apps/janitor/eviction.py` changes:

- `_collect_candidates()` must **exclude `.meta` files** from the LRU
  candidate list (they are sidecars, not independently evictable — their
  access pattern follows the artifact's).
- When evicting a MODEL/KERNEL artifact, delete the sibling
  `{version}.meta` in the same operation, and count its `st_size` toward
  `freed_bytes`.
- Orphan hygiene: a `.meta` without its artifact (or vice versa) found
  during a sweep may be deleted immediately — it is never a valid cache
  entry.

---

## Settings (new)

```
ENCRYPTION_KEY_FILE   str   "ENCRYPTION_KEY"   # filename under SECRET_MOUNT_PATH
```

Chunk thresholds (2MB split point, 1MB middle chunk, 64B header) are fixed
constants in `core/utils/encryption.py`, not deployment-configurable —
changing them would silently break decryption of existing cached files.
If they ever need to change, bump `meta_version` and handle both formats
on read.

---

## Design Decisions (resolved)

- **Fernet over AES-GCM** — aligns with EdgeService's approved scheme;
  accepted trade-off: ~33% size overhead on the encrypted chunk (at most
  ~0.4MB extra per file) and `.meta` must track stored vs. plaintext
  lengths.
- **No EdgeService `.meta` compatibility** — MCS defines its own schema;
  files never move between EdgeService and MCS storage.
- **Single global key** — one Vault-provided raw string for all products,
  both artifact types; derived to a Fernet key via SHA-256 + base64url.
- **Key rotation out of scope** — no `key_version` field in v1; rotating
  the key invalidates the cache (acceptable: every entry re-fetches from
  siteMC on demand). `meta_version` exists if this needs revisiting.
- **Thresholds fixed, not configurable** — see Settings.

---

## Files to Create / Modify

### New files
| File | Purpose |
|---|---|
| `core/utils/encryption.py` | `generate_fernet_key()`, `encrypt_partial()`, `decrypt_partial()`, meta build/parse, chunk constants |
| `core/models/encryption_models.py` | Pydantic models for `.meta` schema (`ArtifactMeta`, `ChunkEntry`) |

### Modified files
| File | Change |
|---|---|
| `core/artifact_service.py` | Apply `encrypt_partial()` for MODEL/KERNEL before `write_atomic()`; write `.meta` sidecar |
| `apps/mcs/router.py` | Cache-hit requires `.meta` for MODEL/KERNEL; chunk-wise decrypt-stream; `_tee_and_cache` write-through encryption |
| `apps/janitor/eviction.py` | Exclude `.meta` from candidates; delete sibling `.meta` on eviction; orphan cleanup |
| `core/config/settings.py` | Add `ENCRYPTION_KEY_FILE` |
| `core/k8s/configmap.py` | Load + derive Fernet key from Vault mount at startup |
| `helm/mcs/values.yaml` | Add `ENCRYPTION_KEY` to Vault secret template |
| `requirements.txt` | Add `cryptography` (Fernet) |

---

## Task List

- [ ] Add `ENCRYPTION_KEY_FILE` setting + Vault secret in `values.yaml`
- [ ] Implement key loading + `generate_fernet_key()` in `core/k8s/configmap.py` / `core/utils/encryption.py`
- [ ] Implement `encrypt_partial()` / `decrypt_partial()` + `.meta` models
- [ ] Update `core/artifact_service.py` — encrypt on write for MODEL/KERNEL
- [ ] Update `apps/mcs/router.py` — `.meta`-aware cache-hit + chunk-wise decrypt streaming
- [ ] Update `apps/mcs/router.py` — `_tee_and_cache` write-through encryption
- [ ] Update `apps/janitor/eviction.py` — `.meta` sibling handling + orphan cleanup
- [ ] Unit tests: round-trip both size classes (>2MB, ≤2MB, ≤64B), stored/plaintext length bookkeeping, InvalidToken → corrupt-entry handling, missing `.meta` → cache miss, PACKAGE untouched
- [ ] Integration test: synchronizer write → mcs-serving serve → bytes identical to original
- [ ] Verify janitor evicts artifact + `.meta` together
