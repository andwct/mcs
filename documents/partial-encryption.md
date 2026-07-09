# Partial Encryption — Design Document

> Branch: `feature/partial-encryption`
> Status: In progress
> Depends on: `feature/synchronizer-artifact`, `feature/mcs-serving` (both merged into `main`)

---

## Overview

Model and kernel artifact files stored on PVC are currently written as
**plaintext** (Phase 1). This branch implements **Phase 2**: partial
AES-GCM encryption at rest for MODEL and KERNEL artifacts, per security
requirements.

**PACKAGE artifacts are exempt** — no encryption, no `.meta` file — per
existing design decision (package files are not sensitive).

---

## Scope

1. Encrypt a subset of each MODEL/KERNEL file's bytes (not the whole file)
   using AES-256-GCM, applied after download/decrypt-from-siteMC and before
   writing to PVC.
2. Generate a `{version}.meta` file alongside each encrypted artifact,
   containing everything needed to decrypt it.
3. Decrypt on read — mcs-serving must reverse the encryption before
   streaming a cache-hit artifact to Model Service.

---

## File Layout

Every MODEL and KERNEL artifact version on PVC is represented by **two
files**:

```
{STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{funcID}/{id}/{version}        ← partially encrypted artifact bytes
{STORAGE_PATH}/{FAB_NAME}/{TYPE}/{productID}/{funcID}/{id}/{version}.meta   ← encryption metadata (JSON)
```

Example:
```
/mnt/mcs/mcs/MODEL/productID_ABC/funcID_123/modelUUID/v1.0.0
/mnt/mcs/mcs/MODEL/productID_ABC/funcID_123/modelUUID/v1.0.0.meta
```

PACKAGE artifacts are unaffected — no `.meta` file, no encryption:
```
/mnt/mcs/mcs/PACKAGE/productID_ABC/funcID_123/v1.0.0   ← plaintext, no .meta
```

A `{version}` file is only considered a valid, servable cache entry if its
matching `{version}.meta` file also exists (MODEL/KERNEL only). If `.meta`
is missing or unreadable, the artifact must be treated as a cache miss and
re-fetched from siteMC.

---

## `{version}.meta` Format

JSON file, one per encrypted artifact:

```json
{
  "version": 1,
  "algorithm": "AES-256-GCM",
  "chunk_size": 1048576,
  "total_size": 104857600,
  "total_chunks": 100,
  "segments": [
    {"index": 3,  "nonce": "base64...", "tag": "base64..."},
    {"index": 17, "nonce": "base64...", "tag": "base64..."},
    {"index": 42, "nonce": "base64...", "tag": "base64..."}
  ]
}
```

| Field | Meaning |
|---|---|
| `version` | `.meta` schema version — allows format changes later without breaking old files |
| `algorithm` | Fixed to `AES-256-GCM` for this design |
| `chunk_size` | Byte size used to split the artifact into chunks (fixed per deployment, from settings) |
| `total_size` | Total artifact size in bytes — used to validate the artifact file hasn't been truncated/corrupted |
| `total_chunks` | `ceil(total_size / chunk_size)` — sanity check against the artifact file |
| `segments` | List of chunks that are encrypted. Chunks NOT listed here are stored as plaintext bytes at their offset |
| `segments[].index` | 0-based chunk index into the artifact file (`offset = index * chunk_size`) |
| `segments[].nonce` | Base64-encoded 12-byte GCM nonce, unique per chunk (never reused with the same key) |
| `segments[].tag` | Base64-encoded 16-byte GCM authentication tag for that chunk |

The `.meta` file contains no secret material — nonces and tags are safe to
store in plaintext. The AES-256 key itself is never written to PVC; it is
loaded from Vault at startup (see Key Management below).

### Segment selection

Which chunks are encrypted (vs. left plaintext) is determined by
`ENCRYPTION_CHUNK_RATIO` (settings, default TBD — e.g. `0.25` for 25% of
chunks encrypted). Selection is deterministic per artifact so re-encryption
of the same file (e.g. after a cache eviction + re-download) reliably
reproduces the same segment indices, simplifying debugging:

```
selected_indices = [i for i in range(total_chunks) if i % round(1 / ratio) == 0]
```

This is intentionally simple (every Nth chunk) rather than random —
randomized selection would need to be persisted anyway (in `.meta`), so
there is no security benefit to randomizing over a fixed stride, and a
fixed stride is easier to reason about and test.

---

## Key Management

- Single symmetric AES-256 key, loaded from Vault-mounted secret at startup
  (same pattern as `MODEL_CENTER_PASSWORD` — see `core/k8s/configmap.py`)
- Env var: `ENCRYPTION_KEY_PATH` → file mounted at
  `{SECRET_MOUNT_PATH}/{PRODUCT_NAME}_ENCRYPTION_KEY` (or a single
  cluster-wide key path if not product-scoped — TBD, see Open Questions)
- Key never written to PVC, never logged, never included in `.meta`
- Same key used for both encryption (synchronizer/mcs-serving write path)
  and decryption (mcs-serving read path) — all 3 pods must have access to
  the same key since PVC is per-pod but the key must decrypt files
  regardless of which pod wrote them

---

## Write Path (Encrypt)

Applies to `core/artifact_service.py::fetch_artifact_bytes()` for MODEL and
KERNEL only, after existing decrypt-from-siteMC step, before
`write_atomic()`:

```
1. fetch_artifact_bytes() returns plaintext bytes (as today)
2. If artifact_type in (MODEL, KERNEL):
   a. Split plaintext into chunks of ENCRYPTION_CHUNK_SIZE
   b. Determine encrypted chunk indices (segment selection)
   c. For each selected chunk:
      - Generate random 12-byte nonce
      - AES-256-GCM encrypt chunk with key + nonce → ciphertext + tag
      - Replace chunk bytes in the output buffer with ciphertext
      - Record {index, nonce, tag} in segments list
   d. Build .meta JSON
   e. write_atomic(dest, output_buffer)
   f. write_atomic(dest.with_suffix(dest.suffix + ".meta"), meta_json_bytes)
3. If artifact_type == PACKAGE:
   - No change — write_atomic(dest, content) as today, no .meta
```

Both the artifact file and its `.meta` file must be written atomically
(existing `write_atomic()` tmp+rename pattern) — a partial/torn write of
either file must not leave a servable-but-corrupt cache entry. The `.meta`
write happens only after the artifact write succeeds; if the artifact write
fails, no `.meta` is written and no stale `.meta` from the same path should
exist (paths are unique per version).

---

## Read Path (Decrypt) — mcs-serving cache-hit

Applies to `apps/mcs/router.py::_serve_artifact()` for MODEL and KERNEL,
before streaming to client:

```
1. Cache-hit check: dest.exists() AND (artifact_type == PACKAGE OR dest_meta.exists())
   - If .meta is missing for MODEL/KERNEL → treat as cache miss, fall back to siteMC
2. If artifact_type in (MODEL, KERNEL):
   a. Read {version}.meta → parse segments
   b. Read {version} bytes
   c. For each segment in .meta:
      - Extract ciphertext chunk at offset (index * chunk_size)
      - AES-256-GCM decrypt with key + nonce + tag → plaintext chunk
      - Replace ciphertext with plaintext in the output buffer
      - GCM tag verification failure → treat as corrupted cache entry,
        delete both files, fall back to siteMC (do not serve corrupted data)
   d. Stream fully-decrypted bytes to client
3. If artifact_type == PACKAGE:
   - Stream file bytes directly, no decryption (unchanged from Phase 1)
```

Decryption happens in memory before streaming begins — the client never
receives partially-decrypted data. This means cache-hit responses for
MODEL/KERNEL can no longer be pure zero-copy file streaming; the full file
must be read and decrypted into memory first. (See Open Questions —
Performance.)

---

## Cache-Miss / Write-Through Path

mcs-serving's existing cache-miss fallback (`_tee_and_cache`) downloads
from siteMC, tees to the client, and writes to PVC. For MODEL/KERNEL, this
path must also apply encryption before/while writing to PVC — the client
still receives plaintext (tee happens before encryption), but the PVC copy
must be encrypted with a `.meta` file, consistent with the synchronizer's
write path. This requires refactoring `_tee_and_cache` so the plaintext
tee to the client and the encrypted write to PVC use the same encrypt
logic as the synchronizer path (shared helper in `core/artifact_service.py`
or a new `core/utils/encryption.py`).

---

## Interaction with Janitor (eviction)

Janitor's eviction sweep (`apps/janitor/eviction.py`) walks MODEL/KERNEL/
PACKAGE directories and deletes files by `atime`. With `.meta` files now
present alongside MODEL/KERNEL artifacts:

- Janitor must delete `{version}.meta` together with `{version}` — an
  orphaned `.meta` file with no matching artifact (or vice versa) must not
  be left behind.
- `{version}.meta` itself should NOT be treated as a separate eviction
  candidate — it has no independent `atime` relevant to LRU (its access
  pattern always follows the artifact file's).
- `_collect_candidates()` in `eviction.py` needs to skip `.meta` files
  from the primary candidate list, and `_prune_empty_parents` /  deletion
  logic needs to delete the sibling `.meta` whenever a MODEL/KERNEL
  artifact is evicted.

---

## Interaction with mcs-serving `os.utime()` LRU Touch

The existing cache-hit `atime` touch (`os.utime(dest, (now, mtime))`,
`feature/janitor`) only touches the artifact file. This branch does not
need to additionally touch `{version}.meta`'s atime, since janitor never
independently evaluates `.meta` files for eviction (see above) — it always
follows the primary artifact file.

---

## Settings (new)

```
ENCRYPTION_KEY_PATH        str    # Vault-mounted path to AES-256 key file
ENCRYPTION_CHUNK_SIZE       int    1048576   # 1MB — bytes per chunk
ENCRYPTION_CHUNK_RATIO      float  0.25      # fraction of chunks encrypted (TBD — see Open Questions)
```

---

## Open Questions

- [ ] **Chunk size** — is 1MB reasonable, or does EdgeService/siteMC use a
      different standard chunk size we should match?
- [ ] **Chunk ratio / selection strategy** — is 25% (every 4th chunk)
      reasonable, or is there a specific ratio mandated by the security
      requirement this phase is satisfying?
- [ ] **Key scope** — one cluster-wide key, or per-product keys (matching
      the per-product `MODEL_CENTER_PASSWORD` pattern)? Per-product keys
      complicate the read path (need to know which product's key to use
      before decrypting — already available via `productID` in the PVC
      path, so feasible either way)
- [ ] **Key rotation** — out of scope for this phase, or does `.meta` need
      a `key_version` field to support rotating keys without invalidating
      the entire cache?
- [ ] **Performance** — decrypting the full file into memory before
      streaming (vs. today's zero-copy `_read_file_chunks` generator)
      changes the memory profile of cache-hit serves for large model
      files. Is streaming decryption (decrypt-as-you-stream, chunk by
      chunk) required instead of decrypt-then-stream? This would preserve
      the generator-based streaming response pattern.
- [ ] **Corrupted `.meta` / partial write recovery** — if a pod crashes
      between writing `{version}` and `{version}.meta`, is falling back to
      cache-miss (re-fetch from siteMC) sufficient, or does janitor need an
      active sweep to detect and clean up orphaned files?

---

## Files to Create / Modify

### New files
| File | Purpose |
|---|---|
| `core/utils/encryption.py` | `encrypt_partial()`, `decrypt_partial()`, `.meta` build/parse helpers |
| `core/models/encryption_models.py` | Pydantic models for `.meta` schema |

### Modified files
| File | Change |
|---|---|
| `core/artifact_service.py` | Apply `encrypt_partial()` for MODEL/KERNEL before `write_atomic()`; write `.meta` alongside |
| `apps/mcs/router.py` | Cache-hit check requires `.meta` for MODEL/KERNEL; decrypt before streaming; `_tee_and_cache` applies encryption on write-through |
| `apps/janitor/eviction.py` | Skip `.meta` in primary candidate walk; delete sibling `.meta` when evicting MODEL/KERNEL artifact |
| `core/config/settings.py` | Add `ENCRYPTION_KEY_PATH`, `ENCRYPTION_CHUNK_SIZE`, `ENCRYPTION_CHUNK_RATIO` |
| `core/k8s/configmap.py` | Load encryption key from Vault mount at startup (mirrors `MODEL_CENTER_PASSWORD` loading) |
| `helm/mcs/values.yaml` | Add encryption settings to `envConfig`; add Vault secret mount for encryption key |

---

## Task List

- [ ] Resolve open questions (chunk size, ratio, key scope, rotation, streaming decrypt)
- [ ] Add encryption settings to `settings.py` + `values.yaml`
- [ ] Implement key loading from Vault mount in `core/k8s/configmap.py`
- [ ] Implement `core/utils/encryption.py` — encrypt/decrypt partial, `.meta` build/parse
- [ ] Implement `core/models/encryption_models.py` — `.meta` schema
- [ ] Update `core/artifact_service.py` — apply encryption on write for MODEL/KERNEL
- [ ] Update `apps/mcs/router.py` — cache-hit `.meta` check + decrypt-before-stream
- [ ] Update `apps/mcs/router.py` — `_tee_and_cache` write-through encryption
- [ ] Update `apps/janitor/eviction.py` — `.meta` sibling handling
- [ ] Unit tests: encrypt/decrypt round-trip, corrupted `.meta` handling, GCM tag verification failure
- [ ] Integration test: full write → serve → decrypt cycle for MODEL and KERNEL
- [ ] Verify PACKAGE artifacts remain unaffected (no `.meta`, no encryption)
