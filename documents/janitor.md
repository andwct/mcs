# Janitor — Design Document

> Branch: `feature/janitor`
> Status: In progress
> Depends on: `feature/mcs-serving` (merged into `main`)

---

## Overview

The janitor is the 3rd container in each MCS StatefulSet pod. Its sole
responsibility is PVC eviction — keeping each pod's `ReadWriteOnce` PVC
below a configured storage threshold by deleting least-recently-used
artifacts.

Since each pod has its own dedicated PVC, janitor only ever manages the
local PVC of the pod it runs in. No cross-pod coordination is needed.

---

## Architecture Position

```
write_atomic() completes (synchronizer pre-warm OR mcs-serving cache-miss)
      ↓
POST http://localhost:{janitor_port}/janitor/check   (fire-and-forget)
      ↓
Janitor: shutil.disk_usage() gate check
      ↓ (only if over HIGH_WATERMARK)
Full PVC walk + stat → sort by mtime (LRU) → evict oldest-first until LOW_WATERMARK
```

---

## Trigger Mechanism

Janitor is **event-driven, not interval-based**. There is no polling loop.

Every time `write_atomic()` in `core/artifact_service.py` successfully
writes an artifact to PVC, the caller fires a non-blocking HTTP POST to
janitor's `/janitor/check` endpoint on localhost. This is the only trigger.

### Why localhost
All 3 containers (`mcs`, `synchronizer`, `janitor`) run in the same pod
and share `localhost`. The trigger call never leaves the pod — no service
discovery, no cross-pod traffic.

### Why fire-and-forget
`write_atomic()` is on the critical path for both synchronizer (pre-warm)
and mcs-serving (cache-miss fallback during Model Service requests). Janitor
must not add latency to either path. If janitor is unreachable or slow, the
caller logs a warning and continues — artifact delivery is never blocked by
eviction.

---

## Settings

```
JANITOR_HIGH_WATERMARK  float  0.90   # PVC usage fraction that triggers eviction
JANITOR_LOW_WATERMARK   float  0.75   # PVC usage fraction eviction targets
```

`JANITOR_INTERVAL_SECONDS` is removed — janitor is event-driven, not
interval-based.

Both watermarks are fractions (0.0–1.0) of total PVC capacity as reported
by `shutil.disk_usage()`.

---

## Gate Check

On every incoming `/janitor/check` trigger:

```python
import shutil

def is_over_high_watermark(storage_path: str, high_watermark: float) -> bool:
    usage = shutil.disk_usage(storage_path)
    return (usage.used / usage.total) > high_watermark
```

`shutil.disk_usage()` makes a single `statvfs()` syscall — it reads the
capacity and usage of the entire PVC filesystem mount, not individual files.
This is O(1) and cheap regardless of how many artifacts are stored.

If under `HIGH_WATERMARK`: no-op, return immediately.
If over `HIGH_WATERMARK`: proceed to eviction sweep.

---

## LRU Tracking — `os.utime()` Touch on Cache-Hit

To support LRU eviction without relying on filesystem `atime` (unreliable on
NFS-backed NetApp PVCs due to `relatime` mount semantics and ONTAP-level
atime settings), mcs-serving explicitly updates a file's `mtime` on every
successful cache-hit serve:

```python
# apps/mcs/router.py — _serve_artifact(), on cache-hit path
os.utime(dest, None)  # sets mtime = now, marks file as recently used
```

Janitor reads `st_mtime` from `os.stat()` as the last-used timestamp.
Files that have never been served retain their original write time as
`mtime` — naturally ranking below files that have been served recently.

This approach:
- Works regardless of NFS mount options or ONTAP atime configuration
- Requires no separate data store (no Redis, no sidecar index)
- Self-cleaning: when a file is evicted, its timestamp disappears with it

---

## Eviction Sweep

Triggered only when `is_over_high_watermark()` returns True.

### Step 1 — Collect all eviction candidates

Walk `{STORAGE_PATH}/{FAB_NAME}/` and collect every regular file that:
- Is **not** a `.tmp` file (`.tmp` = in-progress `write_atomic()` — must not be deleted)
- Is under `MODEL/`, `KERNEL/`, or `PACKAGE/` subdirectories

Note: MODEL and KERNEL paths include an `{id}` segment; PACKAGE does not
(one package per function). The walker handles both depth variants.

```
MODEL:   {STORAGE_PATH}/{FAB_NAME}/MODEL/{productID}/{funcID}/{id}/{version}
KERNEL:  {STORAGE_PATH}/{FAB_NAME}/KERNEL/{productID}/{funcID}/{id}/{version}
PACKAGE: {STORAGE_PATH}/{FAB_NAME}/PACKAGE/{productID}/{funcID}/{version}
```

### Step 2 — Sort by mtime ascending (LRU)

```python
candidates.sort(key=lambda f: f.st_mtime)  # oldest mtime first
```

### Step 3 — Delete oldest-first until LOW_WATERMARK

```python
usage = shutil.disk_usage(storage_path)
freed_bytes = 0

for file_path, stat in candidates:
    current_used = usage.used - freed_bytes
    if (current_used / usage.total) <= low_watermark:
        break
    freed_bytes += stat.st_size
    os.remove(file_path)
    _prune_empty_parents(file_path, storage_path)
    logger.info(f"Evicted: {file_path} size={stat.st_size} mtime={stat.st_mtime}")

if (usage.used - freed_bytes) / usage.total > low_watermark:
    logger.warning(
        "Eviction exhausted all candidates but did not reach LOW_WATERMARK. "
        "Consider setting JANITOR_LOW_WATERMARK to a higher value."
    )
```

`freed_bytes` tracks cumulative bytes deleted as a running subtraction from
the initial `used` value — avoids calling `disk_usage()` on every deletion.

### Step 4 — Prune empty parent directories

After each file deletion, walk up the directory tree and remove any empty
directories up to (but not including) `{STORAGE_PATH}/{FAB_NAME}/`:

```python
def _prune_empty_parents(file_path: Path, storage_root: Path) -> None:
    parent = file_path.parent
    while parent != storage_root and parent != storage_root.parent:
        try:
            parent.rmdir()  # only removes if empty — safe to call unconditionally
            parent = parent.parent
        except OSError:
            break  # directory not empty — stop climbing
```

---

## Re-entrancy Guard

Since multiple `write_atomic()` calls can arrive in quick succession (e.g.,
synchronizer broadcasting artifact downloads to all 3 pods), multiple
`/janitor/check` triggers may arrive while an eviction sweep is already
running. Janitor holds an `asyncio.Lock` on the sweep:

```python
_eviction_lock = asyncio.Lock()

async def check_and_evict():
    if _eviction_lock.locked():
        return  # sweep already in progress — skip, it will drive to LOW_WATERMARK
    async with _eviction_lock:
        if not is_over_high_watermark(...):
            return
        await run_eviction_sweep(...)
```

An incoming trigger while a sweep is in progress is silently dropped — the
in-flight sweep will already drive usage down to `LOW_WATERMARK`.

---

## Mid-Flight Serve Safety

If janitor deletes a file while mcs-serving is actively streaming it to a
Model Service client:

- Linux (and NFS in most implementations) keeps the file's inode and data
  alive until all open file descriptors are closed — `unlink()` only removes
  the directory entry.
- The in-flight `StreamingResponse` continues to completion uninterrupted.
- The file descriptor is released when the response finishes — the inode is
  freed at that point.
- The next request for the same artifact will be a cache miss and fall back
  to siteMC for re-download.

This is an accepted edge case. No locking between janitor and mcs-serving is
required.

---

## `/janitor/check` Endpoint

```
POST /janitor/check
Response: 202 Accepted (always — fire-and-forget, eviction runs async)
```

The endpoint enqueues the check-and-evict coroutine as an `asyncio.Task`
and returns `202` immediately so the caller (synchronizer/mcs-serving) is
never blocked.

```
GET /health
Response: {"ready": true, "evicting": false|true}
```

`evicting` reflects whether an eviction sweep is currently in progress
(i.e., whether `_eviction_lock` is held).

---

## Changes Required in Existing Code

| File | Change |
|---|---|
| `core/artifact_service.py` | After `write_atomic()` succeeds, fire `POST localhost:{JANITOR_PORT}/janitor/check` (non-blocking, swallow errors) |
| `apps/mcs/router.py` | On cache-hit path in `_serve_artifact()`, call `os.utime(dest, None)` to update mtime for LRU tracking |
| `core/config/settings.py` | Remove `JANITOR_INTERVAL_SECONDS`; add `JANITOR_PORT` |
| `helm/mcs/values.yaml` | Remove `JANITOR_INTERVAL_SECONDS` from `envConfig` |

---

## New Files

| File | Purpose |
|---|---|
| `apps/janitor/eviction.py` | `is_over_high_watermark()`, `run_eviction_sweep()`, `_prune_empty_parents()` |
| `apps/janitor/router.py` | `POST /janitor/check`, `GET /health` endpoints |
| `apps/janitor/lifespan.py` | FastAPI lifespan (settings load, logging) |

`apps/janitor/main.py` already exists as a stub — will be replaced with the
full implementation.

---

## Task List

- [ ] Remove `JANITOR_INTERVAL_SECONDS` from `settings.py` and `values.yaml`
- [ ] Add `JANITOR_PORT` to `settings.py` and `values.yaml`
- [ ] Implement `apps/janitor/eviction.py` — gate check, sweep, prune
- [ ] Implement `apps/janitor/router.py` — `/janitor/check`, `/health`
- [ ] Implement `apps/janitor/lifespan.py` — full startup
- [ ] Replace `apps/janitor/main.py` stub with full FastAPI app
- [ ] Update `core/artifact_service.py` — fire trigger after `write_atomic()`
- [ ] Update `apps/mcs/router.py` — `os.utime()` touch on cache-hit
- [ ] Test: write artifact → janitor trigger fires → gate check runs
- [ ] Test: PVC over HIGH_WATERMARK → eviction sweep → reaches LOW_WATERMARK
- [ ] Test: concurrent triggers during active sweep → re-entrancy guard holds
- [ ] Test: empty parent directories pruned after eviction
