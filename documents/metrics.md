# MCS Metrics — Prometheus Reference

> Branch: `feature/prometheus-metrics`

Every MCS container exposes its own `GET /metrics` endpoint (Prometheus
text exposition format, via `prometheus_client`'s default registry). There
is no shared/aggregated endpoint — each of the 3 containers in a pod
answers only for itself.

| Container | Port (see `values.yaml`) | Endpoint |
|---|---|---|
| `mcs` | `mcs-port` (8080 via service) | `GET /metrics` |
| `synchronizer` | `synchronizer-port` (8081 via service) | `GET /metrics` |
| `janitor` | `janitor-port` (8082 via service) | `GET /metrics` |

Scraping: enable `serviceMonitor.enabled: true` in `values.yaml` if your
cluster runs prometheus-operator (scrapes all 3 named ports automatically),
or point your own scrape config at each port's `/metrics` path directly.

---

## `mcs` (serving) metrics

### `mcs_requests_total`

**Type:** Counter
**Labels:** `endpoint` (matched route template, e.g. `/mcs/model`), `status` (HTTP status code)
**What it counts:** Every request to any mcs-serving endpoint, recorded by a FastAPI middleware — covers all 7 endpoints (`/mcs/model`, `/mcs/kernel`, `/mcs/package`, `/mcs/model_list/{function_id}`, `/mcs/kernel_list/{function_id}`, `/mcs/package_list/{function_id}`, `/mcs/active_pats/{function_id}`), plus `/health` and `/metrics` itself.

Use this to see overall traffic volume and error rates per endpoint:
```promql
sum(rate(mcs_requests_total{status=~"5.."}[5m])) by (endpoint)
```

---

### `mcs_cache_hits_total`

**Type:** Counter
**Labels:** `artifact_type` (`MODEL`, `KERNEL`, `PACKAGE`)
**What it counts:** Artifact requests served directly from PVC — no siteMC round trip. Incremented in `_try_serve_cached()` right before streaming the response, for both the PACKAGE (plaintext) and MODEL/KERNEL (decrypt-then-stream) paths.

---

### `mcs_cache_misses_total`

**Type:** Counter
**Labels:** `artifact_type`
**What it counts:** Artifact requests that had nothing usable on PVC and fell back to siteMC. Incremented once per miss, before the siteMC fetch begins (so it counts attempts, not just successes — a subsequent 502 from siteMC still counts as a miss here).

Cache hit ratio:
```promql
sum(rate(mcs_cache_hits_total[15m])) by (artifact_type)
/
(sum(rate(mcs_cache_hits_total[15m])) by (artifact_type) + sum(rate(mcs_cache_misses_total[15m])) by (artifact_type))
```

---

### `mcs_sitemc_fetch_duration_seconds`

**Type:** Histogram
**Labels:** `artifact_type`
**What it measures:** Wall-clock time of the `fetch_artifact_bytes()` call on a cache miss — includes siteMC auth, download, and (for MODEL) the RSA+AES-CBC transport decrypt. Does **not** include the PVC write-through or client streaming time.

Use this to catch siteMC latency degradation before it becomes a Model Service timeout:
```promql
histogram_quantile(0.95, sum(rate(mcs_sitemc_fetch_duration_seconds_bucket[5m])) by (le, artifact_type))
```

---

### `mcs_cache_corrupt_total`

**Type:** Counter
**Labels:** `artifact_type`, `reason` (`orphaned_meta` | `decrypt_failure`)
**What it counts:** Cache entries deleted because they couldn't be served:
- `orphaned_meta` — the artifact file exists but its `.meta` sidecar is missing (e.g. pod crashed between the two atomic writes)
- `decrypt_failure` — `.meta` parses but the artifact fails Fernet decryption, size validation, or has an unrecognized `algorithm` (corrupted bytes, or the artifact was encrypted with a key that's since been rotated)

A non-zero rate here is a signal worth investigating — occasional `orphaned_meta` after a pod restart is expected/self-healing (next request just re-fetches from siteMC), but a sustained `decrypt_failure` rate usually means the `ENCRYPTION_KEY` Vault secret changed without a coordinated cache flush.

```promql
sum(rate(mcs_cache_corrupt_total[15m])) by (reason)
```

---

## `synchronizer` metrics

### `synchronizer_nats_messages_total`

**Type:** Counter
**Labels:** `stream` (`artifact` | `metadata`), `result` (`ack` | `nak`)
**What it counts:** Every NATS message the synchronizer finishes processing, on both the artifact consumer (broadcast, one per pod) and the metadata consumer (queue group, shared across pods). Covers all outcomes — successful processing, unparseable messages, unknown `function_id`/`product_id`, and handler failures all resolve to either `ack` or `nak`.

A sustained `nak` rate means messages are being redelivered — check `synchronizer_artifact_download_failures_total` or the container logs for the underlying cause (siteMC unreachable, bad credentials, etc.):
```promql
sum(rate(synchronizer_nats_messages_total{result="nak"}[5m])) by (stream)
```

---

### `synchronizer_artifact_download_duration_seconds`

**Type:** Histogram
**Labels:** `artifact_type`
**What it measures:** Time to download + transport-decrypt + partially-encrypt-at-rest + write one artifact to PVC (the full `_download_artifact()` call), for artifacts that were not already cached. Does not include time spent on already-cached artifacts (those return immediately and aren't timed).

---

### `synchronizer_artifact_download_failures_total`

**Type:** Counter
**Labels:** `artifact_type`
**What it counts:** Artifact downloads that raised an exception and resulted in a NAK (redelivery). A subset of `synchronizer_nats_messages_total{stream="artifact",result="nak"}` — this one is scoped specifically to download failures (as opposed to e.g. unknown `function_id`, which acks and doesn't retry).

---

### `synchronizer_metadata_updates_total`

**Type:** Counter
**Labels:** `meta_type` (`model_list` | `kernel_list` | `package_list` | `pat_list`)
**What it counts:** Successful Redis writes from incoming `MLOP-MCS-METADATA` messages — incremented only when the handler completes and the message is ACKed, not on every message received (a `model_list` update where the model isn't in the `online` list still ACKs the message but does **not** increment this, since no Redis write happened).

---

### `synchronizer_redis_warmup_duration_seconds`

**Type:** Histogram (effectively a single observation per pod startup)
**Labels:** none
**What it measures:** Total time for the startup warm-up sweep (`warm_up_redis()`) across every configured product and function_id, before NATS consumers are created. A pod that already finds Redis populated (pod-1/2 behind pod-0) will still record a fast warm-up — the per-key existence checks are quick even when fetches are skipped.

Useful for catching startup slowness before it trips the readiness probe:
```promql
synchronizer_redis_warmup_duration_seconds_sum / synchronizer_redis_warmup_duration_seconds_count
```

---

## `janitor` metrics

### `janitor_disk_usage_ratio`

**Type:** Gauge
**Labels:** none
**What it measures:** Current PVC usage as a fraction of total capacity (`0.0`–`1.0`), matching what `shutil.disk_usage()` reports. Updated on every `/janitor/check` trigger (i.e. after every artifact write from synchronizer or mcs-serving) — **not** on a timer, since janitor has no polling loop.

This is the most useful single metric for capacity alerting:
```promql
janitor_disk_usage_ratio > 0.85
```

Because updates are event-driven, this gauge only reflects reality as of the last artifact write. On a quiet pod with no recent writes, it holds its last known value rather than live-polling — acceptable since disk usage on an idle pod doesn't change without a write anyway.

---

### `janitor_eviction_sweeps_total`

**Type:** Counter
**Labels:** `result` (`completed` | `exhausted`)
**What it counts:** Eviction sweeps that actually ran (i.e. usage was over `JANITOR_HIGH_WATERMARK` at trigger time). `completed` means usage was driven down to `JANITOR_LOW_WATERMARK` or below; `exhausted` means every eviction candidate was deleted and usage was still above `LOW_WATERMARK` afterward — a sign `JANITOR_LOW_WATERMARK` may need to be raised for this deployment's traffic pattern.

Triggers that arrive while a sweep is already in progress (re-entrancy guard) are silently dropped and do **not** appear here.

---

### `janitor_files_evicted_total`

**Type:** Counter
**Labels:** none
**What it counts:** Individual files deleted by eviction sweeps. For MODEL/KERNEL artifacts, deleting the `.meta` sidecar alongside the artifact does **not** add a second count here — only the primary artifact file counts (the sidecar's bytes are still included in `janitor_bytes_freed_total`).

---

### `janitor_bytes_freed_total`

**Type:** Counter
**Labels:** none
**What it counts:** Cumulative bytes reclaimed by eviction — sum of each evicted artifact's size plus its `.meta` sidecar's size where applicable.

Disk reclaim rate:
```promql
rate(janitor_bytes_freed_total[1h])
```

---

### `janitor_eviction_duration_seconds`

**Type:** Histogram
**Labels:** none
**What it measures:** Wall-clock time of one full `run_eviction_sweep()` call — the directory walk, sort, and delete loop. Does not include the disk-usage gate check that happens before deciding whether to sweep at all.

---

## Suggested alerts

| Alert | Expression | Why |
|---|---|---|
| PVC filling despite eviction | `janitor_disk_usage_ratio > 0.95` for 10m | Janitor may be exhausting candidates repeatedly — check `janitor_eviction_sweeps_total{result="exhausted"}` |
| Cache mostly missing | `mcs_cache_hits_total / (mcs_cache_hits_total + mcs_cache_misses_total) < 0.5` sustained | Synchronizer may not be keeping PVC warm, or watermark eviction is too aggressive |
| Corrupted cache entries | `increase(mcs_cache_corrupt_total{reason="decrypt_failure"}[1h]) > 0` | Possible key rotation without cache flush, or disk corruption |
| NATS redelivery storm | `rate(synchronizer_nats_messages_total{result="nak"}[5m]) > 0` sustained | siteMC unreachable, bad credentials, or a persistent handler bug |
| siteMC latency creeping up | `histogram_quantile(0.95, rate(mcs_sitemc_fetch_duration_seconds_bucket[5m])) > 5` | siteMC-side degradation, will surface as Model Service timeouts |
