# Synchronizer NATS — Task List

> Branch: `feature/synchronizer-NATS`
> Status: In progress
> Goal: get synchronizer running in SIT, connecting to siteMC NATS, and
> automatically creating consumers + subjects for all configured functions.

---

## 1. `FileNotFoundError: No product_*.json files found in /etc/config`

**Status:** ✅ Code-side fixed on `main` (PR #1, #2). One more bug found and
fixed on this branch:

**Second blocker found:** `values.yaml`'s `appModule` values were
`app.mcs.main:app` / `app.synchronizer.main:app` / `app.janitor.main:app`
(singular `app`), but the actual package is `apps/` (plural). This would
cause `ModuleNotFoundError: No module named 'app'` from uvicorn even after
the `*.json` fix. Fixed: all three now read `apps.<name>.main:app`.

**Action needed:**
- [ ] Rebuild image from latest `main` (includes `*.json` glob fix +
      `appModule` fix from this branch once merged)
- [ ] Redeploy and confirm synchronizer pod passes
      `load_product_configs()` and `uvicorn` boots without
      `ModuleNotFoundError`

---

## 2. Artifact Stream — Push Consumer (Broadcast), one per pod

**Status:** ✅ Redesigned on this branch (see "Consumer scope redesign"
below).

- `ensure_artifact_consumer()` creates **ONE** durable per pod:
  `artifact-sync-{pod_name}` (e.g. `artifact-sync-mcs-statefulset-0`),
  with `filter_subjects` = **all** `{func_id}-{sanitized_name}` subjects
  configured across every product in `/etc/config`
- Unique `deliver_subject` per pod: `artifact-sync-{pod_name}.deliver`
- `deliver_policy=NEW`, `ack_policy=EXPLICIT`, `ack_wait=300s`
- `lifespan.py` step 6 calls this once per pod startup, then subscribes
  to that pod's own deliver subject with `handle_artifact_message`
- Each pod independently fetches new model/kernel artifact versions into
  its own PVC for ANY configured `func_id` when it receives a
  `model-update` style event

**Idempotency / error handling (this branch):** `ensure_artifact_consumer`
distinguishes:
- "consumer already exists" — detected via JetStream API error code
  `10013` (`JSConsumerNameExistErr`, the documented nats-server code for
  "consumer name already in use"), with a fallback check on `description`
  containing "already" in case a different server version returns a
  different code -> log info (with `err_code`/`description` for
  visibility), reuse (expected on every pod restart)
- any other `APIError` or exception -> log error with `err_code` +
  `description`, raise `RuntimeError` -> pod restarts (no silent failures)

---

## 3. Metadata Stream — Pull Consumer (Queue Group), shared across pods

**Status:** ✅ Redesigned on this branch (see "Consumer scope redesign"
below).

- `ensure_metadata_consumer()` creates **ONE** durable for the entire
  deployment: `metadata-sync-{statefulset_name}` (e.g.
  `metadata-sync-mcs-statefulset` — no ordinal, shared identically by all
  3 pods), with `filter_subjects` = **all** `{func_id}-{sanitized_name}`
  subjects configured across every product in `/etc/config`
- `deliver_policy=NEW`, `ack_policy=EXPLICIT`, `ack_wait=30s`,
  `replay_policy=INSTANT`
- All 3 pods call `add_consumer()` with the **same name + config** on
  startup — first pod creates it, the other two get "already exists" and
  reuse it (same error-handling as Task 2)
- `fetch_loop.py` runs a **single** `asyncio.Task` per pod, fetching from
  this one shared durable consumer — NATS delivers each message (for any
  configured `func_id`) to exactly one fetching pod, giving queue-group
  semantics across the whole deployment
- On success: `handle_metadata_message` updates `model_list` in Redis,
  then `msg.ack()`
- On failure: `msg.nak()` -> NATS redelivers to whichever pod fetches next
  after `ack_wait` (30s)

---

## Consumer scope redesign (this branch)

**Previous design (PR #1):** one consumer **per `(pod, func_id)`** for
artifacts, one consumer **per `func_id`** for metadata — i.e. N consumers
where N = number of configured functions.

**New design (this branch):** `func_id` moves entirely into
**`filter_subjects`** (a list), and consumer identity is scoped to
**deployment**, not function:

| Consumer | Scope | Name | filter_subjects |
|---|---|---|---|
| Artifact (push) | Per pod | `artifact-sync-{pod_name}` | all configured `{func_id}-{sanitized_name}` subjects |
| Metadata (pull) | Per deployment (shared) | `metadata-sync-{statefulset_name}` | all configured `{func_id}-{sanitized_name}` subjects |

**Why:**
- Adding/removing a `func_id` from `productConfig` no longer requires
  creating/deleting NATS consumers — just update `filter_subjects` (a
  config-only change)
- Consumer **names** are now stable across config changes and naturally
  unique across deployments as long as `fullnameOverride` is unique
  (artifact consumer includes pod ordinal; metadata consumer is the
  StatefulSet name without ordinal, identical across this deployment's 3
  pods — required for queue-group `fetch()` to work)
- `core/k8s/pod.py` gained `get_statefulset_name()`: strips the trailing
  `-<ordinal>` from `HOSTNAME` (falls back to full pod name with a warning
  if it doesn't match `<name>-<digits>`)

**Verified:**
```
pod=mcs-statefulset-0  artifact_consumer=artifact-sync-mcs-statefulset-0   metadata_consumer=metadata-sync-mcs-statefulset
pod=mcs-statefulset-1  artifact_consumer=artifact-sync-mcs-statefulset-1   metadata_consumer=metadata-sync-mcs-statefulset
pod=mcs-statefulset-2  artifact_consumer=artifact-sync-mcs-statefulset-2   metadata_consumer=metadata-sync-mcs-statefulset
```
3 distinct artifact consumers (broadcast), 1 shared metadata consumer
(queue group) — confirmed with a 2-function product config, both subjects
correctly appear in both consumers' `filter_subjects`.

---

## 4. Consumer creation permissions — platform vs MCS

**Decision:** Platform engineers pre-create **streams only**
(`MLOP-MCS-ARTIFACT`, `MLOP-MCS-METADATA` — already done ✅). MCS's NATS
user needs permission to:
- **create consumers** on both streams (`$JS.API.CONSUMER.CREATE...`,
  `$JS.API.CONSUMER.DURABLE.CREATE...`)
- **query consumer/stream info** (`$JS.API.CONSUMER.INFO...`,
  `$JS.API.STREAM.INFO...`) — used by `find_stream`/`verify_stream` and
  `add_consumer`'s idempotency check
- **subscribe** to its own artifact deliver subject (broadcast push)
- **pull** from the shared metadata consumer (`$JS.API.CONSUMER.MSG.NEXT...`)
- **subscribe** to `_INBOX.>` — NATS request-reply pattern used
  internally by all JetStream API calls

---

## 5. Creating `nats.creds` for MCS (action for platform team / you)

NATS decentralized auth: a `.creds` file bundles a **user JWT** + **NKey
seed**. Created via `nsc` (NATS account/user CLI), scoped to an existing
**operator/account** for siteMC.

**User naming:** each MCS deployment/site needs its own user — include a
site/deployment identifier, e.g. `mcs-<site-name>-synchronizer`
(`mcs-sit01-synchronizer`, `mcs-prod-tpe-synchronizer`, etc.) to avoid
collisions across deployments sharing the same siteMC NATS.

```bash
# 1. Select the siteMC operator + account context (adjust names to your setup)
nsc env -o <SITEMC_OPERATOR>
nsc env -a <SITEMC_ACCOUNT>

# 2. Create a dedicated user for this MCS deployment's synchronizer
#    Replace <SITE> with a unique identifier for this deployment.
nsc add user mcs-<SITE>-synchronizer \
  --allow-pub '$JS.API.STREAM.INFO.MLOP-MCS-ARTIFACT' \
  --allow-pub '$JS.API.STREAM.INFO.MLOP-MCS-METADATA' \
  --allow-pub '$JS.API.CONSUMER.CREATE.MLOP-MCS-ARTIFACT.>' \
  --allow-pub '$JS.API.CONSUMER.CREATE.MLOP-MCS-METADATA.>' \
  --allow-pub '$JS.API.CONSUMER.DURABLE.CREATE.MLOP-MCS-ARTIFACT.>' \
  --allow-pub '$JS.API.CONSUMER.DURABLE.CREATE.MLOP-MCS-METADATA.>' \
  --allow-pub '$JS.API.CONSUMER.INFO.MLOP-MCS-ARTIFACT.>' \
  --allow-pub '$JS.API.CONSUMER.INFO.MLOP-MCS-METADATA.>' \
  --allow-pub '$JS.API.CONSUMER.MSG.NEXT.MLOP-MCS-METADATA.>' \
  --allow-pub '$JS.API.CONSUMER.DELETE.MLOP-MCS-ARTIFACT.>' \
  --allow-sub 'artifact-sync-*.deliver' \
  --allow-sub '_INBOX.>'

# 3. Generate the .creds file (JWT + NKey seed bundled together)
nsc generate creds -a <SITEMC_ACCOUNT> -n mcs-<SITE>-synchronizer > nats.creds

# 4. Push contents into Vault at the path configured in values.yaml
#    (vaultSecrets[0].path, e.g. kv-mlp/mlop-secret/mcs), key "nats.creds"
```

**Notes:**
- `$JS.API.CONSUMER.DURABLE.CREATE.<stream>.>` is the legacy alias for
  creating **durable** consumers; included alongside
  `$JS.API.CONSUMER.CREATE.<stream>.>` for compatibility across NATS
  server versions.
- `artifact-sync-*.deliver` now covers exactly 3 deliver subjects per
  deployment (`artifact-sync-{fullname}-0.deliver`, `-1-`, `-2-`) — one
  per pod, regardless of how many `func_id`s are configured.
- `$JS.API.CONSUMER.DELETE...` included so a future re-deploy with changed
  consumer config (e.g. different `ack_wait` or `filter_subjects`) can
  clean up and recreate — not currently used by the synchronizer but
  harmless to include now. Note: changing `filter_subjects` on an existing
  durable consumer may require delete+recreate depending on server
  version, since `add_consumer` with a changed config for an existing
  name returns "already exists" rather than updating it in place.
- If `nsc` reports the account/operator already exists with a different
  CLI workflow (e.g. memory-resolver vs full-resolver setup), the
  `--allow-pub`/`--allow-sub` flags and `generate creds` command are the
  parts that matter; `nsc env` setup may differ per your existing siteMC
  NATS configuration.

**Action needed:**
- [ ] Run the `nsc` commands above against siteMC's operator/account
- [ ] Verify generated `nats.creds` works: `nats account info
      --creds nats.creds --server <NATS_URL>`
- [ ] Store `nats.creds` content in Vault at `vaultSecrets[0].path`
      (see `values.yaml`), key `nats.creds`

