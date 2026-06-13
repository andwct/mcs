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

**Status:** ✅ Already implemented (PR #1), reviewed and confirmed correct:

- `ensure_artifact_consumer()` creates durable
  `artifact-sync-{pod_name}-{func_id}` with a **unique `deliver_subject`**
  (`artifact-sync-{pod_name}-{func_id}.deliver`), `filter_subject =
  {func_id}-{sanitized_name}`, `deliver_policy=NEW`, `ack_policy=EXPLICIT`,
  `ack_wait=300s`
- `lifespan.py` step 6 calls this for every `(func_id)` on every pod, then
  subscribes to that pod's own deliver subject with
  `handle_artifact_message`
- Each pod independently fetches new model/kernel artifact versions into
  its own PVC when it receives a `model-update` style event

**Idempotency / error handling (this branch):** `ensure_artifact_consumer`
now distinguishes:
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

**Status:** ✅ Already implemented (PR #1), reviewed and confirmed correct:

- `ensure_metadata_consumer()` creates **one durable per `func_id`**
  (`metadata-sync-{func_id}-{sanitized_name}`), `filter_subject =
  {func_id}-{sanitized_name}`, `deliver_policy=NEW`, `ack_policy=EXPLICIT`,
  `ack_wait=30s`, `replay_policy=INSTANT`
- All 3 pods call `add_consumer()` with the **same name + config** on
  startup — first pod creates it, the other two get "already exists" and
  reuse it (now logged distinctly, see Task 2 error handling section,
  same logic applies here)
- `fetch_loop.py` runs one `asyncio.Task` per `func_id`, each pod's task
  calls `consumer.fetch(batch=1, timeout=5.0)` on the **same shared
  durable consumer** — NATS delivers each message to exactly one fetching
  pod, giving queue-group semantics for pull consumers
- On success: `handle_metadata_message` updates `model_list` in Redis,
  then `msg.ack()`
- On failure: `msg.nak()` -> NATS redelivers to whichever pod fetches next
  after `ack_wait` (30s)

**No code changes needed** beyond the shared error-handling fix in Task 2.

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
- **subscribe** to its own artifact deliver subjects (broadcast push)
- **pull** from metadata consumers (`$JS.API.CONSUMER.MSG.NEXT...`)
- **subscribe** to `_INBOX.>` — NATS request-reply pattern used
  internally by all JetStream API calls

---

## 5. Creating `nats.creds` for MCS (action for platform team / you)

NATS decentralized auth: a `.creds` file bundles a **user JWT** + **NKey
seed**. Created via `nsc` (NATS account/user CLI), scoped to an existing
**operator/account** for siteMC.

```bash
# 1. Select the siteMC operator + account context (adjust names to your setup)
nsc env -o <SITEMC_OPERATOR>
nsc env -a <SITEMC_ACCOUNT>

# 2. Create a dedicated user for MCS's synchronizer
nsc add user mcs-synchronizer \
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
nsc generate creds -a <SITEMC_ACCOUNT> -n mcs-synchronizer > nats.creds

# 4. Push contents into Vault at the path configured in values.yaml
#    (vaultSecrets[0].path, e.g. kv-mlp/mlop-secret/mcs), key "nats.creds"
```

**Notes:**
- `$JS.API.CONSUMER.DURABLE.CREATE.<stream>.>` is the legacy alias for
  creating **durable** consumers; included alongside
  `$JS.API.CONSUMER.CREATE.<stream>.>` for compatibility across NATS
  server versions.
- `artifact-sync-*.deliver` covers all 3 pods' deliver subjects
  (`artifact-sync-mcs-statefulset-0-<func_id>.deliver`, `-1-`, `-2-`)
  since they all share the `artifact-sync-*` prefix and `.deliver` suffix.
- `$JS.API.CONSUMER.DELETE...` included so a future re-deploy with changed
  consumer config (e.g. different `ack_wait`) can clean up and recreate —
  not currently used by the synchronizer but harmless to include now.
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

