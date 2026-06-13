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

**User naming:** each MCS deployment/site needs its own user. Use the
**StatefulSet name** (`fullnameOverride` in `values.yaml` — same value
used to derive `metadata-sync-{statefulset_name}`) as the identifier, so
NATS user naming stays consistent with consumer naming and is
automatically unique per deployment:

```
mcs-{statefulset_name}-synchronizer
```

e.g. if `fullnameOverride: mcs-statefulset`, the user is
`mcs-mcs-statefulset-synchronizer`. If `fullnameOverride` is already
deployment-unique (which it must be, per Task "Consumer scope redesign"),
this naming requires no additional site/environment identifier.

Full script (also at `documents/generate-mcs-creds.sh`) — edit
`STATEFULSET_NAME` and `NATS_URL` at the top, then run via
[`synadia/nats-box`](https://github.com/synadia-io/nats-box) (bundles
`nsc` + `nats` CLI + `nk`):

```bash
docker run --rm -it -v $(pwd):/work -w /work synadia/nats-box:latest \
  bash documents/generate-mcs-creds.sh
```

```bash
#!/bin/bash
# ============================================================================
# Generate nats.creds for an MCS synchronizer deployment
#
# Run inside the synadia/nats-box container, which bundles nsc + nats CLI:
#
#   docker run --rm -it -v $(pwd):/work -w /work synadia/nats-box:latest \
#     bash generate-mcs-creds.sh
#
# Or, if nsc/nats are installed locally, just run this script directly.
# ============================================================================
set -euo pipefail

# ── EDIT THIS ───────────────────────────────────────────────────────────────
SITEMC_OPERATOR="mlp"                        # siteMC NATS operator (only one exists)
SITEMC_ACCOUNT="mlop"                        # siteMC NATS account
STATEFULSET_NAME="<STATEFULSET_NAME>"        # .Values.fullnameOverride, e.g. mcs-statefulset
NATS_URL="<NATS_URL>"                        # e.g. nats://mlop-nats-new.mlop-site-model-center.svc.cluster.local:4222

ARTIFACT_STREAM="MLOP-MCS-ARTIFACT"
METADATA_STREAM="MLOP-MCS-METADATA"
USER_NAME="mcs-${STATEFULSET_NAME}-synchronizer"
OUTPUT_CREDS="nats.creds"
# ─────────────────────────────────────────────────────────────────────────────

echo "== Selecting operator/account context =="
nsc env -o "${SITEMC_OPERATOR}"
nsc env -a "${SITEMC_ACCOUNT}"

echo "== Creating user: ${USER_NAME} =="

# If the user already exists from a previous run, remove it first so the
# permission set below is applied cleanly (nsc add user fails if it exists).
if nsc describe user "${USER_NAME}" >/dev/null 2>&1; then
  echo "User ${USER_NAME} already exists — deleting before recreate"
  nsc delete user "${USER_NAME}"
fi

nsc add user "${USER_NAME}" \
  --allow-pub "\$JS.API.STREAM.INFO.${ARTIFACT_STREAM}" \
  --allow-pub "\$JS.API.STREAM.INFO.${METADATA_STREAM}" \
  --allow-pub "\$JS.API.CONSUMER.CREATE.${ARTIFACT_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.CREATE.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.DURABLE.CREATE.${ARTIFACT_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.DURABLE.CREATE.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.INFO.${ARTIFACT_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.INFO.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.DELETE.${ARTIFACT_STREAM}.>" \
  --allow-sub "artifact-sync-${STATEFULSET_NAME}-*.deliver" \
  --allow-sub '_INBOX.>'

echo "== Generating ${OUTPUT_CREDS} =="
nsc generate creds -a "${SITEMC_ACCOUNT}" -n "${USER_NAME}" > "${OUTPUT_CREDS}"
echo "Wrote ${OUTPUT_CREDS}"

echo "== Verifying connectivity =="
if command -v nats >/dev/null 2>&1; then
  nats account info --creds "${OUTPUT_CREDS}" --server "${NATS_URL}"
else
  echo "nats CLI not found — skipping connectivity check."
  echo "Run manually: nats account info --creds ${OUTPUT_CREDS} --server ${NATS_URL}"
fi

echo
echo "== Done =="
echo "Next: store the contents of ${OUTPUT_CREDS} in Vault at the path"
echo "configured in values.yaml (vaultSecrets[0].path), under key 'nats.creds'."
```

**Notes:**
- `$JS.API.CONSUMER.DURABLE.CREATE.<stream>.>` is the legacy alias for
  creating **durable** consumers; included alongside
  `$JS.API.CONSUMER.CREATE.<stream>.>` for compatibility across NATS
  server versions.
- `artifact-sync-<STATEFULSET_NAME>-*.deliver` covers exactly 3 deliver
  subjects for this deployment (`artifact-sync-{fullname}-0.deliver`,
  `-1-`, `-2-`) — one per pod, regardless of how many `func_id`s are
  configured. Scoping the wildcard to `<STATEFULSET_NAME>` (rather than a
  bare `artifact-sync-*`) also prevents this user from subscribing to
  another deployment's deliver subjects.
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

