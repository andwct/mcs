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
  --allow-pub '$JS.API.INFO' \
  --allow-pub "\$JS.API.STREAM.INFO.${ARTIFACT_STREAM}" \
  --allow-pub "\$JS.API.STREAM.INFO.${METADATA_STREAM}" \
  --allow-pub "\$JS.API.CONSUMER.CREATE.${ARTIFACT_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.CREATE.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.DURABLE.CREATE.${ARTIFACT_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.DURABLE.CREATE.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.INFO.${ARTIFACT_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.INFO.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.${ARTIFACT_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.MSG.NEXT.${METADATA_STREAM}.>" \
  --allow-pub "\$JS.API.CONSUMER.DELETE.${ARTIFACT_STREAM}.>" \
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
