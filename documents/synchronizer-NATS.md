# Synchronizer NATS — Task List

> Branch: `feature/synchronizer-NATS`
> Status: In progress
> Goal: get synchronizer running in SIT, connecting to siteMC NATS, and
> automatically creating consumers + subjects for all configured functions.

---

## 1. `FileNotFoundError: No product_*.json files found in /etc/config`

**Symptom (from pod logs):**
```
raise FileNotFoundError(f"No product_*.json files found in {mount}")
FileNotFoundError: No product_*.json files found in /etc/config
Application startup failed. Exiting.
Worker exiting (pid: 7)
Shutting down: Master Reason: Worker failed to boot
```

**Status:** ✅ Already fixed on `main` (PR #1, PR #2) — `core/k8s/configmap.py`
now globs `*.json` (excluding `CONFIGMAP_ENV_CONFIG_FILE`), matching the
`{ProductName}.json` convention (e.g. `ABC.json`) used in `values.yaml`'s
`configMap.data`.

**Root cause:** the error message format (`product_*.json` with underscore)
matches the **old** pre-fix glob pattern (`mount.glob("product_*.json")`).
The pod that produced this log is running an **older image** built before
this fix landed on `main`.

**Action needed:**
- [ ] Confirm the image tag deployed in SIT corresponds to a build that
      includes the `main` branch changes through PR #3 (requirements.txt
      alignment) — i.e. rebuild/repush the image if it predates the fix
- [ ] Re-deploy with the updated image and confirm `/etc/config` now
      contains `one.properties` + `ABC.json` (per rolled-back
      `values.yaml` on this branch) and the synchronizer starts past
      `load_product_configs()`

---

## 2. (next task — TBD)

---

## Notes

- `values.yaml` on this branch (`rollback-values-v1.0.13`/`main` after
  merge) mounts `configMap.data` containing `one.properties` (key=value,
  includes NATS_URL/REDIS_SENTINEL_*/etc) and `ABC.json` (product config
  with `FUNCTION_LIST` + `FUNCTION_NAME_MAPPING`).
- `apps/synchronizer/main.py` calls
  `core.k8s.bootstrap.bootstrap_env_from_one_properties()` first, before
  any module-level `get_settings()` call, so `one.properties` values land
  in `os.environ` before `pydantic-settings` reads them.
- Expected successful startup sequence (per
  `documents/synchronizer.md` §6):
  1. Load `one.properties` + `{Product}.json` from `/etc/config`
  2. Connect NATS (`nats.creds` from Vault-synced secret)
  3. Verify `MLOP-MCS-ARTIFACT` / `MLOP-MCS-METADATA` streams exist
  4. Connect Redis Sentinel
  5. Resolve pod name from `HOSTNAME`
  6. Per function_id: create artifact push consumer + metadata pull consumer
  7. Subscribe to artifact deliver subjects
  8. Start metadata fetch loops
  9. `/health` returns 200
