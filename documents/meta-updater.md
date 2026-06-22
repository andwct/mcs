# MetaUpdater — Design Document

> Language: Java (existing service, adding publish logic)
> Status: Design phase
> Role: Bridge between Model Center NATS and siteMC NATS for MCS streams

---

## Overview

MetaUpdater is an upstream service of MCS that bridges two NATS clusters:

```
Model Center NATS                    siteMC NATS
─────────────────                    ──────────────────────────────
MCS-UPDATE stream          →         MLOP-MCS-ARTIFACT stream
(one stream, all updates)  →         MLOP-MCS-METADATA stream
```

It subscribes to a single stream on Model Center NATS, transforms each
message, and publishes to one of two streams on siteMC NATS depending
on the message content.

---

## NATS Connections

MetaUpdater maintains **two simultaneous NATS connections**:

| Connection | Server | Purpose |
|---|---|---|
| MC NATS | Model Center NATS server | Subscribe to `MCS-UPDATE` stream |
| siteMC NATS | `mlop-nats-new` (site NATS) | Publish to `MLOP-MCS-ARTIFACT` and `MLOP-MCS-METADATA` |

---

## Model Center NATS — Incoming Stream

### Stream name
`MCS-UPDATE`

### Subject pattern
`MCS-UPDATE.{function_id}`

### Message schema (new design)

All updates (both artifact and metadata) arrive on this single stream.
The `update_type` field determines which siteMC stream to forward to.

```json
{
  "update_type": "ARTIFACT" | "METADATA",

  "function_id": "funcID_123",
  "product_id": "productID_ABC",

  "meta_type": "model_list" | "kernel_list" | "package_list" | "pat_list",

  "artifact_type": "MODEL" | "KERNEL" | "PACKAGE",

  "model_id": "uuid-model-1",
  "kernel_id": "uuid-kernel-1",
  "package_id": "uuid-package-1",
  "deployed_version": "v1.0.0"
}
```

**Field usage by update_type:**

| Field | ARTIFACT msg | METADATA msg |
|---|---|---|
| `update_type` | `"ARTIFACT"` | `"METADATA"` |
| `function_id` | ✅ required | ✅ required |
| `product_id` | ✅ required | ✅ required |
| `artifact_type` | ✅ required (`MODEL`/`KERNEL`/`PACKAGE`) | ❌ omit |
| `deployed_version` | ✅ required | ❌ omit |
| `model_id` | ✅ required when `artifact_type=MODEL` | ✅ required when `meta_type=model_list` |
| `kernel_id` | ✅ required when `artifact_type=KERNEL` | ❌ omit |
| `package_id` | ✅ required when `artifact_type=PACKAGE` | ❌ omit |
| `meta_type` | ❌ omit | ✅ required |

---

## Transformation Logic

```
Receive msg from MCS-UPDATE stream
  │
  ├── update_type == "ARTIFACT"
  │     → Strip: meta_type
  │     → Build ArtifactMessage:
  │         {function_id, product_id, artifact_type,
  │          deployed_version, model_id, kernel_id, package_id}
  │     → Publish to MLOP-MCS-ARTIFACT.{function_id}-{function_name}
  │
  └── update_type == "METADATA"
        → Strip: artifact_type, deployed_version, kernel_id, package_id
        → Build MetadataMessage:
            {function_id, product_id, meta_type, model_id}
        → Publish to MLOP-MCS-METADATA.{function_id}-{function_name}
```

No external lookups needed — all fields required for transformation
are present in the incoming message.

---

## siteMC NATS — Outgoing Messages

### ArtifactMessage (→ `MLOP-MCS-ARTIFACT`)

Exact schema MCS synchronizer already consumes:

```json
{
  "function_id": "funcID_123",
  "product_id": "productID_ABC",
  "artifact_type": "MODEL",
  "deployed_version": "v1.0.0",
  "model_id": "uuid-model-1",
  "kernel_id": null,
  "package_id": null
}
```

### MetadataMessage (→ `MLOP-MCS-METADATA`)

Exact schema MCS synchronizer already consumes:

```json
{
  "function_id": "funcID_123",
  "product_id": "productID_ABC",
  "meta_type": "model_list",
  "model_id": "uuid-model-1"
}
```

---

## Java Implementation Scope

### Already exists
- NATS subscribe logic (subscribes to MC NATS)

### New work required
1. **Second NATS connection** — connect to siteMC NATS
   - Credentials: NATS creds file for siteMC (`nats.creds`)
   - JetStream publish setup for `MLOP-MCS-ARTIFACT` and `MLOP-MCS-METADATA`

2. **Message transformation** — parse incoming JSON, build outgoing JSON
   - Deserialize incoming `MCS-UPDATE` message
   - Route by `update_type`
   - Serialize to appropriate outgoing schema

3. **JetStream publish** — publish to correct siteMC stream + subject
   - Subject format: `MLOP-MCS-ARTIFACT.{function_id}-{function_name}`
   - Subject format: `MLOP-MCS-METADATA.{function_id}-{function_name}`
   - Use `PublishOptions` with `MsgId` for deduplication

4. **Error handling** — if publish to siteMC fails:
   - Log error
   - NAK the MC NATS message → MC NATS redelivers

---

## Open Questions

1. **`function_name` in subject** — MetaUpdater needs to know the
   `function_name` (human-readable name) to construct the subject
   `MLOP-MCS-ARTIFACT.{function_id}-{function_name}`. Does MetaUpdater
   have access to a `FUNCTION_NAME_MAPPING` config (like MCS's
   `FUNCTION_NAME_MAPPING` in `ABC.json`), or should we include
   `function_name` directly in the incoming MC NATS message?

2. **MC NATS consumer type** — is the existing subscribe logic using
   a push consumer or pull consumer on MC NATS?

3. **siteMC NATS auth** — does MetaUpdater use the same NATS creds
   as MCS synchronizer, or separate credentials?

4. **Deduplication** — should MetaUpdater set a `MsgId` on published
   messages to prevent duplicate processing if it redelivers?
