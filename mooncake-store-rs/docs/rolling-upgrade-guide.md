# Mooncake Store Rolling Upgrade Guide

## Overview

This document describes how to perform a rolling upgrade on a Mooncake Store cluster without downtime. The rolling upgrade leverages the predecessor-successor hot-upgrade mechanism to replace old versions (V1) with new versions (V2) one node at a time, keeping the cluster available throughout the process.

### Prerequisites

- The new version only increments `store_api_minor_version` (minor version) and does **not** change `store_api_version` (major version)
- The changes in the new version are backward-compatible (e.g., adding fields, adding RPCs) and do not alter existing API semantics
- The new version binary has been compiled and deployed to the target machine

### Compatibility Guarantees

| Scenario | Behavior |
|----------|----------|
| V1 node interoperates with V2 node | ✅ Compatible (same major version; minor is not checked) |
| Data written by V2 is read by V1 | ✅ Compatible (new fields use serde default / protobuf ignores unknown fields) |
| Data written by V1 is read by V2 | ✅ Compatible (missing fields use default values) |

---

## Procedure

### Step 0: Preparation

```bash
# Verify the Redis metadata backend is available
redis-cli -p 6380 ping

# Verify the new version binary is compiled
ls -la target/debug/mooncake-store-client

# Back up the old version binary (optional, for rollback)
cp target/debug/mooncake-store-client target/debug/mooncake-store-client-v1
```

### Step 1: Start the New Version Successor in Standby Mode

Launch the new version binary with the **same `stable_id`** as the predecessor. The metadata backend allocates the next epoch automatically.

```bash
./target/debug/mooncake-store-client \
  --local-hostname 127.0.0.1 \
  --metadata-url "redis://127.0.0.1:6380/0" \
  --storage-bytes 1048576 \
  --scratch-bytes 1048576 \
  --protocol tcp \
  --route-control metadata-only \
  --keyspace "${KEYSPACE}" \
  --lease-ttl-ms 4000 \
  --heartbeat-interval-ms 500 \
  --label pool=pool-a \
  --label storage=true \
  --label route=false \
  --drain-on-exit \
  --stable-id client-a \
  --initial-state standby \
  --local-segment-name "seg-a-v2"
```

**Key Parameters**:

| Parameter | Description |
|-----------|-------------|
| `--stable-id client-a` | Must match the predecessor |
| `--initial-state standby` | Starts in Standby mode, waiting for handoff |
| `--local-segment-name` | Must differ from the predecessor's segment name |
| `--drain-on-exit` | Ensures future upgrades can also trigger hot-upgrade |

Wait for the log to confirm successful startup (the assigned epoch is printed in the line):

```
mooncake-store-client started stable_id=client-a epoch=2 initial_state=standby
```

If you need to observe the per-stable-id epoch high-water mark, query it directly:

```bash
redis-cli -p 6380 GET "${KEYSPACE}/state/client-epoch-hwm/client-a"
```

### Step 2: Trigger Hot-Upgrade on the Predecessor

Send a SIGTERM signal to the old version predecessor:

```bash
# Find the predecessor's PID (match by stable_id and segment, not epoch, since
# the epoch is server-assigned)
pgrep -f "mooncake-store-client.*--stable-id client-a"

# Send SIGTERM
kill -TERM <predecessor_pid>
```

Monitor the logs to confirm the hot-upgrade is complete:

**Predecessor log** (should appear in order):
```
mooncake-store-client handoff stable_id=client-a from_epoch=1 to_runtime=client-a:2
mooncake-store-client upgraded stable_id=client-a from_epoch=1 to_epoch=2
```

**Successor log** (should appear):
```
mooncake-store-client promoted stable_id=client-a epoch=2 from_epoch=1 kind=HotUpgrade
```

### Step 3: Verify Data Integrity

```python
import time
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
assert store.setup(
    "127.0.0.1",                       # local_hostname
    "redis://127.0.0.1:6380/0",        # metadata_url
    4 * 1024 * 1024,                    # storage_bytes
    1 * 1024 * 1024,                    # scratch_bytes
    "tcp",                              # protocol
    "",                                 # rdma_devices
    "",                                 # transport_metadata_url
    stable_id="verify-driver",
    keyspace="<your-keyspace>",         # replace with actual keyspace
    labels={"pool": "pool-a", "storage": "false", "route": "false"},
    route_control="metadata_only",
) == 0

# Wait for the route to converge to the new owner (successor's epoch)
for _ in range(100):
    route = store.query_route("existing-key")
    if route is not None:
        owners = [r["owner"] for r in route["replicas"]]
        if owners == ["client-a:2"]:  # confirm owner switched to successor
            break
    time.sleep(0.1)

# Verify data written before the upgrade is still readable
assert store.get("existing-key") == b"expected-value"

# Verify new writes also work
assert store.put("new-key", b"new-value") == 0
assert store.get("new-key") == b"new-value"

store.close()
```

### Step 4: Repeat Steps 1–3 for the Next Node

Repeat Steps 1–3 for each node in the cluster until all nodes have been upgraded to the new version.

---

## Rollback Procedure

If the new version encounters issues, you can roll back using the same mechanism:

1. Start a successor using the old version binary with the same `stable_id`; the metadata backend assigns the next epoch automatically
2. Send SIGTERM to the new version predecessor
3. The old version successor takes over

```bash
# Roll back client-a using the V1 binary — the successor gets the next epoch
# from the metadata backend (epoch=3 in a 1 -> 2 -> 3 rollout sequence).
./target/debug/mooncake-store-client-v1 \
  --local-hostname 127.0.0.1 \
  --metadata-url "redis://127.0.0.1:6380/0" \
  --storage-bytes 1048576 \
  --scratch-bytes 1048576 \
  --protocol tcp \
  --route-control metadata-only \
  --keyspace "${KEYSPACE}" \
  --lease-ttl-ms 4000 \
  --heartbeat-interval-ms 500 \
  --label pool=pool-a \
  --label storage=true \
  --label route=false \
  --drain-on-exit \
  --stable-id client-a \
  --initial-state standby \
  --local-segment-name "seg-a-v1-rollback"

# After the successor starts, send SIGTERM to the V2 predecessor
kill -TERM <v2_pid>
# Wait for the logs to confirm handoff completion; V1 resumes service
```

---

## Important Notes

1. **Upgrade order does not matter**: Nodes can be upgraded in any order; the cluster operates normally during the mixed-version period
2. **Upgrade one node at a time**: Avoid upgrading multiple nodes simultaneously; ensure each node's handoff completes before proceeding to the next
3. **Confirm handoff completion**: After sending SIGTERM, wait for the predecessor log to show `upgraded` and the successor log to show `promoted` before moving on
4. **Unique segment names**: Each successor must use a different `--local-segment-name` than its predecessor
5. **Epoch is server-assigned**: The metadata backend allocates the next epoch on every startup; operators do not pick it
6. **Minor version upgrades only**: If the new version changes `store_api_version` (major), rolling upgrade is not supported — a full cluster restart is required
