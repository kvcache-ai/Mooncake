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

### Step 1: Identify the Node to Upgrade

Choose a node in the cluster (e.g., `client-a`) and note its `stable_id` and current `epoch`.

```bash
# List active clients (the client index is stored in a Redis SET)
# Key format: {keyspace}/indexes/clients
# Values: {keyspace}/clients/{stable_id}:{epoch}
redis-cli -p 6380 SMEMBERS "${KEYSPACE}/indexes/clients" | cat

# Example output:
# mc/store-rs/my-cluster/clients/client-a:1
# mc/store-rs/my-cluster/clients/client-b:1
# Here client-a has stable_id=client-a, epoch=1
```

### Step 2: Start the New Version Successor in Standby Mode

Launch the new version binary with the **same `stable_id`** and a **higher `epoch`**:

```bash
# Assuming client-a is currently at epoch=1, start a successor at epoch=2
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
  --epoch 2 \
  --initial-state standby \
  --local-segment-name "seg-a-v2"
```

**Key Parameters**:

| Parameter | Description |
|-----------|-------------|
| `--stable-id client-a` | Must match the predecessor |
| `--epoch 2` | Must be higher than the predecessor's epoch |
| `--initial-state standby` | Starts in Standby mode, waiting for handoff |
| `--local-segment-name` | Must differ from the predecessor's segment name |
| `--drain-on-exit` | Ensures future upgrades can also trigger hot-upgrade |

Wait for the log to confirm successful startup:

```
mooncake-store-client started stable_id=client-a epoch=2 initial_state=standby
```

### Step 3: Trigger Hot-Upgrade on the Predecessor

Send a SIGTERM signal to the old version predecessor:

```bash
# Find the predecessor's PID
pgrep -f "mooncake-store-client.*--stable-id client-a.*--epoch 1"

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

### Step 4: Verify Data Integrity

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

### Step 5: Repeat Steps 2–4 for the Next Node

Repeat Steps 2–4 for each node in the cluster until all nodes have been upgraded to the new version.

---

## Rollback Procedure

If the new version encounters issues, you can roll back using the same mechanism:

1. Start a successor with a higher epoch using the old version binary
2. Send SIGTERM to the new version predecessor
3. The old version successor takes over

```bash
# Roll back client-a: start an epoch=3 successor using the V1 binary
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
  --epoch 3 \
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
5. **Monotonically increasing epoch**: Each upgrade requires a higher epoch value
6. **Minor version upgrades only**: If the new version changes `store_api_version` (major), rolling upgrade is not supported — a full cluster restart is required
