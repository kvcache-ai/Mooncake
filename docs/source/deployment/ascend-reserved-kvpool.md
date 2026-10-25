---
orphan: true
---

# Reserve Ascend KVPool capacity before model startup

For Ascend FabricMem, aggregate Linux free memory does not guarantee that a
late physical allocation will succeed. Reserve the required Store memory before
model loading, keep its owners alive across model restarts, and admit the model
only after the owners' actual mounted capacities satisfy the requested budget.

The example `scripts/ascend/reserved_kvpool.py` uses the installed
`MooncakeDistributedStore` API. It does not start or replace the shared master,
change allocator policy, or modify driver settings. Memory owners and the
admission command must run in the same Linux user and PID namespace with access to the
assigned NPUs and `/proc`. Source the CANN environment and install the Ascend
Mooncake wheel first; the owners use PyACL, not PyTorch.

## Reserve physical backing

Keep the existing shared master and metadata endpoints. The master must export
`segment_total_capacity_bytes` on its metrics endpoint. Missing or unreachable
metrics close admission. Example addresses below are documentation addresses;
replace them with the deployment's endpoints and this container's address.

```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
export ASCEND_ENABLE_USE_FABRIC_MEM=1

python3 scripts/ascend/reserved_kvpool.py reserve \
  --state-dir /data/kvpool-reservation-001 \
  --local-hostname 192.0.2.10 \
  --master 192.0.2.1:50051 \
  --metadata-server http://192.0.2.1:8080/metadata \
  --metrics-url http://192.0.2.1:9003/metrics \
  --devices 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 \
  --per-device-gib 30
```

Run this command under the container's process supervisor, independently of the
model. It creates one owner per logical device: 16 × 30 GiB = 480 GiB. Use a
fresh state directory for a new reservation. Native setup may return a smaller
best-effort allocation; the example never declares that short allocation ready.

Each owner reports its actual Store hostname after setup. Capacity checks select
only those hostnames from the shared master's segment metrics, require the full
requested amount for **every owner**, and exclude other nodes' contributions.
A larger unrelated pool cannot hide a missing or undersized local owner.

Preserve any platform-specific resource configuration required by the installed
CANN/HDK release. This example does not choose a driver workaround or change the
existing P/D transport configuration.

## Admit zero-contribution model clients

```bash
python3 scripts/ascend/reserved_kvpool.py admit \
  --state-dir /data/kvpool-reservation-001 \
  --minimum-gib 480 --timeout 600 \
  -- vllm serve /models/example \
  --kv-transfer-config '{"kv_connector":"AscendStoreConnector","kv_role":"kv_both","kv_connector_extra_config":{"backend":"mooncake"}}'
```

The admission command rechecks live owner identities and their current mounted
capacity before executing the application. It sets:

- `MOONCAKE_CONFIG_PATH` to the generated `consumer.json`, using the same master
  and metadata service with `preferred_segment=false`.
- `MOONCAKE_MASTER` to the reservation's master, rejecting an inherited conflict.
- `MOONCAKE_GLOBAL_SEGMENT_SIZE=0`, so model clients contribute **zero additional
  pool memory** and use the pre-reserved pool. The owners still hold 480 GiB.

A conflicting inherited `MOONCAKE_MASTER` or modified consumer configuration
is rejected rather than admitting an application against a different pool.

The application must support zero-contribution Store clients. Retain its normal
Ascend backend and P/D transport settings. The command above illustrates the
connector settings; model-specific parallelism and inference flags are separate.

Without an application command, `admit` performs a single readiness check and
exits nonzero on insufficient capacity, dead/recycled owner PIDs, unavailable
metrics, or a reservation that is not ready. Use it together with model health
in the serving deployment's readiness probe:

```bash
python3 scripts/ascend/reserved_kvpool.py admit \
  --state-dir /data/kvpool-reservation-001 --minimum-gib 480
```

The controller revokes readiness on metrics failures but retains physical
backing while monitoring recovers. An owner exit or an initial reservation
shortfall is fatal; keep application admission closed. SIGTERM/SIGINT closes
admission before shutting down the owners. Losing the owners loses volatile KV
contents; it does not merely pause allocation.

## Capacity limits

This ordering protects an acquired reservation; it does not defragment an
already pinned node. Apply it before loading the model on a controlled restart
or replacement. If the full budget cannot be acquired, do not silently reduce
the pool or start the model: release the failed reservation and use an eligible
idle node. Whole-node free bytes are not an admission criterion.

This is an opt-in deployment example. It adds no automatic retry policy,
transport fallback, or full-cluster lifecycle management.
