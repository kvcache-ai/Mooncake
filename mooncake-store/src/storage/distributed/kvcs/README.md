# KVCS Distributed Storage

This document describes the optional KVCS integration and its deployment
contract. The integration never starts EFC or a storage server in a Mooncake
process; deploy those components separately using KVCS tooling.

Mooncake exposes KVCS Standard and Low Level as two explicit implementations
of the distributed object-storage backend:

| Adapter | Provider metadata owner | Placement and health | Physical layout |
| --- | --- | --- | --- |
| `kvcs-standard` | KVCS Redis, Master, and Node Manager | KVCS selects and monitors replicas | KVCS Standard shards |
| `kvcs-lowlevel` | KVCS Low Level records | Mooncake selects one EFC target and falls back per operation | Inline root value, or chunks plus a manifest stored in KVCS |

Mooncake's master remains responsible for the logical object lifecycle in both
modes. It persists only the logical object descriptor and selected adapter;
SDK query results and target locations remain request-local and are never
copied into Master metadata. The adapter name prevents an object from being
reopened through the other mode's incompatible physical-key rules.

## Deployment contract

Use the official KVCS YAML or Helm chart to deploy and configure EFC. Remote
disk registration, capacity, health, and mountpoints belong to that deployment
configuration; they must not be duplicated in Mooncake flags.

All Mooncake processes that access KVCS need the EFC socket and shared-memory
volume mounted at the paths configured for KVCS:

```yaml
volumeMounts:
  - {name: kvcs-runtime, mountPath: /var/run/kvcs}
  - {name: kvcs-shm, mountPath: /dev/shm}
volumes:
  - name: kvcs-runtime
    hostPath: {path: /var/run/kvcs, type: Directory}
  - name: kvcs-shm
    hostPath: {path: /dev/shm, type: Directory}
```

The Mooncake master and clients must use the same explicit adapter name. The
legacy `kvcs` value is accepted and canonicalized from `MOONCAKE_KVCS_MODE`,
but new deployments should not use it. KVCS has no dedicated health endpoint.
When `MOONCAKE_DISTRIBUTED_HEALTH_CHECK` is enabled, Mooncake performs a query
probe during initialization and treats `OBJECT_NOT_FOUND` as a healthy result.

### Standard

Standard mode requires the complete KVCS control plane. KVCS owns namespace,
shard, replica, target, capacity, load-balancing, and node-health metadata.
Mooncake does not persist a replica location hint: query and get let KVCS
select a healthy replica, and delete asks KVCS to remove all replicas.

```yaml
env:
  - {name: MOONCAKE_KVCS_MODE, value: standard}
  - {name: MOONCAKE_KVCS_EFC_SOCKET, value: /var/run/kvcs/efc-grpc.sock}
  - {name: MOONCAKE_KVCS_REDIS_ENDPOINTS, value: "tcp://redis:6379"}
  - {name: MOONCAKE_KVCS_NAMESPACE, value: mooncake}
  - {name: MOONCAKE_KVCS_SINGLE_TENANT, value: "true"}
```

The adapter gets or creates the namespace during initialization. Standard
mode does not accept a nonzero Mooncake mountpoint index because target and
replica selection are provider-owned.

### Low Level

Low Level talks directly to EFC and has no Redis metadata plane. Mooncake reads
the official EFC deployment configuration instead of accepting a separate
mountpoint selector. A local primary backend with
`extra_backends: [kvcachestore]` exposes the main backend as target index 0 and
every configured KVCacheStore mountpoint as a target with index greater than
0. Low Level batches therefore use EFC's native routing contract. KVCS remains
responsible for watermarks and space reclamation inside each backend.

For a G3/G3.5 deployment, Mooncake uses a stable hash of the tenant-qualified
key. Each object is written to exactly one target: there is no Mooncake-side
replication, write-through, or post-write migration. A transient operation
failure falls back to the next target in deterministic route order. Requests
are regrouped per target before calling the native KVCS batch API.

Explicit delete and the delete phase of an upsert process index 0 before the
KVCacheStore mountpoints, so an archive created by local eviction is removed by
the following remote delete.

```yaml
env:
  - {name: MOONCAKE_KVCS_MODE, value: low-level}
  - {name: MOONCAKE_KVCS_EFC_SOCKET, value: /var/run/kvcs/efc-grpc.sock}
  - {name: MOONCAKE_KVCS_SINGLE_TENANT, value: "true"}
  # Inject the same official EFC deployment values into Mooncake.
  - {name: KVCS_BACKEND, value: disk}
  - {name: KVCS_EXTRA_BACKENDS, value: kvcachestore}
  - {name: KVCS_MOUNTPOINTS_JSON, value: '{"mountPoints":[{"mountPointID":"kvcs-xxx.example.com"}]}' }
```

Bare-metal deployments may point `MOONCAKE_KVCS_EFC_CONFIG` at the actual EFC
configuration file instead of injecting the three `KVCS_*` values:

```yaml
backend: kvcachestore
mountpoints:
  - {mountpoint_id: efc-a, mountpoint_index: 1, default: true}
  - {mountpoint_id: efc-b, mountpoint_index: 2, default: false}
```

There is no Mooncake mountpoint-index flag or fallback. Missing or inconsistent
deployment discovery is an initialization error. Multiple discovered targets
are used for stable-hash routing and failure fallback, not for Mooncake-side
replication.

For independent mountpoints, Mooncake uses only stable-hash placement. It does
not estimate capacity, queue depth, or bandwidth, and does not maintain a
process-local target-health state machine. Reads first use the stable-hash
route, then try remaining targets after a miss, incomplete object, or transient
failure. Requests are regrouped by target before calling the native KVCS batch
API.

Provider metadata query is disabled by default. Enable the controlled parallel
query path only when the KVCS backend is initialized:

```yaml
- {name: MOONCAKE_KVCS_ENABLE_PARALLEL_QUERY, value: "true"}
- {name: MOONCAKE_KVCS_QUERY_TIMEOUT_MS, value: "50"}
```

When enabled, a complete memory replica returns without waiting for provider
metadata. Otherwise the provider query is bounded by the configured timeout;
the default is 50 ms. The Master remains authoritative for whether an object
exists.

The official SDK does not provide a Mooncake-facing capacity or bandwidth
contract that would make a local load model authoritative. Capacity, watermarks,
space reclamation, and provider-side health remain KVCS/EFC responsibilities;
Mooncake reacts to the result of each operation and optionally uses an
inexpensive query probe as its startup health check.

Mooncake keeps tenant isolation in the provider key. The adapter encodes
`tenant_id` and the logical key as the printable, injective raw key
`mc1:<tenant-hex>.<logical-key-hex>`. The encoded key is checked against the
configured KVCS raw-key limit before any SDK call. `MOONCAKE_KVCS_MAX_KEY_SIZE`
defaults to 256 bytes and may be raised up to the SDK maximum of 1024 bytes.

Low Level requires `MOONCAKE_KVCS_MAX_VALUE_SIZE`. A practical starting value
is `4194304` (4 MiB); tune it against the deployed EFC and shared-memory
capacity. The SDK supports values up to 4 GiB, but that is an upper bound, not
a production default. Values up to and including the configured limit use the
inline fast path: the logical object is stored under one Mooncake-owned
physical root key. Larger objects use chunks of at most the configured size
plus a Mooncake-owned manifest. The manifest has a versioned, fixed 32-byte
little-endian wire format; it is not a serialized C++ structure. Standard mode
keeps the SDK's 4 MiB default unless the same parameter explicitly overrides
it, and uses KVCS-owned shard metadata.

KVCS sizes shared memory as approximately:

```text
(encoded_value_size * max_keys_per_batch) *
    (get_workers + set_workers + simple_workers)
```

For a 4 MiB Low Level configuration, the following values provide a
starting point; tune batch and worker concurrency to match the EFC and
shared-memory resources available to the container:

```yaml
env:
  - {name: MOONCAKE_KVCS_MAX_VALUE_SIZE, value: "4194304"}
  - {name: MOONCAKE_KVCS_MAX_KEYS_PER_BATCH, value: "64"}
  - {name: MOONCAKE_KVCS_GET_WORKERS, value: "8"}
  - {name: MOONCAKE_KVCS_SET_WORKERS, value: "8"}
  - {name: MOONCAKE_KVCS_SIMPLE_WORKERS, value: "4"}
  - {name: MOONCAKE_KVCS_OPERATION_TIMEOUT_MS, value: "30000"}
```

`MOONCAKE_KVCS_OPERATION_TIMEOUT_MS` bounds a Low Level operation with an
absolute monotonic-clock deadline. The default is 30 seconds. Low Level Put
first probes each physical key because KVCS 0.4.7 backends may overwrite an
existing key while returning success. Existing complete records are rejected;
an existing multi-part chunk is reused only when its stored bytes exactly
match the requested chunk. Under the same-key serialization requirement
described below, this avoids overwriting a record left by an earlier sequential
attempt without treating the reused record as owned by the new Put. The probe
is not an atomic put-if-absent guarantee.

When per-item results prove that part of a chunked Put was rejected before
insertion, Mooncake makes one synchronous best-effort delete of the chunks
that were definitely inserted. It does not retry a timed-out delete, because
the first delete may have succeeded and a retry could remove data from a
concurrent writer. Ambiguous chunk or manifest Put outcomes are not rolled
back for the same reason. Instead, the affected physical keys are quarantined
in that Low-Level driver, and later writes using any quarantined chunk or
manifest key fail closed with `KVCS_INCOMPLETE` until the process restarts.
Rollback and explicit delete failures with an indeterminate outcome quarantine
the attempted physical keys as well. This prevents a sequential retry in the
same process from committing a new object that an older timed-out Put or Delete
could later corrupt.

This recovery path relies on the Mooncake master serializing writes to the
same logical key. KVCS Low Level physical keys do not carry a write-generation
token, and KVCS 0.4.7 does not expose conditional delete. The process-local
quarantine therefore cannot protect a different Mooncake process that writes
the same key. Cross-master concurrent writes and automated quarantine cleanup
require a future generation/CAS or cancel-and-drain protocol. Matching-record
reuse remains available only for physical chunks left by a failure whose Put
outcomes were known and whose rollback delete did not have an indeterminate
result.

`MOONCAKE_KVCS_QUERY_TIMEOUT_MS` also bounds Low-Level query calls. Its default
is 50 milliseconds, matching the Mooncake-side parallel-query wait window.
Standard mode uses the public SDK query API, which has no per-call deadline;
therefore Standard provider query is not enabled on the parallel query path.

Low Level query uses the stable-hash preferred target first. If that target
misses, reports an incomplete object, or encounters a transient error,
Mooncake queries the remaining targets in fixed route order before returning.

## I/O semantics

- Put is insert-only. Existing objects return `OBJECT_ALREADY_EXISTS`.
- Upsert is an idempotent delete followed by Put because KVCS has no native
  upsert operation.
- Low Level deletes chunks first and removes the manifest only after all chunk
  deletes succeed, preserving enough metadata to retry a partial failure.
- Standard deletes without a location hint so KVCS removes every replica.
- A one-slice Low Level read goes directly into the caller's buffer. A
  multi-slice destination uses one staging buffer and then scatters the data.
- Key listing is unsupported. Mooncake master metadata remains the DFS source
  of truth.

Delete followed by Put is not an atomic provider-side replacement. If Put
fails after a successful delete, the old value cannot be restored by KVCS.

Before starting Mooncake, verify both IPC paths from the client container:

```bash
test -S /var/run/kvcs/efc-grpc.sock
df -h /dev/shm
```
