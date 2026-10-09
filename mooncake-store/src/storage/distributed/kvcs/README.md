# KVCS Distributed Storage

This document describes the optional KVCS integration and its deployment
contract. The integration never starts EFC or a storage server in a Mooncake
process; deploy those components separately using KVCS tooling.

Mooncake exposes KVCS Low Level as an explicit distributed object-storage
backend:

| Adapter | Provider metadata owner | Placement and health | Physical layout |
| --- | --- | --- | --- |
| `kvcs-lowlevel` | KVCS Low Level records | Mooncake selects one EFC target and falls back per operation | Inline root value, or chunks plus a manifest stored in KVCS |

Mooncake's master remains responsible for the logical object lifecycle.
It persists only the logical object descriptor and selected adapter;
SDK query results and target locations remain request-local and are never
copied into Master metadata.

## Deployment contract

The KVCS SDK is optional at build time. Without it, Mooncake and its other
storage backends still build; explicitly initializing the KVCS adapter returns
`NOT_SUPPORTED`. To use KVCS, build with the SDK visible to `pkg-config`.

Use the official KVCS YAML or Helm chart to deploy and configure EFC. Remote
disk registration, capacity, health, and mountpoints belong to that deployment
configuration; they must not be duplicated in Mooncake flags.

All Mooncake processes that access KVCS need the EFC socket and shared-memory
volume mounted at the paths configured for KVCS. Opt in explicitly with
`MOONCAKE_KVCS_MODE=low-level` or `MOONCAKE_DFS_FS_ADAPTER=kvcs-lowlevel`.
The presence of an EFC socket alone does not enable KVCS.

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

The Mooncake master and clients resolve the same adapter from the shared
startup environment. The legacy `kvcs` value is accepted and canonicalized
from `MOONCAKE_KVCS_MODE`, but new deployments should not use it. KVCS has no
dedicated health endpoint.
Startup health checking follows the common
`MOONCAKE_DISTRIBUTED_HEALTH_CHECK` setting. It defaults to `true` for Low
Level and remains `false` for other adapters. The check queries a reserved
probe key on every KVCS target.
A missing probe key is healthy; other provider errors fail backend
initialization. Individual provider read/write failures are logged and returned
to the caller.

Low Level health queries use `MOONCAKE_KVCS_QUERY_TIMEOUT_MS` (default: 50 ms).

### Low Level

Low Level talks directly to EFC and has no Redis metadata plane. Mooncake
accepts the official EFC deployment variables when they are available, but
does not require them. If they are absent, it uses the built-in single
KVCacheStore (G3.5) target at `mountpoint_index=1`. This is a default, not a
restriction: explicit local index-0 and mixed G3/G3.5 topologies retain their
existing routing behavior. The default index must match the EFC deployment.
KVCS remains responsible for watermarks and space reclamation inside each
backend.

#### G3 and G3.5 configuration

Set the following variables in the environment of every Mooncake client that
accesses KVCS, before starting it. These are Low Level routing settings, not
commands that reconfigure EFC.

| Storage path | `KVCS_BACKEND` | `KVCS_EXTRA_BACKENDS` | Mooncake routes |
| --- | --- | --- | --- |
| G3 (EFC local disk) | `disk` | Empty string | Local target, `mountpoint_index=0` |
| G3.5 (KVCacheStore) | `kvcachestore` (built-in default) | Empty string (built-in default) | KVCacheStore target, `mountpoint_index=1` by default |

G3.5 needs no extra topology environment variables for a single index-1
mountpoint. To select G3 instead, use:

```bash
export KVCS_BACKEND=disk
export KVCS_EXTRA_BACKENDS=''
```

To explicitly select G3.5, including when replacing an inherited G3 setting:

```bash
export KVCS_BACKEND=kvcachestore
export KVCS_EXTRA_BACKENDS=''
```

An inherited `KVCS_EXTRA_BACKENDS=kvcachestore` combined with `KVCS_BACKEND=disk`
exposes both G3 and G3.5 routes; it does not select only G3.5. Explicit
environment values override the built-in defaults. An explicit topology YAML
file takes precedence over these variables.

EFC must actually provide the selected backend and mountpoint. The default
does not create a KVCacheStore mountpoint or validate its storage generation.
For multiple remote mountpoints, pass the actual `KVCS_MOUNTPOINTS_JSON`;
Mooncake assigns indices starting at 1 in its `mountPoints` array order, which
must match EFC. The built-in `kvcachestore-default` ID is only a routing label.

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
  - {name: MOONCAKE_KVCS_SINGLE_TENANT, value: "true"}
  # KVCS_BACKEND / KVCS_EXTRA_BACKENDS / KVCS_MOUNTPOINTS_JSON are optional.
  # If omitted, Mooncake uses one built-in KVCacheStore target (index 1).
```

Bare-metal deployments may optionally point `MOONCAKE_KVCS_EFC_CONFIG` at an
actual EFC YAML configuration file. A legacy EFC socket path is ignored as a
topology file and falls back to the built-in defaults. A missing path, directory,
or other non-socket non-file path is rejected:

```yaml
backend: kvcachestore
mountpoints:
  - {mountpoint_id: efc-a, mountpoint_index: 1, default: true}
  - {mountpoint_id: efc-b, mountpoint_index: 2, default: false}
```

There is no Mooncake mountpoint-index flag. Missing deployment variables use
the built-in index-1 target; explicit YAML or `KVCS_MOUNTPOINTS_JSON` values
still support multiple targets. Multiple configured targets
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
Mooncake reacts to the result of each operation. Startup health probing is
enabled by default for Low Level and honors the common health-check setting.

Mooncake keeps tenant isolation in the provider key. The adapter encodes
`tenant_id` and the logical key as the printable, injective raw key
`mc1:<tenant-hex>.<logical-key-hex>`. The encoded key is checked against the
configured KVCS raw-key limit before any SDK call. `MOONCAKE_KVCS_MAX_KEY_SIZE`
defaults to 256 bytes and may be raised up to the SDK maximum of 1024 bytes.

Low Level defaults `MOONCAKE_KVCS_MAX_VALUE_SIZE` to `4194304` (4 MiB),
`MOONCAKE_KVCS_MAX_KEYS_PER_BATCH` to `8`, and each worker class to `1`. These
safe defaults use about 96 MiB of ring/shared memory; override them only when
the container has more shared memory and a higher concurrency target. The SDK
supports values up to 4 GiB, but that is an upper bound, not a production
default. Values up to and including the configured limit use the
inline fast path: the logical object is stored under one Mooncake-owned
physical root key. Larger objects use chunks of at most the configured size
plus a Mooncake-owned manifest. The manifest has a versioned, fixed 32-byte
little-endian wire format; it is not a serialized C++ structure.

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

Low Level query uses the stable-hash preferred target first. If that target
misses, reports an incomplete object, or encounters a transient error,
Mooncake queries the remaining targets in fixed route order before returning.

## I/O semantics

- Put is insert-only. Existing objects return `OBJECT_ALREADY_EXISTS`.
- Upsert is an idempotent delete followed by Put because KVCS has no native
  upsert operation.
- Low Level deletes chunks first and removes the manifest only after all chunk
  deletes succeed, preserving enough metadata to retry a partial failure.
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
