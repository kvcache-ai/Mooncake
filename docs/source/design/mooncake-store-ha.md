# Mooncake Store High Availability

Mooncake Store now uses a backend-driven high availability (HA) runtime for the
master service instead of treating HA as an etcd-only code path. The current
design separates leadership coordination, standby recovery, snapshot indexing,
snapshot payload storage, and Transfer Engine metadata into explicit planes with
different responsibilities.

This document explains the current architecture after the HA refactor. For
deployment examples and backend-specific configuration, see
[Mooncake Store HA Deployment](../deployment/mooncake-store-ha-deployment.md)
and
[Mooncake Store Deployment & Operations Guide](../deployment/mooncake-store-deployment-guide.md).

## Overview

Mooncake Store HA is centered around one invariant: **only one master may serve
client RPCs at a time**, but **every master process should remain observable and
ready to participate in failover**.

The current architecture is organized into the following planes:

| Plane | Responsibility | Current implementations |
| --- | --- | --- |
| Leadership backend | Elect a leader, publish the current master view, renew leadership ownership, and notify clients and standbys when the view changes | `etcd`, `redis` |
| Standby replication runtime | Keep a standby process alive, bootstrap state before promotion, and optionally follow the leader continuously | Snapshot bootstrap for `etcd` and `redis`; oplog following currently `etcd` only |
| Snapshot catalog store | Track snapshot descriptors and resolve the latest recoverable snapshot | `embedded`, `redis` |
| Snapshot object store | Store snapshot payload files such as manifest, metadata, segments, and task manager state | `local`, `s3` |
| Transfer Engine metadata | Store segment, RPC, and handshake metadata used by Transfer Engine | `etcd`, `redis`, `http` |

These planes are related, but they are not the same subsystem:

- The HA backend is responsible for **leadership semantics**.
- The snapshot catalog is responsible for **discovering recoverable snapshots**.
- The snapshot object store is responsible for **persisting snapshot payloads**.
- The Transfer Engine metadata service is responsible for **data-plane transfer
  metadata**, not Store master leadership.

This split is the main architectural difference from the older etcd-centric
implementation.

## Master Runtime Model

In HA mode, every master process runs the same binary and starts the same admin
and metrics surface. Runtime state is exposed through the admin server, while
only the elected leader exposes the Store RPC service.

The runtime state machine is:

- `starting`: process boot and admin server startup
- `standby`: connected to the HA backend and waiting for leader changes
- `candidate`: trying to acquire leadership
- `recovering`: rebuilding standby state before it can be promoted
- `catching_up`: standby is alive but still behind the current leader
- `leader_warmup`: leadership has been acquired and the promoted standby state
  is being turned into a serving master
- `serving`: leader RPC service is active

The handoff flow is:

1. Every master starts the admin server and enters standby mode.
2. The runtime reads the current `MasterView` from the HA backend.
3. If no leader exists, a candidate tries to acquire leadership.
4. The winner receives a `LeadershipSession`, which includes:
   - the published `MasterView`
   - a backend-issued `owner_token`
   - a backend-defined `lease_ttl`
5. Before serving RPCs, the winner promotes any preloaded standby state and
   enters `leader_warmup`.
6. The RPC server starts only after leadership is still valid.
7. If leadership renewal fails or leadership is lost, the serving RPC surface is
   stopped and the process falls back to standby mode.

This model keeps all masters observable through the metrics/admin endpoint while
still enforcing a single serving leader.

## Leadership Backend Abstraction

The HA backend abstraction is intentionally small. A backend must provide a
consistent implementation of:

- `TryAcquireLeadership`
- `RenewLeadership`
- `ReleaseLeadership`
- `ReadCurrentView`
- `WaitForViewChange`
- `StartLeadershipMonitor`

These APIs define the minimum contract required by both clients and master
supervisors:

- Clients resolve the current leader by reading the published `MasterView`.
- Master supervisors coordinate promotion and demotion using
  `LeadershipSession`.
- The supervisor does not need to know how the backend stores leases internally;
  it only depends on the backend-issued `owner_token` and `lease_ttl`.

### Supported leadership backends

| Backend | Status | Notes |
| --- | --- | --- |
| `etcd` | Production path | Supports leadership, master view publication, standby snapshot bootstrap, and etcd-based oplog following |
| `redis` | Production path for leadership | Supports leadership, master view publication, and snapshot-backed standby bootstrap |
| `k8s` | Reserved type only | Parsed by config, but not available in the current build/runtime |

### Backend-independent client behavior

Client-side failover is no longer tied to etcd. `master_server_entry` now has
two modes:

- Direct mode: `host:port`
- HA mode: `<backend>://<connstring>`

Examples:

- `10.0.0.10:50051`
- `etcd://10.0.0.1:2379;10.0.0.2:2379;10.0.0.3:2379`
- `redis://redis.example.com:6379`

The client parses the backend URI, reads the current `MasterView`, and connects
to the published leader address. The failover contract is therefore defined by
the HA backend abstraction rather than by etcd-specific client logic.

## Snapshot Architecture

Snapshot handling is now split into two layers:

- **Snapshot catalog** identifies which snapshot is the latest recoverable one.
- **Snapshot object store** holds the actual payload files.

This keeps snapshot payload storage independent from leadership storage.

### Snapshot payloads

The snapshot object store persists the data files required to reconstruct the
master state:

- manifest
- metadata
- segments
- task manager state

Current object-store implementations are:

- `local`
- `s3`

### Snapshot catalogs

The snapshot catalog store records `SnapshotDescriptor` objects, including the
snapshot ID, creation time, manifest key, object prefix, and sequence
boundaries.

Current catalog implementations are:

- `embedded`
  - Uses the snapshot object store as the source of truth.
  - Stores and reads descriptors alongside snapshot payloads.
- `redis`
  - Stores the latest pointer and descriptor index in Redis.
  - Still relies on the object store for the actual payload files.

This means Redis does **not** store snapshot payload objects. It only stores the
catalog metadata when the Redis catalog backend is selected.

### Restore semantics

When snapshot restore is enabled, the master loads the latest descriptor from
the configured catalog and reconstructs its in-memory metadata state from the
object store payloads.

Restore currently behaves as follows:

- Incomplete replicas are dropped during restore.
- Complete objects are kept by default, even if their lease has already
  expired.
- If `cleanup_expired_on_restore` is enabled, complete objects whose lease is
  expired and not soft-pinned are dropped during restore.

## Standby Recovery and Warm Failover

The standby replication runtime is capability-driven rather than backend-driven.
The runtime checks what the configured backend can support and enables the
appropriate recovery path.

Today there are two recovery sources:

- **Snapshot bootstrap**
  - Available when snapshot restore is enabled and a snapshot provider is
    configured.
  - Works with both etcd and redis leadership backends.
- **Oplog following**
  - Currently available only when the HA backend is etcd.
  - Used to keep standby metadata closer to the leader after snapshot bootstrap.

This leads to the following capability matrix:

| Capability | etcd HA backend | redis HA backend |
| --- | --- | --- |
| Leader election and renew | Yes | Yes |
| Master view publication | Yes | Yes |
| Standby snapshot bootstrap | Yes | Yes |
| Continuous oplog following | Yes | No |
| Snapshot boundary from oplog | Yes | No |

In practice:

- etcd can run snapshot bootstrap plus oplog following, which gives the most
  complete warm-standby path in the current code base.
- redis can run snapshot bootstrap and promote from preloaded state, but it
  does not yet have a redis-backed oplog follow path.

That limitation is important when reasoning about failover freshness: Redis HA
currently provides leadership failover plus snapshot-granularity warmup, not a
fully oplog-synchronized hot standby.

## Relationship to Transfer Engine Metadata

Transfer Engine metadata is intentionally outside the Store HA backend
abstraction.

The Transfer Engine metadata service stores:

- segment metadata
- RPC endpoint metadata
- handshake metadata

It may use:

- `etcd`
- `redis`
- `http`

This is independent from the Store leadership backend. A deployment may choose:

- etcd for Store HA and etcd for Transfer Engine metadata
- redis for Store HA and redis for Transfer Engine metadata
- redis for Store HA and HTTP for Transfer Engine metadata

When Store HA and Transfer Engine metadata share the same Redis service, they
must use different namespaces:

- Store HA uses `cluster_id` on the master side and `MC_STORE_CLUSTER_ID` on
  the client side.
- Transfer Engine metadata uses `MC_METADATA_CLUSTER_ID`.

Sharing one Redis service is therefore a deployment choice, not an architectural
requirement.

## Current Limitations

The HA refactor removed most etcd-specific assumptions from leadership and
snapshot catalog handling, but a few etcd-specific paths still remain:

- The oplog store is still etcd-specific.
- Snapshot sequence boundaries are still derived from the etcd oplog when the
  HA backend is etcd.
- Redis HA currently relies on snapshot bootstrap for warm promotion and does
  not yet implement a redis-native oplog follow path.

These limitations are explicit rather than hidden. The architecture now treats
them as separate subsystems that can be abstracted independently instead of
embedding them in a monolithic etcd helper layer.

## Recommended Reading Order

For most readers, the recommended order is:

1. [Mooncake Store](./mooncake-store.md) for the overall product model
2. This document for HA runtime and backend boundaries
3. [Mooncake Store HA Deployment](../deployment/mooncake-store-ha-deployment.md)
   for single-master and HA deployment guidance
