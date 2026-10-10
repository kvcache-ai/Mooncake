# Store-RS NoF Design

NoF is a remote persistence data plane attached below Cold Tier. It reuses Mooncake's existing hot
replica lifecycle, offload path, restore admission and batching, heartbeat ownership, cached health
state, and replica selection.

The included provider is the KVCS 0.4.0 C API executor. `KvcsCapiExecutor::new()` reads process
environment for a single target. For multiple Low-Level targets, one `KvcsLowLevelClient` owns the
SDK client for an EFC socket and creates lightweight executors bound to different mountpoint
indices. The selected mode exposes only the traits supported by that mode. Both KVCS modes are
provider-owned: KVCS can find an object from its derived key, so Mooncake does not persist KVCS
placement metadata.

## Architecture

`NofBackend` wraps an `Arc<dyn NofBacking>`. A backing composes independent, provider-neutral
capabilities:

| Capability | Trait | Responsibility |
| --- | --- | --- |
| logical-object I/O | `NofObjectWrite`, `NofObjectRead`, `NofObjectQuery`, `NofObjectDelete` | provider namespace, object, and manifest semantics |
| provider-addressed physical I/O | `NofPhysicalWrite`, `NofPhysicalRead`, `NofPhysicalQuery`, `NofPhysicalDelete` | complete-object I/O by deterministic key; the provider owns lookup metadata and privately chooses its record layout |
| Mooncake-managed I/O | `NofManagedWrite`, `NofManagedRead`, `NofManagedAllocator` | complete-object I/O that returns an opaque locator; Mooncake persists placement in `ObjectRoute.nof_backing` and the target owner drives reserve/release/recovery through route CAS |
| health | `NofHealth` | liveness and optional capacity information |

Missing capabilities remain missing. Mooncake does not recreate them with a provider CLI, a
private service API, or a parallel disk-management implementation.

For KVCS, no provider placement is stored in `ObjectRoute`. Mooncake does not persist a KVCS
target, owner, locator, manifest, shard map, object length, or checksum. The route keeps only the
logical object identity. Each request derives the provider key from the stable scoped logical key
and asks the configured KVCS targets directly. `ObjectRoute.version` is control-plane state and is
not part of that key. Target and owner information is a request-local I/O descriptor and is never
published through route CAS or exposed by the admin API.

This route-free rule follows metadata authority, not the Standard/Low-Level or logical/physical
API shape. The included ExtentStore executor is the opposite case: its allocator returns an opaque
location, so Mooncake persists a distinct `NofBackingRoute`. That route is authoritative for target
and replica placement and is replayed at startup to rebuild the allocator. Record framing, aligned
extent layout, and free-range management remain private to ExtentStore. KVCS never enters this
managed-route path.

### Target registration and owner assignment

NoF target registration reuses the existing Cold Tier target catalog and runtime lifecycle, but
its owner is assigned differently from a local disk:

| Stage | Local disk | NoF target |
| --- | --- | --- |
| target registration | `ColdTierTargetConfig` is resolved and registered on the node that owns the disk | `NofTargetConfig` binds the remote executor and stable target identity |
| initial owner | the registering node's current runtime is the owner | an eligible active runtime is selected after registration |
| shared management | existing Cold Tier owner, health, allocator, placement, and cleanup paths | the same paths after the owner assignment |

For a local disk, `bootstrap_cold_tier_device` records the current runtime as the device owner
because that runtime is the only one with a local backend. A NoF target is visible to every client
that has the same static target inventory. Managed NoF therefore registers the target in the same
`ColdTierDeviceRecord` catalog and uses its existing compare-and-swap update as the owner claim.
The record stores the owner runtime's stable ID and epoch. Rendezvous selects the preferred owner;
the current owner may hand a stable target to that client under the same management gate and
device-record CAS used for shutdown.

The owner assignment is target-scoped. It uses the existing active client leases and stable
rendezvous balancing to spread targets across clients. The target-set fingerprint is carried in
lease labels. A membership rebalance moves only targets whose indexed routes are all stable and
whose metadata mirror is complete; the new owner runs recovery before allocation. The
device-record CAS is the single owner fence; owner identity is not
written into a separate route or queried on the read path.

Every client keeps a local executor handle for every statically configured NoF target. The owner
does not proxy reads or payload writes. For provider-owned KVCS targets, the owner is used only
for the shared heartbeat and target-health view; KVCS remains authoritative for provider
placement, manifests, and disk maintenance. For Mooncake-managed ExtentStore targets, the owner
also performs target allocation, route publication, release, recovery, and maintenance.

### Device lifecycle and maintenance ownership

NoF does not introduce a second disk register/unregister control plane. Local disks continue to
use the existing Cold Tier device API and `ColdTierDeviceRecord` state/CAS transitions
(`register`, `unregister`, `disable`, `enable`, and drain). `NofTargetConfig` and
`SpdkNofBlockDevice::connect` bind the remote data plane and then enter the same target/runtime
management framework; they do not duplicate local disk lifecycle logic.

Managed ExtentStore targets reuse the existing Cold Tier scheduler and CLOCK victim tracker. The
same high/low watermarks drive bounded victim selection and allocator release through the normal
owner path. A target-scoped release first removes only that target from the route with CAS, then
invalidates and flushes its record. The global NoF backing state is not used as a per-target delete
marker. Managed NoF does not add a separate GC queue, LRU implementation, or compaction loop.

Managed NoF maintenance is owner-scoped. Each background cleanup pass uses the current target owner
assignment and only maintains targets owned by the local runtime. Logical delete and reclaim release
locators through the existing managed owner/CAS path; there is no provider list scan. Watermark
cleanup uses the same Cold Tier high/low thresholds: if a managed target reports capacity and
available bytes and its used space reaches the high watermark, the owner selects bounded candidates
from the target route index and releases target copies until the target falls below the low
watermark or no candidate remains. Managed NoF is a cache: reclaim may remove the last payload copy;
in that case the owner deletes the logical route instead of leaving an empty `Active` route.

Health is heartbeat based, not checked on every request. After short consecutive heartbeat failure,
the owner publishes the target in `nof.unhealthy-targets`, so writers stop selecting it and readers
fail over to other route copies. After a longer consecutive failure window, the owner performs
route-only downline: the bad target is removed from `ObjectRoute.nof_backing` without calling the
unavailable executor. If it was the last payload copy, the route remains as an active route with no
payload location: reads cannot select the failed disk, an explicit user delete can still advance the
normal deletion fence, and the old physical record remains recoverable. A pre-existing delete or
reclaim intent remains `Deleting`, so recovery completes the physical release instead of restoring
the object. When heartbeat later observes the target as healthy, the owner keeps it unavailable for
allocation, runs the same manifest-recovery path used at startup, rebuilds the route and allocator
state from the surviving records, and only then returns the target to service.

Released NoF extents do not have a second persistent quarantine journal. The ExtentStore executor
invalidates and flushes the record header before returning the extent to its free-span set, so a
later recovery scan cannot republish a reclaimed cache entry. Graceful handoff includes active
`PendingOffload` routes, and full recovery reserves authoritative `PendingOffload` locators even
when their record header is not visible yet. A writer that prepared through the old owner can
therefore finish without the successor reallocating its extent. If the record is already present,
recovery repairs interrupted publication to `Materialized`.

Mooncake remains authoritative for logical object existence, while KVCS remains authoritative for
provider layout and provider-internal metadata:

- Standard values at or below the configured limit use one non-sharded root key
  (`shard_id=0`, `total_shard=1`). Larger values use KVCS shards, whose manifest is owned by
  KVCS/Redis.
- Low-Level stores raw values in KVCS. A value that exceeds the configured record limit uses an
  executor-private sidecar and derived record keys in the same KVCS key space.

### Standard and Low-Level

| Mode | Data plane | Replication and placement | Physical maintenance |
| --- | --- | --- | --- |
| `standard` | KVCS logical objects and shards | KVCS owns placement, replicas, and manifest completeness; Mooncake selects one provider target | KVCS owns disks, GC, watermarks, recovery, and rebuild |
| `low-level` | raw physical keys | Mooncake selects configured NoF targets with the shared Cold Tier replica policy; the same derived key is stored on every selected target | KVCS/EFC owns disks, capacity policy, GC, compaction, recovery, and rebuild |

A `StoreClient` cannot mix logical-object and physical-KV NoF targets. Use separate clients when
both data planes are required.

## Request lifecycle

On KVCS offload, Mooncake derives the provider key, selects healthy targets, and writes the
payload. A Standard write goes to one provider target because KVCS owns its internal replication.
A Low-Level write succeeds only after every target selected by `nof_replica_count` accepts the
value. No target list is written back to metadata.

Mooncake resolves the logical route first. Only an `Active` route without a readable hot or
local-disk copy causes the client to derive the provider key and query provider metadata for
existence and length. This keeps KVCS queries off the hot-hit path. The existing Cold Tier target
selector, singleflight, admission control, and batch restore path then call the provider batch-get
API. A Mooncake-managed ExtentStore read instead uses the selected target and opaque locator from
`ObjectRoute.nof_backing`. In both cases the client reads through its local target executor into the
caller's destination buffer. The current ExtentStore transport reads through SPDK DMA memory and
copies the decoded payload into that destination. NoF reads do not enqueue Cold Tier promotion and
do not change the route. Route lookup and provider query are deliberately not issued speculatively
in parallel.

Removal first changes the logical route from `Active` to `Deleting`. It then fans out the stable
provider key to the current target set. A provider error is returned to the caller and the
`Deleting` route remains as the retry identity; put and upsert cannot replace it. After provider
deletion succeeds, Mooncake removes the logical route and reclaims its hot or local-disk copies.
Retry is caller-driven through another remove call; there is no NoF delete worker or provider scan.
This state records logical deletion progress, not KVCS target placement or provider metadata.

Startup recovery is capability-specific. KVCS objects are discovered lazily by request-time
metadata query; Mooncake neither downloads provider values nor scans KVCS active routes to
reconstruct a NoF queue. Mooncake-managed ExtentStore targets are different: their executor stores
the same cold-object manifest shape used by local Cold Tier recovery, so the current target owner
can feed recovered objects into the existing Cold Tier manifest-recovery path.

KVCS targets must therefore be configured consistently on clients that share a route namespace.
Changing a target set does not create a metadata migration. Because KVCS 0.4.0 has no listing API,
Mooncake cannot enumerate or reclaim provider records that are unreachable from a current logical
route and target configuration.

## Read-path TODOs

### TODO: NoF promotion

NoF reads currently leave the object in NoF and do not create a hot replica. Reuse the existing
Cold Tier promotion pipeline after defining the promotion destination policy, especially for an
embedded client that has no local memory segment. The destination must be selected by the normal
memory placement policy rather than by NoF target ownership; the target owner remains a
control-plane and maintenance role.

### TODO: zero-copy NoF reads

Evaluate two implementation paths while retaining the current copy path as the compatibility
fallback:

1. Define an aligned ExtentStore record/payload contract and make an SPDK DMA buffer the returned
   destination buffer, with explicit buffer ownership, registration, and lifetime. This path must
   handle record headers, manifests, payload offsets, and requests that cannot meet the alignment
   contract without introducing another full-value staging copy.
2. Use GDS for capable GPU destinations so data can move directly between NVMe storage and GPU
   memory. This path must be capability-gated and fall back cleanly when GDS is unavailable.

## Replica write coordination

The existing memory-replica policy chooses one primary client for a logical write. When several
clients hold memory replicas, only that primary may trigger the Cold Tier/NoF write. Secondary
memory replicas do not independently write the same object, publish a second route, or select a
different NoF replica set.

The primary reuses the shared Cold Tier replica policy to select the NoF targets and to apply the
configured success condition. Target writes may run in parallel, but the operation has one logical
writer:

```text
memory-replica primary
    -> shared target placement and replica policy
    -> direct writes to selected target executors
    -> target-owner route publication for each successful target
    -> return after the existing replica success condition
```

For Standard KVCS, one provider target is selected because KVCS owns its internal replication and
manifest. For Low-Level KVCS, all targets selected by `nof_replica_count` receive the same
derived key. For Mooncake-managed ExtentStore, each selected target has an independent
`NofBackingRoute` location and its own target owner.

If a request arrives at a secondary memory replica, the existing memory-replica forwarding path
chooses the primary before any NoF side effect. NoF does not add a second leader-election or
replica-repair algorithm.

## Managed ExtentStore and SPDK

`ExtentStoreExecutor` implements the managed traits. A write receives the complete object once,
allocates an aligned ExtentStore record, flushes the block device, and returns an opaque locator.
Publication is owner-driven: the target owner reserves an extent and CAS-publishes a
`PendingOffload` location in `ObjectRoute.nof_backing`; the resulting write request carries the
route identity needed by the shared cold-object manifest codec, so the writer does not need to be
the target owner. The writer performs the direct write and flush, then the owner CAS-publishes
`Materialized`. If the preparation CAS fails, the reserved locator is released.
Reads, deletes, replicas, load balancing, retries, and owner-scoped health reuse the existing Cold
Tier runtime.

The managed owner has two recovery paths, both intentionally thin:

- Graceful handoff transfers the target's current route snapshot directly to the successor. The
  successor passes those opaque locators to `NofManagedAllocator::recover`; ExtentStore rebuilds
  its free ranges without scanning the target and without keeping a second allocation journal.
- Startup without a snapshot, crash takeover, and target re-online use the executor's recovery
  scan. The scan returns `RecoveredColdObject` values backed by the shared cold-object manifest
  codec. The existing Cold Tier startup recovery CAS path creates or repairs routes for physical
  records, removes stale target copies that no longer exist physically, and then rebuilds the
  allocator from the resulting live locators.

This is not a NoF-specific rebuild queue and not a cold-copy planner. It is the same disk-manifest
route recovery used by local Cold Tier, with the final backing field switched from
`cold_backing` to `nof_backing`. KVCS does not use this path because KVCS owns its external
metadata and has no provider list API in the current SDK.

Normal shutdown uses the graceful-drain route-snapshot handoff described in
[Ownership and health](#ownership-and-health). The replacement calls
`NofManagedAllocator::recover` with the transferred snapshot and does not enumerate the target
route index or scan the target again. Crash takeover and target re-online use the full target
reconciliation path above. The fast path transfers metadata only; it does not move payloads or
change executor locators.

`SpdkNofBlockDevice` is the transport below ExtentStore. It attaches an NVMe-oF controller,
serializes I/O through the current SPDK qpair, uses SPDK DMA buffers for submitted chunks,
performs aligned block I/O, exposes geometry, and implements the flush barrier. Extent alignment
is not part of the generic NoF traits. A failed health check or I/O operation invalidates the
controller; the next operation reattaches with the saved transport configuration, verifies that
the target geometry is unchanged, and retries the operation once.

A StoreClient target set uses exactly one authority mode. Provider-owned KVCS targets and
Mooncake-managed ExtentStore targets are configured on separate clients so recovery and deletion
semantics cannot be mixed.

## Object layout and batching

Cold Tier's existing `ValueChunkPlan` is the only object-splitting planner:

- Standard stores values at or below the configured limit directly under the root key with
  `shard_id=0` and `total_shard=1`. This path creates no Mooncake chunk key or manifest. Larger
  values reuse `ValueChunkPlan` and map its ranges to KVCS shards; KVCS owns their manifest.
- Low-Level stores values at or below the configured limit directly at the root key.
- Larger Low-Level values use derived chunk keys and an executor-private 32-byte sidecar containing
  total length, chunk size, and chunk count.

Low-Level writes send data records first and publish the sidecar last. The inline path remains one
direct root-record write and does not execute sidecar or chunk logic.

After provider discovery has supplied the object length, a Standard read at or below the limit
issues `kvcs_batch_get_into` directly for root shard 0. It does not make a second query for shard
details. Larger Standard reads query the KVCS-owned shard manifest before issuing the batch get.

The Low-Level inline put/get path uses one root key. It does not encode a sidecar, generate chunk
keys, or iterate a chunk plan. Inline delete calls the SDK delete API directly. Physical layout
remains an executor concern and is not defined by the NoF framework.

Object splitting and request batching are separate. Object splitting decides how many records
represent one value. Cold Tier groups objects by target, and the executor maps those positional
objects or records directly to the corresponding KVCS batch API.

The configured single-value limit is also passed to the SDK client:

```shell
export MOONCAKE_KVCS_MAX_VALUE_SIZE=3221225472 # 3 GiB
```

KVCS 0.4.0 has no runtime getter for this limit. Unset uses the documented 4 MiB SDK default; valid
configured values are 1 byte through 4 GiB. The value must remain stable for a target because it is
part of the Low-Level private layout.

## Ownership and health

Low-Level `nof_replica_count` is clamped to `1..=8` and defaults to 1. The shared
`ReplicaLoadBalanceStrategy` selects write targets. Provider-managed modes may supply comparable
capacity scores. Managed ExtentStore capacity is owner-local, so healthy targets use the same
score and the existing accumulated-write count plus target ID provide deterministic balancing.

Each managed NoF target has one current runtime owner for its control plane. Registration reuses
the existing `ColdTierDeviceRecord`: its stable ID and epoch name the owner, and its
`updated_at_ms` compare-and-swap guards acquisition. Active compatible leases and stable
rendezvous choose which client may claim an unowned target. An existing live claim is not moved
by a competing client. If membership changes the preferred assignment, only the current owner may
transfer a stable target under the management gate and device-record CAS.

```text
target_id
owner_runtime
preferred = rendezvous(target-set, active-client-leases)
owner = CAS(cold-tier-device-record, preferred)
```

Every owner-scoped allocation, publication, release, downline, and recovery entry verifies the
shared device claim and the owner's active lease. This prevents clients with different cached
membership views from concurrently managing the same ExtentStore allocator. No owner lookup is
performed for ordinary reads or payload writes.

This Mooncake owner record is distinct from provider-owned external metadata. External metadata
is the provider's own object/manifest state (for example KVCS shard manifests) and is not used to
store NoF owner assignments or Mooncake-managed handoff snapshots.

The registration section above defines the initial owner. After assignment, local disks and NoF
targets use the same Cold Tier health, placement, replica, cleanup, and runtime lifecycle. A target
is reassigned by a fenced transfer from its current owner when membership changes, by graceful
ownership release, or after authoritative lease expiry.

All clients still create their own executor handle for every configured NoF target. The owner does
not proxy data I/O:

- reads use the materialized route and call the target executor directly;
- payload writes are sent directly to the executor;
- only target allocation, route publication/deletion, allocator recovery, GC/reclaim, and
  owner-scoped health require the current owner.

### Owner state transitions

The device record is the persisted owner fence. An unowned target may be claimed only when the
recorded owner has no active compatible lease and the claimant is the current
rendezvous-preferred client. A membership rebalance is initiated only by the current owner, after
its management gate drains and only for stable materialized routes; the successor performs full
manifest recovery. On graceful shutdown the same gate waits for current owner actions and rejects
later ones before the lease capability is withdrawn; on a crash, takeover waits for the recorded
owner's lease to expire.

### Graceful drain fast path

Normal shutdown first uses the existing client drain order to stop local offload producers and
flush accepted local offloads while the old owner is still present. Owner operations already in
flight finish before the snapshot is taken; later operations are rejected after ownership release.
Before withdrawing its NoF lease labels, the old owner:

1. recomputes the current target assignment;
2. selects the next compatible runtime using the same rendezvous rule, excluding itself;
3. lists `ObjectRoute` entries in every live backing state filtered by that target ID;
4. sends the route snapshot to the successor through the control plane.

The successor accepts the snapshot only while the shared device record still names the declared
old owner. The in-memory snapshot is retained only for that observed owner transition. After the
successor wins the device-record CAS, managed ExtentStore recovery consumes the snapshot and calls
the executor's existing `recover` interface. This fast path requires a complete route-index mirror
and active routes; it includes both materialized records and `PendingOffload` reservations.
Otherwise no snapshot is sent and the successor uses full manifest reconciliation. The target
remains excluded from allocation until recovery succeeds. The snapshot is metadata only: it does
not move payloads, change locators, or grant the successor data-proxy rights.

If there is no eligible successor, the transfer fails, or the process crashes, owner takeover
falls back to the shared manifest scan and two-way route/disk reconciliation. A target that was
route-downlined and later becomes healthy uses the same path because its routes have already been
removed. The target remains excluded from allocation until reconciliation succeeds. The fast path
is used only by Mooncake-managed ExtentStore targets; provider-owned KVCS targets do not have
Mooncake route snapshots.

The owner management gate does not wrap payload I/O performed by another writer. The transferred
or authoritatively recovered `PendingOffload` reservation is the protection against reallocating a
locator while that writer is still completing its direct SPDK write.

### Heartbeat health

Owner heartbeat is separate from owner handoff. A background heartbeat runs once per second and
request paths consume its cached result:

- each client probes only targets assigned to it;
- three consecutive failures exclude a target from placement. One success restores a target that
  was only short-circuited; a target whose routes were downlined after the long-failure window is
  admitted again only after its manifest recovery succeeds;
- the owner publishes unhealthy target IDs in the existing client lease; capacity remains in the
  owner's cached health result and is used locally by owner-scoped watermark maintenance;
- a target health failure does not by itself reassign the owner;
- owner reassignment occurs through the fenced membership rebalance described above, explicit
  drain, or authoritative client lease expiry.

KVCS Low-Level health uses the public existence query and reports no capacity because the 0.4.0 ABI
has no capacity call. Standard exposes no KVCS health capability, so the framework treats a
configured Standard target as available and does not probe it. Health is never checked per object
request.

## KVCS maintenance boundary

The public KVCS 0.4.0 Low-Level API exposes put, get/get-into, delete, and existence query. It does
not expose key listing, capacity, disk lifecycle, GC, watermarks, compaction, recovery, rebuild, or
a separate durability barrier. KVCS/EFC owns those responsibilities. Mooncake does not scan KVCS,
reconcile provider disks, rebuild provider metadata, or call private maintenance APIs.



For build, runtime configuration, and multi-node validation, see the
[NoF operations guide](../../../deployment/store-rs/nof.md).
