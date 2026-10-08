# KVCacheStore Low Level backend

Mooncake can explicitly opt into the `kvcs-lowlevel` distributed-storage
adapter. The Mooncake Master owns logical object existence, size, and commit
state; the requesting client queries its local EFC and reads a provider record
only after checking the Master. EFC is deployed separately. Its socket and
shared-memory region must be visible to every Mooncake client using KVCS.
Size that region for the configured maximum value, batch size and SDK worker
counts according to the KVCacheStore deployment guide.
The presence of a socket never enables the adapter automatically.

## Initial scope

This first change supports **one KVCacheStore target and one value per object**.
The default target is the official G3.5 mountpoint at index 1. An explicit
single KVCacheStore target can instead be supplied through the EFC deployment
variables or `MOONCAKE_KVCS_EFC_CONFIG` YAML. Multiple targets, including
mixed G3/G3.5 routes, are rejected at initialization rather than silently
selecting one. The deployment's index must match the configured EFC.

The value must be nonempty and fit the configured
`MOONCAKE_KVCS_MAX_VALUE_SIZE` (default 4 MiB). A larger value fails
validation **before a provider write**; there is no truncation or partial
large-object write. Chunking, manifests, multi-target routing and dedicated
metrics are separate follow-up changes.

Set `MOONCAKE_KVCS_MODE=low-level` (or explicitly select
`MOONCAKE_DFS_FS_ADAPTER=kvcs-lowlevel`) on the cooperating Mooncake
processes. The initial distributed-storage configuration is single-tenant;
each provider key nevertheless includes the tenant ID and logical key in a
printable, injective encoding. Upsert/replace remains unsupported in this
initial change; insert-only writes do not overwrite existing keys. Single-key
`Remove` and `BatchRemove` first delete the physical value and then release
the Master name. This ordering is safe for insert-only Put: a concurrent Put
either still sees the old Master name and is rejected, or starts after the
physical delete has completed. A missing provider value is idempotent. If
Master rejects the removal, the physical value is gone and the logical
metadata remains for a retry; an atomic cross-system reservation is a later
follow-up. `RemoveByRegex` and `RemoveAll` return `NOT_SUPPORTED` in this
initial KVCS mode because provider-wide conditional deletion is not defined.

The KVCS SDK is optional at build time. With no SDK, other Mooncake backends
build normally and explicit KVCS initialization returns `NOT_SUPPORTED`.
With the SDK installed, CMake discovers `kvcs` using `pkg-config`. Deploy
EFC using the official KVCacheStore tooling; Mooncake does not start EFC,
configure storage capacity, or modify `/dev/shm`.

The SDK's Query proves presence but does not return the value size. Reads use
the Master descriptor's size and reject a shorter or longer provider value.
Provider misses and incomplete records never become valid data merely
because the Master has a COMPLETE descriptor. The adapter does not yet repair
stale Master metadata automatically; safe conditional invalidation belongs to
the later eviction-repair change. A timed-out insert or delete may have
completed at the provider, so the Low Level driver retains its in-process
uncertain-outcome protection; process restart is not durable fencing.
