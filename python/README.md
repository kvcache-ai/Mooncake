# Mooncake Python APIs

The Python package exposes the native Store and framework-neutral structured
objects. Existing `put_structured_object`, DataProto, whole-object
`materialize` and matrix reads keep their current behavior.

## Reading member byte ranges

`MooncakeBundleTransfer.materialize_ranges_into` reads selected byte intervals
from raw-layout `bytes` or `ndarray` members. Source offsets address the logical
member, not a particular chunk. The API splits cross-chunk intervals, groups
fragments by chunk, and submits all selected members in one `get_into_ranges`
call. Adjacent fragments merge only when both source and destination addresses
are contiguous.

```python
from mooncake.structured_object_store import StructuredMemberRange, raw_destination

# lease is an existing caller-owned registered BufferPool lease.
destination = raw_destination(lease.ptr, nbytes, owner=lease, pre_registered=True)
spec = transfer.read_spec(ref).select_members(["values"])
ranges = (StructuredMemberRange("values", 1024, 256, 0, alignment=4),)
result = transfer.materialize_ranges_into(spec, ranges, {"values": destination})
```

The member-payload path only **borrows already-registered destination memory**.
For that destination it does not register, unregister, release a lease, create
a pool or copy member bytes through a Python staging buffer. Manifest and
metadata reads retain their existing path, including small temporary buffers,
registration and metadata copies where needed. Keep the lease, registration
and exporting allocation alive, and do
not modify/release the destination until the synchronous call completes. A
non-registered destination is rejected; existing whole-object APIs retain
their automatic registration and fallback behavior.

Range validation precedes member-payload submission: selected members, nonempty ranges,
natural element alignment, source/destination bounds, address-space overflow,
and actual destination-address overlap across members. Encoded/serialized
members and simultaneous member-slice plus byte-range requests are rejected.
Malformed native result counts and byte counts are failures; a missing ranged
transport does not silently fetch the whole member.

## Preparing repeated range geometry

```python
prepared = transfer.prepare_member_ranges(spec, ranges, destination)
# fresh_ref and fresh_destination belong to the current generation.
result = transfer.materialize_prepared_ranges_into(
    transfer.read_spec(fresh_ref).select_members(["values"]),
    prepared,
    fresh_destination,
)
```

Preparing reads only manifest/metadata and performs no member-payload GET.
The immutable plan stores schema and relative chunk geometry, **not** generation
keys, destination pointers, owners or values. Each execution resolves current
keys and rechecks the current owner, registration declaration, capacity and
alignment. A schema, total-byte or chunk-size change raises
`PreparedRangeLayoutChanged`; prepare again instead of reusing a stale plan.

Reuse is valid only while the relative ranges remain unchanged. For tiled sparse arrays,
the directory read geometry may stay fixed, but the directory contents and
resulting coordinate/value spans must be rebound every generation. Sparse
thresholds, coordinates, update arithmetic, replica selection and generation
fencing remain outside this generic API.

These APIs do not change native completion semantics. They require a
synchronous Store `get_into_ranges` implementation and retain current owners
through its return. No asynchronous lifetime or performance guarantee is
inferred from the Python contract tests.
