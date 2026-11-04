# DataManager V2 allocator node capacity

V2 DRAM and SSD tiers accept two optional fields in each `tiers[]` entry:

| Field | Default | Meaning |
| --- | ---: | --- |
| `allocator_init_capacity` | 131072 | Initially constructed allocator metadata nodes. |
| `allocator_max_capacity` | 1048576 | Maximum allocator metadata nodes; also controls initial storage reservation. |

Both fields are unsigned integer **node counts**, not bytes or byte-size
strings. Validation requires `1 <= init <= max` and
`2 <= max < 4294967295`. Lowering `max` below the default `init` requires setting
`init` explicitly as well. Invalid values fail with `INVALID_PARAMS` before
DataManager creates its pools. The same validation applies to direct C++ pool
construction. Metadata allocation failure returns `NO_AVAILABLE_HANDLE`.

For a large SSD containing small KV blocks, increase the SSD tier's budget
without increasing the DRAM tier's metadata reservation:

```json
{
    "data_manager_version": "v2",
    "tiers": [
        {"type": "DRAM", "capacity": "16GB", "priority": 100},
        {
            "type": "SSD",
            "capacity": "1TB",
            "priority": 10,
            "file_path": "/data/mooncake-v2/ssd.data",
            "allocator_init_capacity": 131072,
            "allocator_max_capacity": 16777216
        }
    ]
}
```

Each JSON tier currently creates one allocator. C++ callers can independently
set the same fields on each `DramArenaConfig` or `SSDDeviceConfig` when building
a pool with multiple arenas/devices. Omitted fields keep the original defaults;
V1 and the shared OffsetAllocator defaults are unchanged.

Nodes describe both live extents and free regions. A million-node budget does
not guarantee a million live keys: free tails, fragmented holes, pending writes
and allocations retained by readers also consume nodes. Size the maximum using
the actual per-key block sizes, desired occupancy, fragmentation and in-flight
allocations. Increasing the budget does not increase the pool's byte capacity
or remove alignment, bin-rounding and large-device offset-granularity overhead.

On a typical ABI, the allocator's node and freelist storage together reserve
about 32 bytes per maximum node: roughly 32 MiB for `1048576`, 512 MiB for
`16777216`, or 2 GiB for `67108864`, **per allocator**. Both vectors reserve their
maximum storage at initialization; a small `init` reduces initial construction
and touching of nodes, not this reservation. Allow additional memory for key
strings, handle tables, block/eviction indexes and other per-key state. These
figures are estimates, not measured RSS values.

Splitting one SSD into same-priority logical tiers is not a substitute for
configuring its allocator: the tier placement policy does not balance those
tiers or retry a different tier on metadata exhaustion. This configuration
change keeps existing placement and eviction semantics. JSON multi-device
configuration and allocator physical-usage accounting are separate concerns.

See [the full V2 example](datamanager_v2_full_example.json) for other settings.
