# Key Access Path Debugging

This guide explains how to trace a single key through Mooncake Store RS: where a
`put` writes the object, how a `get` resolves and reads it, which route layer is
involved, and which storage node finally owns the data.

## What You Can See Today

Mooncake Store RS already exposes two useful observability surfaces:

- **Tracing logs** show request spans, route lookup stages, local/remote read and
  write decisions, control-plane failures, and transport fallbacks.
- **`query_route()`** returns the authoritative object route for one key,
  including replica owners, segment names, offsets, lengths, priorities, and
  route version.

Use both together:

- Use logs to answer: **which client issued the request, did it go local or
  remote, and did route/control-plane access fail?**
- Use `query_route()` to answer: **where is this key stored right now?**

Metrics are useful for aggregate health and throughput, but they are not
per-key traces.

## Enable Tracing

### Standalone Store Client

Start the daemon with a targeted trace filter:

```bash
mooncake-store-client ... \
  --trace-filter 'info,mooncake_store_client::client=debug,mooncake_store_client::route_directory=debug,mooncake_store_client::control_plane=debug,mooncake_store_py::dummy_service=debug,hyper=warn,h2=warn,tower=warn,tonic=warn' \
  --metrics-addr 0.0.0.0:19101
```

The `hyper`, `h2`, `tower`, and `tonic` targets are intentionally kept at
`warn`. Setting the whole process to `debug` usually produces too much HTTP/2
and gRPC protocol noise.

### Python Real Client / SGLang Real Client

For Python real clients, including SGLang when it loads Mooncake through the
Python package, use environment variables:

```bash
export MC_STORE_RS_TRACE=1
export MC_STORE_RS_TRACE_FILTER='info,mooncake_store_client::client=debug,mooncake_store_client::route_directory=debug,mooncake_store_client::control_plane=debug,hyper=warn,h2=warn,tower=warn,tonic=warn'
export MC_STORE_RS_METRICS_ADDR='0.0.0.0:19101'

python -m sglang.launch_server ...
```

`MC_STORE_RS_TRACE=1` initializes Rust tracing before `setup(...)`.
`MC_STORE_RS_TRACE_FILTER` is passed directly to `tracing_subscriber`.
`MC_STORE_RS_METRICS_ADDR` starts the in-process `/metrics` endpoint after
real-client setup.

## Find One Key in Logs

Use the unscoped key and the scoped route key when grepping:

```bash
KEY='YOUR_KEY'
TENANT='default'

grep -E \
  "key=\"${KEY}\"|key=${KEY}|${TENANT}::${KEY}|resolved object routes|writing reserved replicas|batch get completed|remote direct|get fallback|route .*failed|marked route authority" \
  sglang.log
```

The scoped route key format is:

```text
<tenant>::<key>
```

For the default tenant and key `foo`, the route key is:

```text
default::foo
```

## Put Path

A normal single-key `put` emits a span like:

```text
store.put{runtime=rw-node:1 tenant="default" key="YOUR_KEY" bytes=4096}: ...
```

Important fields:

- `runtime` is the client runtime that issued the request.
- `tenant` is the logical tenant.
- `key` is the user key.
- `bytes` is the object payload size.

During the data write stage, look for:

```text
writing reserved replicas runtime=rw-node:1 replicas=1 local_writes=0 remote_writes=1 value_bytes=4096
```

Interpretation:

- `replicas` is the number of reserved replica targets for this object.
- `local_writes` is the number of replicas written into the caller's local
  storage memory.
- `remote_writes` is the number of replicas written through the transport layer
  into remote storage memory.
- `value_bytes` is the object size written to each replica.

This line tells you whether the put data path was local or remote. To see the
exact storage owner, segment, and offset, use `query_route()`.

For batched puts, the same flow is split into stage metrics and logs such as:

- `batch_put_stage_rank`
- `batch_put_stage_reserve`
- `batch_put_stage_load_routes`
- `batch_put_stage_write`
- `batch_put_stage_route_cas`

## Get Path

A normal single-key `get` emits a span like:

```text
store.get{runtime=rw-node:1 tenant="default" key="YOUR_KEY"}:store.batch_get{runtime=rw-node:1 items=1}: ...
```

When route resolution succeeds, look for:

```text
resolved object routes runtime=rw-node:1 items=1
```

If the selected readable replica is local, the get finishes with:

```text
batch get completed via local copies only runtime=rw-node:1 total_items=1 local_items=1 local_bytes=4096
```

If the selected readable replica is remote, the get finishes with:

```text
batch get completed across local and remote paths runtime=rw-node:1 total_items=1 local_items=0 local_bytes=0 remote_items=1 remote_bytes=4096 remote_batch_chunks=1 remote_direct_fallbacks=0
```

Important fields:

- `local_items` is the number of objects read from local memory.
- `remote_items` is the number of objects read from remote storage owners.
- `remote_bytes` is the remote data volume.
- `remote_batch_chunks` is the number of batched remote transfer chunks.
- `remote_direct_fallbacks` is the number of direct remote fallback reads.

If batch scratch planning cannot serve the read path, the client may fall back
to direct remote get:

```text
executed remote direct get fallback runtime=rw-node:1 tenant="default" key="YOUR_KEY" bytes_out=4096
```

## Query Where a Key Is Stored

Use `query_route()` to inspect the storage placement for one key:

```python
import json

from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup({
    "local_hostname": "127.0.0.1:19999",
    "metadata_url": "redis://127.0.0.1:6379/0",
    "global_segment_size": 0,
    "local_buffer_size": 16 * 1024 * 1024,
    "protocol": "tcp",
    "rdma_devices": "",
    "master_server": "",
    "labels": {
        "storage": "false",
        "route": "false",
    },
    "routed_writes": False,
    "route_control": "embedded_wrh",
})

route = store.query_route("YOUR_KEY", tenant="default")
print(json.dumps(route, indent=2, ensure_ascii=False))

store.close()
```

The inspector client uses `storage=false` and `route=false` so it does not join
the storage pool or route-authority pool.

Example output:

```json
{
  "key": "default::YOUR_KEY",
  "version": 1,
  "state": "Active",
  "replicas": [
    {
      "owner": "storage-node-a:1",
      "segment_name": "storage-node-a-segment",
      "offset": 140733193388032,
      "segment_offset": 1048576,
      "length": 4096,
      "priority": 0,
      "tier": "Dram"
    }
  ]
}
```

Field meanings:

- `key` is the scoped route key.
- `version` is the route version.
- `state` should be `Active` for readable objects.
- `replicas[*].owner` is the **storage owner / replica owner**, not the route
  authority owner.
- `replicas[*].segment_name` is the storage segment containing the object.
- `replicas[*].segment_offset` is the offset inside that segment.
- `replicas[*].offset` is the transport-visible absolute address recorded for
  the replica.
- `replicas[*].length` is the object length in bytes.
- `replicas[*].priority` is the replica priority; lower priority is preferred.
- `replicas[*].tier` is the storage tier.

For reads, the client selects a readable replica from this route. In normal
cases, priority `0` is the first choice if that owner is still readable.

## Route Lookup and Route Authority

The route control plane uses embedded WRH routing. For each scoped key, clients
rank route-capable peers and read route state from the selected route
authorities. Route state then points to storage owners.

Keep these two owners separate:

- **Route authority owner**: the client responsible for serving route metadata
  for a key shard.
- **Storage owner**: the client that owns the memory segment containing the
  object replica.

`query_route()` returns storage owners. It does not directly tell you which
route authority answered this lookup.

Failure paths do log the route authority. Example:

```text
control stream batch_get_routes failed; falling back to unary batch rpc authority=py-store-xxx items=1 error=...
marked route authority as suspect after request failure runtime=py-store-xxx:1 context="route_batch_read_transport_failed"
mirrored authority route batch read failed; trying other authorities or metadata runtime=py-store-xxx:1 ...
```

These logs tell you which route authority was tried and why the request fell
back to another authority.

Successful route lookups currently log the resolved route stage, but they do
not print a full per-key line containing `key`, `rank`, `authority`, `source`,
`version`, and `replica_count`. If a test requires that exact successful
authority path, add a `debug` log in the route directory success path.

## Metrics for Correlation

Expose metrics:

```bash
curl -s http://127.0.0.1:19101/metrics | grep -E \
  'route_lookup_many|control_route_batch_get|put_stage|batch_get|segment_used|replica_distribution|transport_bytes'
```

Useful metric families:

- `mooncake_store_request_total{operation="route_lookup_many",...}` tracks route
  lookup results.
- `mooncake_store_request_total{operation="control_route_batch_get",...}` tracks
  control-plane route reads.
- `mooncake_store_request_total{operation="put_stage_reserve",...}` tracks
  reservation stage results.
- `mooncake_store_request_total{operation="put_stage_write",...}` tracks data
  write stage results.
- `mooncake_store_request_total{operation="put_stage_route_cas",...}` tracks
  route publish CAS stage results.
- `mooncake_store_segment_used_bytes{runtime=...,segment=...}` shows segment
  usage.
- `mooncake_store_replica_distribution{runtime=...,tier="dram"}` shows replica
  distribution.
- `mooncake_store_transport_bytes_total{direction="read"|"write",peer_kind="storage"}` shows
  storage transport traffic.

Metrics are process-level aggregate signals. They cannot reconstruct one key's
full path by themselves.

## Recommended Debug Procedure

1. Enable the targeted trace filter on the process that issues `put/get`.
2. If using a daemon/dummy path, enable tracing on the daemon too.
3. Run one `put` and one `get` for a unique key prefix.
4. Grep logs by both raw key and scoped key.
5. Use `query_route()` to dump storage placement.
6. Use `/metrics` only to confirm aggregate route, control-plane, data-plane,
   and storage usage counters changed as expected.

## Current Limitation

The system can already show the request span, local/remote data path, failures,
and final storage placement. The missing piece is a concise success log for
route-authority hits.

If that becomes a test requirement, add a `debug` line in the route directory
success path with:

- `key`
- `rank`
- `authority`
- `source`
- `route_version`
- `replica_count`

Keep it at `debug` level so production logs stay quiet by default.
