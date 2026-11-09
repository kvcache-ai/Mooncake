# Store-RS Python Operations

The root wheel installs the standalone Store-RS commands. Build and install it
using the [source-checkout quickstart](../../getting_started/store-rs.md).

## Standalone Store-RS Commands

The root CMake target build_store_rs_cli builds the client, admin, and benchmark binaries. The unified root wheel installs the standalone commands under their mooncake-store-rs-* names. Use the [source-checkout quickstart](../../getting_started/store-rs.md) to configure the root build and install the wheel.

Start a storage client:

```bash
mooncake-store-rs-client \
  --local-hostname 127.0.0.1 \
  --metadata-url redis://127.0.0.1:6380/0 \
  --storage-bytes $((128 * 1024 * 1024)) \
  --scratch-bytes $((16 * 1024 * 1024)) \
  --stable-id store-a \
  --tenant default \
  --label pool=pool-a \
  --label storage=true \
  --metrics-addr 127.0.0.1:9091
```

Useful flags:

- `--transport-metadata-url` to override the Transfer Engine metadata input (default `P2PHANDSHAKE`; `tent` requires `redis://...`)
- `--transport-backend tent|classic-te` to choose the real data-plane backend
- `--transport-rpc-port <port>` to pin the real data-plane TCP port used by real clients
- `--routed-writes` and `--replica-count` to enable routed writer mode
- `--route-topk <n>` as a compatibility fallback for WRH route-authority fanout; it must be `>= 2`, should match any policy already stored in metadata, and admin-managed tenant policy is preferred
- `--route-control metadata-only|embedded-wrh` as a compatibility fallback for route authority mode; prefer admin-managed tenant policy in metadata
- `--heartbeat-interval-ms`, `--heartbeat-timeout-ms`, and `--lease-ttl-ms` to tune lease refresh; `--lease-ttl-ms` defaults to `30000`
- `--request-timeout-ms` to set the outer request deadline for routed operations and dispatcher calls
- `--startup-timeout-ms` to override the compatibility registration timeout used by startup local-memory registration plus real-mode `register_buffer` / `unregister_buffer`; when unset the runtime uses `max(20s, ceil(registration_bytes / 1 GiB))`
- `--transfer-stall-timeout-ms` to set the inner transport stall detector for TENT / classic TE
- `--drain-on-exit` to enter draining mode and evacuate owned replicas before shutdown
- `--client-server-address host:port` to expose the standalone compatibility server for dummy clients only
- `--use-hugepage` and `--hugepage-size 2MB|1GB` to enable hugepage-backed local memory

Role reminder:

- use `--label storage=true` on storage nodes that should accept routed placement and run local CLOCK eviction
- use `--label storage=false` on routed rw nodes that should place remotely without owning local storage
- `--label storage=true` requires `--storage-bytes > 0`
- when `--storage-bytes 0` is used without an explicit storage label, the runtime defaults to `storage=false`

Port reminder:

- `transport_rpc_port` / `--transport-rpc-port` is the real-mode data-plane port for the selected backend
- `client_server_address` / `--client-server-address` is the dummy compatibility gRPC port
- `metrics_addr` / `--metrics-addr` is only for `/metrics`
- cross-host real-mode deployments should set both a reachable `local_hostname` and a fixed `transport_rpc_port`
- `local_hostname` may also be passed as `host:port`; the compatibility layer will split the port into `transport_rpc_port`

Heartbeat behavior:

- timeout knobs now come from one shared helper across the standalone client, Python compatibility runtime, and dummy client
- `--request-timeout-ms` / `MC_STORE_RS_REQUEST_TIMEOUT_MS` sets the outer request deadline
- `--startup-timeout-ms` / `MC_STORE_RS_STARTUP_TIMEOUT_MS` sets the registration-specific timeout budget; when unset the runtime derives it from the current registration size with a `20s` floor
- `--heartbeat-timeout-ms` / `MC_STORE_RS_HEARTBEAT_TIMEOUT_MS` sets the dedicated heartbeat publish timeout
- `--transfer-stall-timeout-ms` / `MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS` sets the inner transfer stall detector
- `MC_STORE_RS_DUMMY_RPC_TIMEOUT_MS` controls dummy gRPC calls and falls back to `MC_STORE_RS_REQUEST_TIMEOUT_MS`
- heartbeat publish uses a dedicated timeout instead of the generic request deadline
- a single failed heartbeat no longer exits the standalone client process
- failed heartbeat publishes are retried on a short backoff
- `MC_STORE_RS_CONTROL_PLANE_THREADS` sets the worker count of the shared control-plane RPC client runtime; default `2`
- `MC_STORE_RS_CONTROL_PLANE_SERVER_THREADS` sets the worker count of the embedded control-plane gRPC server runtime; default `4`

## Metadata Maintenance

Use the standalone Rust admin binary when metadata still contains stale segment registrations
from dead storage owners:

```bash
mooncake-store-rs-admin \
  --metadata-url redis://127.0.0.1:6380/0 \
  cleanup-stale-segments
```


## Admin HTTP Migration Tasks

Use the [route migration guide](route-migration.md) for operator workflows and
the [Store-RS Admin HTTP API reference](../../api-reference/http/store-rs-admin.md)
for endpoint details.
