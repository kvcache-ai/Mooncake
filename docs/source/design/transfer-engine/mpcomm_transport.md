# MPComm Transport

## Overview

UCL-MPComm (Unified Communication Library - Memory Pool Communication) is a high-performance RDMA
communication library for heterogeneous memory pooling, developed by the Tencent Astral Network
Team. In Mooncake it is integrated as a transport backend of the TENT transfer framework and
registered as `MPCOMM`; the library is shortened to MPComm throughout this document.

- Upstream repository: <https://github.com/Tencent/UCL-MPComm>

MPComm drives multiple RDMA NICs concurrently and performs two-level load balancing (across NICs
and across QPs within a NIC), with NUMA-aware worker placement. It exposes one-sided
`put`/`get` primitives that the transport maps onto TENT's `WRITE`/`READ` requests.

At runtime, `TransferEngineImpl` loads MPComm Transport when `USE_MPCOMM` is enabled at build time
and `transports/mpcomm/enable=true` is set in the configuration.

> **TENT only.** This transport is implemented for the TENT runtime
> (`mooncake-transfer-engine/tent/`) and is not available through the legacy Transfer Engine
> transport path. Builds must therefore enable `USE_TENT` together with `USE_MPCOMM`, and
> benchmarks must run with `--backend=tent`.

### Code Structure

The transport is split so that everything reaching MPComm goes through one interface. That keeps the
provider out of most translation units, which is what makes the TENT-side logic buildable and
testable without libmpcomm - the same arrangement `TpuPjrtShim` and `UrmaAdapter` use for their
providers.

| Unit | Responsibility | Needs `mpcomm.h` |
|------|----------------|:----------------:|
| `mpcomm_adapter.{h,cpp}` | The MPComm boundary. A thin pass-through: MPComm owns its slicing, NIC/QP selection and worker threads, so there is no scheduling to model here. Compiles to an unavailable stub when `USE_MPCOMM` is off | **yes** (only here) |
| `mpcomm_peer_registry.{h,cpp}` | Peer cache and endpoint attribute parsing: which peers are connected, whose keys are current, and who may talk to a peer at any moment | no |
| `mpcomm_task_mapping.{h,cpp}` | Request and completion mapping: WRITE/READ onto put/get, provider outcome onto `TransferStatus`, handle release | no |
| `mpcomm_transport.{h,cpp}` | What needs the TENT runtime: resolving a `SegmentID` to a peer, reading the endpoint it advertises, driving batches, publishing capabilities | no |

`MpcommTransport` also accepts an adapter through a second constructor, which is how tests
substitute the provider.

### Transfer Pipeline

1. **Init**: `MPComm::init()` is called with the local segment name as the MPComm host id, the
   RDMA device list derived from the TENT `Topology`, and a TCP port used for MPComm's own
   metadata handshake. `startAcceptThread()` then serves incoming handshakes.
2. **Advertisement**: the transport publishes `v1:<host>:<mpcomm_tcp_port>` into the local segment's
   `transport_attrs` under the `MPCOMM` key, via `SegmentManager::updateLocal()` followed by
   `synchronizeLocal()`. Peers read this to learn where to complete the MPComm handshake. The `v1:`
   prefix makes a later format change detectable instead of silently misparsed; an attribute without
   a prefix is read as `v1`. Only IPv4 endpoints are accepted.
3. **Registration**: `addMemoryBuffer()` calls `registerMemory()` and `publishBuffer()` so the
   buffer's rkeys become visible to peers. NUMA placement is auto-detected by MPComm.
4. **Connection**: on the first request to a peer, `ensurePeerConnected()` resolves the peer's
   MPComm endpoint from its `transport_attrs`, calls `connect()`, and then fetches the peer's
   memory keys with `queryRemoteBuffer()`. Concurrent callers for the same peer wait for the first
   one rather than starting a second handshake.

   The cache is keyed by **MPComm host id** (the segment name), not by `SegmentID`, because that is
   what MPComm keys connections by. Closing and reopening a segment yields a fresh `SegmentID` for
   the same peer, and a second `connect()` to an already connected peer replaces its connection
   record wholesale - discarding the keys it carries and leaking its queue pairs - so keying on the
   `SegmentID` would reconnect a peer that is already connected.

   For the same reason the connection and the keys are tracked separately. A connection cannot be
   closed, so once established it is kept and reused; if only the key query failed, the peer is left
   in `CONNECTED_NO_KEYS` and the next request retries **the query alone**. A connection that was
   never established is not cached at all, so the next request retries the full handshake.

   Keys are also refetched when the peer registers memory after they were fetched, which TENT
   permits at any time. The transport compares the peer's currently published buffer ranges against
   those the cached keys cover; a range that is not covered triggers a refresh. Unregistered ranges
   do not, since a key for memory the peer no longer publishes is never used. Because segment
   descriptors are cached per thread with a TTL, a newly registered buffer becomes visible only
   after that TTL expires.
5. **Execution**: `submitTransferTasks()` issues `putAsync()` for `WRITE` and `getAsync()` for
   `READ`, one MPComm transfer per request. MPComm performs its own slicing and NIC/QP selection
   internally.
6. **Completion**: `getTransferStatus()` polls lazily with `isTransferComplete()`, then reads
   `getTransferResult()` to obtain the error code and a byte count, and releases the handle. The
   count is of bytes *posted*, which equals the request length once MPComm reports success; a
   transfer reported as successful but short is demoted to failed rather than trusted.

---

## Additional Dependencies

In addition to Mooncake's base dependencies, MPComm Transport requires the MPComm library:

- **Header**: `${MPCOMM_ROOT}/include/mpcomm.h`
- **Library**: `${MPCOMM_ROOT}/lib/libmpcomm.so`
- **Version**: **1.4 or newer, and major version 1.** Configuration reads MPComm's CMake package
  config and fails with an explicit message if the version is older or the major differs - MPComm
  declares `SameMajorVersion` compatibility, so a different major is an ABI break by its own
  definition. Both the standalone install and the wheel ship that config, and the resolved version
  is printed as `MPComm version: <x.y.z>`. A prefix that has only the headers and the library copied
  into it reports `unknown` instead, and the version is then not checked.

Both are provided by an MPComm installation, which may come either from a standalone CMake install
or from the MPComm Python wheel (in which case `MPCOMM_ROOT` is the `mpcomm` package directory
inside `site-packages`, since the wheel ships `include/` and `lib/` under the package root).

The two routes are not necessarily equivalent: MPComm gates several features behind build options
that default to `OFF`, notably `USE_CUDA` (device memory support) and `USE_MLNX` (Mellanox-specific
QP tuning that spreads traffic across ECMP paths, which affects multi-QP throughput). Its own
`build.sh` turns them on, whereas a plain `pip install` does not. Check how the library you install
was configured if device memory or multi-QP performance matters.

Make sure `libmpcomm.so` can be found by the dynamic linker at run time, for example via
`LD_LIBRARY_PATH`.

### Wheel Packaging

The `mooncake-transfer-engine` wheel **deliberately does not bundle MPComm**. `scripts/build_wheel.sh`
passes `--exclude libmpcomm.so*` to `auditwheel repair`, so `engine.so` keeps its `DT_NEEDED` entry
on `libmpcomm.so.<N>` and the library stays an external dependency resolved at run time. This keeps
MPComm independently upgradable: replacing `libmpcomm.so` does not require rebuilding or repackaging
Mooncake, as long as the MPComm major version (its `SOVERSION`) is unchanged. A major bump does
require rebuilding Mooncake against the new headers.

Consequently, importing the Python extension without MPComm available fails with:

```
ImportError: libmpcomm.so.1: cannot open shared object file: No such file or directory
```

Provide the library through either MPComm installation form, then point the linker at it:

```bash
# From the MPComm wheel
export MPCOMM_ROOT=$(python3 -c "import mpcomm, os; print(os.path.dirname(mpcomm.__file__))")
# ...or from a standalone CMake install, e.g. MPCOMM_ROOT=/opt/mpcomm

export LD_LIBRARY_PATH=$MPCOMM_ROOT/lib:$LD_LIBRARY_PATH
python3 -c "from mooncake import engine"   # should now import cleanly
```

---

## Build and Compile

**Prerequisites**

- MPComm is installed, providing both `include/mpcomm.h` and `lib/libmpcomm.so`
- RDMA devices are available and `libibverbs` is installed
- Build environment can access Mooncake and its base dependencies

**CMake Configuration**

```bash
# Clone Mooncake
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake

# Enable TENT + MPComm. Add -DUSE_CUDA=ON if you need VRAM segments.
mkdir build && cd build
cmake .. -DUSE_TENT=ON -DUSE_MPCOMM=ON -DMPCOMM_ROOT=/opt/mpcomm

# Build
make -j$(nproc)
```

`MPCOMM_ROOT` is mandatory when `USE_MPCOMM=ON`. Configuration fails early if it is unset, or if
the expected header and library cannot be found underneath it. On success the configure log
reports the resolved paths:

```
-- MPComm transport is enabled
--   MPComm include: /opt/mpcomm/include
--   MPComm library: /opt/mpcomm/lib/libmpcomm.so
```

---

## Enabling the Transport

Loading the transport and selecting it for a transfer are two separate steps.

**1. Load it.** The transport is only instantiated when its config gate is on:

```json
{ "transports": { "mpcomm": { "enable": true } } }
```

**2. Select it.** Any of the standard TENT mechanisms work:

- A transport policy (see [Transport Selector](../tent/transport-selector.md)):

  ```json
  {
    "transports": { "mpcomm": { "enable": true } },
    "policy": [
      { "name": "mpcomm_memory", "segment_type": "memory", "transports": ["mpcomm"] }
    ]
  }
  ```

- A per-request override, which takes precedence over policies:

  ```cpp
  Request r{};
  r.transport_hint = TransportType::MPCOMM;
  ```

A policy or a hint is effectively **required**. In the default ordering returned by
`getSupportedTransports()`, `MPCOMM` comes second to last -- only `TPU` follows it -- so leaving
the choice to the default means MPComm is unlikely to be picked at all.

---

## Run and Test

`tebench` (the TENT benchmark, built at `build/mooncake-transfer-engine/benchmark/tebench`)
supports `mpcomm` for connectivity and performance validation. The role is determined by
`--target_seg_name`: empty means target, otherwise initiator.

There is also a unit test, which is built when `USE_MPCOMM=ON` and unit tests are enabled. Its
functional case needs RDMA devices and MPComm at run time and skips itself otherwise:

```bash
ctest -R tent_mpcomm_transport_test --output-on-failure
```

`--xport_type=mpcomm` selects the `MemoryOptions.type` used when registering buffers. It also
restricts the enabled transports, but setting `MC_TENT_CONF` replaces the configuration wholesale
and undoes that (see [Important Notes](#important-notes)), so the examples below list the
`transports` gates explicitly. Keeping them in a shell variable avoids repeating the block:

```bash
XPORTS='"transports":{"mpcomm":{"enable":true},"rdma":{"enable":false},
        "tcp":{"enable":false},"shm":{"enable":false},"nvlink":{"enable":false},
        "mnnvl":{"enable":false},"gds":{"enable":false},"io_uring":{"enable":false}}'

# Terminal 1: target
MC_TENT_CONF="{\"rpc_server_hostname\":\"10.0.0.1\",$XPORTS}" \
MPCOMM_TCP_PORT=13579 \
./tebench --backend=tent --xport_type=mpcomm \
  --metadata_type=p2p --rpc_server_port=12345 \
  --seg_type=DRAM --total_buffer_size=2147483648

# Terminal 2: initiator
MC_TENT_CONF="{\"rpc_server_hostname\":\"10.0.0.2\",$XPORTS}" \
MPCOMM_TCP_PORT=13579 \
./tebench --backend=tent --xport_type=mpcomm --tent_transport_hint=mpcomm \
  --metadata_type=p2p --rpc_server_port=12346 \
  --target_seg_name=10.0.0.1:12345 \
  --seg_type=DRAM --total_buffer_size=2147483648 \
  --op_type=read --start_block_size=262144 --max_block_size=262144 \
  --start_batch_size=32 --max_batch_size=32 \
  --start_num_threads=4 --max_num_threads=4 --duration=30
```

Setting `rpc_server_hostname` is strongly recommended on multi-homed or containerized hosts; see
[Important Notes](#important-notes). `--seg_name` is deliberately absent: with `metadata_type=p2p`
TENT derives the local segment name from `rpc_server_hostname` and `rpc_server_port` and ignores
the flag.

**On a single host** both processes share the port namespace, so give them different handshake
ports. Avoid the 15000-17000 range, which the TENT RPC server allocates from:

```bash
MPCOMM_TCP_PORT=13579 ...   # target
MPCOMM_TCP_PORT=13580 ...   # initiator
```

To confirm that traffic really went over MPComm, look for these lines:

```
MpcommTransport: Installed successfully, host_id=10.0.0.2:12346, tcp_port=13579, devices=mlx5_0,mlx5_1
MpcommTransport: Connected to segment 10.0.0.1:12345
```

If MPComm fails to start, the engine only logs `Transport mpcomm skipped: ...` and continues with
the remaining transports. With every other transport disabled the run then fails later and less
obviously, when buffer registration finds no usable transport.

Add `--check_consistency=true` to verify payload correctness. It writes and reads every block back,
so it changes the access pattern as well as lowering the reported bandwidth; leave it off when
measuring throughput.

### Unit Tests

There are two suites, because the data path needs hardware and the logic around it does not.

`tent_mpcomm_boundary_test` drives the transport against an injected `MpcommAdapter`, so it needs
neither an RDMA device nor libmpcomm and is built in every configuration:

```bash
cmake .. -DUSE_TENT=ON -DBUILD_UNIT_TESTS=ON   # USE_MPCOMM not required
make -j tent_mpcomm_boundary_test
ctest -R tent_mpcomm_boundary_test --output-on-failure
```

It covers endpoint publication and parsing, single-flight connection under concurrency, key-query
retry over an existing connection, refreshing keys when a peer registers memory, the WRITE/READ
mapping, short-transfer and error handling, releasing each handle exactly once, and teardown. It does
not cover MPComm's own behaviour - slicing, NIC and QP selection, worker scheduling - which is the
provider's responsibility.

`tent_mpcomm_transport_test` exercises the real data path: it forks a target, drives a WRITE followed
by a READ over MPComm and verifies the payload. It requires RDMA devices and a working MPComm
installation, and skips itself when the engine cannot be brought up:

```bash
cmake .. -DUSE_TENT=ON -DUSE_MPCOMM=ON -DBUILD_UNIT_TESTS=ON -DMPCOMM_ROOT=<prefix>
make -j tent_mpcomm_transport_test
ctest -R tent_mpcomm_transport_test --output-on-failure
```

Parent and child use distinct `MPCOMM_TCP_PORT` values derived from the pid, since MPComm's handshake
listener would otherwise collide; see the note on port uniqueness above.

---

## GPU (Device) Memory

MPComm transfers device memory as well as host DRAM, so VRAM segments are supported in all four
combinations (DRAM to DRAM, DRAM to GPU, GPU to DRAM, GPU to GPU).

Three preconditions must all hold, and only the first one is checked at run time:

1. **`nvidia-peermem` is loaded.** Device memory is registered through the ordinary `ibv_reg_mr`
   path: the kernel's `get_user_pages()` is intercepted by `nvidia-peermem`
   (`ib_peer_memory_client`) to pin GPU pages. There is no dma-buf fallback. This is the same
   dependency `RdmaTransport` has for GPU-Direct.
2. **MPComm itself was built with `-DUSE_CUDA=ON`.** That option defaults to `OFF`, and its device
   detection is compiled out entirely when it is off, so device pointers are then registered as
   host memory and NUMA/PCIe affinity selection silently degrades. The upstream `build.sh` enables
   it; a plain `pip install` of the MPComm package does not.
3. **Mooncake was built with `-DUSE_CUDA=ON`**, otherwise `--seg_type=VRAM` is rejected outright.

The transport probes `/proc/modules` during `install()` and advertises the GPU capabilities only
when `nvidia-peermem` is present. Otherwise it logs

```
MpcommTransport: nvidia_peermem not detected, GPU memory support is disabled
```

and reports `dram_to_dram` only, so transport selection will not route device memory to MPComm.
Setting `transports/mpcomm/disable_gpu_direct_rdma` to `true` forces the same behaviour on a host
that does have the module, which is reported separately:

```
MpcommTransport: GPU memory support disabled by transports/mpcomm/disable_gpu_direct_rdma
```

Note that conditions 2 and 3 are **not** detectable by that probe. If GPU transfers behave oddly
while the module is loaded, confirm how MPComm was built.

Buffer registration is attempted for every loaded transport irrespective of capabilities, so on a
host without the module registering a VRAM buffer logs a warning from MPComm. That is harmless:
the buffer simply does not list `MPCOMM` among its transports, and selection skips it.

To exercise device memory with `tebench`, pass `--seg_type=VRAM` on either or both sides, or
`--seg_type_mix=dram,vram` to drive both memory types from a single process:

```bash
./tebench --backend=tent --xport_type=mpcomm --seg_type=VRAM ...
```

---

## Environment Variables

MPComm reads its own tuning parameters directly from the environment. The transport adds the two
`*_TCP_PORT` variables.

| Variable | Description | Default |
|----------|-------------|---------|
| `MPCOMM_TCP_PORT` | Local TCP port for MPComm's metadata handshake. Must be unique per process on a host: MPComm binds it and fails to initialise if it is taken, with no retry. `0` does not mean "pick one". The value is validated, and a non-numeric or out-of-range one fails `install()` rather than being silently treated as `0`. This port hands out remote memory keys - see [Security and Trust Boundary](#security-and-trust-boundary) | `13579` |
| `MPCOMM_REMOTE_TCP_PORT` | Peer handshake port, used only when the peer published no `transport_attrs`. Without it such a peer is rejected: the attribute is written by `install()`, so its absence means the peer runs no MPComm transport, and guessing a port would point `connect()` at an unrelated process - which, since the handshake has no timeout, can block the submitting thread indefinitely | (none; peer is rejected) |
| `MPCOMM_GID_INDEX` | RoCE/IB GID index used when creating QPs. Devices whose GID at this index is all zeroes are skipped, which can leave MPComm reporting no usable devices. Use `-1` to pick the first non-zero GID | `3` |
| `MPCOMM_NIC_FILTER` | Comma-separated list of allowed RDMA device names. When unset, the transport passes the device list derived from the TENT `Topology`; setting this variable overrides that list | (TENT topology) |
| `MPCOMM_MAX_RDMA_TRANSFER_SIZE` | Maximum bytes per RDMA operation; larger requests are sliced across NICs | `1 GB` |
| `MPCOMM_QPS_PER_CONNECTION` | Number of QPs per NIC per connection. Values above 16 make initialisation fail | `1` |
| `MPCOMM_POLL_BATCH_SIZE` | Maximum work completions per `ibv_poll_cq` call | `64` |
| `MPCOMM_MAX_IDLE_SPINS` | Number of idle polling iterations a worker spins (with a CPU pause hint) before backing off | `10000` |
| `MPCOMM_MAX_SEND_WR` | QP send queue depth | `512` |
| `MPCOMM_MAX_OUTSTANDING_PER_QP` | Maximum outstanding work requests per QP | `256` |
| `MPCOMM_LOG_LEVEL` | `error` / `warn` / `info` / `debug` | `info` |
| `MPCOMM_TRANSFER_STATS_INTERVAL` | Print statistics every N transfers (requires `debug`) | `0` (every transfer) |

---

## Configuration Options (TENT)

- `transports/mpcomm/enable`: enable or disable the transport (default: `true` once
  `USE_MPCOMM` is compiled in)
- `transports/mpcomm/disable_gpu_direct_rdma`: force the GPU capabilities off even when
  `nvidia-peermem` is loaded (default: `false`). See [GPU (Device) Memory](#gpu-device-memory).

MPComm's own behaviour is tuned through the environment variables above rather than through TENT
configuration keys.

---

## Important Notes

1. **TENT only.** There is no MPComm backend on the legacy Transfer Engine transport path, so
   `--backend=classic` and `transfer_engine_bench --protocol=mpcomm` are not supported.

2. **Set `rpc_server_hostname` on multi-homed or containerized hosts.** With
   `metadata_type=p2p`, TENT overwrites `local_segment_name` with `rpc_server_hostname:port`. When
   `rpc_server_hostname` is unset it is auto-discovered, which may pick an address that peers
   cannot route to (for example a container overlay address). Since the published
   `transport_attrs` are derived from that name, MPComm would then advertise an unreachable
   endpoint. `tebench` has no command-line flag for it, so pass it through `MC_TENT_CONF`.

3. **`MC_TENT_CONF` replaces the configuration wholesale.** Keys other than the metadata identity
   (`metadata_type`, `metadata_servers`, `local_segment_name`, `rpc_server_hostname`,
   `rpc_server_port`) are not preserved across the load. If you set `MC_TENT_CONF` *and* rely on
   `--xport_type` to disable the other transports, list the `transports` gates explicitly in
   `MC_TENT_CONF` as well, otherwise they revert to their defaults.

4. **Segment metadata mutation.** The transport publishes its endpoint through
   `SegmentManager::updateLocal()`. Snapshots returned by `getLocal()` are copy-on-write and must
   never be written through.

5. **Buffer registration cost.** `registerMemory()` pins and maps memory on every NIC, so
   registration of large buffers takes noticeable time at startup. Register once and reuse.

6. **Optional transport interfaces are not implemented.** MPComm Transport does not provide
   cancellation (`supportsCancellation()` returns `false`), notification
   (`supportNotification()` returns `false`, so `tebench --notifi` is unavailable), bandwidth
   estimation, or NIC load statistics, and it does not consume the `qp_pool` or
   progress-notification facilities of `Transport::SubBatch`. Failover and QoS features that
   depend on those hooks fall back to their defaults.

7. **The first transfer to a peer performs the MPComm handshake inline.** Connection setup happens
   on the submitting thread the first time a segment is used, so that submission takes noticeably
   longer than subsequent ones.

---

## Security and Trust Boundary

MPComm's metadata handshake is a plain TCP exchange with **no authentication and no encryption**,
and what it exchanges is remote memory metadata:

```c++
struct RemoteBufferEntry {
    uint64_t addr;
    uint64_t length;
    int numa_node;
    std::vector<uint32_t> rkeys;   // <- remote keys
};
```

An address together with its rkey is exactly what an RDMA READ or WRITE needs. Anything that can
reach `MPCOMM_TCP_PORT` can therefore obtain the means to read and write the process's registered
memory directly, without going through TENT at all.

**Treat that port as being at the same trust level as the RDMA fabric itself.** In practice:

- Keep it on the same trusted network as the fabric, and do not expose it to untrusted networks or
  to the internet. Restrict it with host firewall rules if the host also carries untrusted traffic.
- Registered buffers are reachable by any peer that completes the handshake. There is no per-peer
  authorisation, and no per-buffer permission model beyond what the fabric enforces.
- This is the same exposure model as raw RDMA between trusted nodes; MPComm adds no protection of
  its own and does not weaken the fabric's either.

### Addressing

- **Only IPv4 endpoints are supported.** The handshake sockets are `AF_INET`, and the endpoint
  attribute is parsed as `<ipv4>:<port>`; an IPv6 literal is rejected at parse time rather than
  being silently split at the wrong colon.
- **The listener binds all interfaces** (`INADDR_ANY`) and this is not configurable - it is
  MPComm's own behaviour. Use firewall rules to restrict which interfaces are actually reachable.
- **The advertised address follows `rpc_server_hostname`**, since the endpoint attribute is derived
  from the local segment name. On a multi-homed host this ties the handshake path to whichever
  address TENT uses for RPC. Setting `rpc_server_hostname` explicitly is therefore the way to
  control it today; a dedicated setting for advertising an address separate from the RPC one is not
  implemented yet.

---

## Troubleshooting

### Configuration fails with a missing `MPCOMM_ROOT`

```
USE_MPCOMM=ON requires MPCOMM_ROOT to point at the MPComm install prefix
```

Pass `-DMPCOMM_ROOT=<prefix>`. The prefix must contain `include/mpcomm.h` and
`lib/libmpcomm.so` (`lib64` is searched as well).

### Configuration fails with MPComm not found

```
MPComm not found under MPCOMM_ROOT=<prefix>
```

Verify the layout, and remember that CMake caches find results:

```bash
ls $MPCOMM_ROOT/include/mpcomm.h $MPCOMM_ROOT/lib/libmpcomm.so
cmake .. -UMPCOMM_LIBRARY -UMPCOMM_INCLUDE_DIR
```

### `error while loading shared libraries: libmpcomm.so`

The library is resolved by absolute path at link time, but the dynamic linker still needs to find
it at run time:

```bash
export LD_LIBRARY_PATH=$MPCOMM_ROOT/lib:$LD_LIBRARY_PATH
ldd ./tebench | grep -E "mpcomm|not found"
```

### The peer connects to an unexpected address

Symptom: the initiator logs `MPComm: Connecting to <host_id> at <ip>:<port>` with an address peers
cannot reach, and stalls, while the segment itself was opened successfully.

The address MPComm uses comes from the peer's segment name and `transport_attrs`, which are
derived from `rpc_server_hostname`. Set it explicitly on **both** sides, then confirm the target
advertises the intended address:

```
MpcommTransport: Installed successfully, host_id=<expected-ip>:<port>, ...
```

### `MPComm: No RDMA devices found`

MPComm skips any device whose GID at `MPCOMM_GID_INDEX` (default `3`) is all zeroes, which is
common on InfiniBand and on RoCE setups with a different GID layout. Set `MPCOMM_GID_INDEX=-1` to
pick the first non-zero GID, or point it at the correct index.

### `Failed to query remote buffers`

```
MpcommTransport: Failed to query remote buffers from <host>, error=<code>
```

The connection succeeded but fetching the peer's memory keys did not. Those keys are the only way
to address the peer's memory, so this is a hard failure and the request fails.

The connection itself is kept - MPComm cannot close one, and reconnecting would replace its
connection record and leak its queue pairs - so the peer is left in `CONNECTED_NO_KEYS` and the
next request retries **only the query**, not the handshake.

The usual cause is that the peer had not finished registering its buffers yet, so start the target
and let it finish registration before starting the initiator. Since the retry is a query, the
recovery needs no restart on either side.

### Lower than expected bandwidth

- Remove `--check_consistency=true`; it writes and reads every block back.
- Increase `--duration`; the inline handshake on the first transfer to a peer is included in the
  measurement.
- Increase `--start_batch_size` / `--max_batch_size`; with a batch size of 1 there are not enough
  in-flight requests to fill `MPCOMM_MAX_OUTSTANDING_PER_QP`.
- Check `MPCOMM_MAX_RDMA_TRANSFER_SIZE` against the block size. Slicing is what spreads a transfer
  over several NICs, so a request smaller than this limit becomes a single chunk on a single NIC.
  Lower it to engage more NICs per request.
- For `--seg_type=DRAM`, `tebench` allocates one buffer of `--total_buffer_size` per NUMA node.
  Restricting memory to a single node with `numactl --membind` while NICs on another node drive
  traffic results in cross-socket access.
