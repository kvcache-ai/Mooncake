# FlagOS FlagCX Transport

## Overview

[FlagCX](https://github.com/flagos-ai/FlagCX) is the unified communication library in the FlagOS
ecosystem for multi-vendor and cross-vendor deployments. Mooncake's FlagCX transport connects the
classic Transfer Engine to the FlagCX P2P Engine. The integration keeps Mooncake's existing
segment, memory-registration, batch, and completion APIs and maps them to FlagCX connections and
P2P read/write requests.

The protocol name used by Mooncake configuration and APIs is `flagcx`. `flagos` is not a protocol
name.

> **Classic Transfer Engine only.** The FlagCX transport is implemented under
> `mooncake-transfer-engine/src/transport/` and is not a TENT transport. Build with
> `USE_FLAGCX=ON`, select `--backend=classic` in `tebench`, and use `flagcx` as a standalone
> protocol.

### Code Structure

| Unit | Responsibility |
|------|----------------|
| `flagcx_transport.{h,cpp}` | Creates the P2P Engine, advertises its endpoint, registers memory, opens peer connections, submits transfers, and polls completion |
| `flagcx_transport_internal.h` | Overflow-safe registered-range checks and descriptor-length conversion |
| `mooncake-common/common.cmake` | Locates the external FlagCX headers and library and defines `FlagCX::flagcx` |
| `multi_transport.cpp` | Instantiates `FlagCxTransport` when the protocol name is `flagcx` |

### Transfer Pipeline

1. **Initialization:** Mooncake creates a FlagCX P2P Engine and starts its RPC server.
2. **Advertisement:** the transport obtains the FlagCX endpoint and publishes it in the local
   Mooncake segment descriptor.
3. **Registration:** `registerLocalMemory()` registers each local range with FlagCX and publishes
   the corresponding Mooncake buffer descriptor.
4. **Connection:** the first request to a remote segment reads its published FlagCX endpoint and
   opens a connection. FlagCX caches the connection for later requests.
5. **Submission:** Mooncake groups requests with the same target and operation, creates remote
   descriptors, and calls the FlagCX vector read or write API.
6. **Completion:** a Mooncake worker polls the returned FlagCX transfer ID and updates the original
   Mooncake task when the transfer becomes terminal.

---

## Dependencies

Use compatible FlagCX revisions on all nodes. Each node may build the accelerator backend that
matches its local platform. The prefix passed as `FLAGCX_HOME` must contain:

- `include/flagcx_p2p.h`
- `lib/libflagcx.so` or `lib64/libflagcx.so`
- The accelerator runtime, communication library, and network dependencies required by the chosen
  FlagCX backend

Choose the FlagCX backend that matches each node's local platform; for example, `USE_NVIDIA=1`,
`USE_METAX=1`, or `USE_MUSA=1`. See the
[FlagCX getting-started guide](https://github.com/flagos-ai/FlagCX/blob/main/docs/getting_started.md)
for the current backend list and platform prerequisites.

## Build and Compile

### 1. Build FlagCX

```bash
git clone https://github.com/flagos-ai/FlagCX.git
cd FlagCX
git submodule update --init --recursive

# Replace USE_NVIDIA with the backend for the local platform.
make USE_NVIDIA=1 -j$(nproc)
```

The default build output is suitable for an in-tree Mooncake build:

```text
FlagCX/build/include/flagcx_p2p.h
FlagCX/build/lib/libflagcx.so
```

Alternatively, install FlagCX under a dedicated prefix. Installing under `/opt` normally requires
root privileges:

```bash
sudo make PREFIX=/opt/flagcx install
```

### 2. Build Mooncake

From the Mooncake repository root:

```bash
cmake -S . -B build \
  -DUSE_FLAGCX=ON \
  -DFLAGCX_HOME=/path/to/FlagCX/build
cmake --build build -j$(nproc)
```

For the installed layout above, use `-DFLAGCX_HOME=/opt/flagcx`. If `FLAGCX_HOME` is not passed to
CMake, Mooncake first checks the environment variable of the same name, then defaults to
`$HOME/FlagCX/build`.

Configuration fails early if either `flagcx_p2p.h` or the FlagCX shared library cannot be found.
A successful configuration includes a message similar to:

```text
FlagCX transport enabled, include=/path/to/FlagCX/build/include, library=/path/to/FlagCX/build/lib/libflagcx.so
```

Make the shared library and its backend dependencies visible to the dynamic linker when they are
not installed in a system search path:

```bash
export FLAGCX_HOME=/path/to/FlagCX/build
export LD_LIBRARY_PATH="$FLAGCX_HOME/lib:${LD_LIBRARY_PATH:-}"
```

If Mooncake also needs to allocate or identify device memory, enable its matching hardware option,
such as `USE_CUDA`, `USE_MACA`, `USE_MUSA`, `USE_HIP`, `USE_COREX`, `USE_HYGON`, or `USE_MLU`.
See the [build guide](../../getting_started/build.md) for the options and SDK requirements.

---

## Enabling the Transport

### Python API

A Mooncake Python extension built from source with `USE_FLAGCX=ON` accepts `flagcx` through the
existing initialization API:

```python
engine.initialize(
    hostname="node1",
    metadata_server="P2PHANDSHAKE",
    protocol="flagcx",
    device_name="",
)
```

The Python binding disables automatic RDMA transport discovery for this protocol and installs the
FlagCX transport explicitly. No new Python API is required.

### C++ API

Applications using the classic C++ Transfer Engine can install the transport by name after engine
initialization. Keep automatic transport discovery disabled so that `flagcx` remains the standalone
transport:

```cpp
mooncake::TransferEngine engine(false);
if (engine.init("P2PHANDSHAKE", "node1") != 0) {
    return -1;
}

auto* transport = engine.installTransport("flagcx", nullptr);
if (transport == nullptr) {
    return -1;
}
```

Continue to use the standard Transfer Engine APIs for memory registration, segment discovery,
batch submission, and status queries.

---

## Run and Test

### Connectivity Test with `tebench`

`tebench` can validate registration, endpoint exchange, connection setup, read/write transfers,
and completion. Both peers must use the classic backend and the `flagcx` transport. An empty
`--target_seg_name` starts the target; setting it starts the initiator.

Choose a network interface reachable by the other peer on both hosts:

```bash
export FLAGCX_SOCKET_IFNAME=eth0
```

Start the target first:

```bash
./build/mooncake-transfer-engine/benchmark/tebench \
  --backend=classic \
  --xport_type=flagcx \
  --metadata_type=p2p \
  --seg_type=DRAM \
  --total_buffer_size=1073741824
```

The target prints a command containing its segment name. Use that value on the initiator:

```bash
./build/mooncake-transfer-engine/benchmark/tebench \
  --backend=classic \
  --xport_type=flagcx \
  --metadata_type=p2p \
  --target_seg_name=<target-segment-name> \
  --seg_type=DRAM \
  --total_buffer_size=1073741824 \
  --op_type=read \
  --start_block_size=4096 \
  --max_block_size=67108864 \
  --start_batch_size=1 \
  --max_batch_size=1 \
  --start_num_threads=1 \
  --max_num_threads=1
```

Use `--op_type=write` to test the opposite direction. The classic `tebench` VRAM allocator currently
supports CUDA builds. For that path, add `-DUSE_CUDA=ON` when building Mooncake and run with
`--seg_type=VRAM`. Other accelerator backends should validate device buffers through their
application integration until `tebench` provides a matching allocator.

Useful initialization messages include:

```text
FlagCxTransport: engine up, endpoint=...
FlagCxTransport: install OK (direct submit)
tebench: FlagCX transport installed
```

### Hardware-Free Checks

The internal range and descriptor-boundary test does not require FlagCX hardware:

```bash
ctest --test-dir build -R '^flagcx_transport_internal_test$' --output-on-failure
```

The Python source integration check verifies that the binding recognizes and installs `flagcx`:

```bash
python3 -m unittest mooncake-wheel/tests/test_transfer_engine_flagcx_source.py
```

---

## Runtime Configuration

FlagCX owns network-device selection and most transport tuning. The variables most relevant to the
Mooncake integration are:

| Variable | Purpose |
|----------|---------|
| `FLAGCX_SOCKET_IFNAME` | Selects the interface used for socket bootstrap and endpoint advertisement; it does not select the RDMA HCA |
| `FLAGCX_IB_HCA` | Selects the InfiniBand/RoCE HCA or HCA set used by FlagCX |
| `FLAGCX_P2P_TRANSPORT=accl` | Selects the optional ACCL P2P implementation when the FlagCX build includes it; both peers must set it |
| `LD_LIBRARY_PATH` | Makes `libflagcx.so` and non-system backend libraries visible at run time |

Set `FLAGCX_SOCKET_IFNAME` and `FLAGCX_IB_HCA` for each node's local interfaces and devices. For
interface filtering syntax, GID selection, retry settings, and backend-specific tuning, see the
[FlagCX environment-variable reference](https://github.com/flagos-ai/FlagCX/blob/main/docs/environment_variables.md).

## Important Notes

### Memory Registration Lifecycle

The FlagCX connection handshake exchanges the peer's registered-memory table, and the P2P Engine
caches established connections. Use the following lifecycle for the current Mooncake integration:

1. Register every buffer that a peer may access.
2. Establish or use the peer connection only after registration is complete.
3. Keep those buffers registered while the connection or its transfers are active.
4. Stop submitting work and allow outstanding work to finish before unregistering or freeing the
   buffers.

Registering a new remote buffer after a peer has already cached its connection does not refresh the
connection's memory table. Reconnect both processes before using a changed registration set.

### Deployment Scope

- Use the literal protocol name `flagcx`; do not use `flagos`.
- Use `flagcx` as a standalone protocol. Multi-protocol selection and routing are outside the
  current integration scope.
- The peers may use different accelerator backends, but they must use compatible FlagCX revisions
  and the same P2P network transport: default IBRC on both peers, or ACCL on both peers.
- The transport relies on FlagCX's endpoint exchange and completion behavior; it does not add a
  separate Mooncake cancellation API.

### Security and Trust

Treat the FlagCX endpoint like other RDMA-capable transport endpoints. Only expose it to trusted
peers, restrict the selected interface with network policy or host firewall rules, and register only
the memory required by the application. Mooncake segment metadata exposes the endpoint and buffer
addresses, while the FlagCX handshake exchanges the registered-region table. Do not expose either
channel to untrusted peers.

## Troubleshooting

### CMake Cannot Find FlagCX

```text
USE_FLAGCX=ON but flagcx_p2p.h was not found
```

Check that `FLAGCX_HOME/include/flagcx_p2p.h` exists. If the header remains only in the FlagCX
source tree, finish the FlagCX build so its public headers are copied to `build/include`, or run its
install target and point `FLAGCX_HOME` at that prefix.

```text
USE_FLAGCX=ON but the FlagCX library was not found
```

Check for `FLAGCX_HOME/lib/libflagcx.so` or `FLAGCX_HOME/lib64/libflagcx.so` and confirm that the
same prefix is passed during CMake configuration.

### The Executable Cannot Load `libflagcx.so`

If startup reports that `libflagcx.so` or a platform backend library is missing, add their library
directories to `LD_LIBRARY_PATH` or install them in a configured system loader path. This is a
run-time linker issue, not a Mooncake protocol-selection issue.

### The FlagCX Engine Advertises the Wrong Interface

Set `FLAGCX_SOCKET_IFNAME` before starting each process. Use an interface whose advertised address
is reachable from the peer. If InfiniBand/RoCE device selection is also ambiguous, set
`FLAGCX_IB_HCA` to the appropriate local HCA set on each side.

### `installTransport(flagcx)` Fails

Confirm all of the following:

- Mooncake was configured with `USE_FLAGCX=ON`.
- The FlagCX shared library and its platform dependencies load successfully.
- `FLAGCX_SOCKET_IFNAME` resolves to a usable local address.
- The FlagCX log does not report a P2P Engine or RPC-server initialization error.

### Connection or Remote-Descriptor Creation Fails

Verify that the target process is still running, the endpoint printed in the target log is reachable,
and both peers selected the same FlagCX P2P network transport. Check host firewall rules on the
connection path. If the target's registrations changed after the connection was first used, restart
both processes and register all buffers before reconnecting.
