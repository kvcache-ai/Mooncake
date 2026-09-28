# Sunrise Link Transport

## Overview
Sunrise Link is a GPU-to-GPU direct communication interconnect developed by Sunrise. In Mooncake, it is integrated as a GPU transport backend in the TENT transfer framework. It relies on Tang Runtime (`tangrt`) for device memory allocation, pointer attribute queries, peer copy, and IPC handle operations, and is registered as `SUNRISE_LINK` in TENT.

At runtime, `TransferEngineImpl` loads Sunrise Link Transport when `USE_SUNRISE` is enabled at build time and `transports/sunrise_link/enable=true` is set in config.

### Transfer Pipeline

1. **Init**: `dlopen` `libptml_shared.so` + `libtangrt_shared.so`, initialize PTML.
2. **Topology**: `ptmlPtlinkPhytopoDetect` discovers inter-chip ports, builds C2C mapping (`local_chipid, remote_chipid → local_port`).
3. **Registration**: `tangIpcGetMemHandle` generates IPC handles for GPU buffers.
4. **Execution**: Same device → `tangMemcpy`; Cross-device → `tangMemcpyPeer` / `tangMemcpyPeer_v2`, fallback to `tangDeviceGetPeerPointer` + C2C; Remote → `tangIpcOpenMemHandle` + peer copy; Host ↔ Device → `tangMemcpy` with direction.
5. **Async**: When size exceeds `async_memcpy_threshold`, uses `tangMemcpyAsync` with per-device stream.

---

## Additional Dependencies
In addition to Mooncake's base dependencies, Sunrise Link Transport requires Tang Runtime:

- **Header path**: `/usr/local/tangrt/include`
- **Library path**: `/usr/local/tangrt/lib/linux-x86_64`
- **Shared library**: `libtangrt_shared.so` (loaded from the default installation path at runtime)

Make sure runtime libraries related to `tangrt` and `ptml` can be found by the dynamic linker (for example, via `LD_LIBRARY_PATH` or system library search paths).

---

## Build and Compile

**Prerequisites**

- Tang Runtime is installed and available (default path: `/usr/local/tangrt`)
- Build environment can access Mooncake and its base dependencies

**CMake Configuration**

```bash
# Clone Mooncake
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake

# Enable TENT + Sunrise Link
mkdir build && cd build
cmake .. -DUSE_TENT=ON -DUSE_SUNRISE=ON

# Build
make -j$(nproc)
```

If Tang Runtime is installed at a non-default location:

```bash
cmake .. -DUSE_TENT=ON -DUSE_SUNRISE=ON -DMC_TANGRT_ROOT=/opt/tangrt
```

---

## Run and Test

`transfer_engine_bench` supports the `sunrise_link` protocol for basic connectivity and performance validation.

```bash
# Terminal 1: target
./transfer_engine_bench \
  --mode=target \
  --protocol=sunrise_link \
  --local_server_name=10.0.0.2 \
  --metadata_server=P2PHANDSHAKE \
  --gpu_id=0

# Terminal 2: initiator
./transfer_engine_bench \
  --mode=initiator \
  --protocol=sunrise_link \
  --metadata_server=P2PHANDSHAKE \
  --segment_id=10.0.0.2:$PORT \
  --gpu_id=0 \
  --block_size=8388608 \
  --batch_size=32
```

> Note: when `metadata_server=P2PHANDSHAKE`, the target node may listen on a dynamically assigned port. Replace `$PORT` in `--segment_id` with the actual port printed in target logs.

### Using `tebench` (TENT Backend)

Role is determined by `--target_seg_name`: empty → target, otherwise → initiator.

```bash
# Terminal 1: target
./tebench --backend=tent --metadata_type=p2p --xport_type=sunrise_link \
  --seg_type=VRAM --local_gpu_id=0

# Terminal 2: initiator
./tebench --backend=tent --metadata_type=p2p --xport_type=sunrise_link \
  --seg_type=VRAM --target_seg_name=192.168.172.52:168 \
  --local_gpu_id=1 --target_gpu_id=0
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MC_TANGRT_ROOT` | Tang Runtime root directory | `/usr/local/tangrt` |
| `MC_TANGRT_LIB_DIR` | Direct path to `.so` dir. Overrides `MC_TANGRT_ROOT` + arch lookup. | (unset) |
| `MC_TANGRT_LIB_ARCH` | Architecture subdirectory under `<root>/lib/` | `linux-x86_64` |

---

## Configuration Options (TENT)

Sunrise Link behavior can be tuned through config file options:

- `transports/sunrise_link/enable`: enable or disable the transport (default: `true`)
- `transports/sunrise_link/async_memcpy_threshold`: threshold for async memcpy in MiB, 0 to disable (default: `0`)
- `transports/sunrise_link/enable_port_striping`: enable multi-port striping for large cross-device copies >4 MiB (default: `false`)

Tune these options based on workload characteristics and hardware topology.

---

## Important Notes

1. **Device Context**: Internally manages `tangSetDevice`. When calling Tang API directly, set the correct device before querying pointer attributes.
2. **IPC Handle Lifecycle**: IPC handles are cached and released in `uninstall()`.
3. **Concurrent Peer Copy**: Per-device mutexes protect context-bound ops. Cross-device copies acquire source/destination mutexes in fixed order (lower device ID first) to prevent deadlocks.
4. **Mapped Host Pointer Normalization**: `tangHostAlloc` host pointers with corresponding device pointers are auto-normalized for correct peer copy path selection.
