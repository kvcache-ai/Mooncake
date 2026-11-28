# NDS Transport

## Background

TENT currently lacks a direct transfer path between storage media and Ascend
NPU HBM. Routing storage I/O through host memory and page cache incurs extra
data copies, CPU overhead, and latency. NDS (NPU Direct Storage) Transport
provides a direct DMA read/write path between storage media and Ascend HBM
through the public `ascend_nds.h` / `libascend_nds` C interface.

## Overview

NDS Transport lives in `mooncake-transfer-engine/tent/src/transport/nds` and
provides direct storage reads and writes in TENT through the third-party
`ascend_nds.h` / `libascend_nds.so` C interface.

Key capabilities:

- **Standard TENT integration**: Reuses existing `Request`, memory registration,
  File Segment, batch submission, and status polling APIs. Requires both the
  `USE_NDS` build option and the `transports/nds/enable` runtime setting.
- **Default HBM memory registration**: Registering NPU HBM via
  `registerLocalMemory` automatically pins buffers with `ndsFileBufRegister`
  (reference-counted process-wide) and submits batches with
  `NDS_FILE_IO_REGISTERED_MEM`.
- **Multi-iovec coalescing and rolling submission**: Adjacent requests in the
  same submission targeting the same file and opcode with contiguous offsets are
  coalesced into a single I/O entry within `io_bytes` and `NDS_FILE_MAX_IOV`,
  and refilled continuously within `io_batch_depth`.
- **Safe DMA fencing and selection**: In-flight DMA is drained before reporting
  failures or releasing buffers. Default file selection follows
  `GDS -> NDS -> IOURING`, skipping unavailable candidates.

> Scope: NDS handles direct storage transfers; Ascend Direct handles NPU-to-NPU memory transfers.

## Prerequisites

### Hardware and software

| Component | Requirement |
| --- | --- |
| Host & driver | Compatible OS, Kernel, CANN, HDK, and matching kernel modules (`.ko`); see [UBSIO NDS SDK](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds) for supported hardware models and version constraints |
| Storage | A regular file with initialized extents on a filesystem supported by NDS |
| NDS SDK | [UBSIO](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds) with the vector batch API in `ascend_nds.h` and matching `libascend_nds.so` |
| Mooncake dependencies | Follow the [build guide](../../../getting_started/build.md), including dependencies for the selected Ascend platform |

- Use the header and shared library from the same SDK build, matching the loaded kernel module (`.ko`) version.

### Installing the NDS SDK

Install the UBSIO NDS SDK and configure its driver following the
[UBSIO NDS SDK guide](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds),
then point `NDS_ROOT` to the installed prefix and verify readability:

```bash
export NDS_ROOT=/path/to/ubsio-nds/dist
test -r "$NDS_ROOT/include/ascend_nds.h"
test -r "$NDS_ROOT/lib/libascend_nds.so"
```

## Building

Run from the Mooncake repository root in the target Python environment:

```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
export NDS_INSTALL_PREFIX="$HOME/.local/mooncake-nds"

cmake -S . -B build-nds -G Ninja \
  -DUSE_TENT=ON -DUSE_NDS=ON -DNDS_ROOT="$NDS_ROOT" \
  -DUSE_ASCEND_DIRECT=ON -DUSE_CUDA=OFF \
  -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF \
  -DBUILD_UNIT_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF \
  -DTENT_METRICS_ENABLED=ON -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$NDS_INSTALL_PREFIX"
cmake --build build-nds --parallel 2 --target tent_shared tentpy tent_cli
cmake --install build-nds/mooncake-common/src
cmake --install build-nds/mooncake-transfer-engine/tent
```

| CMake option | Default | Purpose |
| --- | --- | --- |
| `USE_TENT` | `OFF` | Build TENT; required when `USE_NDS=ON` |
| `USE_NDS` | `OFF` | Build and link the NDS transport |
| `NDS_ROOT` | Unset | SDK prefix containing `include/` and `lib/` or `lib64/` |
| `NDS_INCLUDE_DIR` / `NDS_LIBRARY` | Auto-detected | Explicit header directory and library path (overrides `NDS_ROOT`) |
| `TENT_METRICS_ENABLED` | `OFF` | Compile TENT metrics support |

Build notes:

- `USE_ASCEND_DIRECT=ON` enables the Ascend platform memory probe (requiring its
  CANN/ADXL/HIXL dependencies). `USE_NDS=ON` alone does not enable the Ascend
  probe; `ascend_direct` can remain disabled at runtime.
- When selecting Python explicitly, set both `PYTHON_EXECUTABLE` and
  `Python3_EXECUTABLE` to the application's interpreter.
- The native install provides `libtent_shared.so` and the `tent` diagnostic
  CLI. C/C++ consumers include headers from
  `mooncake-transfer-engine/tent/include`; Python applications use the binding
  built under `build-nds/mooncake-transfer-engine/tent/src/python` (no separate
  NDS wheel is required).

## Environment Variables

| Variable | Required | Description |
| --- | --- | --- |
| `LD_LIBRARY_PATH` | When libraries are outside loader search paths | TENT, NDS SDK, and CANN library directories |
| `PYTHONPATH` | For Python bindings | Directory containing the built `tent` module |
| `MC_TENT_CONF` | For application configuration | TENT JSON file path or inline JSON |
| `MC_USE_TENT` | For the classic TE compatibility entry | Set to `1`; not needed when calling `tent.TransferEngine()` directly |

```bash
export LD_LIBRARY_PATH="$NDS_INSTALL_PREFIX/lib:$NDS_ROOT/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="$PWD/build-nds/mooncake-transfer-engine/tent/src/python:${PYTHONPATH:-}"
export MC_TENT_CONF=/absolute/path/nds-tent.json
```

### TENT configuration file

Save the following as `nds-tent.json` to pin NPU direct storage I/O to NDS:

```json
{
  "metadata_type": "p2p",
  "rpc_server_hostname": "127.0.0.1",
  "rpc_server_port": 0,
  "merge_requests": false,
  "transports": {
    "tcp": {"enable": false},
    "rdma": {"enable": false},
    "shm": {"enable": false},
    "io_uring": {"enable": false},
    "ascend_direct": {"enable": false},
    "nds": {
      "enable": true,
      "io_batch_depth": 128,
      "io_bytes": 16777216
    }
  },
  "policy": [
    {
      "name": "nds_storage",
      "segment_type": "file",
      "transports": ["nds"]
    }
  ]
}
```

| NDS setting | Default | Range and description |
| --- | --- | --- |
| `enable` | `false` | Enable NDS transport in this engine |
| `io_batch_depth` | `128` | Maximum in-flight I/O entries per batch (`1`–`1024`) |
| `io_bytes` | `16777216` (16 MiB) | Maximum bytes per coalesced/sliced I/O entry, 4 KiB aligned (`4 KiB`–`64 MiB`) |

> Settings take effect when creating `TransferEngine`; recreate the engine after
> changing them. NPU HBM buffers are registered automatically via
> `ndsFileBufRegister` with no extra config switch.

## Verification

### Installation check

```bash
"$NDS_INSTALL_PREFIX/bin/tent" diagnostics version --json
python3 -c 'import tent, torch, torch_npu; print(tent.__file__); print(torch.npu.is_available())'
```

Verify Python loads `tent` from the expected build directory and exposes
`tent.TransportType.NDS`. `engine.available()` reflects overall engine state;
confirm NDS initialization and data transfer through application I/O tests.

## Important Notes

1. **4 KiB alignment and bounds**: HBM addresses, transfer lengths, and storage
   offsets must be 4 KiB (4096 B) aligned. Every fragment must lie within
   registered HBM, and each request must fit within both the File Segment and
   actual file size.
2. **File preparation and ownership**: Write and `fsync` the full target region
   before NDS access. `fallocate` or `truncate` alone leaves unwritten extents
   that can pass an NDS round trip while POSIX reads return zeros. Avoid
   truncation, hole punching, concurrent buffered I/O, and overlapping writes;
   use `O_DIRECT` for cross-checks to bypass the page cache.
3. **Buffer and session lifetime**: Keep HBM allocations valid until completion
   or successful unregister. Error cleanup and unregister drain in-flight DMA;
   retry a failed unregister before releasing the buffer. Close any
   application-owned NDS driver session before installing `NdsTransport`.
4. **Failure handling and failover**: A short SDK submission may have queued a
   prefix. NDS drains all submitted DMA before reporting failure; partial writes
   are not rolled back. Reads and writes follow the existing file-transport
   failover policy unless restricted to `["nds"]`.
5. **Observability**: Enable `metrics/enabled` in a metrics-enabled build to
   inspect `transport="nds"` request, byte, failure, and attempt counters. Use
   TENT's `log_level` for diagnostics.

## Troubleshooting

| Symptom | Check or action |
| --- | --- |
| CMake reports a missing NDS SDK | Verify `NDS_ROOT`, or set `NDS_INCLUDE_DIR` and `NDS_LIBRARY` explicitly |
| `import tent` fails or loads another build | Check Python version, `PYTHONPATH`, `tent.__file__`, and shared-library paths |
| `ndsFileDriverOpen` fails | Check SDK/driver compatibility and NDS device permissions |
| NDS is unavailable or registration/selection fails | Check `USE_NDS` and `transports/nds/enable`, Ascend platform build, selected NPU, memory location, 4 KiB alignment, and selection policy |
| Transfer fails or completes fewer bytes | Inspect transfer status, file size, submitted range, and driver logs; keep buffers alive until drained |
| Small-I/O bandwidth is low | Verify data integrity first, then check fragment layout, multi-iovec coalescing, batch size, `io_batch_depth`, and device load |

## See Also

- [UBSIO NDS SDK](https://gitcode.com/openeuler/ubs-io/tree/develop/ubsio-nds)
