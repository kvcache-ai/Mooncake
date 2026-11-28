# Kunpeng UB.MEM Transport for Mooncake

This document describes how to build and use Mooncake with the `ubmem`
transport, which carries Transfer Engine traffic over OBMM
shared-memory devices.

## Overview

The `ubmem` transport runs on the same Kunpeng UB interconnect as the
[UB (URMA) transport](kunpeng_ub_transport.md), but uses OBMM
(Ownership Based Memory Management) instead of the URMA stack. OBMM
is a kernel memory management system for supernode environments,
supporting cross-node physical memory sharing through a kernel module
(`obmm.ko`) and a user-space library (`libobmm.so`). Open-source
repository: https://atomgit.com/openeuler/obmm

- **ub (URMA)**: device-to-device DMA through the URMA stack.
- **ubmem**: the owner's segment and the initiator's local buffer are
  placed in OBMM shared-memory devices (`/dev/obmm_shmdev*`). A transfer
  is a CPU memory copy between device mappings. Same-node access uses
  the local pool device; cross-node access goes through a local mirror
  device that exposes the peer node's memory at the same offsets.

## Prerequisites

- **Hardware Platform**: Kunpeng platform with native UB interconnect
  architecture
- **OS Version**: openEuler 24.03 LTS SP3 (the same base as the UB
  transport)
- **OBMM**: deployed per its repository documentation
  (https://atomgit.com/openeuler/obmm), with shared-memory devices
  exposed as `/dev/obmm_shmdev*`
- No additional SDK: OBMM device nodes are sufficient; the transport
  does not require `libobmm`

Verify that the devices are present:

```bash
ls /dev/obmm_shmdev*
```

Each node needs one local device for its pool. Cross-node access
additionally needs a mirror device imported from the peer node by the
OBMM deployment; which device nodes are local and which are mirrors is
a property of the deployment. A mirror device exposes the peer
device's memory at identical offsets.

## Building Mooncake with UB.MEM Support

```bash
mkdir build && cd build
cmake .. -DUSE_UBMEM=ON -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

## Configuration

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `MC_UBMEM_DEV_PATH` | (required) | Local pool device path, e.g. `/dev/obmm_shmdev1`. |
| `MC_UBMEM_DEV_SIZE` | auto-detected | Device size in bytes. Required in Mooncake Store mode, where the device is partitioned between segment and local buffer. |
| `MC_UBMEM_PEER_DEV_MAP` | (empty) | Cross-node mirror resolution: `peerhost:peerdev=localdev,...`. `peerhost` is compared against the host part of segment names. |
| `MC_UBMEM_PEER_DEV_MAP_FILE` | (empty) | Cluster-wide JSON mirror topology shared by all nodes; each node keeps the entries that name it as a reader. Ignored when `MC_UBMEM_PEER_DEV_MAP` is set. |
| `MC_UBMEM_INLINE_MEMCPY` | `1` | Copy engine: `1` uses `memcpy`; `0` uses a software load pipeline tuned for single-threaded reads from a mirror device. |
| `MC_UBMEM_COPY_THREADS` | `8` | Workers of the parallel copy pool for large (>= 1 MiB) copies. `0` disables the pool. |
| `MC_UBMEM_FORCE_O_SYNC` | off | Diagnostic switch: map all devices strongly ordered (`O_SYNC`). |

## Usage

### Mooncake Store

Start the store client with `--protocol=ubmem`. The device is
partitioned between the two memory regions the store registers:

- the owner's segment starts at offset 0 and spans
  `[0, global_segment_size)`
- the local buffer (`local_buffer_size`) occupies the end of the
  device: `[device_size - local_buffer_size, device_size)`

`MC_UBMEM_DEV_SIZE` must be set to the full device size and cover both
regions. An owner-only process uses `local_buffer_size=0`; a pure
reader uses `global_segment_size=0`.

```bash
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev2
export MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))

mooncake_client --host=127.0.0.1 \
  --global_segment_size=4GB \
  --master_server_address=localhost:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ubmem --port=50052
```

### Cross-Node

The peer's segment is accessed through a local mirror device. Configure
the mapping on the initiator side:

```bash
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev3
export MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))
export MC_UBMEM_PEER_DEV_MAP="192.168.1.161:/dev/obmm_shmdev2=/dev/obmm_shmdev10"
```

With this mapping, data published by host `192.168.1.161` in
`/dev/obmm_shmdev2` is read through local `/dev/obmm_shmdev10` at the
same offsets. No master-side or owner-side change is required for
cross-node operation.

For clusters, the mirror topology can be placed in a JSON file and
shared by all nodes:

```bash
export MC_UBMEM_PEER_DEV_MAP_FILE=/etc/mooncake/ubmem_peer_dev_map.json
```

```json
{
  "192.168.1.161": {
    "/dev/obmm_shmdev2": {
      "192.168.1.162": "/dev/obmm_shmdev10",
      "192.168.1.163": "/dev/obmm_shmdev6"
    }
  },
  "192.168.1.162": {
    "/dev/obmm_shmdev3": {
      "192.168.1.161": "/dev/obmm_shmdev11"
    }
  }
}
```

Each entry names a source host, one of its devices, and for each reader
host the local mirror device through which it accesses that device.
Every node loads the same file and keeps the entries that name it as a
reader: host `192.168.1.162` reads `192.168.1.161`'s `shmdev2` through
its local `shmdev10`, host `192.168.1.163` reads the same device
through its local `shmdev6`, and host `192.168.1.161` reads
`192.168.1.162`'s `shmdev3` through its local `shmdev11`. A node
identifies itself by the host part of its server name, falling back to
the system hostname. A node that only exports devices never appears as
a reader and simply has no mirror mappings — the same file still
serves it.

## Unit Tests

```bash
# start an HTTP metadata server first (port 8080), then:
./build/mooncake-transfer-engine/tests/ubmem_transport_test \
    --ubmem_device=/dev/obmm_shmdev5
```

| Mode | Description |
|------|-------------|
| `gtest` (default) | Write/read-back byte-exact verification on a local device, plus out-of-bounds rejection. |
| `target` | Host a segment on `--ubmem_device`, wait `--verify_seconds`, then verify the data written by the initiator. |
| `initiator` | Open the target's segment (`--target_server`) and write to it through the local mirror (`--peer_dev_map`). |

The gtest mode only needs a running metadata server: when `--ubmem_device`
does not exist, the fixture backs it with a sparse file and removes it
afterwards, so the test is registered with CTest and needs no hardware.
Passing an existing device, as above, runs the same cases on real hardware.
The cross-node `target` and `initiator` modes still need a real mirror pair.

## Technical Details

- **Device model**: the transport owns its segment (the same model as
  `cxl`): each device is mapped whole once, and transfers address it by
  offset. The published segment descriptor carries the owner's mapping
  base so peers can convert absolute addresses into device offsets.
- **Completion semantics**: transfers complete synchronously inline;
  there is no asynchronous completion path.
- **Mirror devices**: mapped strongly ordered (`O_SYNC`); reads bypass
  local caches.
- **Resolve cache**: segment id to device mapping is resolved once per
  segment via the metadata service and cached per transport instance;
  subsequent requests hit the cache without locks.
- **Copy pool**: copies of at least 1 MiB are split across
  `MC_UBMEM_COPY_THREADS` workers plus the submitting thread; `copy()`
  returns only after the copy completes.

## Troubleshooting

### Mirror device fails to open

```
UbmemTransport: cannot open device /dev/obmm_shmdev10 (Permission denied)
```

Solution: the device path configured for this peer is not accessible on
this node. Check that the path (`localdev` in `MC_UBMEM_PEER_DEV_MAP`,
or this host's row in `MC_UBMEM_PEER_DEV_MAP_FILE`) actually names a
mirror device imported by the OBMM deployment on this node.

### `no mirror configured for peer ...`

The initiator found no mirror entry for the peer host and device. Add
`peerhost:peerdev=localdev` to `MC_UBMEM_PEER_DEV_MAP`, or the matching
entry to this host's row in `MC_UBMEM_PEER_DEV_MAP_FILE`.

### `MC_UBMEM_DEV_SIZE not set (ubmem store mode)`

Store mode partitions the device and requires the full device size.
Set `MC_UBMEM_DEV_SIZE` to the size of the device.

### `ubmem device partition invalid`

`global_segment_size + local_buffer_size` exceeds `MC_UBMEM_DEV_SIZE`.
Reduce either region or point `MC_UBMEM_DEV_PATH` at a larger device.

## Conclusion

The `ubmem` transport carries Transfer Engine traffic over OBMM
shared-memory devices on Kunpeng UB platforms: transfers are CPU copies
between device mappings, same-node through the local pool device and
cross-node through mirror devices. It complements the `ub` (URMA)
transport on the same interconnect and requires no SDK beyond the OBMM
device nodes themselves.
