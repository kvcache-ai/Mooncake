# Sunrise Link Transport

## Overview
Sunrise Link is a GPU transport backend in the Mooncake TENT transfer framework. It relies on Tang Runtime (`tangrt`) for device memory allocation, pointer attribute queries, peer copy, and IPC handle operations, and is registered as `SUNRISE_LINK` in TENT.

At runtime, `TransferEngineImpl` loads Sunrise Link Transport when `USE_SUNRISE` is enabled at build time and `transports/sunrise_link/enable=true` is set in config.

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

---

## Configuration Options (TENT)

Sunrise Link behavior can be tuned through config file options:

- `transports/sunrise_link/enable`: enable or disable the transport (default: `true`)
- `transports/sunrise_link/async_memcpy_threshold`: threshold for async memcpy (MiB)

Tune these options based on workload characteristics and hardware topology.
