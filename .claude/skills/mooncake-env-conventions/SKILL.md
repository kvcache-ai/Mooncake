---
name: mooncake-env-conventions
description: Reference for all Mooncake environment variables and their conventions. Use when configuring Mooncake, writing deployment scripts, debugging configuration issues, or when any MC_* environment variable is mentioned.
---

# Mooncake Environment Variables

## Naming Convention

All Mooncake env vars use the `MC_` prefix. When adding a new env var:
1. Use `MC_` prefix
2. Use SCREAMING_SNAKE_CASE
3. Document in this file AND in `mooncake-common/` config parsing code
4. Provide a sensible default

## Core Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_FORCE_TCP` | `false` | Force TCP transport instead of RDMA |
| `MC_GID_INDEX` | `3` | RDMA GID index for RoCE networks |
| `MC_IB_PORT` | `1` | InfiniBand port number |
| `MC_LOG_LEVEL` | `INFO` | Log level: `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `MC_LOG_DIR` | `""` | Directory for log files (empty = stderr) |
| `MC_METADATA_SERVER` | (required) | Metadata server URI, e.g. `etcd://host:2379/prefix` |
| `MC_USE_IPV6` | `false` | Use IPv6 for networking |

## Transfer Engine Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_SLICE_SIZE` | `65536` | Transfer slice size in bytes |
| `MC_NUM_QP_PER_EP` | `2` | Queue pairs per endpoint |
| `MC_MAX_WR` | `256` | Max work requests |
| `MC_MAX_SGE` | `4` | Max scatter-gather elements |
| `MC_WORKERS_PER_CTX` | `2` | Worker threads per RDMA context |
| `MC_RETRY_CNT` | `9` | RDMA retry count |
| `MC_MTU` | `4096` | Maximum transfer unit |
| `MC_FORCE_HCA` | `false` | Force HCA device selection |
| `MC_FORCE_MNNVL` | `false` | Force Multi-Node NVLink |
| `MC_INTRA_NVLINK` | `false` | Use intra-node NVLink |
| `MC_PATH_ROUNDROBIN` | `false` | Enable path round-robin |
| `MC_ENABLE_PARALLEL_REG_MR` | `-1` | Parallel memory registration (-1 = auto) |
| `MC_EFA_CQ_THREADS` | `1` | EFA completion queue threads |

## Advanced / Tuning Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_IB_TC` | `-1` | Traffic class (-1 = default) |
| `MC_IB_PCI_RELAXED_ORDERING` | `0` | PCI relaxed ordering |
| `MC_MAX_CQE_PER_CTX` | `4096` | Max CQ entries per context |
| `MC_MAX_EP_PER_CTX` | `65536` | Max endpoints per context |
| `MC_DISABLE_METACACHE` | `false` | Disable metadata caching |
| `MC_FRAGMENT_RATIO` | `4` | Memory fragment ratio |
| `MC_ENABLE_DEST_DEVICE_AFFINITY` | `false` | Enable destination device affinity |
| `MC_MIN_RPC_PORT` | `15000` | Minimum RPC port range |
| `MC_MAX_RPC_PORT` | `17000` | Maximum RPC port range |
| `MC_ENDPOINT_STORE_TYPE` | `SIEVE` | Endpoint store eviction policy |
| `MC_REDIS_PASSWORD` | `""` | Redis password (if using Redis metadata) |
| `MC_REDIS_DB_INDEX` | `0` | Redis database index |
| `WITH_NVIDIA_PEERMEM` | `true` | Use NVIDIA peer memory |

## Adding New Environment Variables

When adding a new env var, update these locations:
1. `mooncake-common/src/environ.cpp` — config parsing code
2. `mooncake-common/include/environ.h` — header declaration
3. This skill file — for AI agent reference
4. `docs/source/` — user-facing documentation
5. Relevant test scripts — verify default behavior
