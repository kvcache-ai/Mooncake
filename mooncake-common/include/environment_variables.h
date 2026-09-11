#pragma once

#include <cstdint>
#include <string>

#include "environment_variable.h"

namespace mooncake {

#define MC_DEFINE_ENV_VAR(Type, Name) \
    inline static constexpr EnvironmentVariable<Type> Name { #Name }

struct FileStorageEnvironmentVariables {
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_FILE_STORAGE_PATH);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES);
    MC_DEFINE_ENV_VAR(int64_t, MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES);
    MC_DEFINE_ENV_VAR(uint32_t, MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS);
    MC_DEFINE_ENV_VAR(uint32_t,
                      MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS);
    MC_DEFINE_ENV_VAR(uint64_t, MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS);

    // Keep legacy bool/ratio values as strings so their custom parsing and
    // silent invalid-value behavior remain unchanged.
    MC_DEFINE_ENV_VAR(std::string,
                      MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION);
    MC_DEFINE_ENV_VAR(std::string,
                      MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO);
    MC_DEFINE_ENV_VAR(std::string,
                      MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_USE_URING);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_USE_URING);
};

struct FilePerKeyEnvironmentVariables {
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_FSDIR);
    MC_DEFINE_ENV_VAR(bool, MOONCAKE_OFFLOAD_ENABLE_EVICTION);
    MC_DEFINE_ENV_VAR(bool, ENABLE_EVICTION);
};

struct BucketBackendEnvironmentVariables {
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_BUCKET_MAX_TOTAL_SIZE);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_BUCKET_EVICTION_POLICY);
};

struct ClientAutoPortEnvironmentVariables {
    MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_SETUP_RETRIES);
    MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_MIN_PORT);
    MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_MAX_PORT);
};

struct CxlSegmentEnvironmentVariables {
    // Keep the raw string so an unset value remains distinguishable from a
    // present but invalid value, which the legacy path resolves to zero.
    MC_DEFINE_ENV_VAR(std::string, MC_CXL_DEV_SIZE);
};

struct ClientNumaEnvironmentVariables {
    // Keep the raw string to preserve the legacy strtol syntax and warning
    // behavior.
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_NUMA_SOCKET_ID);
};

struct RegisteredPinnedMemoryEnvironmentVariables {
    // Keep the raw string because the legacy parser rejects a leading '+',
    // unlike the shared typed integer parser.
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_PIN_MEMORY_MAX_BYTES);
};

struct MmapArenaEnvironmentVariables {
    // Keep these values as strings to preserve the existing byte-size and
    // canonical-bool parsing, opt-in, fallback, and logging behavior.
    MC_DEFINE_ENV_VAR(std::string, MC_MMAP_ARENA_POOL_SIZE);
    MC_DEFINE_ENV_VAR(std::string, MC_DISABLE_MMAP_ARENA);
};

struct HugepageEnvironmentVariables {
    // Keep these values as strings to preserve presence-based enablement and
    // the existing byte-size parser, fallback, and logging behavior.
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_USE_HUGEPAGE);
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_HUGEPAGE_SIZE);
};

struct LocalHotCacheEnvironmentVariables {
    // Keep these values as strings to preserve their existing per-setting
    // parsing, fallback, and logging behavior.
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_CACHE_SIZE);
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_BLOCK_SIZE);
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_CACHE_USE_SHM);
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD);
};

struct ClientMetricEnvironmentVariables {
    // Keep these values as strings because ClientMetricConfig preserves the
    // existing per-setting fallback and logging behavior.
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_CLIENT_METRIC);
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_CLIENT_METRIC_INTERVAL);
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_CLIENT_METRIC_BANDWIDTH);
};

struct DistributedStorageEnvironmentVariables {
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DFS_ROOT_DIR);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DISTRIBUTED_ROOT_DIR);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DFS_FS_ADAPTER);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DISTRIBUTED_FS_TYPE);
    MC_DEFINE_ENV_VAR(bool, MOONCAKE_DISTRIBUTED_HEALTH_CHECK);
    MC_DEFINE_ENV_VAR(int, MOONCAKE_DFS_SHARD_COUNT);
    MC_DEFINE_ENV_VAR(uint64_t, MOONCAKE_DFS_SHARD_CAPACITY);
    MC_DEFINE_ENV_VAR(uint64_t, MOONCAKE_DFS_ALIGNMENT);
    MC_DEFINE_ENV_VAR(bool, MOONCAKE_DFS_SINGLE_TENANT);
    MC_DEFINE_ENV_VAR(bool, MOONCAKE_DFS_EVICTION_ENABLED);
    MC_DEFINE_ENV_VAR(double, MOONCAKE_DFS_EVICTION_HIGH_WATERMARK);
    MC_DEFINE_ENV_VAR(double, MOONCAKE_DFS_EVICTION_LOW_WATERMARK);
    MC_DEFINE_ENV_VAR(int, MOONCAKE_DFS_DEFERRED_FREE_SECONDS);
    MC_DEFINE_ENV_VAR(int, MOONCAKE_DFS_EVICTION_CHECK_INTERVAL);
};

struct OffsetAllocatorBackendEnvironmentVariables {
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_EVICTION_POLICY);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_HIGH_RATIO);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_LOW_RATIO);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFSET_MAX_CAPACITY_NODES);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_PERSIST_MODE);
    MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS);
    MC_DEFINE_ENV_VAR(bool, MOONCAKE_OFFSET_RECORD_CRC);
};

struct ReplicaSelectionEnvironmentVariables {
    // Only the exact string "1" enables scoring, unlike canonical bool parsing.
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_REPLICA_SCORING);
};

struct RpcTimeoutEnvironmentVariables {
    // Preserve atoll parsing: empty/nonnumeric values become zero, and numeric
    // prefixes are accepted, unlike the shared typed integer parser.
    MC_DEFINE_ENV_VAR(std::string, MC_RPC_TIMEOUT_MS);
    MC_DEFINE_ENV_VAR(std::string, MC_RPC_CONNECT_TIMEOUT_MS);
};

struct RpcProtocolEnvironmentVariables {
    // Preserve the legacy exact, case-sensitive "rdma" token check.
    MC_DEFINE_ENV_VAR(std::string, MC_RPC_PROTOCOL);
};

struct LocalFileSnapshotEnvironmentVariables {
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_SNAPSHOT_LOCAL_PATH);
};

struct TransferSubmitterEnvironmentVariables {
    // Keep the raw string to preserve the legacy token set, whitespace,
    // invalid-value fallback, and warning behavior.
    MC_DEFINE_ENV_VAR(std::string, MC_STORE_MEMCPY);
};

struct NoFRegisterEnvironmentVariables {
    // Keep the raw string to preserve case normalization and warning behavior.
    MC_DEFINE_ENV_VAR(std::string, MC_NOF_TRTYPE);
};

struct SpdkControllerEnvironmentVariables {
    MC_DEFINE_ENV_VAR(uint32_t, MC_NVME_NUM_IO_QUEUES);
    MC_DEFINE_ENV_VAR(uint32_t, MC_NVME_IO_QUEUE_SIZE);
    MC_DEFINE_ENV_VAR(uint32_t, MC_NVME_IO_QUEUE_REQUESTS);
    MC_DEFINE_ENV_VAR(uint8_t, MC_NVME_TRANSPORT_ACK_TIMEOUT);
    MC_DEFINE_ENV_VAR(uint16_t, MC_NVME_ADMIN_QUEUE_SIZE);
    MC_DEFINE_ENV_VAR(uint64_t, MC_NVME_FABRICS_CONNECT_TIMEOUT_US);
    MC_DEFINE_ENV_VAR(bool, MC_NVME_HEADER_DIGEST);
    MC_DEFINE_ENV_VAR(bool, MC_NVME_DATA_DIGEST);
};

struct NvmeKvConnectorEnvironmentVariables {
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_DEVICE_PATH);
    // Keep numeric values as strings because the existing NVMe parser accepts
    // base prefixes, a leading plus, and leading whitespace.
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_NSID);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_QUEUE_DEPTH);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_TRANSPORT);
};

struct ClientAutoDiscoveryEnvironmentVariables {
    // Keep the raw strings to preserve std::stoi prefix acceptance and the
    // distinction between unset and explicitly empty filter values.
    MC_DEFINE_ENV_VAR(std::string, MC_MS_AUTO_DISC);
    MC_DEFINE_ENV_VAR(std::string, MC_MS_FILTERS);
};

struct NvmeKvIoConcurrencyEnvironmentVariables {
    // Keep these values as strings to preserve the existing NVMe unsigned
    // syntax, zero fallback, and silent invalid-value behavior.
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_IO_CONCURRENCY);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY);
    MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_PREPARE_CONCURRENCY);
};

#undef MC_DEFINE_ENV_VAR

}  // namespace mooncake
