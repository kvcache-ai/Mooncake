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

struct ClientAutoPortEnvironmentVariables {
    MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_SETUP_RETRIES);
    MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_MIN_PORT);
    MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_MAX_PORT);
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

#undef MC_DEFINE_ENV_VAR

}  // namespace mooncake
