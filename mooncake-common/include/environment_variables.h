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

#undef MC_DEFINE_ENV_VAR

}  // namespace mooncake
