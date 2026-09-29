#pragma once

// Environment variables read only by the Mooncake Store client process.
// Variables that the master also reads are declared in
// mooncake-common/src/environment_variables.h.

#include <cstdint>
#include <string>

#include "environment_variable.h"

namespace mooncake {

struct ClientEnvironmentVariables {
    struct AutoPort {
        MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_SETUP_RETRIES);
        MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_MIN_PORT);
        MC_DEFINE_ENV_VAR(int, MC_STORE_CLIENT_MAX_PORT);
    };
    struct AutoDiscovery {
        // Keep the raw strings to preserve std::stoi prefix acceptance and the
        // distinction between unset and explicitly empty filter values.
        MC_DEFINE_ENV_VAR(std::string, MC_MS_AUTO_DISC);
        MC_DEFINE_ENV_VAR(std::string, MC_MS_FILTERS);
    };
    struct HostIdentity {
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_HOST_ID);
    };
    struct Metric {
        // Keep these values as strings because ClientMetricConfig preserves the
        // existing per-setting fallback and logging behavior.
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_CLIENT_METRIC);
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_CLIENT_METRIC_INTERVAL);
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_CLIENT_METRIC_BANDWIDTH);
    };
    struct Numa {
        // Keep the raw string to preserve the legacy strtol syntax and warning
        // behavior.
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_NUMA_SOCKET_ID);
    };
    struct ObjectChecksum {
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_STORE_CHECKSUM);
    };
    struct Hugepage {
        // Keep these values as strings to preserve presence-based enablement
        // and the existing byte-size parser, fallback, and logging behavior.
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_USE_HUGEPAGE);
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_HUGEPAGE_SIZE);
    };
    struct MmapArena {
        // Keep these values as strings to preserve the existing byte-size and
        // canonical-bool parsing, opt-in, fallback, and logging behavior.
        MC_DEFINE_ENV_VAR(std::string, MC_MMAP_ARENA_POOL_SIZE);
        MC_DEFINE_ENV_VAR(std::string, MC_DISABLE_MMAP_ARENA);
    };
    struct PinnedMemory {
        // Keep the raw string because the legacy parser rejects a leading '+',
        // unlike the shared typed integer parser.
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_PIN_MEMORY_MAX_BYTES);
    };
    struct LocalHotCache {
        // Keep these values as strings to preserve their existing per-setting
        // parsing, fallback, and logging behavior.
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_CACHE_SIZE);
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_BLOCK_SIZE);
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_CACHE_USE_SHM);
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD);
    };
    struct ReplicaSelection {
        // Only the exact string "1" enables scoring, unlike canonical bool
        // parsing.
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_REPLICA_SCORING);
    };
    struct RpcTimeout {
        // Preserve atoll parsing: empty/nonnumeric values become zero, and
        // numeric prefixes are accepted, unlike the shared typed integer
        // parser.
        MC_DEFINE_ENV_VAR(std::string, MC_RPC_TIMEOUT_MS);
        MC_DEFINE_ENV_VAR(std::string, MC_RPC_CONNECT_TIMEOUT_MS);
    };
    struct TransferSubmitter {
        // Keep the raw string to preserve the legacy token set, whitespace,
        // invalid-value fallback, and warning behavior.
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_MEMCPY);
    };
    struct FilereadWorkerPool {
        // Preserve the raw value in invalid-value warnings.
        MC_DEFINE_ENV_VAR(std::string, MC_FILEREAD_WORKERS);
    };
    struct ShmSpdkRegistration {
        // The registration opt-in is enabled only by the exact string "1".
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_REGISTER_SPDK);
    };
    struct Offload {
        struct FileStorage {
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_FILE_STORAGE_PATH);
            MC_DEFINE_ENV_VAR(int64_t,
                              MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES);
            MC_DEFINE_ENV_VAR(int64_t,
                              MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES);
            MC_DEFINE_ENV_VAR(int64_t,
                              MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT);
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT);
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT);
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES);
            MC_DEFINE_ENV_VAR(uint32_t,
                              MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS);
            MC_DEFINE_ENV_VAR(
                uint32_t, MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS);
            MC_DEFINE_ENV_VAR(uint64_t,
                              MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS);

            // Keep legacy bool/ratio values as strings so their custom parsing
            // and silent invalid-value behavior remain unchanged.
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION);
            MC_DEFINE_ENV_VAR(
                std::string,
                MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO);
            MC_DEFINE_ENV_VAR(
                std::string,
                MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_USE_URING);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_USE_URING);
        };
        struct Bucket {
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT);
            MC_DEFINE_ENV_VAR(int64_t,
                              MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES);
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE);
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_BUCKET_MAX_TOTAL_SIZE);
            MC_DEFINE_ENV_VAR(int64_t,
                              MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES);
            MC_DEFINE_ENV_VAR(int64_t,
                              MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_BUCKET_EVICTION_POLICY);
        };
        struct FilePerKey {
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFLOAD_FSDIR);
            MC_DEFINE_ENV_VAR(bool, MOONCAKE_OFFLOAD_ENABLE_EVICTION);
            MC_DEFINE_ENV_VAR(bool, ENABLE_EVICTION);
        };
        struct OffsetAllocator {
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_EVICTION_POLICY);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_HIGH_RATIO);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_LOW_RATIO);
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFSET_MAX_CAPACITY_NODES);
            MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OFFSET_PERSIST_MODE);
            MC_DEFINE_ENV_VAR(int64_t,
                              MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS);
            MC_DEFINE_ENV_VAR(bool, MOONCAKE_OFFSET_RECORD_CRC);
        };
        struct Oss {
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OSS_ENDPOINT);
            MC_DEFINE_ENV_VAR(std::string, OSS_ENDPOINT);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OSS_BUCKET);
            MC_DEFINE_ENV_VAR(std::string, OSS_BUCKET);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OSS_REGION);
            MC_DEFINE_ENV_VAR(std::string, OSS_REGION);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OSS_ACCESS_KEY_ID);
            MC_DEFINE_ENV_VAR(std::string, OSS_ACCESS_KEY_ID);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OSS_ACCESS_KEY_SECRET);
            MC_DEFINE_ENV_VAR(std::string, OSS_ACCESS_KEY_SECRET);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_OSS_SECURITY_TOKEN);
            MC_DEFINE_ENV_VAR(std::string, OSS_SESSION_TOKEN);
            MC_DEFINE_ENV_VAR(bool, MOONCAKE_OSS_PATH_STYLE);
            MC_DEFINE_ENV_VAR(bool, MOONCAKE_OSS_ANONYMOUS);
            MC_DEFINE_ENV_VAR(int, MOONCAKE_OSS_MAX_CONNECTIONS);
            MC_DEFINE_ENV_VAR(int, MOONCAKE_OSS_RECEIVE_BUFFER_SIZE);
            MC_DEFINE_ENV_VAR(int, MOONCAKE_OSS_UPLOAD_BUFFER_SIZE);
        };
    };
    struct NvmeKv {
        struct Connector {
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_DEVICE_PATH);
            // Keep numeric values as strings because the existing NVMe parser
            // accepts base prefixes, a leading plus, and leading whitespace.
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_NSID);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_QUEUE_DEPTH);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_TRANSPORT);
        };
        struct Executor {
            // Keep these values as strings because the executor parser accepts
            // base prefixes, a leading plus, and leading whitespace.
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE);
        };
        struct IoConcurrency {
            // Keep these values as strings to preserve the existing NVMe
            // unsigned syntax, zero fallback, and silent invalid-value
            // behavior.
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY);
            MC_DEFINE_ENV_VAR(std::string, MOONCAKE_NVME_KV_IO_CONCURRENCY);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY);
            MC_DEFINE_ENV_VAR(std::string,
                              MOONCAKE_NVME_KV_PREPARE_CONCURRENCY);
        };
    };
    struct NoF {
        struct Register {
            // Keep the raw string to preserve case normalization and warning
            // behavior.
            MC_DEFINE_ENV_VAR(std::string, MC_NOF_TRTYPE);
        };
        struct WorkerPool {
            // Preserve the raw value in invalid-value warnings.
            MC_DEFINE_ENV_VAR(std::string, MC_NOF_WORKERS);
        };
        struct Debug {
            // Preserve the legacy case-insensitive true tokens and strtol
            // syntax; neither parser trims trailing whitespace.
            MC_DEFINE_ENV_VAR(std::string, MC_NOF_DEBUG);
            MC_DEFINE_ENV_VAR(std::string, MC_NOF_DEBUG_INTERVAL_MS);
        };
    };
    struct CxlSegment {
        // Keep the raw string so an unset value remains distinguishable from a
        // present but invalid value, which the legacy path resolves to zero.
        MC_DEFINE_ENV_VAR(std::string, MC_CXL_DEV_SIZE);
    };
};

}  // namespace mooncake
