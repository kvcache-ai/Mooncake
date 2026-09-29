#pragma once

// Environment variables read by both Mooncake Store processes (client and
// master). Variables read by only one of them are declared in
// mooncake-store/src/config/{client,master}_environment_variables.h.

#include <cstdint>
#include <string>

#include "environment_variable.h"

namespace mooncake {

struct CommonEnvironmentVariables {
    struct Rpc {
        // Preserve the legacy exact, case-sensitive "rdma" token check.
        MC_DEFINE_ENV_VAR(std::string, MC_RPC_PROTOCOL);
        MC_DEFINE_ENV_VAR(int, MC_RPC_CLIENT_IO_THREADS);
        MC_DEFINE_ENV_VAR(int, MC_STORE_RPC_CLIENT_IO_THREADS);
        MC_DEFINE_ENV_VAR(int, MC_TE_RPC_CLIENT_IO_THREADS);
    };
    struct DistributedStorage {
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DFS_ROOT_DIR);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DISTRIBUTED_ROOT_DIR);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DFS_FS_ADAPTER);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DISTRIBUTED_FS_TYPE);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_DFS_ALLOCATOR);
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_DISTRIBUTED_HEALTH_CHECK);
        MC_DEFINE_ENV_VAR(int, MOONCAKE_DFS_SHARD_COUNT);
        MC_DEFINE_ENV_VAR(uint64_t, MOONCAKE_DFS_SHARD_CAPACITY);
        MC_DEFINE_ENV_VAR(uint64_t, MOONCAKE_DFS_BUCKET_CAPACITY);
        MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_DFS_MAX_BUCKET_COUNT);
        MC_DEFINE_ENV_VAR(uint64_t, MOONCAKE_DFS_ALIGNMENT);
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_DFS_SINGLE_TENANT);
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_DFS_EVICTION_ENABLED);
        MC_DEFINE_ENV_VAR(double, MOONCAKE_DFS_EVICTION_HIGH_WATERMARK);
        MC_DEFINE_ENV_VAR(double, MOONCAKE_DFS_EVICTION_LOW_WATERMARK);
        MC_DEFINE_ENV_VAR(int, MOONCAKE_DFS_DEFERRED_FREE_SECONDS);
        MC_DEFINE_ENV_VAR(int, MOONCAKE_DFS_EVICTION_CHECK_INTERVAL);
    };
    struct ClusterIdentity {
        MC_DEFINE_ENV_VAR(std::string, MC_STORE_CLUSTER_ID);
    };
    struct SpdkController {
        MC_DEFINE_ENV_VAR(uint32_t, MC_NVME_NUM_IO_QUEUES);
        MC_DEFINE_ENV_VAR(uint32_t, MC_NVME_IO_QUEUE_SIZE);
        MC_DEFINE_ENV_VAR(uint32_t, MC_NVME_IO_QUEUE_REQUESTS);
        MC_DEFINE_ENV_VAR(uint8_t, MC_NVME_TRANSPORT_ACK_TIMEOUT);
        MC_DEFINE_ENV_VAR(uint16_t, MC_NVME_ADMIN_QUEUE_SIZE);
        MC_DEFINE_ENV_VAR(uint64_t, MC_NVME_FABRICS_CONNECT_TIMEOUT_US);
        MC_DEFINE_ENV_VAR(bool, MC_NVME_HEADER_DIGEST);
        MC_DEFINE_ENV_VAR(bool, MC_NVME_DATA_DIGEST);
    };
    struct Redis {
        // Keep the DB index as a string so an explicitly empty value remains
        // distinguishable from a nonempty malformed value.
        MC_DEFINE_ENV_VAR(std::string, MC_REDIS_DB_INDEX);
        MC_DEFINE_ENV_VAR(std::string, MC_REDIS_USERNAME);
        MC_DEFINE_ENV_VAR(std::string, MC_REDIS_PASSWORD);
    };
};

}  // namespace mooncake
