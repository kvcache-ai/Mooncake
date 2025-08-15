#pragma once

#include <cstdint>
#include <string>

namespace mooncake {

struct MasterConfig {
    bool enable_gc;
    bool enable_metric_reporting;
    uint32_t metrics_port;
    uint32_t rpc_port;
    uint32_t rpc_thread_num;
    std::string rpc_address;
    int32_t rpc_conn_timeout_seconds;
    bool rpc_enable_tcp_no_delay;

    uint64_t default_kv_lease_ttl;
    uint64_t default_kv_soft_pin_ttl;
    bool allow_evict_soft_pinned_objects;
    double eviction_ratio;
    double eviction_high_watermark_ratio;
    int64_t client_live_ttl_sec;

    bool enable_ha;
    std::string etcd_endpoints;

    std::string cluster_id;
    std::string root_fs_dir;
    std::string memory_allocator;
};

}  // namespace mooncake