#pragma once

#include <cstdint>
#include <string>

#include "p2p/common/p2p_types.h"
#include "types.h"

namespace mooncake {

struct P2PMasterConfig {
    struct Rpc {
        uint32_t port = 50051;
        uint32_t thread_num = 4;
        std::string address = "0.0.0.0";
        int32_t connection_timeout_seconds = 0;
        bool enable_tcp_no_delay = true;
        uint32_t heartbeat_port = 0;
        uint32_t heartbeat_thread_num = 1;
    } rpc;

    struct Metrics {
        bool enable_reporting = true;
        uint32_t http_port = 9003;
    } metrics;

    struct ClientLifecycle {
        int64_t live_ttl_seconds = DEFAULT_CLIENT_LIVE_TTL_SEC;
        int64_t crashed_ttl_seconds = DEFAULT_CLIENT_CRASHED_TTL_SEC;
    } client_lifecycle;

    struct Routes {
        uint64_t max_clients_per_key = 1;
    } routes;

    struct Redis {
        std::string endpoint;
        std::string username;
        std::string password;
        int db_index = 0;
        int master_view_ttl_seconds = 4;
        int heartbeat_interval_seconds = 1;
    } redis;

    struct OpLog {
        bool enabled = false;
        std::string store_type = "localfs";
        std::string data_dir = "/tmp/mooncake_oplog";
        uint64_t async_queue_max_entries = 100000;
        std::string async_queue_overflow_mode = "reject";
        uint64_t best_effort_max_retries = 3;
    } oplog;

    struct HA {
        bool enabled = false;
        ElectionBackend election_backend = ElectionBackend::ETCD;
        std::string etcd_endpoints;
        uint32_t snapshot_service_port = 0;
        std::string snapshot_service_endpoint;
        std::string snapshot_sources;
        uint32_t snapshot_chunk_size = 256;
    } ha;

    std::string cluster_id = DEFAULT_CLUSTER_ID;
};

}  // namespace mooncake
