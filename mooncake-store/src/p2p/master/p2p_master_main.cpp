#include <glog/logging.h>

#include "p2p/common/p2p_master_config.h"
#include "p2p/master/p2p_master.h"
#include "types.h"

#include <gflags/gflags.h>

#include <cstdint>
#include <exception>
#include <limits>
#include <string>

#include "default_config.h"
#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"

DEFINE_string(config_path, "", "P2P master config file path");
DEFINE_bool(enable_metric_reporting, true,
            "Enable periodic P2P master metric reporting");
DEFINE_uint32(metrics_port, 9003, "P2P master HTTP metrics port");
DEFINE_uint32(rpc_thread_num, 4, "P2P master RPC server threads");
DEFINE_uint32(rpc_port, 50051, "P2P master RPC server port");
DEFINE_string(rpc_address, "0.0.0.0", "P2P master RPC bind address");
DEFINE_int32(rpc_conn_timeout_seconds, 0,
             "RPC connection timeout in seconds (0 = no timeout)");
DEFINE_bool(rpc_enable_tcp_no_delay, true,
            "Enable TCP_NODELAY for RPC connections");
DEFINE_uint32(heartbeat_rpc_port, 0,
              "Dedicated P2P heartbeat RPC port (0 = main RPC port)");
DEFINE_uint32(heartbeat_rpc_thread_num, 1,
              "Dedicated P2P heartbeat RPC server threads");
DEFINE_int64(client_ttl, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
             "P2P client live timeout in seconds");
DEFINE_int64(client_crashed_ttl, mooncake::DEFAULT_CLIENT_CRASHED_TTL_SEC,
             "P2P client crashed timeout in seconds");
DEFINE_uint64(max_client_per_key, 1,
              "Maximum client owners per key (0 = unlimited)");
DEFINE_string(cluster_id, mooncake::DEFAULT_CLUSTER_ID,
              "P2P master cluster ID");
DEFINE_string(redis_endpoint, "", "Redis endpoint for P2P HA and OpLog");
DEFINE_string(redis_username, "", "Redis ACL username");
DEFINE_string(redis_password, "", "Redis AUTH password");
DEFINE_int32(redis_db_index, 0, "Redis database index");
DEFINE_int32(redis_master_view_ttl_sec, 4, "Redis leader key TTL in seconds");
DEFINE_int32(redis_heartbeat_interval_sec, 1,
             "Redis leader heartbeat interval in seconds");
DEFINE_bool(enable_oplog, false, "Enable P2P metadata OpLog");
DEFINE_string(oplog_store_type, "localfs",
              "P2P OpLog backend: localfs or redis");
DEFINE_string(oplog_data_dir, "/tmp/mooncake_oplog",
              "Local filesystem OpLog root directory");
DEFINE_uint64(oplog_async_queue_max_entries, 100000,
              "Maximum queued asynchronous OpLog entries");
DEFINE_string(oplog_async_queue_overflow_mode, "reject",
              "Asynchronous OpLog overflow mode: reject or bypass");
DEFINE_uint64(oplog_best_effort_max_retries, 3,
              "Maximum best-effort Redis OpLog retries");
DEFINE_bool(enable_ha, false, "Enable P2P master HA");
DEFINE_string(election_backend, "etcd",
              "P2P HA election backend: etcd or redis");
DEFINE_string(etcd_endpoints, "", "Etcd endpoints separated by semicolons");
DEFINE_uint32(standby_snapshot_service_port, 0,
              "Standby snapshot RPC port (0 = disabled)");
DEFINE_string(standby_snapshot_service_endpoint, "",
              "Advertised standby snapshot endpoint override");
DEFINE_string(standby_snapshot_sources, "",
              "Comma-separated standby snapshot source overrides");
DEFINE_uint32(standby_snapshot_chunk_size, 256,
              "Maximum records in a standby snapshot chunk");

namespace mooncake {
namespace {

constexpr int kDefaultRedisPort = 6379;

bool IsCommandLineFlagExplicitlySet(const char* name) {
    google::CommandLineFlagInfo info;
    return google::GetCommandLineFlagInfo(name, &info) && !info.is_default;
}

void LoadConfigFile(const DefaultConfig& source, P2PMasterConfig& config,
                    std::string& election_backend) {
    source.GetBool("enable_metric_reporting", &config.metrics.enable_reporting,
                   config.metrics.enable_reporting);
    source.GetUInt32("metrics_port", &config.metrics.http_port,
                     config.metrics.http_port);
    source.GetUInt32("rpc_port", &config.rpc.port, config.rpc.port);
    source.GetUInt32("rpc_thread_num", &config.rpc.thread_num,
                     config.rpc.thread_num);
    source.GetString("rpc_address", &config.rpc.address, config.rpc.address);
    source.GetInt32("rpc_conn_timeout_seconds",
                    &config.rpc.connection_timeout_seconds,
                    config.rpc.connection_timeout_seconds);
    source.GetBool("rpc_enable_tcp_no_delay", &config.rpc.enable_tcp_no_delay,
                   config.rpc.enable_tcp_no_delay);
    source.GetUInt32("heartbeat_rpc_port", &config.rpc.heartbeat_port,
                     config.rpc.heartbeat_port);
    source.GetUInt32("heartbeat_rpc_thread_num",
                     &config.rpc.heartbeat_thread_num,
                     config.rpc.heartbeat_thread_num);
    source.GetInt64("client_live_ttl_sec",
                    &config.client_lifecycle.live_ttl_seconds,
                    config.client_lifecycle.live_ttl_seconds);
    source.GetInt64("client_crashed_ttl_sec",
                    &config.client_lifecycle.crashed_ttl_seconds, -1);
    source.GetUInt64("max_client_per_key", &config.routes.max_clients_per_key,
                     config.routes.max_clients_per_key);
    source.GetString("cluster_id", &config.cluster_id, config.cluster_id);
    source.GetString("redis_endpoint", &config.redis.endpoint,
                     config.redis.endpoint);
    source.GetString("redis_username", &config.redis.username,
                     config.redis.username);
    source.GetString("redis_password", &config.redis.password,
                     config.redis.password);
    source.GetInt32("redis_db_index", &config.redis.db_index,
                    config.redis.db_index);
    source.GetInt32("redis_master_view_ttl_sec",
                    &config.redis.master_view_ttl_seconds,
                    config.redis.master_view_ttl_seconds);
    source.GetInt32("redis_heartbeat_interval_sec",
                    &config.redis.heartbeat_interval_seconds,
                    config.redis.heartbeat_interval_seconds);
    source.GetBool("enable_oplog", &config.oplog.enabled, config.oplog.enabled);
    source.GetString("oplog_store_type", &config.oplog.store_type,
                     config.oplog.store_type);
    source.GetString("oplog_data_dir", &config.oplog.data_dir,
                     config.oplog.data_dir);
    source.GetUInt64("oplog_async_queue_max_entries",
                     &config.oplog.async_queue_max_entries,
                     config.oplog.async_queue_max_entries);
    source.GetString("oplog_async_queue_overflow_mode",
                     &config.oplog.async_queue_overflow_mode,
                     config.oplog.async_queue_overflow_mode);
    source.GetUInt64("oplog_best_effort_max_retries",
                     &config.oplog.best_effort_max_retries,
                     config.oplog.best_effort_max_retries);
    source.GetBool("enable_ha", &config.ha.enabled, config.ha.enabled);
    source.GetString("election_backend", &election_backend, election_backend);
    source.GetString("etcd_endpoints", &config.ha.etcd_endpoints,
                     config.ha.etcd_endpoints);
    source.GetUInt32("standby_snapshot_service_port",
                     &config.ha.snapshot_service_port,
                     config.ha.snapshot_service_port);
    source.GetString("standby_snapshot_service_endpoint",
                     &config.ha.snapshot_service_endpoint,
                     config.ha.snapshot_service_endpoint);
    source.GetString("standby_snapshot_sources", &config.ha.snapshot_sources,
                     config.ha.snapshot_sources);
    source.GetUInt32("standby_snapshot_chunk_size",
                     &config.ha.snapshot_chunk_size,
                     config.ha.snapshot_chunk_size);
}

void ApplyCommandLineOverrides(P2PMasterConfig& config,
                               std::string& election_backend) {
#define APPLY_OVERRIDE(flag, target)             \
    if (IsCommandLineFlagExplicitlySet(#flag)) { \
        target = FLAGS_##flag;                   \
    }

    APPLY_OVERRIDE(enable_metric_reporting, config.metrics.enable_reporting);
    APPLY_OVERRIDE(metrics_port, config.metrics.http_port);
    APPLY_OVERRIDE(rpc_port, config.rpc.port);
    APPLY_OVERRIDE(rpc_thread_num, config.rpc.thread_num);
    APPLY_OVERRIDE(rpc_address, config.rpc.address);
    APPLY_OVERRIDE(rpc_conn_timeout_seconds,
                   config.rpc.connection_timeout_seconds);
    APPLY_OVERRIDE(rpc_enable_tcp_no_delay, config.rpc.enable_tcp_no_delay);
    APPLY_OVERRIDE(heartbeat_rpc_port, config.rpc.heartbeat_port);
    APPLY_OVERRIDE(heartbeat_rpc_thread_num, config.rpc.heartbeat_thread_num);
    APPLY_OVERRIDE(client_ttl, config.client_lifecycle.live_ttl_seconds);
    APPLY_OVERRIDE(client_crashed_ttl,
                   config.client_lifecycle.crashed_ttl_seconds);
    APPLY_OVERRIDE(max_client_per_key, config.routes.max_clients_per_key);
    APPLY_OVERRIDE(cluster_id, config.cluster_id);
    APPLY_OVERRIDE(redis_endpoint, config.redis.endpoint);
    APPLY_OVERRIDE(redis_username, config.redis.username);
    APPLY_OVERRIDE(redis_password, config.redis.password);
    APPLY_OVERRIDE(redis_db_index, config.redis.db_index);
    APPLY_OVERRIDE(redis_master_view_ttl_sec,
                   config.redis.master_view_ttl_seconds);
    APPLY_OVERRIDE(redis_heartbeat_interval_sec,
                   config.redis.heartbeat_interval_seconds);
    APPLY_OVERRIDE(enable_oplog, config.oplog.enabled);
    APPLY_OVERRIDE(oplog_store_type, config.oplog.store_type);
    APPLY_OVERRIDE(oplog_data_dir, config.oplog.data_dir);
    APPLY_OVERRIDE(oplog_async_queue_max_entries,
                   config.oplog.async_queue_max_entries);
    APPLY_OVERRIDE(oplog_async_queue_overflow_mode,
                   config.oplog.async_queue_overflow_mode);
    APPLY_OVERRIDE(oplog_best_effort_max_retries,
                   config.oplog.best_effort_max_retries);
    APPLY_OVERRIDE(enable_ha, config.ha.enabled);
    APPLY_OVERRIDE(election_backend, election_backend);
    APPLY_OVERRIDE(etcd_endpoints, config.ha.etcd_endpoints);
    APPLY_OVERRIDE(standby_snapshot_service_port,
                   config.ha.snapshot_service_port);
    APPLY_OVERRIDE(standby_snapshot_service_endpoint,
                   config.ha.snapshot_service_endpoint);
    APPLY_OVERRIDE(standby_snapshot_sources, config.ha.snapshot_sources);
    APPLY_OVERRIDE(standby_snapshot_chunk_size, config.ha.snapshot_chunk_size);

#undef APPLY_OVERRIDE
}

void NormalizeRedisEndpoint(std::string& endpoint) {
    if (endpoint.empty()) {
        return;
    }
    if (endpoint.front() == '[') {
        if (endpoint.back() == ']') {
            endpoint += ":" + std::to_string(kDefaultRedisPort);
        }
        return;
    }
    if (endpoint.find(':') == std::string::npos) {
        endpoint += ":" + std::to_string(kDefaultRedisPort);
    }
}

tl::expected<void, std::string> ValidateAndNormalize(
    P2PMasterConfig& config, const std::string& election_backend) {
    NormalizeRedisEndpoint(config.redis.endpoint);

    if (config.client_lifecycle.crashed_ttl_seconds == -1) {
        config.client_lifecycle.crashed_ttl_seconds =
            config.client_lifecycle.live_ttl_seconds * 3;
    }
    if (config.client_lifecycle.crashed_ttl_seconds <
        config.client_lifecycle.live_ttl_seconds) {
        return tl::make_unexpected("client_crashed_ttl must be >= client_ttl");
    }

    if (election_backend == "etcd") {
        config.ha.election_backend = ElectionBackend::ETCD;
    } else if (election_backend == "redis") {
        config.ha.election_backend = ElectionBackend::REDIS;
    } else if (config.ha.enabled) {
        return tl::make_unexpected(
            "election_backend must be 'etcd' or 'redis'");
    }

    if (config.ha.enabled && config.oplog.enabled &&
        (config.ha.snapshot_service_port >
             std::numeric_limits<uint16_t>::max() ||
         config.ha.snapshot_chunk_size == 0 ||
         config.ha.snapshot_chunk_size > kMaxStandbySnapshotChunkSize)) {
        return tl::make_unexpected("invalid standby snapshot configuration");
    }

    if (!config.ha.enabled) {
        return {};
    }
    if (config.ha.election_backend == ElectionBackend::ETCD) {
        if (config.ha.etcd_endpoints.empty()) {
            return tl::make_unexpected(
                "etcd_endpoints is required for etcd HA");
        }
        return {};
    }

#ifndef STORE_USE_REDIS
    return tl::make_unexpected("Redis election requires STORE_USE_REDIS");
#else
    if (config.redis.endpoint.empty()) {
        return tl::make_unexpected("redis_endpoint is required for Redis HA");
    }
    if (config.redis.master_view_ttl_seconds <= 0 ||
        config.redis.heartbeat_interval_seconds <= 0 ||
        config.redis.heartbeat_interval_seconds >=
            config.redis.master_view_ttl_seconds) {
        return tl::make_unexpected(
            "Redis heartbeat interval must be positive and smaller than TTL");
    }
    return {};
#endif
}

}  // namespace

static tl::expected<P2PMasterConfig, std::string> LoadP2PMasterConfig() {
    P2PMasterConfig config;
    config.client_lifecycle.crashed_ttl_seconds = -1;
    std::string election_backend = "etcd";

    if (!FLAGS_config_path.empty()) {
        DefaultConfig source;
        source.SetPath(FLAGS_config_path);
        try {
            source.Load();
        } catch (const std::exception& error) {
            return tl::make_unexpected("failed to load P2P master config: " +
                                       std::string(error.what()));
        }
        LoadConfigFile(source, config, election_backend);
    }

    ApplyCommandLineOverrides(config, election_backend);
    auto validated = ValidateAndNormalize(config, election_backend);
    if (!validated) {
        return tl::make_unexpected(validated.error());
    }
    return config;
}

}  // namespace mooncake

int main(int argc, char* argv[]) {
    mooncake::init_ylt_log_level();
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }

    auto config = mooncake::LoadP2PMasterConfig();
    if (!config) {
        LOG(ERROR) << config.error();
        return 1;
    }
    return mooncake::P2PMaster(config.value()).Run();
}
