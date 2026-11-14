#pragma once

#include <fstream>
#include <sstream>
#include <json/json.h>
#include <ylt/util/tl/expected.hpp>
#include "client_config_builder.h"
#include "p2p/common/p2p_types.h"

namespace mooncake {

enum class LocalTransferMode {
    MEMCPY = 0,
    TE = 1,
};

struct P2PClientConfig : RealClientConfigBase {
    // Dedicated master heartbeat port; must match --heartbeat_rpc_port.
    // Zero sends heartbeats to the main RPC port.
    uint16_t heartbeat_rpc_port = 0;

    // Port for HTTP server.
    // Only used when enable_http_server is true.
    uint16_t http_port = 9003;

    // Whether to enable HTTP server.
    bool enable_http_server = true;

    // P2P metrics; centralized metrics use native environment controls.
    bool enable_metric_collection = true;
    uint64_t metric_report_interval_seconds = 60;

    // Redis election backend configuration.
    // Only used when master_server_entry starts with "redis://".
    std::string redis_cluster_id = DEFAULT_CLUSTER_ID;
    std::string redis_username;
    std::string redis_password;
    int redis_db_index = 0;
    int redis_master_view_ttl_sec = 4;  // Redis master_view TTL in seconds.
    // Master-side Redis master_view refresh interval. Must be less than
    // redis_master_view_ttl_sec.
    int redis_heartbeat_interval_sec = 1;

    // Skip Master discovery/registration/heartbeat until an explicit /register.
    // Startup-only policy; master_server_entry is retained for the later join.
    bool start_local_only = false;

    // Parsed runtime read/write config JSON.
    // Loaded from file path, inline JSON string, or env MC_RUNTIME_CONFIG
    Json::Value runtime_config_json;

    // Port for P2P RPC service.
    uint16_t client_rpc_port = 12345;

    // Num threads for P2P RPC service.
    uint32_t rpc_thread_num = 2;

    // Parsed custom tiered backend configuration
    Json::Value tiered_backend_config;

    // Select the local data-plane implementation through the tier JSON.
    std::string data_manager_version = "v1";

    // Number of TieredBackend metadata index shards (and matching DataManager
    // pending-write/pinned-key lease shards). Higher values reduce contention.
    size_t lock_shard_count = 1024;

    // RouteCache is disabled by default
    // each size of route entry is about 240B:
    // Aligned Node(64B) + hash_bucket(8B) + Key(assume 64B)
    // + P2PRouteData(each item is 96B and count is 8B)
    size_t route_cache_max_memory_bytes = 0;  // 0 disables RouteCache.
    uint64_t route_cache_ttl_ms = 60 * 1000;  // 1min

    // Async route notification.
    // async_sender_thread_count > 0 enables async notifier.
    // async_route_queue_size controls queue capacity
    // (minimum async_max_batch_size * async_sender_thread_count).
    size_t async_sender_thread_count = 4;
    size_t async_max_batch_size = 2000;
    size_t async_route_queue_size = 0;

    // Local transfer mode for P2P local Get/Put path.
    // - MEMCPY: copy through local CPU memory path
    // - TE: transfer through local TransferEngine path
    LocalTransferMode local_transfer_mode = LocalTransferMode::TE;

    // Local I/O workers: both modes in V2, MEMCPY only in V1.
    // 0 defers local I/O until the caller waits on the task.
    size_t local_io_worker_num = 32;

    // Size of the dedicated coro_io pool for TE wait coroutines
    // (poll getTransferStatus, yield via sleep_for) in DataManager: local TE
    // Put/Get and remote forward TE (co_await) paths. Independent of
    // local_transfer_mode. 0 means synchronous TE wait on the caller thread.
    size_t te_async_poll_worker_num = 32;

    // Business coroutine pool, independent of TE polling.
    // 0 selects hardware_concurrency(), with a minimum of one worker.
    size_t business_coro_worker_num = 32;

    // PreWrite / PinKey key lease: maximum time (ms) a key may stay in
    // intermediate (lease-protected) state before expiring.
    static constexpr uint32_t kP2pDefaultKeyLeaseDurationMs = 5000;
    // Interval (ms) for the background scanner that removes expired leases.
    static constexpr uint32_t kP2pDefaultKeyLeaseScanIntervalMs = 1000;
    uint32_t p2p_key_lease_duration_ms = kP2pDefaultKeyLeaseDurationMs;
    uint32_t p2p_key_lease_scan_interval_ms = kP2pDefaultKeyLeaseScanIntervalMs;

    // Cross-node transfer direction (reverse = owner TE, forward = accessor
    // TE). Configured at client startup only.
    TransferDirectionMode transfer_direction_mode =
        TransferDirectionMode::REVERSE;
};

class P2PClientConfigBuilder : private ClientConfigBuilder {
   public:
    static constexpr const char* kDefaultTieredBackendConfigPath =
        "conf/tiered_backend.json";

    static tl::expected<P2PClientConfig, ErrorCode> build_p2p_real_client(
        const std::string& local_hostname,
        const std::string& metadata_connstring,
        const std::string& protocol = "tcp",
        const std::optional<std::string>& rdma_devices = std::nullopt,
        const std::string& master_server_entry = "127.0.0.1:50051",
        const std::string& tiered_backend_config_json =
            kDefaultTieredBackendConfigPath,
        uint64_t local_buffer_size = 0,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        const std::string& ipc_socket_path = "",
        uint16_t client_rpc_port = 12345, uint32_t rpc_thread_num = 2,
        size_t lock_shard_count = 1024, size_t route_cache_max_memory_bytes = 0,
        uint64_t route_cache_ttl_ms = 60 * 1000,
        const std::string& local_transfer_mode = "te",
        size_t local_io_worker_num = 32, uint16_t http_port = 9003,
        bool enable_http_server = true,
        const std::map<std::string, std::string>& labels = {},
        size_t async_sender_thread_count = 4,
        size_t async_max_batch_size = 2000, size_t async_route_queue_size = 0,
        uint32_t p2p_key_lease_duration_ms = 0,
        uint32_t p2p_key_lease_scan_interval_ms = 0,
        const std::string& p2p_transfer_direction_mode = "reverse",
        const std::string& runtime_config = "",
        bool enable_metric_collection = true,
        uint64_t metric_report_interval_seconds = 60,
        const std::string& redis_cluster_id = DEFAULT_CLUSTER_ID,
        const std::string& redis_password = "", int redis_db_index = 0,
        int redis_master_view_ttl_sec = 4, int redis_heartbeat_interval_sec = 1,
        const std::string& redis_username = "", uint16_t heartbeat_rpc_port = 0,
        size_t te_async_poll_worker_num = 32, bool start_local_only = false,
        size_t business_coro_worker_num = 32) {
        P2PClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, labels);
        config.http_port = http_port;
        config.enable_http_server = enable_http_server;
        config.enable_metric_collection = enable_metric_collection;
        config.metric_report_interval_seconds = metric_report_interval_seconds;
        const std::string rc_source =
            ResolveRuntimeConfigSource(runtime_config);
        if (!rc_source.empty()) {
            config.runtime_config_json = LoadJsonConfig(rc_source);
            if (config.runtime_config_json.isNull() ||
                !config.runtime_config_json.isObject()) {
                LOG(ERROR) << "P2P runtime configuration must be a JSON object";
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
        }
        fill_redis_discovery_config(config, redis_cluster_id, redis_password,
                                    redis_db_index, redis_master_view_ttl_sec,
                                    redis_heartbeat_interval_sec,
                                    redis_username);
        config.client_rpc_port = client_rpc_port;
        config.rpc_thread_num = rpc_thread_num;
        config.lock_shard_count = lock_shard_count;
        config.route_cache_max_memory_bytes = route_cache_max_memory_bytes;
        config.route_cache_ttl_ms = route_cache_ttl_ms;
        auto parsed_local_mode =
            parse_p2p_local_transfer_mode(local_transfer_mode);
        if (!parsed_local_mode) {
            LOG(ERROR) << "Failed to parse P2P local_transfer_mode";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.local_transfer_mode = *parsed_local_mode;
        config.local_io_worker_num = local_io_worker_num;
        config.te_async_poll_worker_num = te_async_poll_worker_num;
        config.business_coro_worker_num = business_coro_worker_num;
        config.start_local_only = start_local_only;
        config.async_sender_thread_count = async_sender_thread_count;
        config.async_max_batch_size = async_max_batch_size;
        config.async_route_queue_size = async_route_queue_size;

        Json::Value tiered_config = LoadJsonConfig(tiered_backend_config_json);

        if (!tiered_config.isObject() || !tiered_config.isMember("tiers") ||
            tiered_config["tiers"].empty()) {
            LOG(ERROR) << "P2P tiered backend configuration requires tiers";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.tiered_backend_config = tiered_config;

        if (tiered_config.isMember("data_manager_version") &&
            tiered_config["data_manager_version"].isString()) {
            config.data_manager_version =
                tiered_config["data_manager_version"].asString();
        }

        if (p2p_key_lease_duration_ms > 0) {
            config.p2p_key_lease_duration_ms = p2p_key_lease_duration_ms;
        }
        if (p2p_key_lease_scan_interval_ms > 0) {
            config.p2p_key_lease_scan_interval_ms =
                p2p_key_lease_scan_interval_ms;
        }
        auto parsed_direction =
            parse_p2p_transfer_direction_mode(p2p_transfer_direction_mode);
        if (!parsed_direction) {
            LOG(ERROR) << "Failed to parse p2p_transfer_direction_mode";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.transfer_direction_mode = *parsed_direction;

        config.heartbeat_rpc_port = heartbeat_rpc_port;
        return config;
    }

    static tl::expected<P2PClientConfig, ErrorCode> build_p2p_real_client(
        const std::unordered_map<std::string, std::string>& config) {
        std::string local_hostname =
            get_config_str(config, DictCommon::kLocalHostname);
        std::string metadata_server =
            get_config_str(config, DictCommon::kMetadataServer);
        std::string protocol = get_config_str(config, DictCommon::kProtocol,
                                              DictCommon::kDefaultProtocol);
        std::string rdma_devices_str =
            get_config_str(config, DictCommon::kRdmaDevices);
        std::optional<std::string> rdma_devices =
            rdma_devices_str.empty()
                ? std::nullopt
                : std::optional<std::string>(rdma_devices_str);
        std::string master_server_addr =
            get_config_str(config, DictCommon::kMasterServerAddr,
                           DictCommon::kDefaultMasterServerAddr);
        std::string tiered_backend_config =
            get_config_str(config, DictP2P::kTieredBackendConfig,
                           kDefaultTieredBackendConfigPath);
        auto local_buffer_size =
            get_config_size(config, DictCommon::kLocalBufferSize,
                            DictCommon::kDefaultLocalBufferSize);
        std::string ipc_socket_path =
            get_config_str(config, DictCommon::kIpcSocketPath);
        auto client_rpc_port = get_config_integer<uint16_t>(
            config, DictP2P::kClientRpcPort, DictP2P::kDefaultClientRpcPort);
        auto rpc_thread_num = get_config_integer<uint32_t>(
            config, DictP2P::kRpcThreadNum, DictP2P::kDefaultRpcThreadNum);
        auto lock_shard_count = get_config_integer<size_t>(
            config, DictP2P::kLockShardCount, DictP2P::kDefaultLockShardCount);
        auto route_cache_max_memory =
            get_config_size(config, DictP2P::kRouteCacheMaxMemoryBytes,
                            DictP2P::kDefaultRouteCacheMaxMemoryBytes);
        auto route_cache_ttl_ms =
            get_config_integer<uint64_t>(config, DictP2P::kRouteCacheTtlMs,
                                         DictP2P::kDefaultRouteCacheTtlMs);
        std::string local_transfer_mode =
            get_config_str(config, DictP2P::kLocalTransferMode,
                           DictP2P::kDefaultLocalTransferMode);
        auto local_io_worker_num =
            get_config_integer<size_t>(config, DictP2P::kLocalIoWorkerNum,
                                       DictP2P::kDefaultLocalIoWorkerNum);
        auto te_async_poll_worker_num =
            get_config_integer<size_t>(config, DictP2P::kTeAsyncPollWorkerNum,
                                       DictP2P::kDefaultTeAsyncPollWorkerNum);
        auto business_coro_worker_num =
            get_config_integer<size_t>(config, DictP2P::kBusinessCoroWorkerNum,
                                       DictP2P::kDefaultBusinessCoroWorkerNum);
        auto async_sender_thread_count =
            get_config_integer<size_t>(config, DictP2P::kAsyncSenderThreadCount,
                                       DictP2P::kDefaultAsyncSenderThreadCount);
        auto async_max_batch_size =
            get_config_integer<size_t>(config, DictP2P::kAsyncMaxBatchSize,
                                       DictP2P::kDefaultAsyncMaxBatchSize);
        auto async_route_queue_size =
            get_config_integer<size_t>(config, DictP2P::kAsyncRouteQueueSize,
                                       DictP2P::kDefaultAsyncRouteQueueSize);
        std::string runtime_config =
            get_config_str(config, DictP2P::kRuntimeConfig);
        bool enable_metric_collection =
            get_config_bool(config, DictP2P::kEnableMetricCollection,
                            DictP2P::kDefaultEnableMetricCollection);
        auto metric_report_interval_seconds = get_config_integer<uint64_t>(
            config, DictP2P::kMetricReportIntervalSeconds,
            DictP2P::kDefaultMetricReportIntervalSeconds);
        auto redis_config = get_redis_discovery_config(config);
        auto heartbeat_rpc_port =
            get_config_integer<uint16_t>(config, DictP2P::kHeartbeatRpcPort,
                                         DictP2P::kDefaultHeartbeatRpcPort);
        auto http_port =
            get_config_integer<uint16_t>(config, "http_port", 9003);
        bool enable_http_server =
            get_config_bool(config, "enable_http_server", true);
        auto key_lease_duration_ms = get_config_integer<uint32_t>(
            config, "p2p_key_lease_duration_ms", 0);
        auto key_lease_scan_interval_ms = get_config_integer<uint32_t>(
            config, "p2p_key_lease_scan_interval_ms", 0);
        auto transfer_direction_mode =
            get_config_str(config, "p2p_transfer_direction_mode", "reverse");

        if (!local_buffer_size || !client_rpc_port || !rpc_thread_num ||
            !lock_shard_count || !route_cache_max_memory ||
            !route_cache_ttl_ms || !local_io_worker_num ||
            !te_async_poll_worker_num || !business_coro_worker_num ||
            !async_sender_thread_count || !async_max_batch_size ||
            !async_route_queue_size || !metric_report_interval_seconds ||
            !heartbeat_rpc_port || !http_port || !key_lease_duration_ms ||
            !key_lease_scan_interval_ms || !redis_config) {
            LOG(ERROR) << "Failed to parse P2P numeric configuration";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto result = build_p2p_real_client(
            local_hostname, metadata_server, protocol, rdma_devices,
            master_server_addr, tiered_backend_config, *local_buffer_size,
            nullptr, ipc_socket_path, *client_rpc_port, *rpc_thread_num,
            *lock_shard_count, *route_cache_max_memory, *route_cache_ttl_ms,
            local_transfer_mode, *local_io_worker_num, *http_port,
            enable_http_server, {}, *async_sender_thread_count,
            *async_max_batch_size, *async_route_queue_size,
            *key_lease_duration_ms, *key_lease_scan_interval_ms,
            transfer_direction_mode, runtime_config, enable_metric_collection,
            *metric_report_interval_seconds, redis_config->cluster_id,
            redis_config->password, redis_config->db_index,
            redis_config->master_view_ttl_sec,
            redis_config->heartbeat_interval_sec, redis_config->username,
            *heartbeat_rpc_port, *te_async_poll_worker_num,
            get_config_bool(config, DictP2P::kStartLocalOnly, false),
            *business_coro_worker_num);
        if (!result) {
            LOG(ERROR) << "P2P dict configuration is invalid: "
                       << result.error();
        }
        return result;
    }

   private:
    static std::string ResolveRuntimeConfigSource(
        const std::string& runtime_config);

    struct RedisDiscoveryConfig {
        std::string cluster_id = DEFAULT_CLUSTER_ID;
        std::string username;
        std::string password;
        int db_index = 0;
        int master_view_ttl_sec = 4;
        int heartbeat_interval_sec = 1;
    };

    struct DictP2P {
        // Keys
        static constexpr const char* kStartLocalOnly = "start_local_only";
        static constexpr const char* kTieredBackendConfig =
            "tiered_backend_config";
        static constexpr const char* kClientRpcPort = "client_rpc_port";
        static constexpr const char* kRpcThreadNum = "rpc_thread_num";
        static constexpr const char* kLockShardCount = "lock_shard_count";
        static constexpr const char* kRouteCacheMaxMemoryBytes =
            "route_cache_max_memory_bytes";
        static constexpr const char* kRouteCacheTtlMs = "route_cache_ttl_ms";
        static constexpr const char* kLocalTransferMode = "local_transfer_mode";
        static constexpr const char* kLocalIoWorkerNum = "local_io_worker_num";
        static constexpr const char* kTeAsyncPollWorkerNum =
            "te_async_poll_worker_num";
        static constexpr const char* kBusinessCoroWorkerNum =
            "business_coro_worker_num";
        static constexpr const char* kAsyncSenderThreadCount =
            "async_sender_thread_count";
        static constexpr const char* kAsyncMaxBatchSize =
            "async_max_batch_size";
        static constexpr const char* kAsyncRouteQueueSize =
            "async_route_queue_size";
        // Defaults
        static constexpr uint16_t kDefaultClientRpcPort = 12345;
        static constexpr uint32_t kDefaultRpcThreadNum = 2;
        static constexpr size_t kDefaultLockShardCount = 1024;
        static constexpr size_t kDefaultRouteCacheMaxMemoryBytes = 0;
        static constexpr uint64_t kDefaultRouteCacheTtlMs = 1ULL * 60 * 1000;
        static constexpr const char* kDefaultLocalTransferMode = "te";
        static constexpr size_t kDefaultLocalIoWorkerNum = 32;
        static constexpr size_t kDefaultTeAsyncPollWorkerNum = 32;
        static constexpr size_t kDefaultBusinessCoroWorkerNum = 32;
        static constexpr size_t kDefaultAsyncSenderThreadCount = 4;
        static constexpr size_t kDefaultAsyncMaxBatchSize = 2000;
        static constexpr size_t kDefaultAsyncRouteQueueSize = 0;

        static constexpr const char* kRuntimeConfig = "runtime_config";
        static constexpr const char* kRedisClusterId = "redis_cluster_id";
        static constexpr const char* kRedisUsername = "redis_username";
        static constexpr const char* kRedisPassword = "redis_password";
        static constexpr const char* kRedisDbIndex = "redis_db_index";
        static constexpr const char* kRedisMasterViewTtlSec =
            "redis_master_view_ttl_sec";
        static constexpr const char* kRedisHeartbeatIntervalSec =
            "redis_heartbeat_interval_sec";
        static constexpr const char* kMetricReportIntervalSeconds =
            "metric_report_interval_seconds";
        static constexpr const char* kEnableMetricCollection =
            "enable_metric_collection";
        static constexpr const char* kHeartbeatRpcPort = "heartbeat_rpc_port";
        static constexpr uint64_t kDefaultMetricReportIntervalSeconds = 60;
        static constexpr bool kDefaultEnableMetricCollection = true;
        static constexpr uint16_t kDefaultHeartbeatRpcPort = 0;
    };

    static tl::expected<RedisDiscoveryConfig, ErrorCode>
    get_redis_discovery_config(
        const std::unordered_map<std::string, std::string>& config) {
        RedisDiscoveryConfig redis_config;
        redis_config.cluster_id = get_config_str(
            config, DictP2P::kRedisClusterId, DEFAULT_CLUSTER_ID);
        redis_config.username = get_config_str(config, DictP2P::kRedisUsername);
        redis_config.password = get_config_str(config, DictP2P::kRedisPassword);
        auto db_index = get_config_integer<int>(config, DictP2P::kRedisDbIndex,
                                                redis_config.db_index);
        auto view_ttl =
            get_config_integer<int>(config, DictP2P::kRedisMasterViewTtlSec,
                                    redis_config.master_view_ttl_sec);
        auto heartbeat_interval =
            get_config_integer<int>(config, DictP2P::kRedisHeartbeatIntervalSec,
                                    redis_config.heartbeat_interval_sec);
        if (!db_index || !view_ttl || !heartbeat_interval) {
            LOG(ERROR)
                << "Failed to parse Redis discovery integer configuration";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        redis_config.db_index = *db_index;
        redis_config.master_view_ttl_sec = *view_ttl;
        redis_config.heartbeat_interval_sec = *heartbeat_interval;
        return redis_config;
    }

    static void fill_redis_discovery_config(P2PClientConfig& config,
                                            const std::string& redis_cluster_id,
                                            const std::string& redis_password,
                                            int redis_db_index,
                                            int redis_master_view_ttl_sec,
                                            int redis_heartbeat_interval_sec,
                                            const std::string& redis_username) {
        config.redis_cluster_id = redis_cluster_id;
        config.redis_username = redis_username;
        config.redis_password = redis_password;
        config.redis_db_index = redis_db_index;
        config.redis_master_view_ttl_sec = redis_master_view_ttl_sec;
        config.redis_heartbeat_interval_sec = redis_heartbeat_interval_sec;
    }

    static Json::Value LoadJsonConfig(const std::string& json_or_path) {
        Json::Value config;
        std::string json_content;

        // Determine if input is a JSON string or a file path
        std::string trimmed = json_or_path;
        size_t start = trimmed.find_first_not_of(" \t\n\r");
        if (start != std::string::npos) {
            trimmed = trimmed.substr(start);
        }

        if (!trimmed.empty() && trimmed[0] == '{') {
            // Treat as JSON string
            json_content = json_or_path;
        } else {
            // Treat as file path
            std::ifstream file(json_or_path);
            if (!file.is_open()) {
                LOG(ERROR) << "Failed to open P2P config file: "
                           << json_or_path;
                return Json::Value{};
            }
            std::ostringstream ss;
            ss << file.rdbuf();
            json_content = ss.str();
        }

        // Parse JSON
        Json::CharReaderBuilder builder;
        auto reader =
            std::unique_ptr<Json::CharReader>(builder.newCharReader());
        std::string errors;
        if (!reader->parse(json_content.data(),
                           json_content.data() + json_content.length(), &config,
                           &errors)) {
            LOG(ERROR) << "Failed to parse JSON config: " << errors;
            return Json::Value{};
        }
        return config;
    }

    static std::optional<LocalTransferMode> parse_p2p_local_transfer_mode(
        std::string mode) {
        std::transform(
            mode.begin(), mode.end(), mode.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (mode == "memcpy") {
            return LocalTransferMode::MEMCPY;
        }
        if (mode == "te") {
            return LocalTransferMode::TE;
        }
        LOG(ERROR) << "Invalid p2p local transfer mode: " << mode
                   << ". Expected 'memcpy' or 'te'.";
        return std::nullopt;
    }

    static std::optional<TransferDirectionMode>
    parse_p2p_transfer_direction_mode(std::string mode) {
        std::transform(
            mode.begin(), mode.end(), mode.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (mode == "reverse") {
            return TransferDirectionMode::REVERSE;
        }
        if (mode == "forward") {
            return TransferDirectionMode::FORWARD;
        }
        LOG(ERROR) << "Invalid p2p transfer direction mode: " << mode;
        return std::nullopt;
    }
};

}  // namespace mooncake
