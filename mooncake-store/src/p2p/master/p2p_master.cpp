#include "p2p/master/p2p_master.h"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "p2p/ha/oplog/p2p_hot_standby_service.h"
#include "p2p/ha/p2p_master_view.h"
#include "p2p/master/p2p_rpc_service.h"
#include "utils.h"
#ifdef STORE_USE_REDIS
#include "p2p/ha/oplog/redis_oplog_store.h"
#endif

namespace mooncake {
namespace {

std::unique_ptr<coro_rpc::coro_rpc_server> CreateRpcServer(
    const P2PMasterConfig& config) {
    auto server = std::make_unique<coro_rpc::coro_rpc_server>(
        config.rpc.thread_num, config.rpc.port, config.rpc.address,
        std::chrono::seconds(config.rpc.connection_timeout_seconds),
        config.rpc.enable_tcp_no_delay);
    const char* protocol = std::getenv("MC_RPC_PROTOCOL");
    if (protocol && std::string_view(protocol) == "rdma") {
        server->init_ibv();
    }
    return server;
}

#ifdef STORE_USE_REDIS
std::string GenerateMasterInstanceId() {
    std::random_device random;
    std::mt19937_64 generator(random());
    std::ostringstream stream;
    stream << std::hex << std::setfill('0') << std::setw(16) << generator()
           << std::setw(16) << generator();
    return stream.str();
}

std::string BuildSnapshotEndpoint(const std::string& master_endpoint,
                                  uint32_t snapshot_port,
                                  const std::string& override_endpoint) {
    if (!override_endpoint.empty()) {
        return override_endpoint;
    }
    if (snapshot_port == 0 || master_endpoint.empty()) {
        return "";
    }
    std::string host;
    if (master_endpoint.front() == '[') {
        const auto closing_bracket = master_endpoint.find(']');
        if (closing_bracket == std::string::npos) {
            return "";
        }
        host = master_endpoint.substr(0, closing_bracket + 1);
    } else {
        const auto separator = master_endpoint.rfind(':');
        host = separator == std::string::npos
                   ? master_endpoint
                   : master_endpoint.substr(0, separator);
    }
    if (host.empty() || host == "0.0.0.0" || host == "[::]") {
        return "";
    }
    return host + ":" + std::to_string(snapshot_port);
}

class ScopedStandbyRegistryProviders final {
   public:
    ScopedStandbyRegistryProviders(
        RedisMasterRegistryHeartbeat& registry_heartbeat,
        P2PHotStandbyService& standby)
        : registry_heartbeat_(&registry_heartbeat) {
        registry_heartbeat_->SetStandbyProviders(
            [&standby] { return standby.GetLatestAppliedSequenceId(); },
            [&standby] { return standby.IsReadyForSnapshot(); });
    }

    ~ScopedStandbyRegistryProviders() { Reset(); }

    ScopedStandbyRegistryProviders(
        const ScopedStandbyRegistryProviders&) = delete;
    ScopedStandbyRegistryProviders& operator=(
        const ScopedStandbyRegistryProviders&) = delete;

    void Reset() {
        if (registry_heartbeat_ == nullptr) {
            return;
        }
        registry_heartbeat_->ClearStandbyProviders();
        registry_heartbeat_ = nullptr;
    }

   private:
    RedisMasterRegistryHeartbeat* registry_heartbeat_;
};
#endif

std::unique_ptr<P2PMasterRpcService> CreateActiveService(
    const P2PMasterConfig& config, ViewVersionId view_version) {
    auto service =
        std::make_unique<P2PMasterRpcService>(config, view_version);
    service->init();
    return service;
}

int RunActiveRpcServers(const P2PMasterConfig& config,
                        coro_rpc::coro_rpc_server& server,
                        P2PMasterRpcService& p2p_service,
                        std::function<void()> before_start = {}) {
    const bool dedicated_heartbeat = config.rpc.heartbeat_port > 0;
    RegisterP2PRpcService(server, p2p_service,
                          /*include_heartbeat=*/!dedicated_heartbeat);
    if (before_start) {
        before_start();
    }

    std::optional<coro_rpc::coro_rpc_server> heartbeat_server;
    if (dedicated_heartbeat) {
        heartbeat_server.emplace(
            std::max<uint32_t>(1u, config.rpc.heartbeat_thread_num),
            config.rpc.heartbeat_port, config.rpc.address,
            std::chrono::seconds(config.rpc.connection_timeout_seconds),
            config.rpc.enable_tcp_no_delay);
        RegisterP2PHeartbeatRpcService(*heartbeat_server, p2p_service);
        auto heartbeat_result = heartbeat_server->async_start();
        if (heartbeat_result.hasResult()) {
            LOG(ERROR) << "Failed to start heartbeat RPC server: "
                       << heartbeat_result.result().value();
            return -1;
        }
    }

    auto server_result = server.async_start();
    if (server_result.hasResult()) {
        LOG(ERROR) << "Failed to start P2P master RPC server: "
                   << server_result.result().value();
        heartbeat_server.reset();
        return -1;
    }
    auto error = std::move(server_result).get();
    LOG(ERROR) << "P2P master RPC server stopped: " << error;
    heartbeat_server.reset();
    return 0;
}

}  // namespace

int P2PMaster::RunStandalone() {
    auto server = CreateRpcServer(config_);
    auto active_service = CreateActiveService(config_, /*view_version=*/0);
    return RunActiveRpcServers(config_, *server, *active_service);
}

int P2PMaster::RunWithHA() {
    const std::string local_endpoint =
        config_.rpc.address + ":" + std::to_string(config_.rpc.port);
    while (true) {
        auto server = CreateRpcServer(config_);
        std::unique_ptr<P2PMasterView> master_view;
        ErrorCode connect_result = ErrorCode::INVALID_PARAMS;
        if (config_.ha.election_backend == ElectionBackend::REDIS) {
#ifdef STORE_USE_REDIS
            auto redis_view = std::make_unique<P2PRedisMasterView>(
                config_.cluster_id, config_.redis.endpoint,
                config_.redis.password, config_.redis.db_index,
                config_.redis.master_view_ttl_seconds,
                config_.redis.heartbeat_interval_seconds,
                config_.redis.username);
            connect_result = redis_view->Connect();
            master_view = std::move(redis_view);
#else
            LOG(ERROR) << "Redis election requires STORE_USE_REDIS";
            return -1;
#endif
        } else {
            auto etcd_view =
                std::make_unique<P2PEtcdMasterView>(config_.cluster_id);
            connect_result = etcd_view->Connect(config_.ha.etcd_endpoints);
            master_view = std::move(etcd_view);
        }
        if (connect_result != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize P2P master view: "
                       << toString(connect_result);
            return -1;
        }

#ifdef STORE_USE_REDIS
        std::unique_ptr<RedisMasterRegistryHeartbeat> master_registry_heartbeat;
        std::string master_instance_id;
        if (config_.oplog.enabled &&
            config_.ha.election_backend == ElectionBackend::REDIS) {
            master_instance_id = GenerateMasterInstanceId();
            RedisMasterRegistryEntry entry;
            entry.instance_id = master_instance_id;
            entry.master_endpoint = local_endpoint;
            entry.snapshot_endpoint = BuildSnapshotEndpoint(
                local_endpoint, config_.ha.snapshot_service_port,
                config_.ha.snapshot_service_endpoint);
            entry.role = "starting";
            entry.snapshot_ready = false;
            master_registry_heartbeat =
                std::make_unique<RedisMasterRegistryHeartbeat>(
                    std::make_unique<RedisMasterRegistry>(
                        config_.cluster_id, config_.redis.endpoint,
                        config_.redis.username, config_.redis.password,
                        config_.redis.db_index),
                    std::move(entry));
            if (master_registry_heartbeat->Start() != ErrorCode::OK) {
                LOG(WARNING) << "Initial Redis master registration failed; "
                                "heartbeat will retry";
            }
        }
#endif

        std::unique_ptr<P2PHotStandbyService> standby;
#ifdef STORE_USE_REDIS
        std::unique_ptr<ScopedStandbyRegistryProviders>
            standby_registry_providers;
#endif
        if (config_.oplog.enabled) {
            P2PHotStandbyConfig standby_config;
            standby_config.cluster_id = config_.cluster_id;
            standby_config.oplog_store_type =
                ParseOpLogStoreType(config_.oplog.store_type);
            standby_config.oplog_store_root_dir = config_.oplog.data_dir;
            standby_config.redis_endpoint = config_.redis.endpoint;
            standby_config.redis_username = config_.redis.username;
            standby_config.redis_password = config_.redis.password;
            standby_config.redis_db_index = config_.redis.db_index;
            standby_config.snapshot_service_port =
                static_cast<uint16_t>(config_.ha.snapshot_service_port);
#ifdef STORE_USE_REDIS
            standby_config.master_instance_id = master_instance_id;
#endif
            standby_config.snapshot_chunk_size = config_.ha.snapshot_chunk_size;
            if (!config_.ha.snapshot_sources.empty()) {
                standby_config.snapshot_source_endpoints = splitString(
                    config_.ha.snapshot_sources, ',', /*trim=*/true);
            }
            standby = std::make_unique<P2PHotStandbyService>(standby_config);
            auto start_result = standby->Start();
            if (start_result != ErrorCode::OK) {
                LOG(ERROR) << "Failed to start P2P standby: "
                           << toString(start_result);
                return -1;
            }
#ifdef STORE_USE_REDIS
            if (master_registry_heartbeat) {
                standby_registry_providers =
                    std::make_unique<ScopedStandbyRegistryProviders>(
                        *master_registry_heartbeat, *standby);
                master_registry_heartbeat->UpdateRole(
                    "standby", standby->IsReadyForSnapshot());
            }
#endif
        }

        EtcdLeaseId lease_id = 0;
        ViewVersionId view_version = 0;
        master_view->ElectLeader(local_endpoint, view_version, lease_id);
        auto keep_leader_thread =
            std::thread([server = server.get(), view = master_view.get(),
                         lease_id] {
                view->KeepLeader(lease_id);
                server->stop();
            });
        std::this_thread::sleep_for(
            std::chrono::seconds(master_view->GetLeaderLeaseTTLSeconds()));

#ifdef STORE_USE_REDIS
        if (master_registry_heartbeat) {
            master_registry_heartbeat->UpdateRole("promoting", false);
        }
#endif

        std::optional<P2PStandbyMetadataStore::ExportedMetadata>
            promoted_metadata;
        uint64_t promoted_sequence_id = 0;
        if (standby) {
            auto promote_result = standby->Promote();
            if (promote_result != ErrorCode::OK) {
                LOG(ERROR) << "Failed to promote P2P standby: "
                           << toString(promote_result);
                master_view->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
            promoted_sequence_id = standby->GetLatestAppliedSequenceId();
            promoted_metadata = standby->ExportMetadata();
#ifdef STORE_USE_REDIS
            standby_registry_providers.reset();
#endif
            standby.reset();
        }

        auto active_service = CreateActiveService(config_, view_version);
        if (promoted_metadata.has_value()) {
            auto restore_result =
                active_service->GetMasterService().RestoreFromStandbyMetadata(
                    promoted_metadata.value(), promoted_sequence_id);
            if (restore_result != ErrorCode::OK) {
                LOG(ERROR) << "Failed to restore promoted metadata: "
                           << toString(restore_result);
                master_view->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
        }

#ifdef STORE_USE_REDIS
        auto mark_primary = [&master_registry_heartbeat] {
            if (master_registry_heartbeat) {
                master_registry_heartbeat->UpdateRole("primary", false);
            }
        };
        const int run_result = RunActiveRpcServers(
            config_, *server, *active_service, std::move(mark_primary));
#else
        const int run_result =
            RunActiveRpcServers(config_, *server, *active_service);
#endif
        active_service.reset();
        master_view->CancelKeepAlive(lease_id);
        keep_leader_thread.join();
        if (run_result != 0) {
            return run_result;
        }
    }
    return 0;
}

P2PMaster::P2PMaster(const P2PMasterConfig& config) : config_(config) {}

int P2PMaster::Run() {
    return config_.ha.enabled ? RunWithHA() : RunStandalone();
}

}  // namespace mooncake
