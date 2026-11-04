#include "p2p/master/p2p_master.h"

#include <algorithm>
#include <atomic>
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

#include "p2p/ha/ha_status.h"
#include "p2p/ha/oplog/p2p_hot_standby_service.h"
#include "p2p/ha/p2p_master_view.h"
#include "p2p/master/p2p_rpc_service.h"
#include "utils.h"
#ifdef STORE_USE_REDIS
#include "p2p/ha/oplog/redis_oplog_store.h"
#include "p2p/ha/oplog/redis_oplog_trim_worker.h"
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

void UpdateHAStatusFromStandby(P2PHotStandbyService& standby) {
    const auto status = standby.GetSyncStatus();
    auto& ha_status = HAStatusManager::instance();
    ha_status.SetRoleState("standby", StandbyStateToString(status.state));
    ha_status.SetOpLogProgress(status.applied_seq_id, status.primary_seq_id);
    ha_status.SetSnapshotReady(standby.IsReadyForSnapshot());
    ha_status.SetDoctorIssue(
        "OPLOG_APPLY_FAILED",
        status.apply_healthy
            ? ""
            : "seq=" + std::to_string(status.failed_sequence_id) +
                  ", op=" + std::to_string(status.failed_op_type) + ": " +
                  status.failure_reason);
}

class ScopedStandbyRegistryProviders final {
   public:
    ScopedStandbyRegistryProviders(
        RedisMasterRegistryHeartbeat& registry_heartbeat,
        P2PHotStandbyService& standby)
        : registry_heartbeat_(&registry_heartbeat) {
        registry_heartbeat_->SetStandbyProviders(
            [&standby] {
                UpdateHAStatusFromStandby(standby);
                return standby.GetLatestAppliedSequenceId();
            },
            [&standby] { return standby.IsReadyForSnapshot(); });
    }

    ~ScopedStandbyRegistryProviders() { Reset(); }

    ScopedStandbyRegistryProviders(const ScopedStandbyRegistryProviders&) =
        delete;
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
    const P2PMasterConfig& config, ViewVersionId view_version,
    std::function<void()> stepdown_callback = {}) {
    auto service = std::make_unique<P2PMasterRpcService>(config, view_version);
    if (stepdown_callback) {
        service->SetStepdownCallback(std::move(stepdown_callback));
    }
    service->init();
    return service;
}

struct P2PPrimaryDemotionHandoff {
    P2PStandbyMetadataStore::ExportedMetadata metadata;
    uint64_t sequence_id{0};
};

}  // namespace

int P2PMaster::RunActiveRpcServers(coro_rpc::coro_rpc_server& server,
                                   P2PMasterRpcService& p2p_service,
                                   std::function<void()> before_start) {
    const bool dedicated_heartbeat = config_.rpc.heartbeat_port > 0;
    RegisterP2PRpcService(server, p2p_service,
                          /*include_heartbeat=*/!dedicated_heartbeat);
    if (before_start) {
        before_start();
    }

    std::optional<coro_rpc::coro_rpc_server> heartbeat_server;
    if (dedicated_heartbeat) {
        heartbeat_server.emplace(
            std::max<uint32_t>(1u, config_.rpc.heartbeat_thread_num),
            config_.rpc.heartbeat_port, config_.rpc.address,
            std::chrono::seconds(config_.rpc.connection_timeout_seconds),
            config_.rpc.enable_tcp_no_delay);
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
        // A concurrent stop can also complete the future during startup.
        server.stop();
        heartbeat_server.reset();
        return -1;
    }
    auto error = std::move(server_result).get();
    LOG(ERROR) << "P2P master RPC server stopped: " << error;
    // The start future only waits for the accept loop. Drain in-flight RPCs
    // before callers can destroy the service, including when another thread
    // is already stopping the server after losing the leader lease.
    server.stop();
    heartbeat_server.reset();
    return 0;
}

int P2PMaster::RunStandalone() {
    HAStatusManager::instance().Configure(
        /*ha_enabled=*/false, config_.cluster_id,
        config_.rpc.address + ":" + std::to_string(config_.rpc.port));
    HAStatusManager::instance().SetRoleState("disabled", "STANDALONE");
    auto server = CreateRpcServer(config_);
    auto active_service = CreateActiveService(config_, /*view_version=*/0);
    return RunActiveRpcServers(*server, *active_service);
}

int P2PMaster::RunWithHA() {
    HAStatusManager::instance().Configure(/*ha_enabled=*/true,
                                          config_.cluster_id, "");
#ifdef STORE_USE_REDIS
    std::string master_instance_id;
    if (config_.oplog.enabled &&
        config_.ha.election_backend == ElectionBackend::REDIS) {
        master_instance_id = GenerateMasterInstanceId();
    }
#endif  // STORE_USE_REDIS
    const std::string local_endpoint =
        config_.rpc.address + ":" + std::to_string(config_.rpc.port);
    std::optional<P2PPrimaryDemotionHandoff> demoted_primary_handoff;
    while (true) {
        HAStatusManager::instance().ClearDoctorIssues();
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
                config_.redis.username, master_instance_id);
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
        HAStatusManager::instance().SetRoleState(
            config_.oplog.enabled ? "standby" : "starting", "INITIALIZING");
        std::unique_ptr<RedisMasterRegistryHeartbeat> master_registry_heartbeat;
        std::string master_instance_id;
        std::string p2p_snapshot_endpoint;
        if (config_.oplog.enabled &&
            config_.ha.election_backend == ElectionBackend::REDIS) {
            RedisMasterRegistryEntry entry;
            entry.instance_id = master_instance_id;
            entry.master_endpoint = local_endpoint;
            p2p_snapshot_endpoint = BuildSnapshotEndpoint(
                local_endpoint, config_.ha.snapshot_service_port,
                config_.ha.snapshot_service_endpoint);
            entry.snapshot_endpoint = p2p_snapshot_endpoint;
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
            standby_config.local_master_endpoint = local_endpoint;
            standby_config.local_snapshot_endpoint = p2p_snapshot_endpoint;
#endif
            standby_config.oplog_pending_max_entries =
                config_.ha.standby_oplog_pending_max_entries;
            standby_config.oplog_poll_batch_size =
                config_.ha.standby_oplog_poll_batch_size;
            standby_config.snapshot_chunk_size = config_.ha.snapshot_chunk_size;
            if (!config_.ha.snapshot_sources.empty()) {
                standby_config.snapshot_source_endpoints = splitString(
                    config_.ha.snapshot_sources, ',', /*trim=*/true);
            }
            standby = std::make_unique<P2PHotStandbyService>(standby_config);
            uint64_t standby_baseline_sequence_id = 0;
            std::optional<P2PStandbyMetadataStore::ExportedMetadata>
                standby_initial_metadata;
            if (demoted_primary_handoff.has_value()) {
                standby_baseline_sequence_id =
                    demoted_primary_handoff->sequence_id;
                standby_initial_metadata =
                    std::move(demoted_primary_handoff->metadata);
                demoted_primary_handoff.reset();
                LOG(INFO) << "Starting P2P standby from demoted primary "
                             "handoff"
                          << ", baseline_sequence_id="
                          << standby_baseline_sequence_id;
            }
            auto start_result =
                standby->Start(standby_baseline_sequence_id,
                               std::move(standby_initial_metadata));
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
                UpdateHAStatusFromStandby(*standby);
                master_registry_heartbeat->UpdateRole(
                    "standby", standby->IsReadyForSnapshot());
            }
#endif
        }

        EtcdLeaseId lease_id = 0;
        ViewVersionId view_version = 0;
        master_view->ElectLeader(local_endpoint, view_version, lease_id);

        std::atomic<bool> leadership_lost{false};
        std::atomic<OpLogManager*> primary_oplog_manager{nullptr};
        auto stop_primary_rpc = [&server, &leadership_lost,
                                 &primary_oplog_manager] {
            leadership_lost.store(true, std::memory_order_release);
            if (auto* manager =
                    primary_oplog_manager.load(std::memory_order_acquire)) {
                manager->RequestStopWriting();
            }
            server->stop();
        };
        auto keep_leader_thread =
            std::thread([view = master_view.get(), lease_id, &stop_primary_rpc,
                         &leadership_lost] {
                view->KeepLeader(lease_id);
                stop_primary_rpc();
            });
        std::this_thread::sleep_for(
            std::chrono::seconds(master_view->GetLeaderLeaseTTLSeconds()));

#ifdef STORE_USE_REDIS
        HAStatusManager::instance().SetRoleState("promoting", "PROMOTING");
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
            HAStatusManager::instance().SetOpLogProgress(
                promoted_sequence_id, promoted_sequence_id);
#ifdef STORE_USE_REDIS
            standby_registry_providers.reset();
#endif
            standby.reset();
        }

        HAStatusManager::instance().SetRoleState("primary", "STARTING");
        auto stepdown_callback = [&stop_primary_rpc, &primary_oplog_manager] {
            if (auto* manager =
                    primary_oplog_manager.load(std::memory_order_acquire)) {
                auto drain = manager->DrainWriting(3000);
                if (!drain.drained) {
                    LOG(WARNING)
                        << "P2P primary stepdown OpLog drain incomplete"
                        << ", queued=" << drain.queued_count
                        << ", inflight=" << drain.inflight_count
                        << ", committed_sequence_id="
                        << drain.committed_sequence_id
                        << ", max_known_sequence_id="
                        << drain.max_known_sequence_id;
                } else {
                    LOG(INFO) << "P2P primary stepdown OpLog drain completed"
                              << ", committed_sequence_id="
                              << drain.committed_sequence_id
                              << ", max_known_sequence_id="
                              << drain.max_known_sequence_id;
                }
            }
            stop_primary_rpc();
        };
        auto active_service =
            CreateActiveService(config_, view_version, stepdown_callback);
        {
            auto* manager =
                active_service->GetMasterService().GetOpLogManager();
            primary_oplog_manager.store(manager, std::memory_order_release);
            // Handle a leader loss that races with service publication. The
            // keep-leader callback either reads the published manager or sees
            // leadership_lost here and requests the stop itself.
            if (leadership_lost.load(std::memory_order_acquire) && manager) {
                manager->RequestStopWriting();
            }
        }
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

        std::unique_ptr<P2PStandbySnapshotService> primary_snapshot_service;
        std::unique_ptr<coro_rpc::coro_rpc_server> primary_snapshot_server;
#ifdef STORE_USE_REDIS
        const bool redis_election =
            config_.ha.election_backend == ElectionBackend::REDIS;
        const bool dedicated_heartbeat = config_.rpc.heartbeat_port > 0;
        const bool invalid_snapshot_port =
            config_.ha.snapshot_service_port == config_.rpc.port ||
            (dedicated_heartbeat &&
             config_.ha.snapshot_service_port == config_.rpc.heartbeat_port);
        if (redis_election && config_.oplog.enabled &&
            config_.ha.snapshot_service_port > 0 && invalid_snapshot_port) {
            LOG(ERROR) << "P2P primary snapshot fallback port conflicts with "
                          "active RPC port";
            master_view->CancelKeepAlive(lease_id);
            keep_leader_thread.join();
            return -1;
        }
        if (redis_election && config_.oplog.enabled &&
            config_.ha.snapshot_service_port > 0) {
            auto* primary_service = &active_service->GetMasterService();
            primary_snapshot_service =
                std::make_unique<P2PStandbySnapshotService>(
                    MakeP2PPrimarySnapshotSource(config_.cluster_id,
                                                 primary_service));
            primary_snapshot_server =
                std::make_unique<coro_rpc::coro_rpc_server>(
                    1, config_.ha.snapshot_service_port, config_.rpc.address);
            RegisterP2PStandbySnapshotRpcHandlers(
                *primary_snapshot_server, primary_snapshot_service.get());
            auto snapshot_result = primary_snapshot_server->async_start();
            if (snapshot_result.hasResult()) {
                LOG(ERROR) << "P2P primary snapshot fallback server failed"
                           << ", port=" << config_.ha.snapshot_service_port;
                primary_snapshot_server->stop();
                primary_snapshot_server.reset();
                primary_snapshot_service.reset();
                master_view->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
            LOG(INFO) << "P2P primary snapshot fallback server started"
                      << ", port=" << config_.ha.snapshot_service_port;
        }
#endif

#ifdef STORE_USE_REDIS
        std::unique_ptr<RedisOpLogTrimWorker> redis_oplog_trim_worker;
        if (config_.oplog.enable_auto_trim) {
            RedisOpLogTrimWorkerConfig trim_config;
            trim_config.cluster_id = config_.cluster_id;
            trim_config.self_instance_id = master_instance_id;
            trim_config.redis_endpoint = config_.redis.endpoint;
            trim_config.redis_username = config_.redis.username;
            trim_config.redis_password = config_.redis.password;
            trim_config.redis_db_index = config_.redis.db_index;
            trim_config.registry_ttl = std::chrono::seconds(10);
            trim_config.interval =
                std::chrono::seconds(config_.oplog.auto_trim_interval_sec);
            trim_config.retention_entries =
                config_.oplog.trim_retention_entries;
            trim_config.delete_batch_size =
                config_.oplog.trim_delete_batch_size;
            trim_config.min_snapshot_ready_standby_count =
                config_.oplog.trim_min_snapshot_ready_standby_count;
            redis_oplog_trim_worker =
                std::make_unique<RedisOpLogTrimWorker>(std::move(trim_config));
            auto trim_start = redis_oplog_trim_worker->Start();
            if (trim_start != ErrorCode::OK) {
                LOG(ERROR) << "Failed to start Redis OpLog auto trim worker: "
                           << toString(trim_start);
                redis_oplog_trim_worker.reset();
                master_view->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
        }

        auto mark_primary = [&master_registry_heartbeat,
                             primary_service =
                                 &active_service->GetMasterService()] {
            const uint64_t latest_sequence_id =
                primary_service->GetLatestOpLogSequenceId();
            HAStatusManager::instance().SetOpLogProgress(
                latest_sequence_id, latest_sequence_id);
            HAStatusManager::instance().SetRoleState("primary", "RUNNING");
            {
                master_registry_heartbeat->SetAppliedSequenceProvider(
                    [primary_service, latest_sequence_id] {
                        const uint64_t sequence_id =
                            primary_service->GetLatestOpLogSequenceId();
                        HAStatusManager::instance().SetOpLogProgress(
                            sequence_id, sequence_id);
                        return sequence_id;
                    });
                master_registry_heartbeat->SetSnapshotReadyProvider({});
                master_registry_heartbeat->UpdateRole("primary", false);
            }
        };
        const int run_result = RunActiveRpcServers(*server, *active_service,
                                                   std::move(mark_primary));
        redis_oplog_trim_worker.reset();
#else
        const int run_result = RunActiveRpcServers(*server, *active_service);
#endif
        HAStatusManager::instance().SetRoleState(
            leadership_lost.load(std::memory_order_acquire) ? "standby"
                                                           : "primary",
            leadership_lost.load(std::memory_order_acquire) ? "LEADERSHIP_LOST"
                                                            : "STOPPED");
        if (auto* manager =
                primary_oplog_manager.load(std::memory_order_acquire)) {
            manager->StopWriting();
        }
        if (primary_snapshot_server) {
            primary_snapshot_server->stop();
        }
        primary_snapshot_server.reset();
        primary_snapshot_service.reset();
#ifdef STORE_USE_REDIS
        if (master_registry_heartbeat) {
            master_registry_heartbeat->SetAppliedSequenceProvider({});
            master_registry_heartbeat->SetSnapshotReadyProvider({});
        }
#endif
        auto* active_p2p_service = &active_service->GetMasterService();
        if (config_.oplog.enabled &&
            leadership_lost.load(std::memory_order_acquire)) {
            const uint64_t local_assigned_sequence_id =
                active_p2p_service->GetLatestOpLogSequenceId();
            auto local_committed_sequence_id =
                active_p2p_service->GetLocalCommittedOpLogSequenceId();
            demoted_primary_handoff.reset();
            if (!local_committed_sequence_id.has_value()) {
                LOG(WARNING)
                    << "Skip P2P primary demotion handoff because local "
                       "committed OpLog sequence is unavailable"
                    << ", local_assigned_sequence_id="
                    << local_assigned_sequence_id << ", error="
                    << toString(local_committed_sequence_id.error());
            } else {
                auto metadata =
                    active_p2p_service->ExportLocalCommittedMetadataForStandby(
                        local_committed_sequence_id.value());
                if (!metadata.has_value()) {
                    LOG(WARNING) << "Skip P2P primary demotion handoff because "
                                    "uncommitted metadata rollback failed"
                                 << ", local_committed_sequence_id="
                                 << local_committed_sequence_id.value()
                                 << ", local_assigned_sequence_id="
                                 << local_assigned_sequence_id
                                 << ", error=" << toString(metadata.error());
                } else {
                    demoted_primary_handoff.emplace();
                    demoted_primary_handoff->sequence_id =
                        local_committed_sequence_id.value();
                    // TODO(P2P HA): Avoid a full metadata copy on demotion by
                    // moving primary metadata into the next standby or by
                    // supporting an in-place role transition.
                    demoted_primary_handoff->metadata =
                        std::move(metadata.value());
                    LOG(INFO)
                        << "Captured P2P primary demotion handoff"
                        << ", baseline_sequence_id="
                        << demoted_primary_handoff->sequence_id
                        << ", local_committed_sequence_id="
                        << local_committed_sequence_id.value()
                        << ", local_assigned_sequence_id="
                        << local_assigned_sequence_id << ", routes="
                        << demoted_primary_handoff->metadata.routes.size()
                        << ", clients="
                        << demoted_primary_handoff->metadata.clients.size();
                }
            }
        } else {
            demoted_primary_handoff.reset();
            LOG(WARNING) << "Skip P2P primary demotion handoff because master "
                            "service stopped without losing leadership";
        }
        master_view->CancelKeepAlive(lease_id);
        keep_leader_thread.join();
        active_service.reset();
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
