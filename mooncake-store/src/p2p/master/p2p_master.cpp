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

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"
#include "p2p/ha/p2p_election_readiness.h"
#include "p2p/ha/p2p_status_http_server.h"
#include "p2p/ha/oplog/p2p_hot_standby_service.h"
#include "p2p/ha/p2p_master_view.h"
#include "p2p/master/p2p_rpc_service.h"
#include "utils.h"
#ifdef STORE_USE_REDIS
#include "p2p/ha/oplog/redis_oplog_store.h"
#include "p2p/ha/oplog/redis_oplog_trim_worker.h"
#endif

#include "common.h"

namespace mooncake {

std::atomic<bool> P2PMaster::shutdown_requested_{false};

void P2PMaster::RequestShutdown() {
    shutdown_requested_.store(true, std::memory_order_release);
}

bool P2PMaster::IsShutdownRequested() {
    return shutdown_requested_.load(std::memory_order_acquire);
}

void P2PMaster::HandleSignal(int signal) { RequestShutdown(); }

namespace {

constexpr uint32_t kShutdownDrainTimeoutMs = 3000;

std::unique_ptr<coro_rpc::coro_rpc_server> CreateRpcServer(
    const P2PMasterConfig& config) {
    auto server = std::make_unique<coro_rpc::coro_rpc_server>(
        config.rpc.thread_num,
        buildHostNameWithPort(config.rpc.address, config.rpc.port),
        std::chrono::seconds(config.rpc.connection_timeout_seconds),
        config.rpc.enable_tcp_no_delay);
    const char* protocol = std::getenv("MC_RPC_PROTOCOL");
    if (protocol && std::string_view(protocol) == "rdma") {
        server->init_ibv();
    }
    return server;
}

uint64_t NonNegativeSequenceMetric(int64_t value) {
    return value > 0 ? static_cast<uint64_t>(value) : 0;
}

void UpdateHAOpLogProgress(uint64_t applied_sequence_id,
                           std::optional<uint64_t> latest_sequence_id) {
    auto& metrics = HAMetricManager::instance();
    HAStatusManager::instance().SetOpLogProgress(
        applied_sequence_id, latest_sequence_id,
        NonNegativeSequenceMetric(metrics.get_oplog_trimmed_sequence_id()),
        NonNegativeSequenceMetric(metrics.get_oplog_gc_cursor_sequence_id()));
}

uint64_t UpdatePrimaryHAOpLogProgress(P2PMasterService& primary_service) {
    const uint64_t local_sequence_id =
        primary_service.GetLatestOpLogSequenceId();
    const auto latest_sequence_id = primary_service.ReadLatestOpLogSequenceId();
    UpdateHAOpLogProgress(local_sequence_id,
                          latest_sequence_id.has_value()
                              ? std::optional<uint64_t>(*latest_sequence_id)
                              : std::nullopt);
    return local_sequence_id;
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
    UpdateHAOpLogProgress(status.applied_seq_id,
                          status.primary_seq_id_known
                              ? std::optional<uint64_t>(status.primary_seq_id)
                              : std::nullopt);
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
    P2PMasterRpcService::StepdownCallback stepdown_callback = {}) {
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

bool WaitForElectionPause(std::chrono::steady_clock::time_point deadline,
                          const std::function<bool()>& stopping) {
    bool waited = false;
    while (!stopping()) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            if (waited) {
                LOG(INFO) << "HA election pause completed";
            }
            return true;
        }
        if (!waited) {
            LOG(INFO) << "HA election paused after stepdown"
                      << ", remaining_ms="
                      << std::chrono::duration_cast<std::chrono::milliseconds>(
                             deadline - now)
                             .count();
            waited = true;
        }
        std::this_thread::sleep_until(
            std::min(deadline, now + std::chrono::milliseconds(100)));
    }
    return false;
}

}  // namespace

int P2PMaster::RunActiveRpcServers(coro_rpc::coro_rpc_server& server,
                                   P2PMasterRpcService& p2p_service,
                                   std::function<void()> before_start,
                                   std::function<void()> after_stop,
                                   std::function<bool()> stopping,
                                   std::function<bool()> stepping_down) {
    auto is_stopping = [&stopping] { return stopping && stopping(); };
    std::optional<coro_rpc::coro_rpc_server> heartbeat_server;
    auto finish = [&] {
        // Join all admitted handlers before draining. The expiry monitor can
        // also enqueue unregister entries, so stop it while the writer and
        // leader renewal are still running. HTTP metadata endpoints are
        // read-only.
        server.stop();
        if (heartbeat_server) {
            heartbeat_server->stop();
            heartbeat_server.reset();
        }
        p2p_service.GetMasterService().GetClientManager().Stop();
        if (after_stop) {
            after_stop();
        }
    };
    if (is_stopping()) {
        finish();
        return 0;
    }

    const bool dedicated_heartbeat = config_.rpc.heartbeat_port > 0;
    RegisterP2PRpcService(server, p2p_service,
                          /*include_heartbeat=*/!dedicated_heartbeat);
    if (before_start) {
        before_start();
    }

    if (dedicated_heartbeat) {
        heartbeat_server.emplace(
            std::max<uint32_t>(1u, config_.rpc.heartbeat_thread_num),
            buildHostNameWithPort(config_.rpc.address,
                                  config_.rpc.heartbeat_port),
            std::chrono::seconds(config_.rpc.connection_timeout_seconds),
            config_.rpc.enable_tcp_no_delay);
        RegisterP2PHeartbeatRpcService(*heartbeat_server, p2p_service);
        auto heartbeat_result = heartbeat_server->async_start();
        if (heartbeat_result.hasResult()) {
            LOG(ERROR) << "Failed to start heartbeat RPC server: "
                       << heartbeat_result.result().value();
            finish();
            return -1;
        }
    }

    auto server_result = server.async_start();
    if (server_result.hasResult()) {
        LOG(ERROR) << "Failed to start P2P master RPC server: "
                   << server_result.result().value();
        // A concurrent stop can also complete the future during startup.
        finish();
        return -1;
    }
    while (!is_stopping() && !server_result.hasResult()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (is_stopping()) {
        finish();
        return 0;
    }
    auto error = std::move(server_result).get();
    // Closing the listener for voluntary stepdown cancels its accept loop.
    // An unexpected exit, including lease loss, must remain an error.
    if (stepping_down && stepping_down() &&
        error == coro_rpc::errc::operation_canceled) {
        LOG(INFO) << "P2P master RPC server stopped for active stepdown: "
                  << error.message();
    } else {
        LOG(ERROR) << "P2P master RPC server stopped: " << error.message();
    }
    finish();
    return 0;
}

bool P2PMaster::DrainOpLogBeforeStop(OpLogManager& manager, uint32_t timeout_ms,
                                     const char* reason) {
    const std::string prefix = std::string("P2P primary ") + reason;
    LOG(INFO) << prefix << " OpLog drain started"
              << ", drain_timeout_ms=" << timeout_ms;
    const auto drain = manager.DrainWriting(timeout_ms);
    auto& status = HAStatusManager::instance();
    const std::string error_prefix = prefix + " OpLog drain incomplete";
    if (drain.drained) {
        status.SetDoctorIssue("OPLOG_DRAIN_INCOMPLETE");
        status.ClearLastErrorIfPrefix(
            "P2P primary stepdown OpLog drain incomplete");
        status.ClearLastErrorIfPrefix(
            "P2P primary shutdown OpLog drain incomplete");
        LOG(INFO) << prefix << " OpLog drain completed"
                  << ", drain_timeout_ms=" << timeout_ms
                  << ", committed_sequence_id=" << drain.committed_sequence_id
                  << ", max_known_sequence_id=" << drain.max_known_sequence_id;
        return true;
    }
    std::string error = error_prefix +
                        ", drain_timeout_ms=" + std::to_string(timeout_ms) +
                        ", queued=" + std::to_string(drain.queued_count) +
                        ", inflight=" + std::to_string(drain.inflight_count) +
                        ", committed_sequence_id=" +
                        std::to_string(drain.committed_sequence_id) +
                        ", max_known_sequence_id=" +
                        std::to_string(drain.max_known_sequence_id);
    LOG(WARNING) << error;
    status.SetDoctorIssue("OPLOG_DRAIN_INCOMPLETE", error);
    status.SetLastError(std::move(error));
    return false;
}

int P2PMaster::RunStandalone() {
    HAStatusManager::instance().Configure(
        /*ha_enabled=*/false, config_.cluster_id,
        buildHostNameWithPort(config_.rpc.address, config_.rpc.port),
        config_.oplog.latest_stall_timeout_sec,
        config_.oplog.catchup_stall_timeout_sec);
    HAStatusManager::instance().SetRoleState("disabled", "STANDALONE");
    auto server = CreateRpcServer(config_);
    auto active_service = CreateActiveService(config_, /*view_version=*/0);
    return RunActiveRpcServers(
        *server, *active_service, /*before_start=*/{}, /*after_stop=*/{},
        [this] { return P2PMaster::IsShutdownRequested(); });
}

int P2PMaster::RunWithHA() {
    auto stopping = [] { return P2PMaster::IsShutdownRequested(); };
    HAStatusManager::instance().Configure(
        /*ha_enabled=*/true, config_.cluster_id, "",
        config_.oplog.latest_stall_timeout_sec,
        config_.oplog.catchup_stall_timeout_sec);
    std::string master_instance_id;
#ifdef STORE_USE_REDIS
    if (config_.oplog.enabled &&
        config_.ha.election_backend == ElectionBackend::REDIS) {
        master_instance_id = GenerateMasterInstanceId();
        HAStatusManager::instance().SetInstanceId(master_instance_id);
    }
#endif  // STORE_USE_REDIS
    const std::string local_endpoint =
        buildHostNameWithPort(config_.rpc.address, config_.rpc.port);
    std::optional<P2PPrimaryDemotionHandoff> demoted_primary_handoff;
    // Preserve the accepted request's deadline across active/standby rounds.
    // The HTTP callback writes it before publishing the stepdown flag; the
    // next round reads it after the active service has been destroyed.
    std::chrono::steady_clock::time_point election_paused_until{};
    while (!stopping()) {
        // The previous primary's drain result must remain visible after
        // demotion; clear it on a successful drain instead of hiding it here.
        HAStatusManager::instance().ClearDoctorIssues(
            /*preserve_stepdown_drain=*/true);
        // Read-only diagnostics must be reachable in every role. A standby
        // blocks inside ElectLeader below, so start the diagnostic server
        // before the active (primary-only) HTTP server exists.
        P2PStatusHttpServer status_http_server(
            static_cast<uint16_t>(config_.metrics.http_port),
            config_.metrics.host);
        status_http_server.Start();
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
            // TODO(P2P HA): the etcd election backend is not fully supported
            // by the HA status/doctor surface yet. It publishes no master
            // view into the diagnostics cache, so a running etcd primary can
            // report MASTER_VIEW_MISMATCH/MASTER_VIEW_LEASE_EXPIRED false
            // positives. Treat Redis election as the supported HA backend
            // until etcd state sampling is implemented.
            auto etcd_view =
                std::make_unique<P2PEtcdMasterView>(config_.cluster_id);
            connect_result = etcd_view->Connect(config_.ha.etcd_endpoints);
            master_view = std::move(etcd_view);
        }
        if (connect_result != ErrorCode::OK) {
            const std::string error =
                std::string("Failed to initialize P2P master view: ") +
                toString(connect_result);
            HAStatusManager::instance().SetRoleState("starting", "FAILED");
            HAStatusManager::instance().SetLastError(error);
            LOG(ERROR) << error;
            return -1;
        }

#ifdef STORE_USE_REDIS
        HAStatusManager::instance().SetRoleState(
            config_.oplog.enabled ? "standby" : "starting", "INITIALIZING");
        std::unique_ptr<RedisMasterRegistryHeartbeat> master_registry_heartbeat;
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

#ifdef STORE_USE_REDIS
        if (standby && config_.ha.election_backend == ElectionBackend::REDIS) {
            auto* redis_view =
                dynamic_cast<P2PRedisMasterView*>(master_view.get());
            if (redis_view) {
                redis_view->SetShouldElect([this,
                                            self_instance_id =
                                                master_instance_id,
                                            standby = standby.get()] {
                    if (P2PMaster::IsShutdownRequested()) {
                        return false;
                    }
                    const auto self_status = standby->GetSyncStatus();
                    auto readiness_decision =
                        EvaluateP2PElectionStandbyReadiness(
                            self_status.state, self_status.primary_seq_id_known,
                            self_status.primary_seq_id,
                            self_status.applied_seq_id,
                            config_.oplog.election_max_standby_lag_entries);
                    if (!readiness_decision.should_wait) {
                        HAStatusManager::instance().SetElectionReadinessBlocked(
                            false);
                        return true;
                    }
                    if (self_status.state == StandbyState::WATCHING) {
                        HAStatusManager::instance().SetElectionReadinessBlocked(
                            true, readiness_decision.lag_entries,
                            config_.oplog.election_max_standby_lag_entries);
                    } else {
                        HAStatusManager::instance().SetElectionReadinessBlocked(
                            false);
                    }
                    LOG_EVERY_N(WARNING, 30)
                        << "P2P election attempt blocked by standby readiness"
                        << ", self_instance_id=" << self_instance_id
                        << ", self_endpoint=" << config_.rpc.address
                        << ", standby_state="
                        << StandbyStateToString(self_status.state)
                        << ", self_applied_sequence_id="
                        << self_status.applied_seq_id
                        << ", latest_sequence_id_known="
                        << self_status.primary_seq_id_known
                        << ", latest_sequence_id=" << self_status.primary_seq_id
                        << ", lag_entries=" << readiness_decision.lag_entries
                        << ", max_lag_entries="
                        << config_.oplog.election_max_standby_lag_entries;
                    return false;
                });
            }
        }
#endif
        // Keep standby replication and diagnostics running during cooldown.
        if (!WaitForElectionPause(election_paused_until, stopping)) {
            LOG(INFO) << "HA shutdown requested during election pause";
            return 0;
        }
        if (standby) {
            if (!WaitForP2PElectionReadiness(config_, master_instance_id,
                                             *standby, stopping)) {
                LOG(INFO) << "HA shutdown requested during election readiness "
                             "wait";
                return 0;
            }
        }

        LOG(INFO) << "Trying to elect self as leader...";
        EtcdLeaseId lease_id = 0;
        ViewVersionId view_version = 0;
        std::atomic<bool> election_done{false};
        std::thread election_thread([&] {
            master_view->ElectLeader(local_endpoint, view_version, lease_id);
            election_done.store(true, std::memory_order_release);
        });
        while (!election_done.load(std::memory_order_acquire) && !stopping()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (stopping()) {
            master_view->CancelElection();
        }
        election_thread.join();
        if (stopping()) {
            // Election may have won before cancellation; release only our view.
            const auto release_result = master_view->ReleaseLeader();
            LOG(INFO) << "HA shutdown requested during election"
                      << ", release_result=" << toString(release_result);
            return 0;
        }

        std::atomic<bool> leadership_lost{false};
        std::atomic<bool> active_stepdown_requested{false};
        // Published with active_stepdown_requested's release/acquire pair.
        uint32_t stepdown_drain_timeout_ms = 0;
        std::atomic<OpLogManager*> primary_oplog_manager{nullptr};
        const auto stop_for_leadership_loss = [&server, &leadership_lost,
                                               &primary_oplog_manager] {
            leadership_lost.store(true, std::memory_order_release);
            // A lost lease can overlap with the next primary. Stop accepting
            // and enqueueing new OpLog write immediately; DrainWriting below is
            // deliberately skipped for this path. TODO(P2P HA): add an
            // epoch/fencing check in the shared store so an already in-flight
            // command cannot modify the new primary's OpLog.
            if (auto* manager =
                    primary_oplog_manager.load(std::memory_order_acquire)) {
                manager->RequestStopWriting();
            }
            server->stop();
        };
        const auto stop_for_active_stepdown =
            [&server, &active_stepdown_requested, &election_paused_until,
             &stepdown_drain_timeout_ms](
                const P2PMasterRpcService::StepdownRequest& request) {
                election_paused_until = request.election_paused_until;
                stepdown_drain_timeout_ms = request.drain_timeout_ms;
                // Active stepdown is voluntary: close the write request entry
                // first, then let RunActiveRpcServers wait for accepted
                // handlers and drain confirmed writes while this primary still
                // renews its lease.
                active_stepdown_requested.store(true,
                                                std::memory_order_release);
                server->stop();
            };
        auto keep_leader_thread = std::thread(
            [view = master_view.get(), lease_id, stop_for_leadership_loss] {
                view->KeepLeader(lease_id);
                stop_for_leadership_loss();
            });
        const auto retire_deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(master_view->GetLeaderLeaseTTLSeconds());
        while (!stopping() &&
               std::chrono::steady_clock::now() < retire_deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (stopping()) {
            master_view->CancelKeepAlive(lease_id);
            keep_leader_thread.join();
            const auto release_result = master_view->ReleaseLeader();
            LOG(INFO) << "HA shutdown requested before promotion"
                      << ", release_result=" << toString(release_result);
            return 0;
        }

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
            UpdateHAOpLogProgress(promoted_sequence_id, promoted_sequence_id);
#ifdef STORE_USE_REDIS
            standby_registry_providers.reset();
#endif
            standby.reset();
        }

        if (stopping()) {
            master_view->CancelKeepAlive(lease_id);
            keep_leader_thread.join();
            const auto release_result = master_view->ReleaseLeader();
            LOG(INFO) << "HA shutdown requested during promotion"
                      << ", release_result=" << toString(release_result);
            return 0;
        }

        HAStatusManager::instance().SetRoleState("primary", "STARTING");
        // A late SIGTERM during another exit path must not report a graceful
        // shutdown unless the admitted writes were actually drained.
        bool graceful_drain_complete = !config_.oplog.enabled;
        auto drain_before_stop = [&active_stepdown_requested, &leadership_lost,
                                  &primary_oplog_manager,
                                  &stepdown_drain_timeout_ms,
                                  &graceful_drain_complete, stopping] {
            const bool shutdown = stopping();
            if (!shutdown &&
                !active_stepdown_requested.load(std::memory_order_acquire)) {
                return;
            }
            auto* manager =
                primary_oplog_manager.load(std::memory_order_acquire);
            if (!manager) {
                return;
            }
            // Graceful shutdown must not cancel accepted writes before drain.
            // Lease loss still cancels immediately, including during this wait.
            if (leadership_lost.load(std::memory_order_acquire)) {
                graceful_drain_complete = false;
                LOG(WARNING)
                    << "Skipping graceful OpLog drain after lease loss";
                return;
            }
            graceful_drain_complete = DrainOpLogBeforeStop(
                *manager,
                shutdown ? kShutdownDrainTimeoutMs : stepdown_drain_timeout_ms,
                shutdown ? "shutdown" : "stepdown");
        };
        auto stepdown_callback = stop_for_active_stepdown;
        // The active service serves the full HTTP surface (including
        // business endpoints) on the same metrics port, so hand it over now.
        status_http_server.Stop();
        auto active_service =
            CreateActiveService(config_, view_version, stepdown_callback);
        {
            auto* manager =
                active_service->GetMasterService().GetOpLogManager();
            primary_oplog_manager.store(manager, std::memory_order_release);
            // Handle a leader loss that races with service publication.
            // The keep-leader callback can fire before the manager is
            // published. Replay the cancellation and server stop here to close
            // that publication race.
            if (leadership_lost.load(std::memory_order_acquire)) {
                stop_for_leadership_loss();
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
                    1, buildHostNameWithPort(
                           config_.rpc.address, config_.ha.snapshot_service_port));
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

#endif
        auto mark_primary_running = [primary_service =
                                         &active_service->GetMasterService()] {
            UpdatePrimaryHAOpLogProgress(*primary_service);
            HAStatusManager::instance().SetRoleState("primary", "RUNNING");
        };
#ifdef STORE_USE_REDIS
        auto mark_redis_registry = [&master_registry_heartbeat,
                                    primary_service =
                                        &active_service->GetMasterService()] {
            if (!master_registry_heartbeat) {
                return;
            }
            master_registry_heartbeat->SetAppliedSequenceProvider(
                [primary_service] {
                    return UpdatePrimaryHAOpLogProgress(*primary_service);
                });
            master_registry_heartbeat->SetSnapshotReadyProvider({});
            master_registry_heartbeat->UpdateRole("primary", false);
        };
        auto mark_primary = [mark_primary_running,
                             mark_redis_registry =
                                 std::move(mark_redis_registry)] {
            mark_primary_running();
            mark_redis_registry();
        };
#else
        auto mark_primary = std::move(mark_primary_running);
#endif
        const int run_result = RunActiveRpcServers(
            *server, *active_service, std::move(mark_primary),
            std::move(drain_before_stop), stopping,
            [&active_stepdown_requested, &leadership_lost] {
                return active_stepdown_requested.load(
                           std::memory_order_acquire) &&
                       !leadership_lost.load(std::memory_order_acquire);
            });
#ifdef STORE_USE_REDIS
        redis_oplog_trim_worker.reset();
#endif
        const bool demoted_primary =
            active_stepdown_requested.load(std::memory_order_acquire) ||
            leadership_lost.load(std::memory_order_acquire);
        HAStatusManager::instance().SetRoleState(
            demoted_primary ? "standby" : "primary",
            leadership_lost.load(std::memory_order_acquire)
                ? "LEADERSHIP_LOST"
                : (active_stepdown_requested.load(std::memory_order_acquire)
                       ? "STEPPED_DOWN"
                       : "STOPPED"));
        if (auto* manager =
                primary_oplog_manager.load(std::memory_order_acquire)) {
            manager->StopWriting();
            if (stopping()) {
                primary_oplog_manager.store(nullptr, std::memory_order_release);
            }
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
        if (config_.oplog.enabled && demoted_primary) {
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
            LOG(WARNING)
                << "Skip P2P primary demotion handoff because master "
                   "service stopped without a primary demotion request";
        }
        master_view->CancelKeepAlive(lease_id);
        keep_leader_thread.join();
        if (stopping()) {
            const auto release_result = master_view->ReleaseLeader();
            HAStatusManager::instance().SetRoleState("primary", "STOPPED");
            if (!graceful_drain_complete) {
                // The drain path already reports queue/sequence details. This
                // aggregate log and exit status distinguish incomplete
                // shutdown.
                LOG(ERROR)
                    << "HA shutdown completed with incomplete OpLog drain"
                    << ", release_result=" << toString(release_result);
                return -1;
            }
            LOG(INFO) << "HA shutdown completed"
                      << ", release_result=" << toString(release_result);
            return 0;
        }
        if (active_stepdown_requested.load(std::memory_order_acquire)) {
            // Release only after RPC handlers, OpLog writes, and keepalive have
            // stopped.
            const auto release_result = master_view->ReleaseLeader();
            LOG(INFO) << "HA primary stepdown completed"
                      << ", release_result=" << toString(release_result);
        }
        active_service.reset();
        if (run_result != 0) {
            return run_result;
        }
    }
    return 0;
}

P2PMaster::P2PMaster(const P2PMasterConfig& config) : config_(config) {}

int P2PMaster::Run() {
    // TODO(P2P HA): graceful HA shutdown currently supports Redis only.
    // The etcd view does not implement CancelElection(), so SIGINT/SIGTERM
    // during an etcd election can leave RunWithHA() waiting for that thread.
    const auto previous_sigint_handler = std::signal(SIGINT, HandleSignal);
    const auto previous_sigterm_handler = std::signal(SIGTERM, HandleSignal);
    const int result = config_.ha.enabled ? RunWithHA() : RunStandalone();
    if (previous_sigint_handler != SIG_ERR) {
        std::signal(SIGINT, previous_sigint_handler);
    }
    if (previous_sigterm_handler != SIG_ERR) {
        std::signal(SIGTERM, previous_sigterm_handler);
    }
    return result;
}

}  // namespace mooncake
