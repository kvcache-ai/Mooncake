#include "ha/leadership/master_service_supervisor.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <thread>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/standby_controller.h"
#include "rpc_service.h"

namespace mooncake {
namespace ha {

namespace {

constexpr auto kAcquireRetryInterval = std::chrono::seconds(1);
constexpr auto kRenewCheckInterval = std::chrono::seconds(1);
constexpr auto kSupervisorRetryInterval = std::chrono::seconds(1);

std::string ResolveHABackendConnstring(
    const MasterServiceSupervisorConfig& config) {
    if (!config.ha_backend_connstring.empty()) {
        return config.ha_backend_connstring;
    }
    return config.etcd_endpoints;
}

tl::expected<HABackendSpec, ErrorCode> BuildHABackendSpec(
    const MasterServiceSupervisorConfig& config) {
    auto backend_type = ParseHABackendType(config.ha_backend_type);
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto connstring = ResolveHABackendConnstring(config);
    if (connstring.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return HABackendSpec{
        .type = backend_type.value(),
        .connstring = connstring,
        .cluster_namespace = config.cluster_id,
    };
}

bool IsFatalHABackendError(ErrorCode err) {
    return err == ErrorCode::INVALID_PARAMS ||
           err == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

void LogLeadershipReleaseWarning(std::string_view context, ErrorCode err) {
    if (err == ErrorCode::OK) {
        return;
    }
    LOG(WARNING) << "Failed to release leadership after " << context << ": "
                 << toString(err);
}

bool HandleSupervisorError(std::string_view action, ErrorCode err,
                           HABackendType backend_type) {
    if (IsFatalHABackendError(err)) {
        LOG(ERROR) << "Failed to " << action << ": " << toString(err)
                   << ", backend_type=" << HABackendTypeToString(backend_type);
        return true;
    }

    LOG(WARNING) << "Failed to " << action << ": " << toString(err)
                 << ", backend_type=" << HABackendTypeToString(backend_type)
                 << ", retrying in " << kSupervisorRetryInterval.count() << "s";
    std::this_thread::sleep_for(kSupervisorRetryInterval);
    return false;
}

bool HandleLeadershipPhaseError(std::string_view release_context,
                                std::string_view action,
                                LeaderCoordinator& coordinator,
                                const LeadershipSession& session, ErrorCode err,
                                HABackendType backend_type) {
    LogLeadershipReleaseWarning(release_context,
                                coordinator.ReleaseLeadership(session));
    return HandleSupervisorError(action, err, backend_type);
}

tl::expected<bool, ErrorCode> WarmupLeadership(
    LeaderCoordinator& coordinator, const LeadershipSession& session) {
    const auto deadline = std::chrono::steady_clock::now() + session.lease_ttl;
    const auto max_sleep_interval =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            kRenewCheckInterval);

    while (true) {
        auto renewed = coordinator.RenewLeadership(session);
        if (!renewed) {
            return tl::make_unexpected(renewed.error());
        }
        if (!renewed.value()) {
            return false;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return true;
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                  now);
        const auto sleep_interval =
            remaining < max_sleep_interval ? remaining : max_sleep_interval;
        std::this_thread::sleep_for(sleep_interval);
    }
}

void SetRuntimeState(MasterAdminServer& admin_server,
                     MasterRuntimeState state) {
    admin_server.SetRuntimeState(state);
    LOG(INFO) << "Master runtime state -> " << MasterRuntimeStateToString(state)
              << ", role=" << MasterRuntimeRoleToString(state);
}

void ActivateServingState(
    MasterAdminServer& admin_server,
    const std::shared_ptr<WrappedMasterService>& service) {
    admin_server.SetServiceDelegate(service);
    admin_server.SetServiceAvailable(true);
    SetRuntimeState(admin_server, MasterRuntimeState::kServing);
}

void DeactivateServingState(MasterAdminServer& admin_server) {
    admin_server.SetServiceAvailable(false);
    admin_server.SetServiceDelegate(nullptr);
}

void StopLeadershipMonitor(std::unique_ptr<LeadershipMonitorHandle>& monitor) {
    if (!monitor) {
        return;
    }
    monitor->Stop();
    monitor.reset();
}

void UpdateObservedLeader(MasterAdminServer& admin_server,
                          StandbyController& standby_controller,
                          const std::optional<MasterView>& leader_view) {
    admin_server.SetObservedLeader(leader_view);
    standby_controller.UpdateObservedLeader(leader_view);
}

void ApplyCurrentView(MasterAdminServer& admin_server,
                      StandbyController& standby_controller,
                      const ViewChangeResult& wait_result) {
    if (!wait_result.current_view.has_value() &&
        (wait_result.changed || wait_result.timed_out)) {
        return;
    }
    UpdateObservedLeader(admin_server, standby_controller,
                         wait_result.current_view);
}

void EnterStandbyMode(MasterAdminServer& admin_server,
                      StandbyController& standby_controller,
                      std::atomic<bool>& accept_runtime_updates,
                      const std::optional<MasterView>& leader_view) {
    accept_runtime_updates.store(true, std::memory_order_release);
    UpdateObservedLeader(admin_server, standby_controller, leader_view);

    auto err = standby_controller.StartStandby(leader_view);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to start standby replication: "
                     << toString(err);
        SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
        return;
    }

    SetRuntimeState(admin_server, standby_controller.GetStandbyRuntimeState());
}

int RunSupervisorLoop(const HABackendSpec& spec,
                      const MasterServiceSupervisorConfig& config,
                      MasterAdminServer& admin_server) {
    SetRuntimeState(admin_server, MasterRuntimeState::kStarting);
    auto standby_controller = CreateStandbyController(spec, config);
    std::atomic<bool> accept_standby_runtime_updates{false};
    standby_controller->SetStandbyRuntimeStateCallback(
        [&](MasterRuntimeState state) {
            if (!accept_standby_runtime_updates.load(
                    std::memory_order_acquire)) {
                return;
            }
            SetRuntimeState(admin_server, state);
        });

    EnterStandbyMode(admin_server, *standby_controller,
                     accept_standby_runtime_updates, std::nullopt);

    while (true) {
        auto coordinator = CreateLeaderCoordinator(spec);
        if (!coordinator) {
            if (HandleSupervisorError("create leader coordinator",
                                      coordinator.error(), spec.type)) {
                return -1;
            }
            continue;
        }

        auto& leader_coordinator = *coordinator.value();
        std::optional<LeadershipSession> leadership_session;

        while (!leadership_session.has_value()) {
            SetRuntimeState(admin_server, MasterRuntimeState::kCandidate);

            auto current_view = leader_coordinator.ReadCurrentView();
            if (!current_view) {
                EnterStandbyMode(admin_server, *standby_controller,
                                 accept_standby_runtime_updates, std::nullopt);
                if (HandleSupervisorError("read current leader view",
                                          current_view.error(), spec.type)) {
                    return -1;
                }
                break;
            }

            UpdateObservedLeader(admin_server, *standby_controller,
                                 current_view.value());
            if (!current_view.value().has_value()) {
                auto acquire = leader_coordinator.TryAcquireLeadership(
                    config.local_hostname);
                if (!acquire) {
                    EnterStandbyMode(admin_server, *standby_controller,
                                     accept_standby_runtime_updates,
                                     std::nullopt);
                    if (HandleSupervisorError("acquire leadership",
                                              acquire.error(), spec.type)) {
                        return -1;
                    }
                    break;
                }

                if (acquire->observed_view.has_value()) {
                    UpdateObservedLeader(admin_server, *standby_controller,
                                         acquire->observed_view);
                }

                if (acquire->status == AcquireLeadershipStatus::ACQUIRED &&
                    acquire->session.has_value()) {
                    leadership_session = *acquire->session;
                    admin_server.SetObservedLeader(leadership_session->view);
                    break;
                }

                EnterStandbyMode(admin_server, *standby_controller,
                                 accept_standby_runtime_updates,
                                 acquire->observed_view);
                std::optional<ViewVersionId> observed_version = std::nullopt;
                if (acquire->observed_view.has_value()) {
                    observed_version = acquire->observed_view->view_version;
                }
                auto wait = leader_coordinator.WaitForViewChange(
                    observed_version, kAcquireRetryInterval);
                if (!wait) {
                    if (HandleSupervisorError("wait for leader view change",
                                              wait.error(), spec.type)) {
                        return -1;
                    }
                    break;
                }
                ApplyCurrentView(admin_server, *standby_controller, *wait);
                continue;
            }

            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             current_view.value());
            const auto& known_view = current_view.value().value();
            auto wait = leader_coordinator.WaitForViewChange(
                known_view.view_version, kAcquireRetryInterval);
            if (!wait) {
                if (HandleSupervisorError("wait for leader view change",
                                          wait.error(), spec.type)) {
                    return -1;
                }
                break;
            }
            ApplyCurrentView(admin_server, *standby_controller, *wait);
        }

        if (!leadership_session.has_value()) {
            continue;
        }

        accept_standby_runtime_updates.store(false, std::memory_order_release);
        auto promote_standby = standby_controller->PromoteStandby();
        if (promote_standby != ErrorCode::OK) {
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             leadership_session->view);
            if (HandleSupervisorError("promote standby for serve",
                                      promote_standby, spec.type)) {
                return -1;
            }
            continue;
        }

        LOG(INFO) << "Entering warmup phase...";
        SetRuntimeState(admin_server, MasterRuntimeState::kLeaderWarmup);
        auto warmup_result =
            WarmupLeadership(leader_coordinator, *leadership_session);
        if (!warmup_result) {
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "renewal startup failure", "start leadership renewal",
                    leader_coordinator, *leadership_session,
                    warmup_result.error(), spec.type)) {
                return -1;
            }
            continue;
        }
        if (!warmup_result.value()) {
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates, std::nullopt);
            LogLeadershipReleaseWarning(
                "warmup expiration",
                leader_coordinator.ReleaseLeadership(*leadership_session));
            LOG(WARNING) << "Leadership expired during warmup phase";
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        LOG(INFO) << "Starting serve phase...";
        coro_rpc::coro_rpc_server server(
            config.rpc_thread_num, config.rpc_port, config.rpc_address,
            config.rpc_conn_timeout, config.rpc_enable_tcp_no_delay);
        const char* protocol = std::getenv("MC_RPC_PROTOCOL");
        if (protocol && std::string_view(protocol) == "rdma") {
            server.init_ibv();
        }

        auto wrapped_master_service = std::make_shared<WrappedMasterService>(
            mooncake::WrappedMasterServiceConfig(
                config, leadership_session->view.view_version));
        mooncake::RegisterRpcService(server, *wrapped_master_service);

        auto serve_preflight =
            leader_coordinator.RenewLeadership(*leadership_session);
        if (!serve_preflight) {
            DeactivateServingState(admin_server);
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "serve preflight failure",
                    "validate leadership before serving", leader_coordinator,
                    *leadership_session, serve_preflight.error(), spec.type)) {
                return -1;
            }
            continue;
        }
        if (!serve_preflight.value()) {
            DeactivateServingState(admin_server);
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates, std::nullopt);
            LogLeadershipReleaseWarning(
                "serve preflight expiration",
                leader_coordinator.ReleaseLeadership(*leadership_session));
            LOG(WARNING) << "Leadership expired before entering serve phase";
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        std::atomic<bool> serve_shutdown_requested{false};
        auto leadership_monitor = leader_coordinator.StartLeadershipMonitor(
            *leadership_session,
            [&server, &admin_server, &serve_shutdown_requested](auto reason) {
                serve_shutdown_requested.store(true, std::memory_order_release);
                admin_server.SetServiceAvailable(false);
                SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
                LOG(INFO) << "Trying to stop server, reason="
                          << LeadershipLossReasonToString(reason);
                server.stop();
            });
        if (!leadership_monitor) {
            DeactivateServingState(admin_server);
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "serve monitor startup failure", "start leadership monitor",
                    leader_coordinator, *leadership_session,
                    leadership_monitor.error(), spec.type)) {
                return -1;
            }
            continue;
        }
        auto leadership_monitor_handle = std::move(leadership_monitor.value());

        async_simple::Future<coro_rpc::err_code> ec = server.async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            StopLeadershipMonitor(leadership_monitor_handle);
            DeactivateServingState(admin_server);
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             leadership_session->view);
            auto err =
                leader_coordinator.ReleaseLeadership(*leadership_session);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to release leadership: " << toString(err);
            }
            return -1;
        }

        if (!serve_shutdown_requested.load(std::memory_order_acquire)) {
            ActivateServingState(admin_server, wrapped_master_service);
        }

        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;

        StopLeadershipMonitor(leadership_monitor_handle);
        DeactivateServingState(admin_server);
        auto err = leader_coordinator.ReleaseLeadership(*leadership_session);
        LOG(INFO) << "Release leadership: " << toString(err);
        auto current_view = leader_coordinator.ReadCurrentView();
        if (current_view) {
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             current_view.value());
        } else {
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates, std::nullopt);
        }
    }

    return 0;
}

}  // namespace

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

int MasterServiceSupervisor::Start() {
    auto spec = BuildHABackendSpec(config_);
    if (!spec) {
        LOG(ERROR) << "Failed to parse HA backend config: "
                   << toString(spec.error())
                   << ", backend_type=" << config_.ha_backend_type;
        return -1;
    }

    mooncake::MasterAdminServer admin_server(
        static_cast<uint16_t>(config_.metrics_port),
        config_.enable_metric_reporting);
    if (!admin_server.Start()) {
        LOG(ERROR) << "Failed to start master admin server, metrics_port="
                   << config_.metrics_port;
        return -1;
    }
    return RunSupervisorLoop(*spec, config_, admin_server);
}

}  // namespace ha
}  // namespace mooncake
