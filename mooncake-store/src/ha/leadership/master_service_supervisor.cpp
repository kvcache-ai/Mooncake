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
#include "ha/replication_controller.h"
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
        return -1;
    }

    auto set_runtime_state = [&admin_server](MasterRuntimeState state) {
        admin_server.SetRuntimeState(state);
        LOG(INFO) << "Master runtime state -> "
                  << MasterRuntimeStateToString(state)
                  << ", role=" << MasterRuntimeRoleToString(state);
    };

    set_runtime_state(MasterRuntimeState::kStarting);
    auto replication_controller = CreateReplicationController(*spec, config_);
    std::atomic<bool> accept_standby_runtime_updates{false};
    replication_controller->SetStandbyRuntimeStateCallback(
        [&](MasterRuntimeState state) {
            if (!accept_standby_runtime_updates.load(
                    std::memory_order_acquire)) {
                return;
            }
            set_runtime_state(state);
        });
    auto enter_standby_mode =
        [&](const std::optional<MasterView>& leader_view) {
            accept_standby_runtime_updates.store(true,
                                                 std::memory_order_release);
            admin_server.SetObservedLeader(leader_view);
            replication_controller->UpdateObservedLeader(leader_view);

            auto err = replication_controller->StartStandby(leader_view);
            if (err != ErrorCode::OK) {
                LOG(WARNING)
                    << "Failed to start standby replication: " << toString(err);
                set_runtime_state(MasterRuntimeState::kStandby);
                return;
            }

            set_runtime_state(replication_controller->GetStandbyRuntimeState());
        };

    enter_standby_mode(std::nullopt);

    while (true) {
        auto coordinator = CreateLeaderCoordinator(*spec);
        if (!coordinator) {
            if (HandleSupervisorError("create leader coordinator",
                                      coordinator.error(), spec->type)) {
                return -1;
            }
            continue;
        }

        auto& leader_coordinator = *coordinator.value();
        std::optional<LeadershipSession> leadership_session;

        while (!leadership_session.has_value()) {
            set_runtime_state(MasterRuntimeState::kCandidate);

            auto current_view = leader_coordinator.ReadCurrentView();
            if (!current_view) {
                enter_standby_mode(std::nullopt);
                if (HandleSupervisorError("read current leader view",
                                          current_view.error(), spec->type)) {
                    return -1;
                }
                break;
            }

            admin_server.SetObservedLeader(current_view.value());
            replication_controller->UpdateObservedLeader(current_view.value());
            if (!current_view.value().has_value()) {
                auto acquire = leader_coordinator.TryAcquireLeadership(
                    config_.local_hostname);
                if (!acquire) {
                    enter_standby_mode(std::nullopt);
                    if (HandleSupervisorError("acquire leadership",
                                              acquire.error(), spec->type)) {
                        return -1;
                    }
                    break;
                }

                if (acquire->observed_view.has_value()) {
                    admin_server.SetObservedLeader(acquire->observed_view);
                    replication_controller->UpdateObservedLeader(
                        acquire->observed_view);
                }

                if (acquire->status == AcquireLeadershipStatus::ACQUIRED &&
                    acquire->session.has_value()) {
                    leadership_session = *acquire->session;
                    admin_server.SetObservedLeader(leadership_session->view);
                    break;
                }

                enter_standby_mode(acquire->observed_view);
                std::optional<ViewVersionId> observed_version = std::nullopt;
                if (acquire->observed_view.has_value()) {
                    observed_version = acquire->observed_view->view_version;
                }
                auto wait = leader_coordinator.WaitForViewChange(
                    observed_version, kAcquireRetryInterval);
                if (!wait) {
                    if (HandleSupervisorError("wait for leader view change",
                                              wait.error(), spec->type)) {
                        return -1;
                    }
                    break;
                }
                if (wait->current_view.has_value() ||
                    (!wait->changed && !wait->timed_out)) {
                    admin_server.SetObservedLeader(wait->current_view);
                    replication_controller->UpdateObservedLeader(
                        wait->current_view);
                }
                continue;
            }

            enter_standby_mode(current_view.value());
            const auto& known_view = current_view.value().value();
            auto wait = leader_coordinator.WaitForViewChange(
                known_view.view_version, kAcquireRetryInterval);
            if (!wait) {
                if (HandleSupervisorError("wait for leader view change",
                                          wait.error(), spec->type)) {
                    return -1;
                }
                break;
            }
            if (wait->current_view.has_value() ||
                (!wait->changed && !wait->timed_out)) {
                admin_server.SetObservedLeader(wait->current_view);
                replication_controller->UpdateObservedLeader(
                    wait->current_view);
            }
        }

        if (!leadership_session.has_value()) {
            continue;
        }

        accept_standby_runtime_updates.store(false, std::memory_order_release);
        auto promote_standby = replication_controller->PromoteStandby();
        if (promote_standby != ErrorCode::OK) {
            enter_standby_mode(leadership_session->view);
            if (HandleSupervisorError("promote standby for serve",
                                      promote_standby, spec->type)) {
                return -1;
            }
            continue;
        }

        LOG(INFO) << "Entering warmup phase...";
        set_runtime_state(MasterRuntimeState::kLeaderWarmup);
        auto warmup_result =
            WarmupLeadership(leader_coordinator, *leadership_session);
        if (!warmup_result) {
            enter_standby_mode(leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "renewal startup failure", "start leadership renewal",
                    leader_coordinator, *leadership_session,
                    warmup_result.error(), spec->type)) {
                return -1;
            }
            continue;
        }
        if (!warmup_result.value()) {
            enter_standby_mode(std::nullopt);
            LogLeadershipReleaseWarning(
                "warmup expiration",
                leader_coordinator.ReleaseLeadership(*leadership_session));
            LOG(WARNING) << "Leadership expired during warmup phase";
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        LOG(INFO) << "Starting serve phase...";
        coro_rpc::coro_rpc_server server(
            config_.rpc_thread_num, config_.rpc_port, config_.rpc_address,
            config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
        const char* protocol = std::getenv("MC_RPC_PROTOCOL");
        if (protocol && std::string_view(protocol) == "rdma") {
            server.init_ibv();
        }

        auto wrapped_master_service = std::make_shared<WrappedMasterService>(
            mooncake::WrappedMasterServiceConfig(
                config_, leadership_session->view.view_version));
        mooncake::RegisterRpcService(server, *wrapped_master_service);
        admin_server.SetServiceDelegate(wrapped_master_service);
        admin_server.SetServiceAvailable(true);
        set_runtime_state(MasterRuntimeState::kServing);

        auto serve_preflight =
            leader_coordinator.RenewLeadership(*leadership_session);
        if (!serve_preflight) {
            admin_server.SetServiceAvailable(false);
            admin_server.SetServiceDelegate(nullptr);
            enter_standby_mode(leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "serve preflight failure",
                    "validate leadership before serving", leader_coordinator,
                    *leadership_session, serve_preflight.error(), spec->type)) {
                return -1;
            }
            continue;
        }
        if (!serve_preflight.value()) {
            admin_server.SetServiceAvailable(false);
            admin_server.SetServiceDelegate(nullptr);
            enter_standby_mode(std::nullopt);
            LogLeadershipReleaseWarning(
                "serve preflight expiration",
                leader_coordinator.ReleaseLeadership(*leadership_session));
            LOG(WARNING) << "Leadership expired before entering serve phase";
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        auto leadership_monitor = leader_coordinator.StartLeadershipMonitor(
            *leadership_session, [&server, &admin_server](auto reason) {
                admin_server.SetServiceAvailable(false);
                admin_server.SetRuntimeState(MasterRuntimeState::kStandby);
                LOG(INFO) << "Trying to stop server, reason="
                          << LeadershipLossReasonToString(reason);
                server.stop();
            });
        if (!leadership_monitor) {
            admin_server.SetServiceAvailable(false);
            admin_server.SetServiceDelegate(nullptr);
            enter_standby_mode(leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "serve monitor startup failure", "start leadership monitor",
                    leader_coordinator, *leadership_session,
                    leadership_monitor.error(), spec->type)) {
                return -1;
            }
            continue;
        }
        auto leadership_monitor_handle = std::move(leadership_monitor.value());

        async_simple::Future<coro_rpc::err_code> ec = server.async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            admin_server.SetServiceAvailable(false);
            admin_server.SetServiceDelegate(nullptr);
            enter_standby_mode(leadership_session->view);
            auto err =
                leader_coordinator.ReleaseLeadership(*leadership_session);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to release leadership: " << toString(err);
            }
            return -1;
        }

        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;

        admin_server.SetServiceAvailable(false);
        admin_server.SetServiceDelegate(nullptr);
        leadership_monitor_handle->Stop();
        auto err = leader_coordinator.ReleaseLeadership(*leadership_session);
        LOG(INFO) << "Release leadership: " << toString(err);
        auto current_view = leader_coordinator.ReadCurrentView();
        if (current_view) {
            enter_standby_mode(current_view.value());
        } else {
            enter_standby_mode(std::nullopt);
        }
    }

    return 0;
}

}  // namespace ha
}  // namespace mooncake
