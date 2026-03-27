#include "ha/leadership/master_service_supervisor.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <thread>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ha/leadership/leader_coordinator_factory.h"
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

tl::expected<LeadershipSession, ErrorCode> AcquireLeadershipOrWait(
    LeaderCoordinator& coordinator, const std::string& leader_address) {
    while (true) {
        auto current_view = coordinator.ReadCurrentView();
        if (!current_view) {
            return tl::make_unexpected(current_view.error());
        }

        if (!current_view.value().has_value()) {
            auto acquire = coordinator.TryAcquireLeadership(leader_address);
            if (!acquire) {
                return tl::make_unexpected(acquire.error());
            }

            if (acquire->status == AcquireLeadershipStatus::ACQUIRED &&
                acquire->session.has_value()) {
                return *acquire->session;
            }

            std::optional<ViewVersionId> observed_version = std::nullopt;
            if (acquire->observed_view.has_value()) {
                observed_version = acquire->observed_view->view_version;
            }
            auto wait = coordinator.WaitForViewChange(observed_version,
                                                      kAcquireRetryInterval);
            if (!wait) {
                return tl::make_unexpected(wait.error());
            }
            continue;
        }

        const auto& known_view = current_view.value().value();
        auto wait = coordinator.WaitForViewChange(known_view.view_version,
                                                  kAcquireRetryInterval);
        if (!wait) {
            return tl::make_unexpected(wait.error());
        }
    }
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

    while (true) {
        LOG(INFO) << "Init master service...";
        coro_rpc::coro_rpc_server server(
            config_.rpc_thread_num, config_.rpc_port, config_.rpc_address,
            config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
        const char* protocol = std::getenv("MC_RPC_PROTOCOL");
        if (protocol && std::string_view(protocol) == "rdma") {
            server.init_ibv();
        }

        auto coordinator = CreateLeaderCoordinator(*spec);
        if (!coordinator) {
            if (HandleSupervisorError("create leader coordinator",
                                      coordinator.error(), spec->type)) {
                return -1;
            }
            continue;
        }

        auto& leader_coordinator = *coordinator.value();
        auto session =
            AcquireLeadershipOrWait(leader_coordinator, config_.local_hostname);
        if (!session) {
            if (HandleSupervisorError("acquire leadership", session.error(),
                                      spec->type)) {
                return -1;
            }
            continue;
        }
        LeadershipSession leadership_session = *session;

        LOG(INFO) << "Entering warmup phase...";
        auto warmup_result =
            WarmupLeadership(leader_coordinator, leadership_session);
        if (!warmup_result) {
            if (HandleLeadershipPhaseError(
                    "renewal startup failure", "start leadership renewal",
                    leader_coordinator, leadership_session,
                    warmup_result.error(), spec->type)) {
                return -1;
            }
            continue;
        }
        if (!warmup_result.value()) {
            LogLeadershipReleaseWarning(
                "warmup expiration",
                leader_coordinator.ReleaseLeadership(leadership_session));
            LOG(WARNING) << "Leadership expired during warmup phase";
            continue;
        }

        LOG(INFO) << "Starting serve phase...";
        mooncake::WrappedMasterService wrapped_master_service(
            mooncake::WrappedMasterServiceConfig(
                config_, leadership_session.view.view_version));
        mooncake::RegisterRpcService(server, wrapped_master_service);

        auto serve_preflight =
            leader_coordinator.RenewLeadership(leadership_session);
        if (!serve_preflight) {
            if (HandleLeadershipPhaseError(
                    "serve preflight failure",
                    "validate leadership before serving", leader_coordinator,
                    leadership_session, serve_preflight.error(), spec->type)) {
                return -1;
            }
            continue;
        }
        if (!serve_preflight.value()) {
            LogLeadershipReleaseWarning(
                "serve preflight expiration",
                leader_coordinator.ReleaseLeadership(leadership_session));
            LOG(WARNING) << "Leadership expired before entering serve phase";
            continue;
        }

        auto leadership_monitor = leader_coordinator.StartLeadershipMonitor(
            leadership_session, [&server](auto reason) {
                LOG(INFO) << "Trying to stop server, reason="
                          << LeadershipLossReasonToString(reason);
                server.stop();
            });
        if (!leadership_monitor) {
            if (HandleLeadershipPhaseError(
                    "serve monitor startup failure", "start leadership monitor",
                    leader_coordinator, leadership_session,
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
            auto err = leader_coordinator.ReleaseLeadership(leadership_session);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to release leadership: " << toString(err);
            }
            return -1;
        }

        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;

        leadership_monitor_handle->Stop();
        auto err = leader_coordinator.ReleaseLeadership(leadership_session);
        LOG(INFO) << "Release leadership: " << toString(err);
    }

    return 0;
}

}  // namespace ha
}  // namespace mooncake
