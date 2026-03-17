#include "ha/master_service_supervisor.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <thread>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ha/ha_backend_factory.h"
#include "rpc_service.h"

namespace mooncake {
namespace ha {

namespace {

constexpr auto kAcquireRetryInterval = std::chrono::seconds(1);
constexpr auto kRenewCheckInterval = std::chrono::seconds(1);

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

            auto wait = coordinator.WaitForViewChange(std::nullopt,
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

}  // namespace

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

int MasterServiceSupervisor::Start() {
    while (true) {
        LOG(INFO) << "Init master service...";
        coro_rpc::coro_rpc_server server(
            config_.rpc_thread_num, config_.rpc_port, config_.rpc_address,
            config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
        const char* protocol = std::getenv("MC_RPC_PROTOCOL");
        if (protocol && std::string_view(protocol) == "rdma") {
            server.init_ibv();
        }

        HABackendSpec spec{
            .type = HABackendType::ETCD,
            .connstring = config_.etcd_endpoints,
            .cluster_namespace = config_.cluster_id,
        };
        auto coordinator = CreateLeaderCoordinator(spec);
        if (!coordinator) {
            LOG(ERROR) << "Failed to create leader coordinator: "
                       << toString(coordinator.error());
            return -1;
        }

        auto session = AcquireLeadershipOrWait(*coordinator.value(),
                                               config_.local_hostname);
        if (!session) {
            LOG(ERROR) << "Failed to acquire leadership: "
                       << toString(session.error());
            return -1;
        }
        LeadershipSession leadership_session = *session;

        auto renew_result =
            coordinator.value()->RenewLeadership(leadership_session);
        if (!renew_result) {
            LOG(ERROR) << "Failed to start leadership renewal: "
                       << toString(renew_result.error());
            return -1;
        }
        if (!renew_result.value()) {
            LOG(ERROR) << "Leadership expired before service start";
            continue;
        }

        auto keep_leader_thread =
            std::thread([&server, &coordinator, leadership_session]() {
                while (true) {
                    auto renewed = coordinator.value()->RenewLeadership(
                        leadership_session);
                    if (!renewed) {
                        LOG(ERROR) << "Leadership renewal failed: "
                                   << toString(renewed.error());
                        break;
                    }
                    if (!renewed.value()) {
                        LOG(INFO) << "Leadership is no longer valid";
                        break;
                    }
                    std::this_thread::sleep_for(kRenewCheckInterval);
                }
                LOG(INFO) << "Trying to stop server...";
                server.stop();
            });

        // Preserve the old safety window before serving requests, so this
        // master does not start immediately after taking over leadership.
        std::this_thread::sleep_for(leadership_session.lease_ttl);

        LOG(INFO) << "Starting master service...";
        mooncake::WrappedMasterService wrapped_master_service(
            mooncake::WrappedMasterServiceConfig(
                config_, leadership_session.view.view_version));
        mooncake::RegisterRpcService(server, wrapped_master_service);

        async_simple::Future<coro_rpc::err_code> ec = server.async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            auto err =
                coordinator.value()->ReleaseLeadership(leadership_session);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to release leadership: " << toString(err);
            }
            keep_leader_thread.join();
            return -1;
        }

        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;

        auto err = coordinator.value()->ReleaseLeadership(leadership_session);
        LOG(INFO) << "Release leadership: " << toString(err);
        keep_leader_thread.join();
    }

    return 0;
}

}  // namespace ha
}  // namespace mooncake
