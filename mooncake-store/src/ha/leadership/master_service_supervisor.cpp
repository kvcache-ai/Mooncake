#include "ha/leadership/master_service_supervisor.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "config/rpc_protocol_config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/leadership/leader_label_reconciler.h"
#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_test_failpoint.h"
#include "ha/standby_controller.h"
#include "k8s_lease_helper.h"
#include "master_admin_service.h"
#include "rpc_service.h"
#include "rpc_worker_pool.h"
#include "shutdown_request.h"

namespace mooncake {
namespace ha {

namespace {

constexpr auto kAcquireRetryInterval = std::chrono::seconds(1);
constexpr auto kRenewCheckInterval = std::chrono::seconds(1);
constexpr auto kSupervisorRetryInterval = std::chrono::seconds(1);
constexpr auto kLabelReconcileRetryInterval = std::chrono::seconds(1);
constexpr auto kShutdownPollInterval = std::chrono::milliseconds(100);
constexpr char kLeaderLabelKey[] = "mooncake.io/store-role";
constexpr char kLeaderLabelValue[] = "leader";

bool HasPodIdentity(const MasterServiceSupervisorConfig& config) {
    return !config.pod_name.empty() && !config.pod_namespace.empty() &&
           config.ha_backend_type == "k8s";
}

LeaderLabelReconciler MakeLeaderLabelReconciler(
    const MasterServiceSupervisorConfig& config) {
    return LeaderLabelReconciler(
        HasPodIdentity(config),
        [ns = config.pod_namespace, pod = config.pod_name](bool desired) {
            return desired ? K8sLeaseHelper::SetPodLabel(
                                 ns, pod, kLeaderLabelKey, kLeaderLabelValue)
                           : K8sLeaseHelper::ClearPodLabel(ns, pod,
                                                           kLeaderLabelKey);
        },
        kLabelReconcileRetryInterval);
}

std::string ResolveHABackendConnstring(
    const MasterServiceSupervisorConfig& config) {
    return ResolveConfiguredHABackendConnstring(config.ha_backend_type,
                                                config.ha_backend_connstring,
                                                config.etcd_endpoints);
}

tl::expected<HABackendSpec, ErrorCode> BuildHABackendSpec(
    const MasterServiceSupervisorConfig& config) {
    auto backend_type = ParseHABackendType(config.ha_backend_type);
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    auto connstring = ResolveHABackendConnstring(config);
    if (connstring.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return HABackendSpec{
        .type = backend_type.value(),
        .connstring = connstring,
        .cluster_namespace = config.cluster_id,
        .leader_lease_ttl_sec = config.ha_leader_lease_ttl_sec,
    };
}

bool IsFatalHABackendError(ErrorCode err) {
    return err == ErrorCode::INVALID_PARAMS ||
           err == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

// Polls RenewLeadership on a side thread so a long Oplog catch-up cannot
// outlive the lease. Etcd keepalive starts on the first successful renew and
// keeps running after this guard stops; the guard only stops polling.
class LeadershipRenewalGuard {
   public:
    LeadershipRenewalGuard(LeaderCoordinator& coordinator,
                           const LeadershipSession& session)
        : coordinator_(coordinator) {
        worker_ = std::thread([this, &session] {
            bool logged_start = false;
            while (!stop_.load(std::memory_order_acquire)) {
                auto renewed = coordinator_.RenewLeadership(session);
                if (!renewed) {
                    LOG(WARNING)
                        << "Leadership renewal failed during recovery: "
                        << toString(renewed.error());
                    lost_.store(true, std::memory_order_release);
                    return;
                }
                if (!renewed.value()) {
                    LOG(WARNING) << "Leadership lease lost during recovery";
                    lost_.store(true, std::memory_order_release);
                    return;
                }
                if (!logged_start) {
                    logged_start = true;
                    LOG(INFO) << "Leadership keepalive covers oplog catch-up, "
                              << "lease_ttl_sec=" << session.lease_ttl.count();
                }
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_for(lock, kRenewCheckInterval, [this] {
                    return stop_.load(std::memory_order_acquire);
                });
            }
        });
    }

    ~LeadershipRenewalGuard() { Stop(); }

    LeadershipRenewalGuard(const LeadershipRenewalGuard&) = delete;
    LeadershipRenewalGuard& operator=(const LeadershipRenewalGuard&) = delete;

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_.store(true, std::memory_order_release);
        }
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    bool Lost() const { return lost_.load(std::memory_order_acquire); }

   private:
    LeaderCoordinator& coordinator_;
    std::thread worker_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> lost_{false};
};

// Every shutdown check goes through here. The first true observation
// latches the request, which starts the watchdog deadline before any
// shutdown work, and keeps later checks true even if an injected predicate
// turns false.
bool ShutdownObserved(const std::function<bool()>& shutdown_requested) {
    if (ShutdownLatched()) {
        return true;
    }
    if (!shutdown_requested()) {
        return false;
    }
    LatchShutdownRequest();
    return true;
}

// What the supervisor holds when a shutdown ends the process.
struct ShutdownLease {
    // Set while this process holds the lease; it is left to expire by TTL.
    const LeadershipSession* session = nullptr;
    // Result of an ordinary release that already ran for the session.
    std::optional<ErrorCode> prior_release{};
};

// Last step before the terminal exit. Returns the exit status.
int FinishShutdown(const ShutdownLease& lease) {
    if (lease.prior_release.has_value()) {
        LOG_IF(INFO, *lease.prior_release == ErrorCode::OK)
            << "Shutdown complete after leadership release, result=OK";
        LOG_IF(WARNING, *lease.prior_release != ErrorCode::OK)
            << "Shutdown complete after leadership release, result="
            << toString(*lease.prior_release);
        return 0;
    }
    if (lease.session != nullptr) {
        LOG(INFO) << "Shutdown complete, no release attempted; the lease is "
                     "left to TTL expiry, view_version="
                  << lease.session->view.view_version;
        return 0;
    }
    LOG(INFO) << "Shutdown complete, no lease held";
    return 0;
}

// Polls the shutdown request while the listener may run and fences the
// serving session through the gate, like a leadership loss.
class ServingShutdownTrigger {
   public:
    ServingShutdownTrigger(const std::function<bool()>& shutdown_requested,
                           std::function<void()> on_shutdown)
        : shutdown_requested_(shutdown_requested),
          on_shutdown_(std::move(on_shutdown)) {
        worker_ = std::thread([this] { Run(); });
    }

    ~ServingShutdownTrigger() { Stop(); }

    ServingShutdownTrigger(const ServingShutdownTrigger&) = delete;
    ServingShutdownTrigger& operator=(const ServingShutdownTrigger&) = delete;

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_.store(true, std::memory_order_release);
        }
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

   private:
    void Run() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stop_.load(std::memory_order_acquire)) {
            if (ShutdownObserved(shutdown_requested_)) {
                // on_shutdown_ runs with the mutex unlocked on purpose:
                // Stop() must be able to signal and join while the callback
                // is inside server.stop(). If that callback blocks, the
                // join in Stop() waits, and the watchdog is what bounds the
                // whole process, not this interlock.
                lock.unlock();
                on_shutdown_();
                return;
            }
            cv_.wait_for(lock, kShutdownPollInterval, [this] {
                return stop_.load(std::memory_order_acquire);
            });
        }
    }

    const std::function<bool()>& shutdown_requested_;
    std::function<void()> on_shutdown_;
    std::thread worker_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};
};

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
                                std::string_view action, ErrorCode release_err,
                                ErrorCode err, HABackendType backend_type) {
    LogLeadershipReleaseWarning(release_context, release_err);
    return HandleSupervisorError(action, err, backend_type);
}

tl::expected<bool, ErrorCode> WarmupLeadership(
    LeaderCoordinator& coordinator, const LeadershipSession& session,
    const std::function<bool()>& shutdown_requested) {
    const auto deadline = std::chrono::steady_clock::now() + session.lease_ttl;
    const auto max_sleep_interval =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            kRenewCheckInterval);

    while (true) {
        // The caller checks the request first and exits on a shutdown.
        if (ShutdownObserved(shutdown_requested)) {
            return true;
        }
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

void ActivateServingState(MasterAdminServer& admin_server,
                          const std::shared_ptr<WrappedMasterService>& service,
                          LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceDelegate(service);
    admin_server.SetServiceAvailable(true);
    SetRuntimeState(admin_server, MasterRuntimeState::kServing);
    label_reconciler.SetLeader(true);
}

void DeactivateServingState(MasterAdminServer& admin_server,
                            LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceAvailable(false);
    admin_server.SetServiceDelegate(nullptr);
    label_reconciler.SetLeader(false);
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

ErrorCode ClaimProducerViewForServing(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config,
    ViewVersionId view_version) {
    if (!config.enable_oplog || config.cluster_id.empty()) {
        return ErrorCode::OK;
    }
    if (!HaBackendSupportsOpLog(spec.type)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (view_version == 0) {
        return ErrorCode::INVALID_PARAMS;
    }

    // Fence the previous producer through the backend selected by spec, not
    // etcd alone. Redis already implements the transactional compare-and-swap
    // ClaimProducerView needs, so an old Redis primary must be fenced too.
    auto backend = CreateHaKvBackend(spec);
    if (!backend) {
        return backend.error();
    }
    OpLogBatchStorage storage(config.cluster_id, *backend.value());
    return storage.ClaimProducerView(view_version);
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
                      MasterAdminServer& admin_server,
                      std::unique_ptr<StandbyController> standby_controller,
                      const std::function<bool()>& shutdown_requested) {
    auto label_reconciler = MakeLeaderLabelReconciler(config);
    label_reconciler.SetLeader(false);
    SetRuntimeState(admin_server, MasterRuntimeState::kStarting);
    std::atomic<bool> accept_standby_runtime_updates{false};
    standby_controller->SetStandbyRuntimeStateCallback(
        [&](MasterRuntimeState state) {
            if (!accept_standby_runtime_updates.load(
                    std::memory_order_acquire)) {
                return;
            }
            SetRuntimeState(admin_server, state);
        });

    // Every supervisor-selected shutdown ends here without unwinding.
    // Returning would destroy the per-iteration coordinator, and the etcd
    // coordinator's destructor revokes a held lease.
    auto shutdown_exit = [&](std::string_view phase,
                             const ShutdownLease& lease) -> int {
        LOG(INFO) << "Shutdown requested, finishing from phase=" << phase;
        admin_server.Stop();
        accept_standby_runtime_updates.store(false, std::memory_order_release);
        standby_controller->StopStandby();
        ExitAfterShutdown(FinishShutdown(lease));
    };
    // Result of the ordinary release that ended the previous term, for a
    // request observed before the next standby check.
    std::optional<ErrorCode> released_last_term;

    if (ShutdownObserved(shutdown_requested)) {
        return shutdown_exit("starting", ShutdownLease{});
    }
    EnterStandbyMode(admin_server, *standby_controller,
                     accept_standby_runtime_updates, std::nullopt);

    while (true) {
        if (ShutdownObserved(shutdown_requested)) {
            return shutdown_exit(
                "standby", ShutdownLease{.prior_release = released_last_term});
        }
        released_last_term.reset();
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
            if (ShutdownObserved(shutdown_requested)) {
                return shutdown_exit("candidate", ShutdownLease{});
            }
            SetRuntimeState(admin_server, MasterRuntimeState::kCandidate);

            auto current_view = leader_coordinator.ReadCurrentView();
            if (ShutdownObserved(shutdown_requested)) {
                return shutdown_exit("candidate", ShutdownLease{});
            }
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
                if (ShutdownObserved(shutdown_requested)) {
                    return shutdown_exit("candidate", ShutdownLease{});
                }
                auto acquire = leader_coordinator.TryAcquireLeadership(
                    config.local_hostname);
                if (!acquire) {
                    if (ShutdownObserved(shutdown_requested)) {
                        return shutdown_exit("candidate", ShutdownLease{});
                    }
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

                if (ShutdownObserved(shutdown_requested)) {
                    return shutdown_exit("candidate", ShutdownLease{});
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

        ShutdownLease term_lease{.session = &*leadership_session};
        if (ShutdownObserved(shutdown_requested)) {
            return shutdown_exit("acquired", term_lease);
        }
        // The ordinary release that ends a failed or lost term. A request
        // observed before it skips the release; one observed after it keeps
        // the result, so shutdown neither repeats it nor reports no lease.
        auto release_term = [&](std::string_view phase) -> ErrorCode {
            if (ShutdownObserved(shutdown_requested)) {
                shutdown_exit(phase, term_lease);  // Does not return.
            }
            const ErrorCode err =
                leader_coordinator.ReleaseLeadership(*leadership_session);
            term_lease = ShutdownLease{.prior_release = err};
            released_last_term = err;
            TestFailPoint::Wait("supervisor_after_term_release");
            if (ShutdownObserved(shutdown_requested)) {
                shutdown_exit(phase, term_lease);  // Does not return.
            }
            return err;
        };

        // Start the lease keepalive before catch-up. The grant happens in
        // TryAcquireLeadership, but etcd KeepAlive previously started only in
        // the warmup phase, so a large Oplog replay expired the lease first.
        LeadershipRenewalGuard renewal(leader_coordinator, *leadership_session);

        auto claim_error = ClaimProducerViewForServing(
            spec, config, leadership_session->view.view_version);
        if (ShutdownObserved(shutdown_requested)) {
            return shutdown_exit("claim", term_lease);
        }
        if (claim_error != ErrorCode::OK) {
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "producer view claim failure", "claim producer view",
                    release_term("claim"), claim_error, spec.type)) {
                return -1;
            }
            continue;
        }

        accept_standby_runtime_updates.store(false, std::memory_order_release);
        auto promotion_ctx = standby_controller->PromoteStandbyAndExport();
        if (ShutdownObserved(shutdown_requested)) {
            return shutdown_exit("promotion", term_lease);
        }
        if (!promotion_ctx) {
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             leadership_session->view);
            if (HandleLeadershipPhaseError("standby promotion failure",
                                           "promote standby for serve",
                                           release_term("promotion"),
                                           promotion_ctx.error(), spec.type)) {
                return -1;
            }
            continue;
        }

        auto return_to_standby =
            [&](const std::optional<MasterView>& leader_view) {
                if (ShutdownObserved(shutdown_requested)) {
                    shutdown_exit("standby_reentry",
                                  term_lease);  // Does not return.
                }
                if (promotion_ctx->metadata_store &&
                    promotion_ctx->applied_cursor.last_seq != 0) {
                    const uint64_t applied_seq =
                        promotion_ctx->applied_cursor.last_seq;
                    auto restore_err =
                        standby_controller->RestorePromotionBaseline(
                            std::move(*promotion_ctx));
                    if (restore_err != ErrorCode::OK) {
                        LOG(ERROR)
                            << "Failed to preserve catch-up cursor seq="
                            << applied_seq << ": " << toString(restore_err);
                    } else {
                        LOG(INFO) << "Preserved catch-up cursor for retry, "
                                  << "applied_seq=" << applied_seq;
                    }
                }
                EnterStandbyMode(admin_server, *standby_controller,
                                 accept_standby_runtime_updates, leader_view);
            };

        if (renewal.Lost()) {
            LOG(WARNING) << "Leadership expired during oplog catch-up, "
                         << "applied_seq="
                         << promotion_ctx->applied_cursor.last_seq;
            return_to_standby(std::nullopt);
            LogLeadershipReleaseWarning("catch-up lease loss",
                                        release_term("catch_up_loss"));
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        LOG(INFO) << "Entering warmup phase...";
        SetRuntimeState(admin_server, MasterRuntimeState::kLeaderWarmup);
        auto warmup_result = WarmupLeadership(
            leader_coordinator, *leadership_session, shutdown_requested);
        if (ShutdownObserved(shutdown_requested)) {
            return shutdown_exit("warmup", term_lease);
        }
        if (!warmup_result) {
            return_to_standby(leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "renewal startup failure", "start leadership renewal",
                    release_term("warmup"), warmup_result.error(), spec.type)) {
                return -1;
            }
            continue;
        }
        if (!warmup_result.value()) {
            return_to_standby(std::nullopt);
            LogLeadershipReleaseWarning("warmup expiration",
                                        release_term("warmup_expiry"));
            LOG(WARNING) << "Leadership expired during warmup phase";
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        LOG(INFO) << "Starting serve phase...";
        coro_rpc::coro_rpc_server server(
            config.rpc_thread_num, config.rpc_port, config.rpc_address,
            config.rpc_conn_timeout, config.rpc_enable_tcp_no_delay);
        if (RpcProtocolConfig::FromEnvironment().use_rdma) {
#ifdef YLT_ENABLE_IBV
            server.init_ibv();
#else
            LOG(WARNING)
                << "RDMA RPC is disabled at compile time; using TCP RPC";
#endif
        }

        mooncake::WrappedMasterServiceConfig wrapped_config(
            config, leadership_session->view.view_version);
        // The serving primary handles heartbeats/unmounts, so forward the
        // metadata cleanup config here like the non-HA path does.
        // Keep the gate alive until after the service is destroyed because the
        // OpLog writer owned by the service invokes a callback that uses it.
        detail::ServingStateGate serving_state;
        auto wrapped_master_service = std::make_shared<WrappedMasterService>(
            wrapped_config, config.http_metadata_server,
            config.http_metadata_remote_url);
        // Runs the term's handlers once they are registered. It drains before
        // the service goes, so release the service through release_service.
        std::unique_ptr<RpcWorkerPool> worker_pool;
        // Stops the term's resources that exist, in the serving-exit order,
        // and keeps the service, the index and the gate alive through the
        // exit. Each step is a no-op on a resource not yet created or
        // already stopped.
        auto stop_term_resources =
            [&](std::unique_ptr<LeadershipMonitorHandle>& monitor) {
                worker_pool.reset();
                StopLeadershipMonitor(monitor);
                wrapped_master_service->StopBatchOpLogWriter();
                DeactivateServingState(admin_server, label_reconciler);
                LOG(INFO) << "Shutdown stopped the term's workers, monitor "
                             "and OpLog writer";
            };
        auto release_service = [&]() {
            if (ShutdownObserved(shutdown_requested)) {
                std::unique_ptr<LeadershipMonitorHandle> no_monitor;
                stop_term_resources(no_monitor);
                shutdown_exit("service_release",
                              term_lease);  // Does not return.
            }
            worker_pool.reset();
            wrapped_master_service.reset();
        };
        wrapped_master_service->SetBatchOpLogTerminalCallback(
            [&](const OrderedOpLogWriterTerminalState& state) {
                serving_state.RequestShutdown([&]() {
                    LOG(ERROR) << "Batch OpLog writer terminal: "
                               << toString(state.error);
                    DeactivateServingState(admin_server, label_reconciler);
                    SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
                    server.stop();
                });
            });

        // Restore is the serving gate: do not register or expose a candidate
        // service until the complete promotion context has been applied.
        // Keep the handoff until serving is established so restore failure
        // or a later lease loss can still resume from the applied cursor.
        SetRuntimeState(admin_server, MasterRuntimeState::kRecovering);
        auto restore_result =
            promotion_ctx->metadata_store
                ? wrapped_master_service->RestoreFromBatchOpLogPromotion(
                      *promotion_ctx->metadata_store,
                      promotion_ctx->applied_cursor, promotion_ctx->segments,
                      promotion_ctx->max_replica_id,
                      static_cast<size_t>(config.snapshot_chunk_object_count))
                : wrapped_master_service->RestoreFromStandby(
                      promotion_ctx->objects, promotion_ctx->applied_seq_id,
                      promotion_ctx->segments, promotion_ctx->weight_metadata);
        if (ShutdownObserved(shutdown_requested)) {
            std::unique_ptr<LeadershipMonitorHandle> no_monitor;
            stop_term_resources(no_monitor);
            return shutdown_exit("restore", term_lease);
        }
        if (!restore_result) {
            LOG(ERROR) << "Standby restore failed: "
                       << toString(restore_result.error());
            release_service();
            DeactivateServingState(admin_server, label_reconciler);
            return_to_standby(leadership_session->view);
            SetRuntimeState(admin_server, MasterRuntimeState::kRecovering);
            if (HandleLeadershipPhaseError("standby restore failure",
                                           "restore standby state",
                                           release_term("restore"),
                                           restore_result.error(), spec.type)) {
                return -1;
            }
            continue;
        }

        // Restore can outlast catch-up. Recheck the side-thread guard and
        // renew once more before registering RPC or publishing a serving
        // endpoint, otherwise a lease lost mid-restore keeps recovering.
        if (renewal.Lost()) {
            LOG(WARNING) << "Leadership expired during restore, applied_seq="
                         << promotion_ctx->applied_cursor.last_seq;
            release_service();
            DeactivateServingState(admin_server, label_reconciler);
            return_to_standby(std::nullopt);
            LogLeadershipReleaseWarning("restore lease loss",
                                        release_term("restore_loss"));
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        SetRuntimeState(admin_server, MasterRuntimeState::kLeaderWarmup);

        auto serve_preflight =
            leader_coordinator.RenewLeadership(*leadership_session);
        if (ShutdownObserved(shutdown_requested)) {
            std::unique_ptr<LeadershipMonitorHandle> no_monitor;
            stop_term_resources(no_monitor);
            return shutdown_exit("preflight", term_lease);
        }
        if (!serve_preflight) {
            release_service();
            DeactivateServingState(admin_server, label_reconciler);
            return_to_standby(leadership_session->view);
            if (HandleLeadershipPhaseError("serve preflight failure",
                                           "validate leadership before serving",
                                           release_term("preflight"),
                                           serve_preflight.error(),
                                           spec.type)) {
                return -1;
            }
            continue;
        }
        if (!serve_preflight.value()) {
            release_service();
            DeactivateServingState(admin_server, label_reconciler);
            return_to_standby(std::nullopt);
            LogLeadershipReleaseWarning("serve preflight expiration",
                                        release_term("preflight_expiry"));
            LOG(WARNING) << "Leadership expired before entering serve phase";
            admin_server.SetObservedLeader(std::nullopt);
            continue;
        }

        worker_pool = std::make_unique<RpcWorkerPool>(
            config.handler_worker_thread_num != 0
                ? config.handler_worker_thread_num
                : config.rpc_thread_num);
        mooncake::RegisterRpcService(server, *wrapped_master_service,
                                     worker_pool.get());

        auto leadership_monitor = leader_coordinator.StartLeadershipMonitor(
            *leadership_session, [&server, &admin_server, &serving_state,
                                  &label_reconciler](auto reason) {
                serving_state.RequestShutdown([&]() {
                    admin_server.SetServiceAvailable(false);
                    label_reconciler.SetLeader(false);
                    SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
                    LOG(INFO) << "Trying to stop server, reason="
                              << LeadershipLossReasonToString(reason);
                    server.stop();
                });
            });
        if (ShutdownObserved(shutdown_requested)) {
            std::unique_ptr<LeadershipMonitorHandle> started_monitor;
            if (leadership_monitor) {
                started_monitor = std::move(leadership_monitor.value());
            }
            stop_term_resources(started_monitor);
            return shutdown_exit("monitor_start", term_lease);
        }
        if (!leadership_monitor) {
            release_service();
            DeactivateServingState(admin_server, label_reconciler);
            return_to_standby(leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "serve monitor startup failure", "start leadership monitor",
                    release_term("monitor_start"), leadership_monitor.error(),
                    spec.type)) {
                return -1;
            }
            continue;
        }
        auto leadership_monitor_handle = std::move(leadership_monitor.value());

        ServingShutdownTrigger shutdown_trigger(shutdown_requested, [&]() {
            serving_state.RequestShutdown([&]() {
                admin_server.SetServiceAvailable(false);
                label_reconciler.SetLeader(false);
                SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
                LOG(INFO) << "Trying to stop server, reason=shutdown requested";
                server.stop();
            });
        });

        std::optional<async_simple::Future<coro_rpc::err_code>> ec;
        TestFailPoint::Wait("supervisor_before_serve_start");
        serving_state.RunIfActive([&]() {
            if (!ShutdownObserved(shutdown_requested)) {
                ec.emplace(server.async_start());
            }
        });
        if (!ec.has_value()) {
            shutdown_trigger.Stop();
            if (ShutdownObserved(shutdown_requested)) {
                stop_term_resources(leadership_monitor_handle);
                return shutdown_exit("serve_start", term_lease);
            }
            StopLeadershipMonitor(leadership_monitor_handle);
            release_service();
            DeactivateServingState(admin_server, label_reconciler);
            return_to_standby(std::nullopt);
            LogLeadershipReleaseWarning("serve startup cancellation",
                                        release_term("serve_start_cancel"));
            continue;
        }
        if (ec->hasResult()) {
            shutdown_trigger.Stop();
            LOG(ERROR) << "Failed to start master service: "
                       << ec->result().value();
            if (ShutdownObserved(shutdown_requested)) {
                stop_term_resources(leadership_monitor_handle);
                return shutdown_exit("serve_start_failure", term_lease);
            }
            StopLeadershipMonitor(leadership_monitor_handle);
            release_service();
            DeactivateServingState(admin_server, label_reconciler);
            return_to_standby(leadership_session->view);
            auto err = release_term("serve_start_failure");
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to release leadership: " << toString(err);
            }
            return -1;
        }

        ErrorCode publish_ready_err = ErrorCode::OK;
        if (serving_state.IsActive() && !ShutdownObserved(shutdown_requested)) {
            // The backend call can take several seconds. Keep it outside the
            // transition gate so leadership loss can stop the listener
            // promptly.
            publish_ready_err =
                leader_coordinator.PublishServiceReady(*leadership_session);
            if (publish_ready_err == ErrorCode::OK) {
                TestFailPoint::Wait("supervisor_before_serve_activation");
                serving_state.RunIfActive([&]() {
                    if (ShutdownObserved(shutdown_requested)) {
                        return;
                    }
                    ActivateServingState(admin_server, wrapped_master_service,
                                         label_reconciler);
                    // Serving owns the restored index now; drop the handoff
                    // so the standby copy is not held for the whole term.
                    promotion_ctx->metadata_store.reset();
                    promotion_ctx->objects.clear();
                    promotion_ctx->segments.clear();
                });
            } else {
                serving_state.RequestShutdown([&]() {
                    DeactivateServingState(admin_server, label_reconciler);
                    server.stop();
                });
            }
        }

        if (publish_ready_err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to publish master service readiness: "
                       << toString(publish_ready_err);
            StopLeadershipMonitor(leadership_monitor_handle);
            auto server_err = std::move(ec.value()).get();
            shutdown_trigger.Stop();
            LOG(ERROR) << "Master service stopped after readiness failure: "
                       << server_err;
            if (ShutdownObserved(shutdown_requested)) {
                stop_term_resources(leadership_monitor_handle);
                return shutdown_exit("readiness_failure", term_lease);
            }
            release_service();
            return_to_standby(leadership_session->view);
            if (HandleLeadershipPhaseError(
                    "service readiness publication failure",
                    "publish master service readiness",
                    release_term("readiness_failure"), publish_ready_err,
                    spec.type)) {
                return -1;
            }
            continue;
        }

        auto server_err = std::move(ec.value()).get();
        shutdown_trigger.Stop();
        // Finish the calls the workers still hold before the term's OpLog
        // writer and leadership go, as inline handlers would have.
        worker_pool.reset();
        LOG(ERROR) << "Master service stopped: " << server_err;

        StopLeadershipMonitor(leadership_monitor_handle);
        wrapped_master_service->StopBatchOpLogWriter();
        DeactivateServingState(admin_server, label_reconciler);
        auto err = release_term("serving");
        LOG(INFO) << "Release leadership: " << toString(err);
        auto current_view = leader_coordinator.ReadCurrentView();
        release_service();
        if (ShutdownObserved(shutdown_requested)) {
            return shutdown_exit("standby_reentry", term_lease);
        }
        // Serving drops the handoff in ActivateServingState. If the listener
        // stopped before that, preserve the still-populated catch-up cursor.
        if (promotion_ctx->metadata_store) {
            return_to_standby(current_view ? current_view.value()
                                           : std::nullopt);
        } else if (current_view) {
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
    const MasterServiceSupervisorConfig& config,
    std::function<bool()> shutdown_requested)
    : config_(config), shutdown_requested_(std::move(shutdown_requested)) {}

int MasterServiceSupervisor::Start() {
    auto spec = BuildHABackendSpec(config_);
    if (!spec) {
        LOG(ERROR) << "Failed to parse HA backend config: "
                   << toString(spec.error())
                   << ", backend_type=" << config_.ha_backend_type;
        return -1;
    }

    std::unique_ptr<StandbyController> standby_controller;
    try {
        standby_controller = CreateStandbyController(*spec, config_);
    } catch (const std::exception& error) {
        LOG(ERROR) << "Standby dependency initialization failed: "
                   << error.what();
        return -1;
    }

    const auto metrics = config_.metrics.Get();
    mooncake::MasterAdminServer admin_server(
        static_cast<uint16_t>(metrics.port), metrics.enabled, metrics.host);
    if (!admin_server.Start()) {
        LOG(ERROR) << "Failed to start master admin server, metrics_port="
                   << metrics.port;
        return -1;
    }
    return RunSupervisorLoop(*spec, config_, admin_server,
                             std::move(standby_controller),
                             shutdown_requested_);
}

}  // namespace ha
}  // namespace mooncake
