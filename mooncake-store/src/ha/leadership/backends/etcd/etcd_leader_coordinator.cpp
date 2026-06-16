#include "ha/leadership/backends/etcd/etcd_leader_coordinator.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <thread>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace etcd {

namespace {

constexpr int kKeepAliveReadyTimeoutMs = 1000;
// Fallback poll interval used only when a watch cannot be armed (e.g. the watch
// RPC fails, or the remaining timeout is too small to be worth a watch). The
// steady-state path blocks on an etcd watch and does not poll.
constexpr auto kViewChangeFallbackPollInterval = std::chrono::milliseconds(200);
// Upper bound for waiting on the watch goroutine to fully stop before the
// watch context is destroyed. Mirrors the value used by the oplog notifier.
constexpr int kWatchStopTimeoutMs = 5000;

// Shared state between WaitForViewChange and the etcd watch callback. The
// callback only flips `changed` and wakes the waiter; the waiter decides what
// the change means by re-reading the view.
struct ViewChangeWatchState {
    std::mutex mutex;
    std::condition_variable cv;
    bool changed = false;
};

// C-style trampoline invoked by the etcd watch goroutine for every event on the
// watched prefix (PUT / DELETE / WATCH_BROKEN). Any event is treated as "the
// view may have changed"; the waiter re-reads to find out what actually
// happened. The watch context outlives every callback because the waiter calls
// CancelWatchWithPrefix + WaitWatchWithPrefixStopped before it is destroyed.
void ViewChangeWatchCallback(void* context, const char* /*key*/,
                             size_t /*key_size*/, const char* /*value*/,
                             size_t /*value_size*/, int /*event_type*/,
                             int64_t /*mod_revision*/) {
    auto* state = static_cast<ViewChangeWatchState*>(context);
    if (state == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->changed = true;
    }
    state->cv.notify_all();
}

// RAII guard that tears the prefix watch down (and waits for the goroutine to
// exit, so no further callbacks can run) before the heap-allocated
// ViewChangeWatchState it owns is released.
//
// The watch state is HEAP-allocated (not stack-scoped) on purpose. The Go watch
// goroutine holds a raw pointer to it and may still invoke the callback until
// it has fully stopped. CancelWatchWithPrefix + WaitWatchWithPrefixStopped are
// used to drain it, but WaitWatchWithPrefixStopped can time out. If it does, a
// callback may still fire later, so freeing the state would be a
// use-after-free. Mirroring the oplog notifier's policy, the guard therefore
// only deletes the state when the stop wait SUCCEEDS; on timeout/failure it
// intentionally LEAKS the state (and logs a warning) to avoid UAF.
class PrefixWatchGuard {
   public:
    PrefixWatchGuard(std::string prefix, ViewChangeWatchState* state)
        : prefix_(std::move(prefix)), state_(state) {}
    ~PrefixWatchGuard() {
        if (!active_) {
            // Watch was never armed: no goroutine ever received the pointer, so
            // it is safe to free the state unconditionally.
            delete state_;
            return;
        }
        EtcdHelper::CancelWatchWithPrefix(prefix_.c_str(), prefix_.size());
        ErrorCode wait_err = EtcdHelper::WaitWatchWithPrefixStopped(
            prefix_.c_str(), prefix_.size(), kWatchStopTimeoutMs);
        if (wait_err == ErrorCode::OK) {
            // Goroutine has fully exited; no callback can be in flight.
            delete state_;
        } else {
            // The watch goroutine did not stop in time. It may still invoke the
            // callback with `state_`, so we intentionally leak the state to
            // avoid a use-after-free.
            LOG(WARNING) << "Prefix watch goroutine did not stop in time for "
                            "prefix "
                         << prefix_
                         << "; leaking ViewChangeWatchState to avoid UAF";
        }
    }

    PrefixWatchGuard(const PrefixWatchGuard&) = delete;
    PrefixWatchGuard& operator=(const PrefixWatchGuard&) = delete;

    void Arm() { active_ = true; }

   private:
    std::string prefix_;
    ViewChangeWatchState* state_ = nullptr;
    bool active_ = false;
};

class EtcdLeadershipMonitorHandle final : public LeadershipMonitorHandle {
   public:
    explicit EtcdLeadershipMonitorHandle(
        std::shared_ptr<std::atomic<bool>> armed)
        : armed_(std::move(armed)) {}

    void Stop() override {
        if (armed_ != nullptr) {
            armed_->store(false);
        }
    }

   private:
    std::shared_ptr<std::atomic<bool>> armed_;
};

}  // namespace

EtcdLeaderCoordinator::EtcdLeaderCoordinator(const HABackendSpec& spec)
    : spec_(spec),
      master_view_key_(
          BuildMasterViewKey(ResolveClusterNamespace(spec.cluster_namespace))) {
}

EtcdLeaderCoordinator::~EtcdLeaderCoordinator() { ShutdownKeepAliveThread(); }

ErrorCode EtcdLeaderCoordinator::Connect() {
    if (connected_) {
        return ErrorCode::OK;
    }

    auto err = EtcdHelper::ConnectToEtcdStoreClient(spec_.connstring);
    if (err == ErrorCode::OK) {
        connected_ = true;
    }
    return err;
}

tl::expected<std::optional<MasterView>, ErrorCode>
EtcdLeaderCoordinator::ReadCurrentView() {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::string leader_address;
    ViewVersionId view_version = 0;
    err = EtcdHelper::Get(master_view_key_.c_str(), master_view_key_.size(),
                          leader_address, view_version);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return std::optional<MasterView>{std::nullopt};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    return std::optional<MasterView>{
        MasterView{.leader_address = std::move(leader_address),
                   .view_version = view_version}};
}

tl::expected<AcquireLeadershipResult, ErrorCode>
EtcdLeaderCoordinator::TryAcquireLeadership(const std::string& leader_address) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    EtcdLeaseId lease_id = 0;
    err = EtcdHelper::GrantLease(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC, lease_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    ViewVersionId view_version = 0;
    err = EtcdHelper::CreateWithLease(
        master_view_key_.c_str(), master_view_key_.size(),
        leader_address.c_str(), leader_address.size(), lease_id, view_version);
    if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
        auto revoke_err = EtcdHelper::RevokeLease(lease_id);
        if (revoke_err != ErrorCode::OK) {
            return tl::make_unexpected(revoke_err);
        }
        auto observed_view = ReadCurrentView();
        if (!observed_view) {
            return tl::make_unexpected(observed_view.error());
        }
        return AcquireLeadershipResult{
            .status = AcquireLeadershipStatus::CONTENDED,
            .session = std::nullopt,
            .observed_view = observed_view.value(),
        };
    }
    if (err != ErrorCode::OK) {
        auto revoke_err = EtcdHelper::RevokeLease(lease_id);
        if (revoke_err != ErrorCode::OK) {
            return tl::make_unexpected(revoke_err);
        }
        return tl::make_unexpected(err);
    }

    LeadershipSession session{
        .view = MasterView{.leader_address = leader_address,
                           .view_version = view_version},
        .owner_token = MakeOwnerToken(lease_id),
        .lease_ttl = std::chrono::seconds(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC),
    };

    {
        std::lock_guard<std::mutex> lock(keepalive_mutex_);
        keepalive_owner_token_ = session.owner_token;
        keepalive_lease_id_ = lease_id;
        keepalive_stopped_ = false;
        keepalive_result_ = ErrorCode::OK;
        keepalive_shutdown_requested_ = false;
        ClearLeadershipMonitorStateLocked();
    }

    return AcquireLeadershipResult{
        .status = AcquireLeadershipStatus::ACQUIRED,
        .session = std::move(session),
        .observed_view = std::nullopt,
    };
}

tl::expected<bool, ErrorCode> EtcdLeaderCoordinator::RenewLeadership(
    const LeadershipSession& session) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    auto lease_id = ParseLeaseId(session.owner_token);
    if (!lease_id) {
        return tl::make_unexpected(lease_id.error());
    }

    // The current etcd wrapper only exposes a blocking KeepAlive loop, not a
    // one-shot renew RPC. Keep the adapter local to the etcd implementation so
    // the public LeaderCoordinator contract can still stay renew-oriented.
    std::thread thread_to_join;
    bool should_return_false = false;
    bool started_keepalive = false;
    {
        std::lock_guard<std::mutex> lock(keepalive_mutex_);
        if (keepalive_shutdown_requested_) {
            return false;
        }

        if (keepalive_thread_.joinable()) {
            if (keepalive_owner_token_ != session.owner_token) {
                return false;
            }

            if (keepalive_stopped_) {
                thread_to_join = std::move(keepalive_thread_);
                keepalive_owner_token_.clear();
                keepalive_lease_id_ = 0;
                ClearLeadershipMonitorStateLocked();
                should_return_false = true;
            } else {
                return true;
            }
        } else {
            keepalive_owner_token_ = session.owner_token;
            keepalive_lease_id_ = lease_id.value();
            keepalive_stopped_ = false;
            keepalive_result_ = ErrorCode::OK;
            started_keepalive = true;
            keepalive_thread_ = std::thread([this, lease = lease_id.value()]() {
                auto rc = EtcdHelper::KeepAlive(lease);
                std::shared_ptr<std::atomic<bool>> monitor_armed;
                LeadershipLostCallback on_leadership_lost;
                LeadershipLossReason loss_reason =
                    ClassifyLeadershipLossReason(rc);
                {
                    std::lock_guard<std::mutex> lock(keepalive_mutex_);
                    keepalive_result_ = rc;
                    keepalive_stopped_ = true;
                    if (!keepalive_shutdown_requested_ &&
                        keepalive_owner_token_ ==
                            leadership_monitor_owner_token_) {
                        monitor_armed = leadership_monitor_armed_;
                        on_leadership_lost =
                            std::move(leadership_monitor_callback_);
                        leadership_monitor_armed_.reset();
                        leadership_monitor_owner_token_.clear();
                    }
                }

                if (monitor_armed != nullptr &&
                    monitor_armed->exchange(false) &&
                    on_leadership_lost != nullptr) {
                    on_leadership_lost(loss_reason);
                }
            });
        }
    }

    if (started_keepalive) {
        auto ready_err = EtcdHelper::WaitKeepAliveReady(
            lease_id.value(), kKeepAliveReadyTimeoutMs);
        if (ready_err != ErrorCode::OK) {
            return tl::make_unexpected(ready_err);
        }
        return true;
    }

    if (thread_to_join.joinable()) {
        thread_to_join.join();
    }

    return !should_return_false;
}

tl::expected<ViewChangeResult, ErrorCode>
EtcdLeaderCoordinator::WaitForViewChange(
    std::optional<ViewVersionId> known_version,
    std::chrono::milliseconds timeout) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    // LIMITATION: prefix watches are keyed globally by prefix in the etcd Go
    // wrapper (storePrefixWatchCtx is a map<prefix, ...>). Because every waiter
    // here watches the same `master_view_key_` prefix, only a SINGLE waiter per
    // process per prefix is supported: a second concurrent WaitForViewChange on
    // the same prefix would collide on that registration. The intended
    // deployment model is one waiter per process/prefix, which holds for the
    // store client. Revisit (e.g. per-waiter watch IDs) if multiple
    // same-process clients ever need to watch the same master_view
    // concurrently.
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        if (timeout <= std::chrono::milliseconds::zero() ||
            std::chrono::steady_clock::now() >= deadline) {
            return ViewChangeResult{
                .changed = false,
                .timed_out = true,
                .current_view = std::nullopt,
            };
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());

        // Arm the watch BEFORE reading the view. The watch is established at
        // some etcd revision R_watch; the subsequent read observes a revision
        // R_read >= R_watch. Any change to master_view at revision C is then
        // caught by exactly one of the two: C < R_read (the read sees it) or
        // C >= R_watch (the watch delivers it), and the two ranges overlap, so
        // a change happening between read and watch cannot be missed. The watch
        // starts from the current revision (start_revision = 0), which also
        // avoids depending on a possibly-compacted historical revision.
        //
        // The watch state is heap-allocated and owned by `guard`. The guard
        // cancels the watch and waits for the goroutine to exit before
        // releasing the state in its destructor; if the goroutine fails to stop
        // in time it leaks the state instead of freeing it, so an in-flight
        // callback can never reference freed memory (see PrefixWatchGuard).
        auto* state = new ViewChangeWatchState();
        PrefixWatchGuard guard(master_view_key_, state);

        bool watching = false;
        if (remaining > kViewChangeFallbackPollInterval) {
            // Defensively clear any lingering watch on this key, then arm a new
            // one. Both calls are no-ops when nothing is registered.
            EtcdHelper::CancelWatchWithPrefix(master_view_key_.c_str(),
                                              master_view_key_.size());
            EtcdHelper::WaitWatchWithPrefixStopped(master_view_key_.c_str(),
                                                   master_view_key_.size(),
                                                   kWatchStopTimeoutMs);
            auto watch_err = EtcdHelper::WatchWithPrefixFromRevision(
                master_view_key_.c_str(), master_view_key_.size(),
                /*start_revision=*/0, state, &ViewChangeWatchCallback);
            if (watch_err == ErrorCode::OK) {
                watching = true;
                guard.Arm();
            }
        }

        auto current_view = ReadCurrentView();
        if (!current_view) {
            return tl::make_unexpected(current_view.error());
        }
        if (!IsSameViewVersion(current_view.value(), known_version)) {
            return ViewChangeResult{
                .changed = true,
                .timed_out = false,
                .current_view = current_view.value(),
            };
        }

        if (!watching) {
            // Could not arm a watch (RPC failed, or too little time left).
            // Fall back to a short poll so a change is still picked up.
            std::this_thread::sleep_for(
                std::min(kViewChangeFallbackPollInterval, remaining));
            continue;
        }

        // Block until the watch reports an event or the caller's deadline
        // elapses. Either way we loop and re-read: an event tells us the view
        // changed (re-read returns it), a timeout falls through to the deadline
        // check above and returns timed_out.
        std::unique_lock<std::mutex> lock(state->mutex);
        state->cv.wait_for(lock, remaining,
                           [state]() { return state->changed; });
    }
}

tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
EtcdLeaderCoordinator::StartLeadershipMonitor(
    const LeadershipSession& session,
    LeadershipLostCallback on_leadership_lost) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    if (!on_leadership_lost) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(keepalive_mutex_);
    if (keepalive_shutdown_requested_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (keepalive_owner_token_ != session.owner_token ||
        !keepalive_thread_.joinable() || keepalive_stopped_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (leadership_monitor_armed_ != nullptr &&
        leadership_monitor_armed_->load()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    leadership_monitor_owner_token_ = session.owner_token;
    leadership_monitor_callback_ = std::move(on_leadership_lost);
    leadership_monitor_armed_ = std::make_shared<std::atomic<bool>>(true);
    return std::unique_ptr<LeadershipMonitorHandle>(
        std::make_unique<EtcdLeadershipMonitorHandle>(
            leadership_monitor_armed_));
}

ErrorCode EtcdLeaderCoordinator::ReleaseLeadership(
    const LeadershipSession& session) {
    auto lease_id = ParseLeaseId(session.owner_token);
    if (!lease_id) {
        return lease_id.error();
    }

    std::thread thread_to_join;
    EtcdLeaseId lease_id_to_cancel = lease_id.value();
    bool should_cancel = false;
    bool should_join = false;
    {
        std::lock_guard<std::mutex> lock(keepalive_mutex_);
        if (!keepalive_owner_token_.empty() &&
            keepalive_owner_token_ != session.owner_token) {
            return ErrorCode::INVALID_PARAMS;
        }

        keepalive_shutdown_requested_ = true;
        if (keepalive_lease_id_ != 0) {
            lease_id_to_cancel = keepalive_lease_id_;
        }
        should_join = keepalive_thread_.joinable();
        should_cancel = should_join && !keepalive_stopped_;
        if (should_join) {
            thread_to_join = std::move(keepalive_thread_);
        }
        keepalive_owner_token_.clear();
        keepalive_lease_id_ = 0;
        keepalive_stopped_ = true;
        ClearLeadershipMonitorStateLocked();
    }

    if (should_cancel) {
        auto err = EtcdHelper::CancelKeepAlive(lease_id_to_cancel);
        if (thread_to_join.joinable()) {
            thread_to_join.join();
        }
        if (err != ErrorCode::OK && err != ErrorCode::ETCD_OPERATION_ERROR) {
            return err;
        }
    } else if (thread_to_join.joinable()) {
        thread_to_join.join();
    }

    return EtcdHelper::RevokeLease(lease_id_to_cancel);
}

ClusterNamespace EtcdLeaderCoordinator::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    if (!cluster_namespace.empty()) {
        return cluster_namespace;
    }

    std::string resolved_namespace;
    const char* env_cluster_id = std::getenv("MC_STORE_CLUSTER_ID");
    if (env_cluster_id != nullptr && std::strlen(env_cluster_id) > 0) {
        resolved_namespace = env_cluster_id;
    } else {
        resolved_namespace = DEFAULT_CLUSTER_ID;
    }
    return resolved_namespace;
}

std::string EtcdLeaderCoordinator::BuildMasterViewKey(
    const ClusterNamespace& cluster_namespace) {
    std::string normalized = cluster_namespace;
    if (!normalized.empty() && normalized.back() == '/') {
        normalized.pop_back();
    }
    return "mooncake-store/" + normalized + "/master_view";
}

tl::expected<EtcdLeaseId, ErrorCode> EtcdLeaderCoordinator::ParseLeaseId(
    const OwnerToken& owner_token) {
    try {
        return static_cast<EtcdLeaseId>(std::stoll(owner_token));
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

OwnerToken EtcdLeaderCoordinator::MakeOwnerToken(EtcdLeaseId lease_id) {
    return std::to_string(static_cast<int64_t>(lease_id));
}

ErrorCode EtcdLeaderCoordinator::EnsureConnected() {
    if (connected_) {
        return ErrorCode::OK;
    }
    return Connect();
}

ErrorCode EtcdLeaderCoordinator::ShutdownKeepAliveThread() {
    std::thread thread_to_join;
    EtcdLeaseId lease_id_to_release = 0;
    bool should_cancel = false;
    bool should_revoke = false;
    bool should_join = false;

    {
        std::lock_guard<std::mutex> lock(keepalive_mutex_);
        keepalive_shutdown_requested_ = true;
        if (keepalive_lease_id_ != 0) {
            lease_id_to_release = keepalive_lease_id_;
            should_revoke = true;
        }
        should_join = keepalive_thread_.joinable();
        should_cancel = should_join && !keepalive_stopped_;
        if (should_join) {
            thread_to_join = std::move(keepalive_thread_);
        }
        keepalive_owner_token_.clear();
        keepalive_lease_id_ = 0;
        keepalive_stopped_ = true;
        ClearLeadershipMonitorStateLocked();
    }

    if (!should_cancel && !should_revoke) {
        return ErrorCode::OK;
    }

    ErrorCode err = ErrorCode::OK;
    if (should_cancel) {
        err = EtcdHelper::CancelKeepAlive(lease_id_to_release);
        if (thread_to_join.joinable()) {
            thread_to_join.join();
        }
    } else if (thread_to_join.joinable()) {
        thread_to_join.join();
    }

    ErrorCode revoke_err = ErrorCode::OK;
    if (should_revoke) {
        revoke_err = EtcdHelper::RevokeLease(lease_id_to_release);
    }

    if (err == ErrorCode::ETCD_OPERATION_ERROR) {
        err = ErrorCode::OK;
    }
    if (err != ErrorCode::OK) {
        return err;
    }
    return revoke_err;
}

void EtcdLeaderCoordinator::ClearLeadershipMonitorStateLocked() {
    if (leadership_monitor_armed_ != nullptr) {
        leadership_monitor_armed_->store(false);
        leadership_monitor_armed_.reset();
    }
    leadership_monitor_callback_ = nullptr;
    leadership_monitor_owner_token_.clear();
}

LeadershipLossReason EtcdLeaderCoordinator::ClassifyLeadershipLossReason(
    ErrorCode err) {
    if (err == ErrorCode::OK || err == ErrorCode::ETCD_CTX_CANCELLED) {
        return LeadershipLossReason::kLostLeadership;
    }
    return LeadershipLossReason::kRenewError;
}

bool EtcdLeaderCoordinator::IsSameViewVersion(
    const std::optional<MasterView>& current_view,
    std::optional<ViewVersionId> known_version) const {
    if (!current_view.has_value() && !known_version.has_value()) {
        return true;
    }
    if (!current_view.has_value() || !known_version.has_value()) {
        return false;
    }
    return current_view->view_version == known_version.value();
}

}  // namespace etcd
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
