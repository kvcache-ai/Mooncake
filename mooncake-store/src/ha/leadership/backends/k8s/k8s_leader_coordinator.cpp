#include "ha/leadership/backends/k8s/k8s_leader_coordinator.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <optional>
#include <thread>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {
namespace ha {
namespace backends {
namespace k8s {

namespace {

constexpr int kDefaultLeaseDurationSec = 5;
constexpr int kDefaultRenewDeadlineSec = 3;
constexpr int kDefaultRetryPeriodSec = 1;
constexpr auto kViewChangePollInterval = std::chrono::milliseconds(200);

class K8sLeadershipMonitorHandle final : public LeadershipMonitorHandle {
   public:
    explicit K8sLeadershipMonitorHandle(
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

K8sLeaderCoordinator::K8sLeaderCoordinator(const HABackendSpec& spec)
    : spec_(spec) {
    auto [ns, name] = ParseConnstring(spec.connstring);
    namespace_ = std::move(ns);
    lease_name_ = std::move(name);
}

K8sLeaderCoordinator::~K8sLeaderCoordinator() { ShutdownElection(); }

ErrorCode K8sLeaderCoordinator::Connect() {
    if (connected_) {
        return ErrorCode::OK;
    }

    auto err = K8sLeaseHelper::Init();
    if (err == ErrorCode::OK) {
        connected_ = true;
    }
    return err;
}

tl::expected<std::optional<MasterView>, ErrorCode>
K8sLeaderCoordinator::ReadCurrentView() {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::string holder;
    int64_t transitions = 0;
    err =
        K8sLeaseHelper::GetHolder(namespace_, lease_name_, holder, transitions);
    if (err == ErrorCode::K8S_LEASE_NOT_FOUND) {
        return std::optional<MasterView>{std::nullopt};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    if (holder.empty()) {
        return std::optional<MasterView>{std::nullopt};
    }

    return std::optional<MasterView>{
        MasterView{.leader_address = std::move(holder),
                   .view_version = static_cast<ViewVersionId>(transitions)}};
}

tl::expected<AcquireLeadershipResult, ErrorCode>
K8sLeaderCoordinator::TryAcquireLeadership(const std::string& leader_address) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    // Start election goroutine
    err = K8sLeaseHelper::RunElection(
        namespace_, lease_name_, leader_address, kDefaultLeaseDurationSec,
        kDefaultRenewDeadlineSec, kDefaultRetryPeriodSec);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    constexpr int kElectionTimeoutSec = 2 * kDefaultLeaseDurationSec;
    int64_t transitions = 0;
    err = K8sLeaseHelper::WaitElected(namespace_, lease_name_,
                                      kElectionTimeoutSec, transitions);
    if (err != ErrorCode::OK) {
        // Election failed — we are not the leader
        K8sLeaseHelper::CancelElection(namespace_, lease_name_);

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

    // We are the leader
    OwnerToken token = namespace_ + "/" + lease_name_;
    LeadershipSession session{
        .view =
            MasterView{.leader_address = leader_address,
                       .view_version = static_cast<ViewVersionId>(transitions)},
        .owner_token = token,
        .lease_ttl = std::chrono::seconds(kDefaultLeaseDurationSec),
    };

    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        election_identity_ = leader_address;
        election_active_ = true;
        election_shutdown_requested_ = false;
        ClearLeadershipMonitorStateLocked();
    }

    return AcquireLeadershipResult{
        .status = AcquireLeadershipStatus::ACQUIRED,
        .session = std::move(session),
        .observed_view = std::nullopt,
    };
}

tl::expected<bool, ErrorCode> K8sLeaderCoordinator::RenewLeadership(
    const LeadershipSession& session) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::lock_guard<std::mutex> lock(election_mutex_);

    if (election_shutdown_requested_) {
        return false;
    }
    if (!election_active_) {
        return false;
    }

    // client-go handles renewal internally. If election is still active,
    // leadership is being renewed. Start the monitor thread if not running.
    if (!election_monitor_thread_.joinable()) {
        election_monitor_thread_ = std::thread([this,
                                                token = session.owner_token]() {
            auto rc = K8sLeaseHelper::WaitLost(namespace_, lease_name_);

            std::shared_ptr<std::atomic<bool>> monitor_armed;
            LeadershipLostCallback on_leadership_lost;
            LeadershipLossReason loss_reason =
                (rc == ErrorCode::OK) ? LeadershipLossReason::kLostLeadership
                                      : LeadershipLossReason::kRenewError;
            {
                std::lock_guard<std::mutex> lock(election_mutex_);
                election_active_ = false;
                if (!election_shutdown_requested_ &&
                    leadership_monitor_owner_token_ == token) {
                    monitor_armed = leadership_monitor_armed_;
                    on_leadership_lost =
                        std::move(leadership_monitor_callback_);
                    leadership_monitor_armed_.reset();
                    leadership_monitor_owner_token_.clear();
                }
            }

            if (monitor_armed != nullptr && monitor_armed->exchange(false) &&
                on_leadership_lost != nullptr) {
                on_leadership_lost(loss_reason);
            }
        });
    }

    return true;
}

tl::expected<ViewChangeResult, ErrorCode>
K8sLeaderCoordinator::WaitForViewChange(
    std::optional<ViewVersionId> known_version,
    std::chrono::milliseconds timeout) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
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
        std::this_thread::sleep_for(
            std::min(kViewChangePollInterval, remaining));
    }
}

tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
K8sLeaderCoordinator::StartLeadershipMonitor(
    const LeadershipSession& session,
    LeadershipLostCallback on_leadership_lost) {
    auto err = EnsureConnected();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    if (!on_leadership_lost) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(election_mutex_);
    if (election_shutdown_requested_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (!election_active_) {
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
        std::make_unique<K8sLeadershipMonitorHandle>(
            leadership_monitor_armed_));
}

ErrorCode K8sLeaderCoordinator::ReleaseLeadership(
    const LeadershipSession& session) {
    std::thread thread_to_join;
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        election_shutdown_requested_ = true;
        election_active_ = false;
        ClearLeadershipMonitorStateLocked();
        if (election_monitor_thread_.joinable()) {
            thread_to_join = std::move(election_monitor_thread_);
        }
    }

    // Cancel the election goroutine (triggers WaitLost to return)
    auto err = K8sLeaseHelper::CancelElection(namespace_, lease_name_);

    if (thread_to_join.joinable()) {
        thread_to_join.join();
    }

    return err;
}

ErrorCode K8sLeaderCoordinator::EnsureConnected() {
    if (connected_) {
        return ErrorCode::OK;
    }
    return Connect();
}

ErrorCode K8sLeaderCoordinator::ShutdownElection() {
    std::thread thread_to_join;
    bool should_cancel = false;
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        election_shutdown_requested_ = true;
        should_cancel = election_active_;
        election_active_ = false;
        ClearLeadershipMonitorStateLocked();
        if (election_monitor_thread_.joinable()) {
            thread_to_join = std::move(election_monitor_thread_);
        }
    }

    if (should_cancel) {
        K8sLeaseHelper::CancelElection(namespace_, lease_name_);
    }

    if (thread_to_join.joinable()) {
        thread_to_join.join();
    }

    return ErrorCode::OK;
}

void K8sLeaderCoordinator::ClearLeadershipMonitorStateLocked() {
    if (leadership_monitor_armed_ != nullptr) {
        leadership_monitor_armed_->store(false);
        leadership_monitor_armed_.reset();
    }
    leadership_monitor_callback_ = nullptr;
    leadership_monitor_owner_token_.clear();
}

bool K8sLeaderCoordinator::IsSameViewVersion(
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

std::pair<std::string, std::string> K8sLeaderCoordinator::ParseConnstring(
    const std::string& connstring) {
    // Format: "namespace/lease-name"
    auto pos = connstring.find('/');
    if (pos == std::string::npos) {
        // Default namespace
        return {"default", connstring};
    }
    return {connstring.substr(0, pos), connstring.substr(pos + 1)};
}

}  // namespace k8s
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
