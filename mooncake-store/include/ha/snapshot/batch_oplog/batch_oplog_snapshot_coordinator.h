#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "ha/oplog/oplog_batch_types.h"
#include "types.h"

namespace mooncake {

inline constexpr uint64_t kDefaultBatchOpLogSnapshotIntervalSeconds = 600;

class HaKvBackend;
class HotStandbyService;
class SnapshotMaintenanceLease;
class SnapshotObjectStore;

struct BatchOpLogSnapshotCoordinatorConfig {
    uint64_t snapshot_interval_seconds{
        kDefaultBatchOpLogSnapshotIntervalSeconds};
    size_t chunk_object_count{1000000};
    std::string snapshot_root;
    std::function<std::chrono::steady_clock::time_point()> clock;
};

struct BatchOpLogSnapshotCoordinatorStatus {
    bool running{false};
    bool attempt_in_flight{false};
    bool promotion_requested{false};
    uint64_t attempts{0};
    ErrorCode last_error{ErrorCode::OK};
    std::optional<DurablePrefix> catch_up_target;
};

// Coordinates the opt-in batch-OpLog snapshot path. Construction alone does
// not start a worker or change HotStandbyService behavior.
class BatchOpLogSnapshotCoordinator final {
   public:
    using LeaseFactory =
        std::function<std::unique_ptr<SnapshotMaintenanceLease>()>;

    BatchOpLogSnapshotCoordinator(HotStandbyService& standby,
                                  HaKvBackend& backend,
                                  SnapshotObjectStore& object_store,
                                  std::string cluster_id,
                                  BatchOpLogSnapshotCoordinatorConfig config,
                                  LeaseFactory lease_factory = {});
    BatchOpLogSnapshotCoordinator(HotStandbyService& standby,
                                  HaKvBackend& backend,
                                  SnapshotObjectStore& object_store,
                                  std::string cluster_id,
                                  std::string snapshot_root,
                                  uint64_t snapshot_interval_seconds =
                                      kDefaultBatchOpLogSnapshotIntervalSeconds,
                                  size_t chunk_object_count = 1000000);
    ~BatchOpLogSnapshotCoordinator();

    BatchOpLogSnapshotCoordinator(const BatchOpLogSnapshotCoordinator&) =
        delete;
    BatchOpLogSnapshotCoordinator& operator=(
        const BatchOpLogSnapshotCoordinator&) = delete;

    // Starts periodic scheduling. RunOnce() remains available for
    // deterministic tests and callers that own the scheduling loop.
    void Start();
    void Stop();

    // Executes at most one attempt. An ineligible cycle returns OK and leaves
    // the standby OpLog apply loop untouched.
    ErrorCode RunOnce();
    ErrorCode PollOnce() { return RunOnce(); }

    // Called by HotStandbyService before promotion/stop. Promotion keeps a
    // fully uploaded candidate eligible for the background publish step.
    void NotifyPromotion();
    void OnPromotion() { NotifyPromotion(); }

    BatchOpLogSnapshotCoordinatorStatus GetStatus() const;
    bool IsRunning() const;
    bool IsAttemptInFlight() const;
    ErrorCode last_error() const;

   private:
    using Clock = std::chrono::steady_clock;

    void SchedulerLoop();
    ErrorCode RunAttempt();
    std::optional<uint64_t> ReadLatestBatchId(ErrorCode& error) const;
    bool CatchUpComplete(const DurablePrefix& target) const;
    void FinishAttempt(ErrorCode error, bool count_attempt);
    Clock::time_point Now() const;
    void OnCaptureReleased();
    std::optional<DurablePrefix> ReadDurablePrefix() const;
    void RequestStop();

    HotStandbyService& standby_;
    HaKvBackend& backend_;
    SnapshotObjectStore& object_store_;
    std::string cluster_id_;
    BatchOpLogSnapshotCoordinatorConfig config_;
    LeaseFactory lease_factory_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread worker_;
    bool running_{false};
    bool stop_requested_{false};
    bool attempt_in_flight_{false};
    bool capture_active_{false};
    bool promotion_requested_{false};
    uint64_t attempts_{0};
    ErrorCode last_error_{ErrorCode::OK};
    std::optional<Clock::time_point> last_attempt_complete_;
    std::optional<DurablePrefix> capture_cursor_;
    std::optional<DurablePrefix> catch_up_target_;
};

}  // namespace mooncake
