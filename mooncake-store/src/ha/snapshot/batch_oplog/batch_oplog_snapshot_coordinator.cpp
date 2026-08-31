#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_coordinator.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>

#include <glog/logging.h>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_publisher.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/batch_oplog/writer.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "hot_standby_service.h"

namespace mooncake {
namespace {

int64_t CurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool IsAtOrAfter(const DurablePrefix& current, const DurablePrefix& target) {
    return !IsSequenceOlder(current.last_seq, target.last_seq) &&
           current.batch_id >= target.batch_id;
}

}  // namespace

BatchOpLogSnapshotCoordinator::BatchOpLogSnapshotCoordinator(
    HotStandbyService& standby, HaKvBackend& backend,
    SnapshotObjectStore& object_store, std::string cluster_id,
    BatchOpLogSnapshotCoordinatorConfig config, LeaseFactory lease_factory)
    : standby_(standby),
      backend_(backend),
      object_store_(object_store),
      cluster_id_(std::move(cluster_id)),
      config_(std::move(config)),
      lease_factory_(std::move(lease_factory)) {
    if (!config_.clock) {
        config_.clock = [] { return Clock::now(); };
    }
    if (!lease_factory_) {
        lease_factory_ = [this] {
            return std::make_unique<SnapshotMaintenanceLease>(cluster_id_);
        };
    }
    standby_.SetBatchOpLogSnapshotCaptureReleasedCallback(
        [this] { OnCaptureReleased(); });
    standby_.SetBatchOpLogSnapshotPromotionCallback(
        [this] { NotifyPromotion(); });
    standby_.SetBatchOpLogSnapshotStopCallback([this] { RequestStop(); });
}

BatchOpLogSnapshotCoordinator::BatchOpLogSnapshotCoordinator(
    HotStandbyService& standby, HaKvBackend& backend,
    SnapshotObjectStore& object_store, std::string cluster_id,
    std::string snapshot_root, uint64_t snapshot_interval_seconds,
    size_t chunk_object_count)
    : BatchOpLogSnapshotCoordinator(
          standby, backend, object_store, std::move(cluster_id),
          BatchOpLogSnapshotCoordinatorConfig{
              .snapshot_interval_seconds = snapshot_interval_seconds,
              .chunk_object_count = chunk_object_count,
              .snapshot_root = std::move(snapshot_root),
              .clock = {}},
          {}) {}

BatchOpLogSnapshotCoordinator::~BatchOpLogSnapshotCoordinator() {
    Stop();
    standby_.SetBatchOpLogSnapshotCaptureReleasedCallback(nullptr);
    standby_.SetBatchOpLogSnapshotPromotionCallback(nullptr);
    standby_.SetBatchOpLogSnapshotStopCallback(nullptr);
}

void BatchOpLogSnapshotCoordinator::Start() {
    std::thread stale_worker;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (running_) {
            return;
        }
        stop_requested_ = true;
        stale_worker = std::move(worker_);
    }
    cv_.notify_all();
    if (stale_worker.joinable()) {
        stale_worker.join();
    }

    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_ = false;
    promotion_requested_ = false;
    running_ = true;
    worker_ = std::thread(&BatchOpLogSnapshotCoordinator::SchedulerLoop, this);
}

void BatchOpLogSnapshotCoordinator::Stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_requested_ = true;
        running_ = false;
    }
    standby_.CancelBatchOpLogSnapshotCapture();
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !attempt_in_flight_; });
}

void BatchOpLogSnapshotCoordinator::NotifyPromotion() {
    bool cancel_capture = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        promotion_requested_ = true;
        cancel_capture = capture_active_;
    }
    if (cancel_capture) {
        standby_.CancelBatchOpLogSnapshotCapture();
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !capture_active_; });
    }
    cv_.notify_all();
}

BatchOpLogSnapshotCoordinatorStatus BatchOpLogSnapshotCoordinator::GetStatus()
    const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {.running = running_,
            .attempt_in_flight = attempt_in_flight_,
            .promotion_requested = promotion_requested_,
            .attempts = attempts_,
            .last_error = last_error_,
            .catch_up_target = catch_up_target_};
}

bool BatchOpLogSnapshotCoordinator::IsRunning() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return running_;
}

bool BatchOpLogSnapshotCoordinator::IsAttemptInFlight() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return attempt_in_flight_;
}

ErrorCode BatchOpLogSnapshotCoordinator::last_error() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_error_;
}

BatchOpLogSnapshotCoordinator::Clock::time_point
BatchOpLogSnapshotCoordinator::Now() const {
    return config_.clock ? config_.clock() : Clock::now();
}

void BatchOpLogSnapshotCoordinator::OnCaptureReleased() {
    const auto prefix = ReadDurablePrefix();
    std::lock_guard<std::mutex> lock(mutex_);
    capture_active_ = false;
    if (prefix) {
        catch_up_target_ = *prefix;
        if (capture_cursor_ && IsSequenceOlder(catch_up_target_->last_seq,
                                               capture_cursor_->last_seq)) {
            catch_up_target_ = *capture_cursor_;
        }
    } else if (capture_cursor_) {
        catch_up_target_ = *capture_cursor_;
    }
    cv_.notify_all();
}

std::optional<DurablePrefix> BatchOpLogSnapshotCoordinator::ReadDurablePrefix()
    const {
    OpLogBatchStorage storage(cluster_id_, backend_);
    DurablePrefix prefix;
    if (storage.ReadDurablePrefix(prefix) != ErrorCode::OK) {
        return std::nullopt;
    }
    return prefix;
}

std::optional<uint64_t> BatchOpLogSnapshotCoordinator::ReadLatestBatchId(
    ErrorCode& error) const {
    error = ErrorCode::OK;
    uint64_t published_batch_id = 0;
    for (const auto& key :
         {ha::BuildBatchOpLogSnapshotLatestKey(cluster_id_),
          ha::BuildBatchOpLogSnapshotFallbackKey(cluster_id_)}) {
        std::string value;
        const auto get_error = backend_.Get(key, value);
        if (get_error == ErrorCode::ETCD_KEY_NOT_EXIST) {
            continue;
        }
        if (get_error != ErrorCode::OK) {
            error = get_error;
            return std::nullopt;
        }
        auto descriptor = ha::DecodeBatchOpLogSnapshotDescriptor(value);
        // Corrupt pointers are handled by the fenced publisher; they do not
        // qualify a newer local cursor on their own.
        if (descriptor) {
            published_batch_id = std::max(published_batch_id,
                                          descriptor->last_included_batch_id);
        }
    }
    return published_batch_id;
}

bool BatchOpLogSnapshotCoordinator::CatchUpComplete(
    const DurablePrefix& target) const {
    const auto current = standby_.GetLastAppliedBatchOpLogSnapshotPrefix();
    return current && IsAtOrAfter(*current, target);
}

void BatchOpLogSnapshotCoordinator::FinishAttempt(ErrorCode error,
                                                  bool count_attempt) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (count_attempt) {
        ++attempts_;
        last_attempt_complete_ = Now();
    }
    last_error_ = error;
    attempt_in_flight_ = false;
    capture_active_ = false;
    cv_.notify_all();
}

ErrorCode BatchOpLogSnapshotCoordinator::RunOnce() {
    try {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (attempt_in_flight_) {
                return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            }
            if (stop_requested_) {
                return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            }
            if (last_attempt_complete_ &&
                Now() - *last_attempt_complete_ <
                    std::chrono::seconds(config_.snapshot_interval_seconds)) {
                return ErrorCode::OK;
            }
            if (promotion_requested_) {
                return ErrorCode::OK;
            }
        }
        return RunAttempt();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Batch snapshot coordinator failed: " << e.what();
        FinishAttempt(ErrorCode::INTERNAL_ERROR, false);
        return ErrorCode::INTERNAL_ERROR;
    } catch (...) {
        LOG(ERROR) << "Batch snapshot coordinator failed with unknown error";
        FinishAttempt(ErrorCode::INTERNAL_ERROR, false);
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode BatchOpLogSnapshotCoordinator::RunAttempt() {
    if (config_.snapshot_root.empty() || config_.chunk_object_count == 0 ||
        !NormalizeAndValidateClusterId(cluster_id_) || cluster_id_.empty()) {
        FinishAttempt(ErrorCode::INVALID_PARAMS, false);
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode read_error = ErrorCode::OK;
    const auto latest_batch_id = ReadLatestBatchId(read_error);
    if (!latest_batch_id) {
        FinishAttempt(read_error, false);
        return read_error;
    }
    const auto local_prefix = standby_.GetLastAppliedBatchOpLogSnapshotPrefix();
    if (!local_prefix || local_prefix->batch_id <= *latest_batch_id) {
        FinishAttempt(ErrorCode::OK, false);
        return ErrorCode::OK;
    }
    bool catch_up_blocked = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (attempt_in_flight_) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
        catch_up_blocked = catch_up_target_.has_value();
        attempt_in_flight_ = true;
    }
    if (catch_up_blocked) {
        std::optional<DurablePrefix> target;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            target = catch_up_target_;
        }
        if (target && !CatchUpComplete(*target)) {
            FinishAttempt(ErrorCode::OK, false);
            return ErrorCode::OK;
        }
    }

    auto lease = lease_factory_();
    if (!lease) {
        // A factory may return null to represent a busy maintenance lease.
        FinishAttempt(ErrorCode::OK, false);
        return ErrorCode::OK;
    }
    const ErrorCode lease_error =
        lease->IsHeld() ? ErrorCode::OK : lease->Acquire();
    if (lease_error != ErrorCode::OK) {
        // A busy maintenance lease is an ordinary skipped cycle.
        const ErrorCode result = lease_error == ErrorCode::ETCD_TRANSACTION_FAIL
                                     ? ErrorCode::OK
                                     : lease_error;
        FinishAttempt(result, false);
        return result;
    }

    auto release_lease = [&] { (void)lease->Release(); };
    bool cancel_before_capture = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cancel_before_capture = stop_requested_ || promotion_requested_;
    }
    if (cancel_before_capture) {
        release_lease();
        FinishAttempt(ErrorCode::OK, true);
        return ErrorCode::OK;
    }

    // Re-read both sides after fencing the maintenance lease.
    const auto reread_latest = ReadLatestBatchId(read_error);
    const auto reread_local = standby_.GetLastAppliedBatchOpLogSnapshotPrefix();
    if (!reread_latest || !reread_local ||
        reread_local->batch_id <= *reread_latest) {
        const ErrorCode result =
            read_error == ErrorCode::OK ? ErrorCode::OK : read_error;
        release_lease();
        FinishAttempt(result, true);
        return result;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        capture_active_ = true;
    }
    auto capture = standby_.BeginBatchOpLogSnapshotCapture();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        capture_active_ = capture.has_value();
    }
    if (!capture || capture->last_included_batch_id <= *reread_latest) {
        release_lease();
        FinishAttempt(ErrorCode::OK, true);
        return ErrorCode::OK;
    }

    const std::string snapshot_id =
        std::to_string(capture->last_included_batch_id) + "-" +
        lease->owner_token();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        capture_cursor_ =
            DurablePrefix{.batch_id = capture->last_included_batch_id,
                          .last_seq = capture->last_included_seq};
    }
    const std::string artifact_prefix =
        ha::BuildBatchOpLogSnapshotArtifactPrefix(config_.snapshot_root,
                                                  snapshot_id);

    BatchOpLogSnapshotWriter writer(object_store_);
    auto descriptor =
        writer.Write(standby_, *capture, config_.snapshot_root, snapshot_id,
                     config_.chunk_object_count, CurrentTimeMs());
    {
        std::lock_guard<std::mutex> lock(mutex_);
        capture_active_ = false;
    }
    if (!descriptor) {
        release_lease();
        FinishAttempt(ErrorCode::INTERNAL_ERROR, true);
        return ErrorCode::INTERNAL_ERROR;
    }

    bool stop_before_publish = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_before_publish = stop_requested_ && !promotion_requested_;
    }
    if (stop_before_publish) {
        auto cleanup = object_store_.DeleteObjectsWithPrefix(artifact_prefix);
        if (!cleanup) {
            LOG(WARNING) << "Failed to clean snapshot candidate after stop: "
                         << cleanup.error();
        }
        release_lease();
        FinishAttempt(ErrorCode::OK, true);
        return ErrorCode::OK;
    }

    BatchOpLogSnapshotPublisher publisher(backend_, cluster_id_);
    ErrorCode publish_error = publisher.Publish(*lease, *descriptor);
    if (publish_error != ErrorCode::OK) {
        auto cleanup = object_store_.DeleteObjectsWithPrefix(artifact_prefix);
        if (!cleanup) {
            LOG(WARNING) << "Failed to clean unpublished snapshot candidate: "
                         << cleanup.error();
        }
    }
    release_lease();
    FinishAttempt(publish_error, true);
    return publish_error;
}

void BatchOpLogSnapshotCoordinator::SchedulerLoop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            const auto delay =
                std::chrono::seconds(config_.snapshot_interval_seconds == 0
                                         ? 1
                                         : config_.snapshot_interval_seconds);
            if (cv_.wait_for(lock, delay, [this] { return stop_requested_; })) {
                return;
            }
        }
        if (RunOnce() == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_requested_) {
                return;
            }
        }
    }
}

void BatchOpLogSnapshotCoordinator::RequestStop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_requested_ = true;
        running_ = false;
    }
    standby_.CancelBatchOpLogSnapshotCapture();
    cv_.notify_all();
}

}  // namespace mooncake
