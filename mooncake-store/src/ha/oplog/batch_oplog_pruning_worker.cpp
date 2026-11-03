#include "ha/oplog/batch_oplog_pruning_worker.h"

#include <algorithm>
#include <exception>

#include <glog/logging.h>

#include "ha_metric_manager.h"

namespace mooncake {
namespace {

using Clock = std::chrono::steady_clock;
constexpr auto kIdlePollInterval = std::chrono::seconds(10);
constexpr auto kErrorRetryInterval = std::chrono::seconds(1);

}  // namespace

BatchOpLogPruningWorker::BatchOpLogPruningWorker(
    OpLogBatchStorage& storage, uint64_t chunk_batch_count,
    uint64_t max_batches_per_second)
    : storage_(storage),
      chunk_batch_count_(chunk_batch_count),
      max_batches_per_second_(max_batches_per_second) {}

BatchOpLogPruningWorker::~BatchOpLogPruningWorker() { Stop(); }

ErrorCode BatchOpLogPruningWorker::Start() {
    if (chunk_batch_count_ == 0 || max_batches_per_second_ == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    if (worker_thread_.joinable()) return ErrorCode::OK;
    stop_requested_.store(false);
    HAMetricManager::instance().reset_batch_oplog_pruner_runtime();
    std::lock_guard cycle_lock(cycle_mutex_);
    HAMetricManager::instance().update_batch_oplog_pruner_state(
        true, cursor_, target_floor_.value_or(0));
    try {
        worker_thread_ = std::thread(&BatchOpLogPruningWorker::WorkerLoop,
                                     this);
    } catch (const std::exception&) {
        HAMetricManager::instance().update_batch_oplog_pruner_state(
            false, cursor_, target_floor_.value_or(0));
        stop_requested_.store(true);
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode BatchOpLogPruningWorker::RunOneChunk(bool& deleted_chunk) {
    std::lock_guard cycle_lock(cycle_mutex_);
    Clock::duration delete_duration{};
    return RunOneChunkLocked(deleted_chunk, delete_duration);
}

void BatchOpLogPruningWorker::RequestStop() {
    {
        std::lock_guard wait_lock(wait_mutex_);
        stop_requested_.store(true);
    }
    wait_condition_.notify_all();
}

void BatchOpLogPruningWorker::Stop() {
    RequestStop();
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    if (worker_thread_.joinable()) worker_thread_.join();
    std::lock_guard cycle_lock(cycle_mutex_);
    HAMetricManager::instance().update_batch_oplog_pruner_state(
        false, cursor_, target_floor_.value_or(0));
}

Clock::duration BatchOpLogPruningWorker::ComputePacingDelay(
    Clock::duration delete_duration) const {
    const std::chrono::duration<double> target_seconds(
        static_cast<double>(chunk_batch_count_) /
        static_cast<double>(max_batches_per_second_));
    if (target_seconds >= std::chrono::duration<double>(Clock::duration::max())) {
        return Clock::duration::max();
    }
    const auto target = std::chrono::duration_cast<Clock::duration>(
        target_seconds);
    return delete_duration >= target ? Clock::duration::zero()
                                     : target - delete_duration;
}

ErrorCode BatchOpLogPruningWorker::RunOneChunkLocked(
    bool& deleted_chunk, Clock::duration& delete_duration) {
    deleted_chunk = false;
    delete_duration = Clock::duration::zero();
    if (chunk_batch_count_ == 0 || max_batches_per_second_ == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    const ErrorCode refresh_error = RefreshFloor();
    if (refresh_error != ErrorCode::OK || !target_floor_.has_value()) {
        return refresh_error;
    }
    if (cursor_ >= *target_floor_) return ErrorCode::OK;

    const uint64_t count = std::min(chunk_batch_count_, *target_floor_ - cursor_);
    const uint64_t first_id = cursor_ + 1;
    const uint64_t last_id = cursor_ + count;
    const auto started = Clock::now();
    const ErrorCode error = storage_.DeleteBatchesInRange(first_id, last_id);
    delete_duration = Clock::now() - started;
    if (error != ErrorCode::OK) {
        HAMetricManager::instance().inc_batch_oplog_pruner_errors();
        return error;
    }

    cursor_ = last_id;
    deleted_chunk = true;
    HAMetricManager::instance().inc_batch_oplog_pruner_chunks_deleted();
    HAMetricManager::instance().inc_batch_oplog_pruner_batch_ids_advanced(
        count);
    HAMetricManager::instance().update_batch_oplog_pruner_state(
        true, cursor_, target_floor_.value_or(0));
    if (cursor_ == *target_floor_ && !stop_requested_.load()) {
        return RefreshFloor();
    }
    return ErrorCode::OK;
}

ErrorCode BatchOpLogPruningWorker::RefreshFloor() {
    uint64_t floor = 0;
    const ErrorCode error = storage_.ReadCompactionFloor(floor);
    if (error == ErrorCode::ETCD_KEY_NOT_EXIST) {
        HAMetricManager::instance().update_batch_oplog_pruner_state(
            true, cursor_, target_floor_.value_or(0));
        return ErrorCode::OK;
    }
    if (error != ErrorCode::OK) {
        HAMetricManager::instance().inc_batch_oplog_pruner_errors();
        return error;
    }
    if (!cursor_initialized_) return RecoverCursor(floor);
    if (!target_floor_.has_value() || floor > *target_floor_) {
        target_floor_ = floor;
    }
    HAMetricManager::instance().update_batch_oplog_pruner_state(
        true, cursor_, target_floor_.value_or(0));
    return ErrorCode::OK;
}

ErrorCode BatchOpLogPruningWorker::RecoverCursor(uint64_t floor) {
    std::optional<uint64_t> oldest_batch_id;
    const ErrorCode error = storage_.FindOldestBatchId(oldest_batch_id);
    if (error != ErrorCode::OK) {
        HAMetricManager::instance().inc_batch_oplog_pruner_errors();
        return error;
    }
    uint64_t recovered_cursor = floor;
    if (oldest_batch_id.has_value()) {
        if (*oldest_batch_id == 0) {
            HAMetricManager::instance().inc_batch_oplog_pruner_errors();
            return ErrorCode::INTERNAL_ERROR;
        }
        if (*oldest_batch_id <= floor) recovered_cursor = *oldest_batch_id - 1;
    }
    cursor_ = recovered_cursor;
    target_floor_ = floor;
    cursor_initialized_ = true;
    HAMetricManager::instance().update_batch_oplog_pruner_state(
        true, cursor_, floor);
    return ErrorCode::OK;
}

bool BatchOpLogPruningWorker::HasPendingWork() const {
    return cursor_initialized_ && target_floor_.has_value() &&
           cursor_ < *target_floor_;
}

bool BatchOpLogPruningWorker::WaitFor(Clock::duration duration,
                                      WaitState state) {
    wait_state_.store(state);
    std::unique_lock wait_lock(wait_mutex_);
    wait_condition_.wait_for(wait_lock, duration,
                             [this] { return stop_requested_.load(); });
    wait_state_.store(WaitState::kNone);
    return !stop_requested_.load();
}

void BatchOpLogPruningWorker::WorkerLoop() {
    bool retry_cycle_immediately = true;
    while (!stop_requested_.load()) {
        bool pending = false;
        {
            std::lock_guard cycle_lock(cycle_mutex_);
            pending = HasPendingWork();
        }
        if (!pending && !retry_cycle_immediately &&
            !WaitFor(kIdlePollInterval, WaitState::kIdlePoll)) {
            break;
        }
        retry_cycle_immediately = false;

        bool deleted_chunk = false;
        Clock::duration delete_duration{};
        ErrorCode error = ErrorCode::OK;
        {
            std::lock_guard cycle_lock(cycle_mutex_);
            error = RunOneChunkLocked(deleted_chunk, delete_duration);
        }
        if (stop_requested_.load()) break;
        if (error != ErrorCode::OK) {
            LOG(ERROR) << "Batch OpLog pruning worker chunk failed: "
                       << toString(error) << "; retrying";
            Clock::duration delay = kErrorRetryInterval;
            if (deleted_chunk) {
                delay = std::max(delay, ComputePacingDelay(delete_duration));
            }
            if (!WaitFor(delay, WaitState::kErrorRetry)) break;
            retry_cycle_immediately = true;
        } else if (deleted_chunk) {
            const auto delay = ComputePacingDelay(delete_duration);
            if (delay > Clock::duration::zero() &&
                !WaitFor(delay, WaitState::kPacing)) {
                break;
            }
        }
    }
}

}  // namespace mooncake
