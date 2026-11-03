#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <thread>

#include "ha/oplog/oplog_batch_storage.h"

namespace mooncake {

class BatchOpLogPruningWorker {
   public:
    BatchOpLogPruningWorker(OpLogBatchStorage& storage,
                            uint64_t chunk_batch_count,
                            uint64_t max_batches_per_second);
    ~BatchOpLogPruningWorker();

    ErrorCode Start();
    ErrorCode RunOneChunk(bool& deleted_chunk);
    void RequestStop();
    void Stop();

   private:
    enum class WaitState { kNone, kPacing, kIdlePoll, kErrorRetry };

    std::chrono::steady_clock::duration ComputePacingDelay(
        std::chrono::steady_clock::duration delete_duration) const;
    ErrorCode RunOneChunkLocked(
        bool& deleted_chunk,
        std::chrono::steady_clock::duration& delete_duration);
    ErrorCode RefreshFloor();
    ErrorCode RecoverCursor(uint64_t floor);
    bool HasPendingWork() const;
    bool WaitFor(std::chrono::steady_clock::duration duration,
                 WaitState state);
    void WorkerLoop();

    OpLogBatchStorage& storage_;
    const uint64_t chunk_batch_count_;
    const uint64_t max_batches_per_second_;
    std::mutex cycle_mutex_;
    std::mutex lifecycle_mutex_;
    std::mutex wait_mutex_;
    std::condition_variable wait_condition_;
    std::thread worker_thread_;
    std::atomic<bool> stop_requested_{false};
    std::atomic<WaitState> wait_state_{WaitState::kNone};
    uint64_t cursor_{0};
    std::optional<uint64_t> target_floor_;
    bool cursor_initialized_{false};
};

}  // namespace mooncake
