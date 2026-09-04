// Copyright 2026 KVCache.AI
#ifndef TENT_HP_TCP_TASK_H_
#define TENT_HP_TCP_TASK_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <utility>

#include "tent/common/types.h"
#include "tent/transport/hp_tcp/hp_tcp_buffer_registry.h"
#include "tent/transport/hp_tcp/hp_tcp_workers.h"

namespace mooncake::tent {

// TENT-visible task shell. Socket callbacks never own the SubBatch; they hold a
// shared_ptr to this state instead. A task becomes admission-reserved only in
// the no-throw batch commit section.
class HighPerformanceTcpTaskState {
   public:
    HighPerformanceTcpTaskState(
        uint64_t reserved_bytes, BatchID progress_batch_id,
        std::function<void(BatchID)> notify_progress,
        HighPerformanceTcpBufferRegistry::Lease local_lease)
        : progress_batch_id_(progress_batch_id),
          notify_progress_(std::move(notify_progress)),
          local_lease_(std::move(local_lease)),
          reserved_bytes_(reserved_bytes) {}

    HighPerformanceTcpTaskState(const HighPerformanceTcpTaskState&) = delete;
    HighPerformanceTcpTaskState& operator=(const HighPerformanceTcpTaskState&) =
        delete;

    void activateReservation(
        HighPerformanceTcpAdmissionController* admission) noexcept {
        admission_ = admission;
        reservation_active_.store(true, std::memory_order_release);
    }

    bool completeOnce(TransferStatusEnum terminal, size_t bytes,
                      std::optional<HighPerformanceTcpStatus> remote_status =
                          std::nullopt) noexcept;
    TransferStatus snapshot() const noexcept;
    std::optional<HighPerformanceTcpStatus> remoteStatus() const noexcept;

    void setDispatchIdentity(size_t owner_worker,
                             uint64_t request_id) noexcept {
        owner_worker_ = owner_worker;
        request_id_ = request_id;
    }
    size_t ownerWorker() const noexcept { return owner_worker_; }
    uint64_t requestId() const noexcept { return request_id_; }

    void requestCancel() noexcept {
        cancel_requested_.store(true, std::memory_order_release);
    }
    bool cancelRequested() const noexcept {
        return cancel_requested_.load(std::memory_order_acquire);
    }

   private:
    std::atomic<TransferStatusEnum> status_{PENDING};
    std::atomic<size_t> bytes_{0};
    std::atomic<HighPerformanceTcpStatus> remote_status_{
        HighPerformanceTcpStatus::kOk};
    std::atomic<bool> completion_claimed_{false};
    std::atomic<bool> cancel_requested_{false};

    BatchID progress_batch_id_{0};
    std::function<void(BatchID)> notify_progress_;
    HighPerformanceTcpBufferRegistry::Lease local_lease_;

    HighPerformanceTcpAdmissionController* admission_{nullptr};
    uint64_t reserved_bytes_{0};
    std::atomic<bool> reservation_active_{false};

    size_t owner_worker_{0};
    uint64_t request_id_{0};
};

}  // namespace mooncake::tent

#endif  // TENT_HP_TCP_TASK_H_
