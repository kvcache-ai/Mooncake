// Copyright 2026 KVCache.AI
#include "tent/transport/hp_tcp/hp_tcp_task.h"

#include <glog/logging.h>

namespace mooncake::tent {

bool HighPerformanceTcpTaskState::completeSlice(
    TransferStatusEnum terminal, size_t bytes,
    std::optional<HighPerformanceTcpStatus> remote_status) noexcept {
    if (terminal == COMPLETED) {
        completed_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    } else if (terminal == TIMEOUT) {
        slice_timed_out_.store(true, std::memory_order_relaxed);
    } else if (terminal == CANCELED) {
        slice_canceled_.store(true, std::memory_order_relaxed);
    } else {
        slice_failed_.store(true, std::memory_order_relaxed);
    }

    if (remote_status.has_value() &&
        *remote_status != HighPerformanceTcpStatus::kOk) {
        HighPerformanceTcpStatus expected = HighPerformanceTcpStatus::kOk;
        (void)remote_status_.compare_exchange_strong(expected, *remote_status,
                                                     std::memory_order_relaxed);
    }

    if (remaining_slices_.fetch_sub(1, std::memory_order_acq_rel) != 1) {
        return false;
    }

    if (slice_failed_.load(std::memory_order_acquire)) {
        return completeOnce(FAILED, 0);
    }
    if (slice_timed_out_.load(std::memory_order_acquire)) {
        return completeOnce(TIMEOUT, 0);
    }
    if (slice_canceled_.load(std::memory_order_acquire)) {
        return completeOnce(CANCELED, 0);
    }
    return completeOnce(COMPLETED,
                        completed_bytes_.load(std::memory_order_acquire));
}

bool HighPerformanceTcpTaskState::completeOnce(
    TransferStatusEnum terminal, size_t bytes,
    std::optional<HighPerformanceTcpStatus> remote_status) noexcept {
    if (terminal == INITIAL || terminal == PENDING || terminal == INVALID) {
        terminal = FAILED;
        bytes = 0;
    }

    bool expected = false;
    if (!completion_claimed_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }

    // This function is called only after the final socket callback that could
    // touch the local user buffer has retired (or before the operation reached
    // a socket). Retire memory ownership and budget before publishing terminal
    // status so teardown observing terminal cannot race unregister/free.
    bytes_.store(bytes, std::memory_order_relaxed);
    if (remote_status.has_value() &&
        *remote_status != HighPerformanceTcpStatus::kOk) {
        remote_status_.store(*remote_status, std::memory_order_relaxed);
    }
    local_lease_.reset();

    if (reservation_active_.exchange(false, std::memory_order_acq_rel)) {
        if (admission_ != nullptr) admission_->release(1, reserved_bytes_);
    }

    status_.store(terminal, std::memory_order_release);

    try {
        if (notify_progress_) notify_progress_(progress_batch_id_);
    } catch (const std::exception& error) {
        LOG(ERROR) << "HP TCP progress notification threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "HP TCP progress notification threw";
    }
    return true;
}

TransferStatus HighPerformanceTcpTaskState::snapshot() const noexcept {
    TransferStatus result;
    result.s = status_.load(std::memory_order_acquire);
    result.transferred_bytes = bytes_.load(std::memory_order_acquire);
    return result;
}

std::optional<HighPerformanceTcpStatus>
HighPerformanceTcpTaskState::remoteStatus() const noexcept {
    const auto status = remote_status_.load(std::memory_order_acquire);
    if (status == HighPerformanceTcpStatus::kOk) return std::nullopt;
    return status;
}

}  // namespace mooncake::tent
