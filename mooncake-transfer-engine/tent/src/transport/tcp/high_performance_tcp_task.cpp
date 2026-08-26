// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_task.h"

#include <glog/logging.h>

namespace mooncake::tent {

bool HighPerformanceTcpTaskState::completeOnce(TransferStatusEnum terminal,
                                               size_t bytes) noexcept {
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

}  // namespace mooncake::tent
