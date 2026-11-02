#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_H

#include <atomic>
#include <cstring>
#include <thread>

#include "device_comm/device_utils/h2d_request_slot_types.h"
#include "error_types.h"

namespace mooncake {

template <typename Request, typename Reply>
typename H2DRequestSlot<Request, Reply>::RequestHandle
H2DRequestSlot<Request, Reply>::publish(const Request& request, bool pinned) {
    auto state = std::atomic_ref(state_);
    while (true) {
        uint32_t observed = state.load(std::memory_order_acquire);
        switch (observed) {
            case Idle:
            case Published:
                if (!state.compare_exchange_strong(observed, Writing,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire))
                    continue;
                PG_ASSERT(sequence_ != UINT64_MAX);
                ++sequence_;
                std::memcpy(&request_, &request, sizeof(request));
                // Also order publication before subsequent host observations,
                // such as a pause owner's stream-completion snapshot.
                state.store(pinned ? Pinned : Published,
                            std::memory_order_seq_cst);
                return RequestHandle(this, sequence_);
            case Pinned:
            case Claimed:
                std::this_thread::yield();
                continue;
            default:
                PG_UNREACHABLE();
                return {};
        }
    }
}

template <typename Request, typename Reply>
bool H2DRequestSlot<Request, Reply>::RequestHandle::poll(
    Reply* response) const noexcept {
    PG_ASSERT(slot_ && sequence_ != 0 && slot_->sequence_ == sequence_,
              "invalid or expired H2D request handle");
    if (std::atomic_ref(slot_->state_).load(std::memory_order_acquire) != Idle)
        return false;
    if (response) *response = slot_->reply_;
    return true;
}

template <typename Request, typename Reply>
void H2DRequestSlot<Request, Reply>::RequestHandle::wait(
    Reply* response) const noexcept {
    while (!poll(response)) std::this_thread::yield();
}

template <typename Request, typename Reply>
bool H2DRequestSlot<Request, Reply>::RequestHandle::tryWithdraw()
    const noexcept {
    PG_ASSERT(slot_ && sequence_ != 0 && slot_->sequence_ == sequence_,
              "invalid or expired H2D request handle");
    auto state = std::atomic_ref(slot_->state_);
    uint32_t observed = state.load(std::memory_order_acquire);
    if (observed != Published && observed != Pinned) return false;
    if (!state.compare_exchange_strong(observed, Idle,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire))
        return false;
    PG_ASSERT(slot_->sequence_ != UINT64_MAX);
    ++slot_->sequence_;
    return true;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_H
