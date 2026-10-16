#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_H

#include <atomic>

#include "device_comm/device_utils/d2h_request_slot_types.h"
#include "error_types.h"

namespace mooncake {

template <typename Request, typename Reply>
void D2HRequestSlot<Request, Reply>::RequestHandle::reply(
    const Reply& response) const noexcept {
    PG_ASSERT(slot_ && sequence_ != 0, "invalid D2H request handle");
    const uint64_t replied = std::atomic_ref(slot_->replied_sequence_)
                                 .load(std::memory_order_relaxed);
    PG_ASSERT(
        sequence_ == slot_->received_sequence_ && sequence_ == replied + 1,
        "D2H reply does not match the received request");
    slot_->reply_ = response;
    std::atomic_ref(slot_->replied_sequence_)
        .store(sequence_, std::memory_order_release);
}

template <typename Request, typename Reply>
bool D2HRequestSlot<Request, Reply>::tryReceive(
    ReceivedRequest& received) noexcept {
    const uint64_t sequence =
        std::atomic_ref(submitted_sequence_).load(std::memory_order_acquire);
    if (sequence == received_sequence_) return false;
    PG_ASSERT(
        received_sequence_ != UINT64_MAX && sequence == received_sequence_ + 1,
        "invalid D2H request sequence");

    received.handle = RequestHandle(this, sequence);
    received.request = request_;
    received_sequence_ = sequence;
    return true;
}

template <typename Request, typename Reply>
bool D2HRequestSlot<Request, Reply>::hasPendingRequest() noexcept {
    const uint64_t replied =
        std::atomic_ref(replied_sequence_).load(std::memory_order_acquire);
    const uint64_t submitted =
        std::atomic_ref(submitted_sequence_).load(std::memory_order_acquire);
    return submitted != replied;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_H
