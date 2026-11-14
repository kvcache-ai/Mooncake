#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_CUH

#include <cuda/atomic>

#include "device_comm/device_utils/h2d_request_slot_types.h"
#include "pg_assert.h"

namespace mooncake {

template <typename Request, typename Reply>
__device__ __forceinline__ bool H2DRequestSlot<Request, Reply>::tryReceive(
    ReceivedRequest& received) {
    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> state(state_);
    while (true) {
        uint32_t observed = state.load(cuda::memory_order_acquire);
        switch (observed) {
            case Idle:
                return false;
            case Writing:
                continue;
            case Published:
            case Pinned: {
                const bool pinned = observed == Pinned;
                if (!state.compare_exchange_strong(observed, Claimed,
                                                   cuda::memory_order_acquire,
                                                   cuda::memory_order_relaxed))
                    continue;
                received.handle = RequestHandle(this, sequence_);
                received.request = &request_;
                received.pinned = pinned;
                return true;
            }
            default:
                // The single receiver replies before receiving again.
                PG_UNREACHABLE();
                return false;
        }
    }
}

template <typename Request, typename Reply>
__device__ __forceinline__ void
H2DRequestSlot<Request, Reply>::RequestHandle::reply(
    const Reply& response) const {
    PG_ASSERT(slot_ && sequence_ != 0 && slot_->sequence_ == sequence_);
    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> state(slot_->state_);
    PG_ASSERT(state.load(cuda::memory_order_relaxed) == Claimed);
    slot_->reply_ = response;
    __threadfence_system();
    state.store(Idle, cuda::memory_order_release);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_CUH
