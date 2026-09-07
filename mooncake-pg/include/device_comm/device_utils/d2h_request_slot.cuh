#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_CUH

#include <transport/device/device_ops.cuh>

#include "device_comm/device_utils/d2h_request_slot_types.h"
#include "device_comm/device_utils/device_assert.cuh"
#include "device_comm/device_utils/device_timeout.cuh"

namespace mooncake {

template <typename Request, typename Reply>
__device__ __forceinline__ bool
D2HRequestSlot<Request, Reply>::RequestHandle::poll(Reply* response) const {
    PG_DEVICE_ASSERT(slot_ && sequence_ != 0 &&
                     device::mc_ld_acquire_u64(&slot_->submitted_sequence_) ==
                         sequence_);
    const uint64_t replied =
        device::mc_ld_acquire_u64(&slot_->replied_sequence_);
    if (replied != sequence_) {
        PG_DEVICE_ASSERT(replied < sequence_);
        return false;
    }
    if (response) *response = slot_->reply_;
    return true;
}

template <typename Request, typename Reply>
__device__ __forceinline__ void
D2HRequestSlot<Request, Reply>::RequestHandle::wait(Reply* response) const {
    (void)wait(0, 0, response);
}

template <typename Request, typename Reply>
__device__ __forceinline__ bool
D2HRequestSlot<Request, Reply>::RequestHandle::wait(uint64_t start_ticks,
                                                    uint64_t timeout_ticks,
                                                    Reply* response) const {
    while (!poll(response)) {
        if (deviceTimedOut(start_ticks, timeout_ticks)) return false;
    }
    return true;
}

template <typename Request, typename Reply>
__device__ __forceinline__ bool
D2HRequestSlot<Request, Reply>::waitUntilIdle(uint64_t start_ticks,
                                              uint64_t timeout_ticks) const {
    const uint64_t submitted =
        device::mc_ld_acquire_u64(&submitted_sequence_);
    while (true) {
        const uint64_t replied =
            device::mc_ld_acquire_u64(&replied_sequence_);
        if (replied == submitted) return true;
        PG_DEVICE_ASSERT(replied < submitted);
        if (deviceTimedOut(start_ticks, timeout_ticks)) return false;
    }
}

template <typename Request, typename Reply>
__device__ __forceinline__
    typename D2HRequestSlot<Request, Reply>::RequestHandle
    D2HRequestSlot<Request, Reply>::submit(const Request& request) {
    return submit(request, 0, 0);
}

template <typename Request, typename Reply>
__device__ __forceinline__
    typename D2HRequestSlot<Request, Reply>::RequestHandle
    D2HRequestSlot<Request, Reply>::submit(const Request& request,
                                           uint64_t start_ticks,
                                           uint64_t timeout_ticks) {
    const uint64_t submitted = device::mc_ld_acquire_u64(&submitted_sequence_);
    while (true) {
        const uint64_t replied = device::mc_ld_acquire_u64(&replied_sequence_);
        if (replied == submitted) break;
        PG_DEVICE_ASSERT(replied < submitted);
        if (deviceTimedOut(start_ticks, timeout_ticks)) return {};
    }
    PG_DEVICE_ASSERT(submitted != UINT64_MAX);

    const uint64_t sequence = submitted + 1;
    request_ = request;
    __threadfence_system();
    device::mc_st_release_u64(&submitted_sequence_, sequence);
    return RequestHandle(this, sequence);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_CUH
