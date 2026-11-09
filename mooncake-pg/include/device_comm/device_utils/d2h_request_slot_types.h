#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_TYPES_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_TYPES_H

#include <cstdint>
#include <type_traits>

#include <cuda_alike.h>

namespace mooncake {

// A reply that only acknowledges the request, without returning a value.
struct D2HRequestAck {};

// One device-to-host request and its host-to-device reply in host-mapped
// memory. There is one GPU submitter and one serialized host receiver. Host
// implementations live in d2h_request_slot.h; device implementations live in
// d2h_request_slot.cuh.
//
//   GPU                                  Host
//   acquire previous reply
//   copy request, system fence
//   release submitted sequence      -->  acquire submitted sequence
//                                        snapshot request once, process it
//                                        copy reply
//   acquire replied sequence        <--  release replied sequence
//   read reply or discard it before submitting the next request
//
// A handle belongs to this slot and expires on the next successful submission.
// Dropping a handle or timing out does not cancel the request: the slot cannot
// be reused until the host replies. Waiting and timeout policies belong to the
// caller.
template <typename Request, typename Reply = D2HRequestAck>
class D2HRequestSlot {
    static_assert(std::is_trivially_copyable_v<Request>);
    static_assert(std::is_trivially_copyable_v<Reply>);

   public:
    class RequestHandle {
       public:
        __host__ __device__ RequestHandle() = default;

        __host__ __device__ explicit operator bool() const {
            return sequence_ != 0;
        }

        // Device side: poll() and wait() require a valid, unexpired handle.
        // Check once and copy the reply to response when ready.
        __device__ __forceinline__ bool poll(Reply* response = nullptr) const;

        // Wait indefinitely, or use the caller's existing timeout budget.
        // On timeout, return false. Zero disables timeout.
        __device__ __forceinline__ void wait(Reply* response = nullptr) const;
        __device__ __forceinline__ bool wait(uint64_t start_ticks,
                                             uint64_t timeout_ticks,
                                             Reply* response = nullptr) const;

        // Host side: reply once using a handle returned by tryReceive().
        void reply(const Reply& response) const noexcept;

       private:
        friend class D2HRequestSlot;

        __host__ __device__ RequestHandle(D2HRequestSlot* slot,
                                          uint64_t sequence)
            : slot_(slot), sequence_(sequence) {}

        D2HRequestSlot* slot_ = nullptr;
        uint64_t sequence_ = 0;
    };

    struct ReceivedRequest {
        RequestHandle handle;
        Request request{};
    };

    // Device side: wait for an empty slot, then publish the request.
    // Wait indefinitely, or use the caller's existing timeout budget.
    // On timeout, return an invalid handle. Zero disables timeout.
    __device__ __forceinline__ RequestHandle submit(const Request& request);
    __device__ __forceinline__ RequestHandle submit(const Request& request,
                                                    uint64_t start_ticks,
                                                    uint64_t timeout_ticks);

    // Device side: after producers stop, wait for the latest request's reply.
    // On timeout, return false. Zero disables timeout.
    __device__ __forceinline__ bool waitUntilIdle(uint64_t start_ticks,
                                                  uint64_t timeout_ticks) const;

    // Host side: nonblocking receive; return false if there is no new request.
    // Each request is received once and remains pending until its reply.
    // The caller must serialize tryReceive() and RequestHandle::reply().
    bool tryReceive(ReceivedRequest& received) noexcept;

    // Host observers may inspect pending work without receiving the request.
    bool hasPendingRequest() noexcept;

   private:
    uint64_t submitted_sequence_ = 0;
    uint64_t replied_sequence_ = 0;
    // Host-only receipt tracking prevents repeated delivery.
    uint64_t received_sequence_ = 0;
    Request request_{};
    Reply reply_{};
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_D2H_REQUEST_SLOT_TYPES_H
