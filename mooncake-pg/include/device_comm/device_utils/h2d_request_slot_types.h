#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_TYPES_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_TYPES_H

#include <cstdint>
#include <type_traits>

#include <cuda_alike.h>

namespace mooncake {

// A request that carries no data.
struct H2DEmptyRequest {};

// A reply that only acknowledges the request, without returning a value.
struct H2DEmptyReply {};

// One host-to-device request and its device-to-host reply in host-mapped
// memory. Host operations are serialized by the caller. One GPU receiver
// claims the request, reads it in place, and replies before receiving again.
//
//   Host                                 GPU
//   Idle/Published -> Writing
//   copy request
//   publish Published or Pinned      --> acquire and claim either publication
//                                        read request, process it
//                                        copy reply
//   acquire Idle, read reply         <-- release Idle
//
// Either publish mode may replace an unclaimed normal request. Both modes
// wait while the current request is Pinned or Claimed.
//
// The host may withdraw either kind of unclaimed request. Withdrawal and GPU
// claiming are mutually exclusive. Replacement and withdrawal allow reuse
// without a reply. This is different with D2HRequestSlot which permits reuse
// only after a reply.
//
// Host handles expire on the next successful publication or withdrawal. A
// withdrawn or replaced request gets no reply. Dropping a handle does not
// cancel it. Waiting and timeout policies belong to the caller.
template <typename Request = H2DEmptyRequest, typename Reply = H2DEmptyReply>
class alignas(64) H2DRequestSlot {
    static_assert(std::is_trivially_copyable_v<Request>);
    static_assert(std::is_trivially_copyable_v<Reply>);

   public:
    class RequestHandle {
       public:
        __host__ __device__ RequestHandle() = default;

        __host__ __device__ explicit operator bool() const {
            return sequence_ != 0;
        }

        // Host: these operations require a valid, unexpired handle.
        bool poll(Reply* response = nullptr) const noexcept;
        void wait(Reply* response = nullptr) const noexcept;

        // True guarantees this request will not execute and expires the
        // handle. False means the GPU has already claimed or completed it.
        bool tryWithdraw() const noexcept;

        // Device: reply once, after all accesses through ReceivedRequest end.
        __device__ __forceinline__ void reply(const Reply& response) const;

       private:
        friend class H2DRequestSlot;

        __host__ __device__ RequestHandle(H2DRequestSlot* slot,
                                          uint64_t sequence)
            : slot_(slot), sequence_(sequence) {}

        H2DRequestSlot* slot_ = nullptr;
        uint64_t sequence_ = 0;
    };

    struct ReceivedRequest {
        RequestHandle handle;
        // Borrows the slot payload until handle.reply(); no request copy.
        const Request* request = nullptr;
        bool pinned = false;
    };

    // Host: publish a complete request, optionally protecting it from later
    // publishers with pinned=true.
    RequestHandle publish(const Request& request, bool pinned = false);

    // Device: claim a pending request, briefly waiting out host publication.
    // Returns false when idle.
    __device__ __forceinline__ bool tryReceive(ReceivedRequest& received);

   private:
    enum State : uint32_t {
        Idle,
        Writing,
        Published,
        Pinned,
        Claimed,
    };

    uint32_t state_ = Idle;
    // Host writes only while publishing or withdrawing; the GPU reads this
    // while it exclusively owns Claimed.
    uint64_t sequence_ = 0;
    Request request_{};
    Reply reply_{};
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_H2D_REQUEST_SLOT_TYPES_H
