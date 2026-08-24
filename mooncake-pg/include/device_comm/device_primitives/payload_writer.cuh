#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_PAYLOAD_WRITER_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_PAYLOAD_WRITER_CUH

#include <cstdint>

#include <cooperative_groups.h>

#include "common_types.h"
#include "device_comm/device_assert.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"

namespace mooncake {

// Protocol-supplied subrange of the DTS's local-only staging region.
// On a non-direct path, the calling GPU kernel writes the payload directly
// into this range, and publish() passes that address to TransferLane::put().
// PayloadWriter never adds another staging copy.
struct StagingRegion {
    void* ptr = nullptr;
    uint64_t size = 0;
};

// A destination slice relative to the base of the peer's registered region.
struct RemotePayloadRegion {
    uint64_t region_offset = 0;
    uint64_t size = 0;
};

// Describes where the calling GPU kernel materializes an outgoing payload, not
// which concrete route later publishes it. Direct writes into peer memory;
// Staging writes into DTS-owned local staging used as the source passed to
// TransferLane::put().
enum class PayloadWritePath : uint32_t {
    Direct = 0,
    Staging = 1,
};

// Publish one payload previously produced through a PayloadWriteView.
// An attached signal has weak lane-ordering semantics: observing it guarantees
// that this view's payload is visible, but does not order unrelated operations
// on the lane.
struct PayloadPublishRequest {
    uint64_t size = 0;
    SignalAction signal;
    uint64_t timeout_ticks = 0;
};

class PayloadWriter;

// A route-adapted writable destination. The calling GPU kernel writes directly
// into data(): peer memory on a directly addressable path, or caller-supplied
// staging on a Staging path. publish() never inserts a staging copy.
//
// publish() is CTA-collective: every thread in the supplied block must call it
// with the same request.
class PayloadWriteView {
   public:
    [[nodiscard]] __device__ __forceinline__ void* data() const {
        return data_;
    }

    template <typename T>
    [[nodiscard]] __device__ __forceinline__ T* dataAs() const {
        return static_cast<T*>(data_);
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t capacity() const {
        return capacity_;
    }

    [[nodiscard]] __device__ __forceinline__ PayloadWritePath path() const {
        return path_;
    }

    // Submission starts before this function returns. The returned ticket
    // proves local completion when waited; an attached remote signal remains
    // the publication point for a peer consumer.
    [[nodiscard]] __device__ __forceinline__ TransferTicket
    publish(const PayloadPublishRequest& request,
            cooperative_groups::thread_block block) const {
        PG_DEVICE_ASSERT(request.size <= capacity_);

        switch (path_) {
            case PayloadWritePath::Direct: {
                // remotePtr() currently exposes direct mappings only for P2P,
                // whose separate signal() operation publishes the CTA's
                // preceding writes. Direct therefore intentionally implies
                // P2P for now.
                // FIXME: Revisit the ordering contract and this assertion when
                // another route gains direct mappings, since Direct will no
                // longer necessarily imply P2P.
                PG_DEVICE_ASSERT(route_kind_ == DeviceRouteKind::P2p);
                SignalRequest signal;
                signal.signal = request.signal;
                signal.timeout_ticks = request.timeout_ticks;
                return lane_.signal(peer_, signal, block);
            }

            case PayloadWritePath::Staging: {
                PutRequest put;
                put.local_ptr = data_;
                put.remote_offset = remote_offset_;
                put.size = request.size;
                put.signal = request.signal;
                put.timeout_ticks = request.timeout_ticks;
                return lane_.put(peer_, put, block);
            }
        }
        PG_DEVICE_UNREACHABLE();
        return lane_.signal(peer_, SignalRequest{}, block);
    }

   private:
    friend class PayloadWriter;

    __device__ __forceinline__ PayloadWriteView(const TransferLane& lane,
                                                GlobalRank peer,
                                                DeviceRouteKind route_kind,
                                                PayloadWritePath path,
                                                void* data, uint64_t capacity,
                                                uint64_t remote_offset)
        : lane_(lane),
          peer_(peer),
          route_kind_(route_kind),
          path_(path),
          data_(data),
          capacity_(capacity),
          remote_offset_(remote_offset) {}

    TransferLane lane_;
    GlobalRank peer_ = kInvalidGlobalRank;
    DeviceRouteKind route_kind_ = DeviceRouteKind::Unreachable;
    PayloadWritePath path_ = PayloadWritePath::Staging;
    void* data_ = nullptr;
    uint64_t capacity_ = 0;
    uint64_t remote_offset_ = 0;
};

// Binds one peer payload destination for the duration of a stable device-route
// epoch. Typical use is:
//
//   PayloadWriter writer(handle, lane, peer, staging, remote_destination);
//   auto payload =
//       writer.view(staging_offset, remote_offset, payload_capacity);
//   produce_payload(payload.data(), ...);
//   auto ticket = payload.publish(publication, block);
//
// On the direct path, payload.data() points into peer memory, and publish()
// releases the calling GPU kernel's writes before notifying the peer. On the
// Staging path, payload.data() points into the bound DTS staging range, and
// publish() submits a put with the same attached notification.
// Construction performs the capability query once; view() and publish() do not
// allocate memory or copy between intermediate buffers.
class PayloadWriter {
   public:
    __device__ __forceinline__
    PayloadWriter(const DeviceTransferHandle& transfer_handle,
                  const TransferLane& lane, GlobalRank peer,
                  StagingRegion staging, RemotePayloadRegion remote_region)
        : lane_(lane),
          peer_(peer),
          staging_(staging),
          remote_region_(remote_region) {
        PG_DEVICE_ASSERT(peer_ != kInvalidGlobalRank);
        route_kind_ = transfer_handle.routeKind(peer_);
        payload_base_ =
            transfer_handle.remotePtr(peer_, remote_region_.region_offset);
        if (payload_base_) {
            path_ = PayloadWritePath::Direct;
        } else {
            PG_DEVICE_ASSERT(staging_.ptr != nullptr);
            PG_DEVICE_ASSERT(staging_.size != 0);
            path_ = PayloadWritePath::Staging;
            payload_base_ = staging_.ptr;
        }
    }

    [[nodiscard]] __device__ __forceinline__ PayloadWritePath path() const {
        return path_;
    }

    [[nodiscard]] __device__ __forceinline__ GlobalRank peer() const {
        return peer_;
    }

    // Return a view over independently selected staging and remote
    // destination subranges. Each offset is relative to its corresponding
    // bound region. The Direct path ignores `staging_offset`; `capacity` bounds
    // the bytes a later publish may expose.
    [[nodiscard]] __device__ __forceinline__ PayloadWriteView
    view(uint64_t staging_offset, uint64_t remote_offset,
         uint64_t capacity) const {
        PG_DEVICE_ASSERT(remote_offset <= remote_region_.size);
        PG_DEVICE_ASSERT(capacity <= remote_region_.size - remote_offset);
        PG_DEVICE_ASSERT(remote_region_.region_offset <=
                         UINT64_MAX - remote_offset);

        const auto selected_path = path();
        void* destination = nullptr;
        if (selected_path == PayloadWritePath::Direct) {
            destination = static_cast<char*>(payload_base_) + remote_offset;
        } else {
            PG_DEVICE_ASSERT(staging_offset <= staging_.size);
            PG_DEVICE_ASSERT(capacity <= staging_.size - staging_offset);
            destination = static_cast<char*>(payload_base_) + staging_offset;
        }

        return PayloadWriteView(lane_, peer_, route_kind_, selected_path,
                                destination, capacity,
                                remote_region_.region_offset + remote_offset);
    }

   private:
    TransferLane lane_;
    GlobalRank peer_ = kInvalidGlobalRank;
    StagingRegion staging_;
    RemotePayloadRegion remote_region_;
    DeviceRouteKind route_kind_ = DeviceRouteKind::Unreachable;
    PayloadWritePath path_ = PayloadWritePath::Staging;
    void* payload_base_ = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_PAYLOAD_WRITER_CUH
