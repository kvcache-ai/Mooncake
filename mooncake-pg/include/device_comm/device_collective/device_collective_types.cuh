#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "pg_assert.h"
#include "device_comm/device_utils/d2h_request_slot_types.h"
#include "device_comm/device_utils/h2d_request_slot_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

struct CollectiveStepResult {
    InGroupRank failed_rank = kInvalidInGroupRank;

    [[nodiscard]] __device__ __forceinline__ bool succeeded() const {
        return failed_rank == kInvalidInGroupRank;
    }
};

struct AllReduceRequest {
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    int32_t* failed_ranks_hint = nullptr;
};

// One remote participant selected by an algorithm. Workspace and signal offsets
// are relative to that peer's DTS region.
struct CollectivePeer {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    uint64_t workspace_offset = 0;
    uint64_t view_epoch_signal_offset = 0;
};

// Non-owning compact list of remote peers selected by an algorithm, excluding
// self. Shared by communication, the View handshake, and failure drain.
// Entries are indexed by list position, not by global or in-group rank.
class RemotePeerList {
   public:
    __host__ __device__ constexpr RemotePeerList(
        const CollectivePeer* entries = nullptr, uint32_t count = 0)
        : entries_(entries), count_(count) {
        PG_ASSERT(entries_ || count_ == 0, "Remote peer list has no storage");
    }

    [[nodiscard]] __host__ __device__ constexpr const CollectivePeer* begin()
        const {
        return entries_;
    }

    [[nodiscard]] __host__ __device__ constexpr const CollectivePeer* end()
        const {
        return count_ == 0 ? entries_ : entries_ + count_;
    }

    [[nodiscard]] __host__ __device__ constexpr uint32_t size() const {
        return count_;
    }

    [[nodiscard]] __host__ __device__ constexpr const CollectivePeer& atIndex(
        uint64_t peer_index) const {
        PG_ASSERT(peer_index < count_, "Remote peer index is out of range");
        return entries_[peer_index];
    }

   private:
    const CollectivePeer* entries_;
    uint32_t count_;
};

// Local data operands for one collective step.
// count is in T elements. Each operation defines which pointers it uses;
// unused pointers may be null.
template <typename T>
struct CollectiveChunk {
    const T* source = nullptr;
    T* destination = nullptr;
    uint64_t count = 0;
};

inline constexpr uint32_t kMaxDeviceCollectiveChannels = 32;
static_assert(kMaxDeviceCollectiveChannels <= kTransferLaneCount);
inline constexpr uint32_t kMaxDeviceControlUpdateOperations = 8;
inline constexpr uint32_t kDeviceControlUpdatePayloadBytes = 8192;

inline constexpr bool isDeviceAllReduceCombinationSupported(
    DataType datatype, ReduceOp op) noexcept {
    switch (op) {
        case ReduceOp::Sum:
        case ReduceOp::Product:
        case ReduceOp::Min:
        case ReduceOp::Max:
            break;
        default:
            return false;
    }
    switch (datatype) {
        case DataType::Float16:
        case DataType::Uint8:
        case DataType::Int8:
        case DataType::Int16:
        case DataType::Int32:
        case DataType::Int64:
        case DataType::Bfloat16:
        case DataType::Float32:
        case DataType::Float64:
        case DataType::Bool:
            return true;
        default:
            return false;
    }
}

enum class DevicePlanStatus : uint8_t {
    Unavailable = 0,
    Ready = 1,
};

enum class ControlUpdateOpKind : uint32_t {
    CopyBytes = 0,
    FillBytes = 1,
    FillU64 = 2,
};

struct alignas(16) ControlUpdateOp {
    struct CopyBytes {
        uint64_t destination;
        uint32_t size;
        uint32_t payload_offset;
    };

    struct FillBytes {
        uint64_t destination;
        uint8_t value;
        uint32_t count;
    };

    struct FillU64 {
        uint64_t destination;
        uint64_t value;
        uint32_t count;
    };

    union Payload {
        CopyBytes copy_bytes;
        FillBytes fill_bytes;
        FillU64 fill_u64;
    };

    ControlUpdateOpKind kind = ControlUpdateOpKind::CopyBytes;
    Payload payload = {};
};

// One complete, idempotent update for device-resident control-plane state.
// The host constructs it locally before briefly acquiring the mapped slot for
// publication.
struct alignas(16) ControlUpdate {
    uint32_t operation_count = 0;
    uint32_t payload_size = 0;
    ControlUpdateOp operations[kMaxDeviceControlUpdateOperations] = {};
    alignas(16) uint8_t payload[kDeviceControlUpdatePayloadBytes] = {};
};

struct CollectiveFailureReport {
    InGroupRank failed_rank = kInvalidInGroupRank;
    uint64_t failed_hint_address = 0;
};

// Host-mapped control state shared by the collective kernel and runtime. The
// control-update slot carries ordinary Plan updates as well as the pinned
// update used by failure recovery.
//
// The device submits a failure report only after every active channel CTA has
// stopped touching the old Plan and algorithm buffers. The host pins a control
// update before replying to the matching recovery request.
// The last channel CTA applies the pinned update before it leaves the failed
// collective.
struct alignas(64) ControlMailbox {
    using RecoverySlot = D2HRequestSlot<CollectiveFailureReport>;
    using ControlUpdateSlot = H2DRequestSlot<ControlUpdate>;

    // D2H: the device reports failure; the host replies after pinning the
    // corresponding recovery update.
    RecoverySlot recovery;

    // H2D: the host publishes updates; the device replies after applying them.
    // Ordinary updates coalesce until collective startup consumes them. Pinned
    // recovery updates must remain available for the last channel CTA after
    // the recovery reply.
    ControlUpdateSlot control_update_slot;
};

// State shared by all channel CTAs in one collective launch. A CTA increments
// completion_arrival_count after all of its threads have stopped using the Plan
// and algorithm buffers; a non-last CTA then returns. If a failure was
// reported, the last CTA publishes the latched metadata to the host control
// mailbox and remains in the kernel until recovery finishes.
struct alignas(64) InvocationState {
    uint32_t startup_arrival_count = 0;
    uint32_t startup_complete = 0;
    uint32_t completion_arrival_count = 0;
    uint32_t failure_latched = 0;
    InGroupRank failed_rank = kInvalidInGroupRank;
    uint64_t failed_hint_address = 0;
};

// Stable runtime bindings borrowed by every collective algorithm. Per-call
// operands live in AllReduceRequest; topology and protocol bindings live in the
// algorithm's device state.
struct CollectiveRuntimeBindings {
    const DeviceTransferHandle* transfer_handle = nullptr;
    uint64_t timeout_ticks = 0;
    const uint64_t* view_epoch_signals = nullptr;
    InvocationState* invocation_state = nullptr;
    ControlMailbox* control_mailbox = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
