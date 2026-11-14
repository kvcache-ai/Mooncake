#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH

#include <cstdint>
#include <variant>

#include <cuda_alike.h>

#include "common_types.h"
#include "pg_assert.h"
#include "device_comm/device_utils/d2h_request_slot_types.h"
#include "device_comm/device_utils/h2d_request_slot_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

enum class CollectiveFailureType : uint32_t {
    Communication,
    // The request requires a participant absent from the installed view.
    // e.g. broadcast without active root.
    UnavailableParticipant,
};

struct CollectiveResult {
    InGroupRank failed_rank = kInvalidInGroupRank;
    CollectiveFailureType failure_type = CollectiveFailureType::Communication;

    [[nodiscard]] __device__ __forceinline__ bool succeeded() const {
        return failed_rank == kInvalidInGroupRank;
    }
};

// Evaluate a collective step once and return its failure unchanged.
#define PG_COLL_TRY(...)                                             \
    do {                                                             \
        const ::mooncake::CollectiveResult pg_coll_result_internal = \
            (__VA_ARGS__);                                           \
        if (!pg_coll_result_internal.succeeded()) {                  \
            return pg_coll_result_internal;                          \
        }                                                            \
    } while (false)

struct AllReduceRequest {
    static constexpr OpType kOperation = OpType::AllReduce;

    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    int32_t* failed_ranks_hint = nullptr;
};

struct AllGatherRequest {
    static constexpr OpType kOperation = OpType::AllGather;

    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    // Number of input elements contributed by each participant.
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    int32_t* failed_ranks_hint = nullptr;
};

struct ReduceScatterRequest {
    static constexpr OpType kOperation = OpType::ReduceScatter;

    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    // Number of output elements per participant. Input uses in-group rank
    // slots.
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    int32_t* failed_ranks_hint = nullptr;
};

struct BroadcastRequest {
    static constexpr OpType kOperation = OpType::Broadcast;

    // Only the root uses send_buffer; every participant uses recv_buffer.
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    InGroupRank root = kInvalidInGroupRank;
    int32_t* failed_ranks_hint = nullptr;
};

struct ReduceRequest {
    static constexpr OpType kOperation = OpType::Reduce;

    // Every participant uses send_buffer; only the root uses recv_buffer.
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    InGroupRank root = kInvalidInGroupRank;
    int32_t* failed_ranks_hint = nullptr;
};

struct AllToAllRequest {
    static constexpr OpType kOperation = OpType::AllToAll;

    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    // Number of elements per peer. Both buffers use in-group rank slots.
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    int32_t* failed_ranks_hint = nullptr;
};

struct GatherRequest {
    static constexpr OpType kOperation = OpType::Gather;

    // Only the root uses recv_buffer, with one slot per in-group rank.
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    // Number of input elements contributed by each participant.
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    InGroupRank root = kInvalidInGroupRank;
    int32_t* failed_ranks_hint = nullptr;
};

struct ScatterRequest {
    static constexpr OpType kOperation = OpType::Scatter;

    // Only the root uses send_buffer, with one slot per in-group rank.
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    // Number of output elements per participant.
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    InGroupRank root = kInvalidInGroupRank;
    int32_t* failed_ranks_hint = nullptr;
};

struct BarrierRequest {
    static constexpr OpType kOperation = OpType::Barrier;

    int32_t* failed_ranks_hint = nullptr;
};

using DeviceCollectiveRequest =
    std::variant<AllReduceRequest, AllGatherRequest, ReduceScatterRequest,
                 BroadcastRequest, ReduceRequest, AllToAllRequest,
                 GatherRequest, ScatterRequest, BarrierRequest>;

// Identity and shared addressing for one participant. Offsets are relative to
// that participant's DTS region.
struct CollectivePeer {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    uint64_t workspace_offset = 0;
    uint64_t view_epoch_signal_offset = 0;
};

// Device membership shared by the runtime, algorithms and protocols.
struct DeviceGroupView {
    uint64_t epoch = kInvalidViewEpoch;
    int32_t self_active_index = -1;
    uint32_t participant_count = 0;
    // Active participants in increasing in-group rank order.
    CollectivePeer participants[kMaxNumRanks] = {};
    // Set by the startup CTA after the handshake; reset when a view is
    // installed.
    bool synchronized = false;

    [[nodiscard]] __device__ __forceinline__ int32_t
    activeIndex(InGroupRank rank) const {
        // Fast path: active indices match in-group ranks before the first gap.
        if (static_cast<uint32_t>(rank) < participant_count &&
            participants[rank].in_group_rank == rank)
            return rank;

        // Missing ranks shift active indices left. Scan backward from the
        // nearest unchecked index.
        int32_t index = static_cast<int32_t>(participant_count);
        if (rank < index) index = rank;
        while (index > 0) {
            --index;
            const auto peer_rank = participants[index].in_group_rank;
            if (peer_rank == rank) return index;
            if (peer_rank < rank) break;
        }
        return -1;
    }

    [[nodiscard]] __device__ __forceinline__ const CollectivePeer& self()
        const {
        PG_ASSERT(self_active_index >= 0 &&
                  static_cast<uint32_t>(self_active_index) < participant_count);
        return participants[self_active_index];
    }
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
// Reserve capacity for additional protocol and algorithm updates.
inline constexpr uint32_t kMaxDeviceControlUpdateOperations = 128;
inline constexpr uint32_t kDeviceControlUpdatePayloadBytes = 8192;

enum class DevicePlanStatus : uint8_t {
    Unavailable = 0,
    Ready = 1,
};

enum class ControlUpdateOpKind : uint32_t {
    CopyBytes = 0,
    Fill = 1,
};

struct alignas(16) ControlUpdateOp {
    struct CopyBytes {
        uint64_t destination;
        uint32_t size;
        uint32_t payload_offset;
    };

    struct Fill {
        uint64_t destination;
        uint32_t element_size;
        uint32_t count;
        uint32_t payload_offset;
    };

    union Payload {
        CopyBytes copy_bytes;
        Fill fill;
    };

    ControlUpdateOpKind kind = ControlUpdateOpKind::CopyBytes;
    Payload payload = {};
};

// One complete update for device-resident control-plane state.
// The host constructs it locally before briefly acquiring the mapped slot for
// publication.
struct alignas(16) ControlUpdate {
    uint32_t operation_count = 0;
    uint32_t payload_size = 0;
    ControlUpdateOp operations[kMaxDeviceControlUpdateOperations] = {};
    alignas(16) uint8_t payload[kDeviceControlUpdatePayloadBytes] = {};
};

struct CollectiveFailureReport {
    CollectiveResult result;
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
    CollectiveResult result;
    uint64_t failed_hint_address = 0;
};

// Stable runtime bindings borrowed by every collective algorithm. Per-call
// operands live in requests.
struct CollectiveRuntimeBindings {
    const DeviceTransferHandle* transfer_handle = nullptr;
    uint64_t timeout_ticks = 0;
    DeviceGroupView* group_view = nullptr;
    const uint64_t* view_epoch_signals = nullptr;
    InvocationState* invocation_state = nullptr;
    ControlMailbox* control_mailbox = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
