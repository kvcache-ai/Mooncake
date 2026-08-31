#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

inline constexpr uint32_t kMaxDeviceCollectiveChannels = 32;
static_assert(kMaxDeviceCollectiveChannels <= kTransferLaneCount);
inline constexpr uint32_t kMaxDeviceControlUpdateOperations = 8;
inline constexpr uint32_t kDeviceControlUpdatePayloadBytes = 1024;

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
            return op == ReduceOp::Sum;
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

// Idle: no update is pending; the host may acquire the slot for writing.
// Writing: the host publisher exclusively owns the slot while copying an
// already constructed update; device code must not read or execute it.
// Published: a complete update is visible. The next collective may claim it,
// or the host may replace it to coalesce another update.
// Pinned: recovery has reserved the published update for the last channel CTA
// of the current failed invocation; ordinary collectives and host publishers
// must leave it untouched.
// Claimed: a device CTA exclusively owns and executes the update, then returns
// the slot to Idle.
//
// Replaceable host publication:
//   Idle      -> Writing -> Published
//   Published -> Writing -> Published (coalescing)
// Direct recovery publication:
//   Idle/Published -> Writing -> Pinned
// Normal collective startup:
//   Published -> Claimed -> Idle
// Failed collective resume:
//   Pinned -> Claimed -> Idle
enum class ControlUpdateState : uint32_t {
    Idle = 0,
    Writing = 1,
    Published = 2,
    Pinned = 3,
    Claimed = 4,
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

// Mapped single-slot state for a ControlUpdate. The host owns the slot
// only during the short Writing publication step, and a collective kernel
// owns it while Claimed. Pinned protects a failure-resume update until the last
// channel CTA can apply it. A newer complete update may replace Published,
// which coalesces existing GroupView updates.
struct alignas(64) ControlUpdateSlot {
    uint32_t state = static_cast<uint32_t>(ControlUpdateState::Idle);
    ControlUpdate update;
};

template <typename Plan>
struct PlanSlot {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    Plan plan{};
};

// Host-mapped control state shared by the collective kernel and runtime. The
// control-update slot carries ordinary Plan updates as well as the pinned
// update used by failure recovery.
//
// The device publishes a new failure generation only after every active
// channel CTA has stopped touching the old Plan and protocol buffers. Recovery
// pins a control update and then acknowledges the matching failure generation.
// The last channel CTA applies the pinned update before it leaves the failed
// collective.
struct alignas(64) ControlMailbox {
    uint64_t failure_generation = 0;
    uint64_t ready_generation = 0;

    // Valid only while failure_generation is newer than ready_generation.
    InGroupRank failed_rank = 0;
    uint64_t failed_hint_address = 0;

    // Valid for the last channel CTA only while its state is Pinned and
    // ready_generation has caught up with failure_generation.
    ControlUpdateSlot control_update_slot;
};

// State shared by all channel CTAs in one collective launch. A CTA increments
// completion_arrival_count after all of its threads have stopped using the Plan
// and protocol buffers; a non-last CTA then returns. If a failure was reported,
// the last CTA publishes the latched metadata to the host control mailbox and
// remains in the kernel until recovery finishes.
struct alignas(64) InvocationState {
    uint32_t startup_arrival_count = 0;
    uint32_t startup_complete = 0;
    uint32_t completion_arrival_count = 0;
    uint32_t failure_latched = 0;
    InGroupRank failed_rank = kInvalidInGroupRank;
    uint64_t failed_hint_address = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
