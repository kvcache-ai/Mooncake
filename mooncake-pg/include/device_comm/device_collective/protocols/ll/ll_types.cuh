#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_TYPES_CUH

#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

enum class LLControlWord : uint32_t {
    BufferReady = 0,
    WrapReady = 1,
    Count,
};

// Communicator-local signals, indexed by the peer publishing each value.
struct LLSignalLayout {
    uint32_t max_group_size = 0;

    [[nodiscard]] __host__ __device__ constexpr uint64_t controlOffset(
        LLControlWord kind, InGroupRank sender) const {
        return (uint64_t{max_group_size} * static_cast<uint32_t>(kind) +
                static_cast<uint32_t>(sender)) *
               sizeof(uint64_t);
    }

    [[nodiscard]] __host__ __device__ constexpr uint64_t signalBytes() const {
        return uint64_t{max_group_size} *
               static_cast<uint32_t>(LLControlWord::Count) * sizeof(uint64_t);
    }
};

// Protocol-owned bindings, indexed by stable InGroupRank.
struct LLPeer {
    GlobalRank global_rank = kInvalidGlobalRank;
    // Remote offsets are relative to the peer's DTS region base.
    uint64_t signal_offset = 0;
    // Runtime-resolved word used by common View synchronization.
    uint64_t view_epoch_signal_offset = 0;
};

// Shared by users of one LL endpoint. Peer bindings and progress are installed
// with the new Plans; advance progress only after acquiring the required peers.
struct LLState {
    const DeviceTransferHandle* transfer_handle = nullptr;
    InGroupRank self_rank = kInvalidInGroupRank;
    uint64_t* signals = nullptr;
    LLSignalLayout signal_layout;
    LLPeer peers[kMaxNumRanks] = {};
    uint64_t buffer_ready_sequence = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_TYPES_CUH
