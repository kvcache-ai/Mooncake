#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_TYPES_CUH

#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {

// One 32-bit payload word and its 32-bit readiness tag share an atomic word.
struct LLPacket {
    uint64_t bits;
    static constexpr uint64_t kPayloadBytes = sizeof(uint32_t);
    static constexpr uint64_t kStorageBytes = sizeof(uint64_t);
    static constexpr uint64_t kFirstSequence = 1;

    [[nodiscard]] __host__ __device__ static constexpr uint32_t tagFor(
        uint64_t sequence) {
        return static_cast<uint32_t>(sequence);
    }

    [[nodiscard]] __host__ __device__ static constexpr uint64_t storageBytes(
        uint64_t payload_bytes) {
        return packetCount(payload_bytes) * kStorageBytes;
    }

    [[nodiscard]] __host__ __device__ static constexpr uint64_t packetCount(
        uint64_t payload_bytes) {
        return ceilDiv(payload_bytes, kPayloadBytes);
    }

    [[nodiscard]] __host__ __device__ static constexpr uint64_t payloadCapacity(
        uint64_t storage_bytes) {
        return storage_bytes / kStorageBytes * kPayloadBytes;
    }
};
static_assert(sizeof(LLPacket) == sizeof(uint64_t));

// Separate entry and exit signals bracket packet-tag recycling.
enum class LLBarrierType : uint32_t {
    TagRecycleEnter,
    TagRecycleExit,
};

inline constexpr uint32_t kLLBarrierCount = 2;

// Each channel advances independently across launches and graph replays.
struct alignas(64) LLChannelProgress {
    uint64_t next_sequence = LLPacket::kFirstSequence;
    // A failed exchange requires a new View before its storage can be reused.
    InGroupRank failed_rank = kInvalidInGroupRank;
};

// Host-installed LL addresses and layout for one communicator.
struct LLState {
    const DeviceTransferHandle* transfer_handle = nullptr;
    uint32_t max_group_size = 0;
    uint64_t* tag_recycle_signals = nullptr;
    // Remote tag-recycle signal offsets, indexed by DeviceGroupView active
    // index.
    uint64_t tag_recycle_signal_offsets[kMaxNumRanks] = {};
    // Local progress, indexed by channel.
    LLChannelProgress* progress = nullptr;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_TYPES_CUH
