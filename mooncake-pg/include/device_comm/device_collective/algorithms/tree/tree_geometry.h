#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_GEOMETRY_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_GEOMETRY_H

#include <cstdint>

// These value types and layout calculations are shared by the host planner
// and CUDA C++17 kernels. Do not include CUDA or host planning dependencies.
#if defined(__CUDACC__)
#define MOONCAKE_PG_TREE_HOST_DEVICE __host__ __device__
#else
#define MOONCAKE_PG_TREE_HOST_DEVICE
#endif

namespace mooncake {

inline constexpr uint32_t kTreeTopologyCapacity = 2;
inline constexpr uint32_t kTreeChildCount = 2;
inline constexpr int32_t kInvalidTreeActiveIndex = -1;
inline constexpr uint32_t kInvalidTreeSlot = UINT32_MAX;
inline constexpr uint32_t kInvalidTreeId = UINT32_MAX;
inline constexpr uint32_t kTreePipelineDepth = 2;
inline constexpr uint32_t kTreeReceiveRoleCount = 3;
inline constexpr uint64_t kTreePayloadAlignment = 16;
inline constexpr uint64_t kMaxTreeChunkBytes = 512ull * 1024;
inline constexpr uint64_t kMinTreeBytesPerChannel = 8ull * 1024;

// Indices address ResolvedGroupView.participants, never actual rank numbers.
// The root's only child occupies logical slot 1; slots are not compacted.
struct TreeTopologyIndices {
    int32_t parent = kInvalidTreeActiveIndex;
    int32_t children[kTreeChildCount] = {kInvalidTreeActiveIndex,
                                         kInvalidTreeActiveIndex};
    uint32_t parent_child_slot = kInvalidTreeSlot;
};

enum class TreeReceiveRole : uint32_t {
    Child0 = 0,
    Child1 = 1,
    Parent = 2,
};

struct TreeBufferRegion {
    uint64_t offset = 0;
    uint64_t bytes = 0;
};

// Each channel owns three receive connections, each with two payload slots.
// Staging uses the same role offsets, giving every neighbor a disjoint source
// region. A caller supplies the common workspace capacity, not local capacity.
struct TreeBufferLayout {
    uint32_t channel_count = 0;
    uint64_t chunk_bytes = 0;

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE static constexpr TreeBufferLayout
    make(uint64_t workspace_bytes, uint32_t channels,
         uint64_t max_chunk_bytes = kMaxTreeChunkBytes) noexcept {
        if (channels == 0) return {};
        const uint64_t slots =
            uint64_t{channels} * kTreeReceiveRoleCount * kTreePipelineDepth;
        const uint64_t available = workspace_bytes / slots;
        const uint64_t bounded =
            available < max_chunk_bytes ? available : max_chunk_bytes;
        const uint64_t chunk = bounded - bounded % kTreePayloadAlignment;
        if (chunk == 0) return {};
        return {channels, chunk};
    }

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr uint64_t
    connectionBytes() const noexcept {
        return chunk_bytes * kTreePipelineDepth;
    }

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr uint64_t channelBytes()
        const noexcept {
        return connectionBytes() * kTreeReceiveRoleCount;
    }

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr uint64_t bufferBytes()
        const noexcept {
        return uint64_t{channel_count} * channelBytes();
    }

    // Invalid indices produce an empty region, never an out-of-bounds offset.
    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr TreeBufferRegion
    connection(uint32_t channel, TreeReceiveRole role) const noexcept {
        const auto role_index = static_cast<uint32_t>(role);
        if (channel >= channel_count || role_index >= kTreeReceiveRoleCount)
            return {};
        return {uint64_t{channel} * channelBytes() +
                    uint64_t{role_index} * connectionBytes(),
                connectionBytes()};
    }

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr TreeBufferRegion slot(
        uint32_t channel, TreeReceiveRole role,
        uint32_t pipeline_slot) const noexcept {
        if (pipeline_slot >= kTreePipelineDepth) return {};
        const auto region = connection(channel, role);
        if (region.bytes == 0) return {};
        return {region.offset + uint64_t{pipeline_slot} * chunk_bytes,
                chunk_bytes};
    }
};

// Deterministic on request parameters and the common plan capacity. The
// runtime supplies its fixed launch capacity as max_channels. Zero means the
// request or workspace cannot support a communicating channel.
[[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr uint32_t
chooseTreeChannelCount(uint64_t element_count, uint64_t element_bytes,
                       uint64_t workspace_bytes,
                       uint32_t max_channels) noexcept {
    if ((element_bytes != 1 && element_bytes != 2 && element_bytes != 4 &&
         element_bytes != 8) ||
        max_channels == 0)
        return 0;

    uint32_t channels = max_channels;
    const uint64_t min_elements = kMinTreeBytesPerChannel / element_bytes;
    while (channels > 1) {
        const auto buffers = TreeBufferLayout::make(workspace_bytes, channels);
        if (element_count / channels >= min_elements &&
            buffers.chunk_bytes >= element_bytes)
            return channels;
        channels /= 2;
    }
    return TreeBufferLayout::make(workspace_bytes, 1).chunk_bytes >=
                   element_bytes
               ? 1
               : 0;
}

struct TreeTileBinding {
    uint64_t element_offset = 0;
    uint64_t element_count = 0;
};

// Invocation operands are partitioned after a pending plan update is applied.
// All active channels use tree 0 in the first version. The tree_id boundary
// allows a later version to assign disjoint channels to the second topology.
struct TreeChannelBinding {
    uint32_t tree_id = kInvalidTreeId;
    uint64_t element_offset = 0;
    uint64_t element_count = 0;

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE static constexpr TreeChannelBinding
    make(uint64_t count, uint32_t channel, uint32_t channels) noexcept {
        if (channels == 0 || channel >= channels) return {};
        const uint64_t per_channel = count / channels;
        const uint64_t remainder = count % channels;
        const uint64_t preceding_extra =
            channel < remainder ? channel : remainder;
        return {0, uint64_t{channel} * per_channel + preceding_extra,
                per_channel + (channel < remainder ? 1 : 0)};
    }

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr uint64_t tileCount(
        uint64_t tile_elements) const noexcept {
        if (tree_id == kInvalidTreeId || tile_elements == 0) return 0;
        return element_count / tile_elements +
               (element_count % tile_elements != 0 ? 1 : 0);
    }

    [[nodiscard]] MOONCAKE_PG_TREE_HOST_DEVICE constexpr TreeTileBinding tile(
        uint64_t tile_index, uint64_t tile_elements) const noexcept {
        if (tile_index >= tileCount(tile_elements)) return {};
        const uint64_t local_offset = tile_index * tile_elements;
        const uint64_t remaining = element_count - local_offset;
        return {element_offset + local_offset,
                remaining < tile_elements ? remaining : tile_elements};
    }
};

}  // namespace mooncake

#undef MOONCAKE_PG_TREE_HOST_DEVICE

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_GEOMETRY_H
