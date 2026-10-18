// Compile this translation unit as C++17, matching the CUDA kernel boundary.
#include "device_comm/device_collective/algorithms/tree/tree_geometry.h"

#include <type_traits>

namespace mooncake {

static_assert(std::is_trivially_copyable_v<TreeTopologyIndices>);
static_assert(std::is_trivially_copyable_v<TreeBufferLayout>);
static_assert(std::is_trivially_copyable_v<TreeChannelBinding>);
static_assert(std::is_standard_layout_v<TreeTopologyIndices>);

constexpr auto layout = TreeBufferLayout::make(96, 1);
static_assert(layout.chunk_bytes == 16);
static_assert(layout.slot(0, TreeReceiveRole::Child0, 0).offset == 0);
static_assert(layout.slot(0, TreeReceiveRole::Child1, 0).offset == 32);
static_assert(layout.slot(0, TreeReceiveRole::Parent, 1).offset == 80);
static_assert(chooseTreeChannelCount(4096, 4, 32ull << 20, 32) == 2);
constexpr auto channel = TreeChannelBinding::make(65539, 1, 2);
static_assert(channel.tree_id == 0);
static_assert(channel.element_offset == 32770);
static_assert(channel.tileCount(8192) == 5);
static_assert(channel.tile(4, 8192).element_count == 1);

}  // namespace mooncake
