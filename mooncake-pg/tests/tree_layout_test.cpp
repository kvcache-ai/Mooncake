#include "device_comm/device_collective/algorithms/tree/tree_geometry.h"

#include <array>
#include <cstdint>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

constexpr auto kCompileTimeLayout = TreeBufferLayout::make(32ull << 20, 32);
static_assert(kCompileTimeLayout.chunk_bytes == 174752);
static_assert(kCompileTimeLayout.bufferBytes() <= (32ull << 20));
static_assert(TreeChannelBinding::make(5, 1, 2).element_offset == 3);
static_assert(TreeChannelBinding::make(5, 1, 2).tileCount(1) == 2);

TEST(TreeLayoutTest, MinimumCapacityAndAlignmentBoundaries) {
    // Three connections * two slots * sixteen bytes is the smallest layout.
    for (uint64_t bytes = 0; bytes < 96; ++bytes) {
        EXPECT_EQ(TreeBufferLayout::make(bytes, 1).chunk_bytes, 0u);
        EXPECT_EQ(chooseTreeChannelCount(65539, 4, bytes, 32), 0u);
    }
    for (uint64_t bytes = 96; bytes < 192; ++bytes) {
        const auto layout = TreeBufferLayout::make(bytes, 1);
        EXPECT_EQ(layout.chunk_bytes, 16u);
        EXPECT_EQ(layout.bufferBytes(), 96u);
    }
    EXPECT_EQ(TreeBufferLayout::make(192, 2).chunk_bytes, 16u);
    EXPECT_EQ(TreeBufferLayout::make(191, 2).chunk_bytes, 0u);
    EXPECT_EQ(TreeBufferLayout::make(1024, 0).channel_count, 0u);
    EXPECT_EQ(TreeBufferLayout::make(1024, 1, 15).chunk_bytes, 0u);
    EXPECT_EQ(TreeBufferLayout::make(1024, 1, 31).chunk_bytes, 16u);
}

TEST(TreeLayoutTest, EveryChannelRoleAndSlotIsDisjointAndWithinCapacity) {
    const std::array<uint64_t, 9> capacities = {96,
                                                192,
                                                4095,
                                                4096,
                                                8192,
                                                (1ull << 20) + 1,
                                                (32ull << 20) - 1,
                                                32ull << 20,
                                                96ull << 20};
    for (const auto bytes : capacities) {
        for (uint32_t channels = 1; channels <= 32; ++channels) {
            SCOPED_TRACE(bytes);
            SCOPED_TRACE(channels);
            const auto layout = TreeBufferLayout::make(bytes, channels);
            EXPECT_LE(layout.bufferBytes(), bytes);
            if (layout.chunk_bytes == 0) continue;
            EXPECT_EQ(layout.chunk_bytes % 16, 0u);
            EXPECT_LE(layout.chunk_bytes, 512ull << 10);
            uint64_t end = 0;
            for (uint32_t channel = 0; channel < channels; ++channel) {
                for (const auto role :
                     {TreeReceiveRole::Child0, TreeReceiveRole::Child1,
                      TreeReceiveRole::Parent}) {
                    const auto connection = layout.connection(channel, role);
                    EXPECT_EQ(connection.offset, end);
                    EXPECT_EQ(connection.bytes, layout.chunk_bytes * 2);
                    for (uint32_t slot = 0; slot < 2; ++slot) {
                        const auto region = layout.slot(channel, role, slot);
                        EXPECT_EQ(region.offset, end);
                        EXPECT_EQ(region.bytes, layout.chunk_bytes);
                        EXPECT_EQ(region.offset % 16, 0u);
                        ASSERT_LE(region.offset, bytes);
                        ASSERT_LE(region.bytes, bytes - region.offset);
                        end = region.offset + region.bytes;
                    }
                }
            }
            EXPECT_EQ(end, layout.bufferBytes());
        }
    }
}

TEST(TreeLayoutTest, SlotCapDoesNotExpandTheSharedWorkspace) {
    const auto small = TreeBufferLayout::make(32ull << 20, 1);
    EXPECT_EQ(small.chunk_bytes, 512ull << 10);
    EXPECT_EQ(small.bufferBytes(), 3ull << 20);
    const auto full = TreeBufferLayout::make(32ull << 20, 32);
    EXPECT_EQ(full.chunk_bytes, 174752u);
    EXPECT_LE(full.bufferBytes(), 32ull << 20);
}

TEST(TreeLayoutTest, InvalidSubViewsAreEmpty) {
    const auto layout = TreeBufferLayout::make(4096, 2);
    EXPECT_EQ(layout.connection(2, TreeReceiveRole::Parent).bytes, 0u);
    EXPECT_EQ(layout.connection(0, static_cast<TreeReceiveRole>(3)).bytes, 0u);
    EXPECT_EQ(layout.slot(1, TreeReceiveRole::Child1, 2).bytes, 0u);
    EXPECT_EQ(layout.slot(UINT32_MAX, TreeReceiveRole::Parent, 0).bytes, 0u);
    EXPECT_EQ(TreeBufferLayout{}.connection(0, TreeReceiveRole::Parent).bytes,
              0u);
}

TEST(TreeLayoutTest, ChildToParentViewsMatchTheLogicalReceiveSlot) {
    const auto layout = TreeBufferLayout::make(32ull << 20, 4);
    // The root receives its only child in Child1; the child receives the
    // root's final broadcast in Parent. These are distinct remote subviews.
    for (uint32_t channel = 0; channel < 4; ++channel) {
        const auto root_receive =
            layout.connection(channel, TreeReceiveRole::Child1);
        const auto child_receive =
            layout.connection(channel, TreeReceiveRole::Parent);
        EXPECT_EQ(root_receive.bytes, child_receive.bytes);
        EXPECT_LE(root_receive.offset + root_receive.bytes,
                  child_receive.offset);
    }
}

TEST(TreeLayoutTest, ChannelSelectionUsesRequestAndCommonCapacity) {
    const uint64_t workspace = 32ull << 20;
    for (const uint64_t size : {1u, 2u, 4u, 8u}) {
        EXPECT_EQ(chooseTreeChannelCount(0, size, workspace, 32), 1u);
        EXPECT_EQ(chooseTreeChannelCount(3, size, workspace, 32), 1u);
        const auto threshold = (8ull << 10) / size;
        EXPECT_EQ(
            chooseTreeChannelCount(threshold * 2 - 1, size, workspace, 32), 1u);
        EXPECT_EQ(chooseTreeChannelCount(threshold * 2, size, workspace, 32),
                  2u);
        EXPECT_EQ(chooseTreeChannelCount(threshold * 32, size, workspace, 32),
                  32u);
        EXPECT_EQ(chooseTreeChannelCount(UINT64_MAX, size, 96, 32), 1u);
        EXPECT_EQ(chooseTreeChannelCount(UINT64_MAX, size, workspace, 32), 32u);
    }
    EXPECT_EQ(chooseTreeChannelCount(65539, 3, workspace, 32), 0u);
    EXPECT_EQ(chooseTreeChannelCount(65539, 0, workspace, 32), 0u);
    EXPECT_EQ(chooseTreeChannelCount(65539, UINT64_MAX, workspace, 32), 0u);
    EXPECT_EQ(chooseTreeChannelCount(65539, 4, workspace, 0), 0u);
}

TEST(TreeLayoutTest, ChannelRangesCoverTheInvocationExactlyOnce) {
    const std::array<uint64_t, 11> counts = {
        0, 1, 2, 3, 5, 31, 32, 33, 65539, (1ull << 20) + 7, UINT64_MAX};
    for (const auto count : counts) {
        for (uint32_t channels = 1; channels <= 32; ++channels) {
            SCOPED_TRACE(count);
            SCOPED_TRACE(channels);
            uint64_t end = 0;
            for (uint32_t channel = 0; channel < channels; ++channel) {
                const auto binding =
                    TreeChannelBinding::make(count, channel, channels);
                EXPECT_EQ(binding.tree_id, 0u);
                EXPECT_EQ(binding.element_offset, end);
                ASSERT_LE(binding.element_count, count - end);
                end += binding.element_count;
                EXPECT_LE(binding.element_count - count / channels, 1u);
            }
            EXPECT_EQ(end, count);
        }
    }
    EXPECT_EQ(TreeChannelBinding::make(3, 0, 0).tree_id, kInvalidTreeId);
    EXPECT_EQ(TreeChannelBinding::make(3, 2, 2).tree_id, kInvalidTreeId);
}

TEST(TreeLayoutTest, TilesCoverTailAndUnalignedLengthsWithoutPointerOverrun) {
    for (const uint64_t count : {0u, 1u, 3u, 31u, 32u, 33u, 65539u}) {
        for (uint32_t channels = 1; channels <= 32; ++channels) {
            const auto layout = TreeBufferLayout::make(8192, channels);
            ASSERT_GT(layout.chunk_bytes, 0u);
            for (const uint64_t size : {1u, 2u, 4u, 8u}) {
                const auto tile_elements = layout.chunk_bytes / size;
                for (uint32_t channel = 0; channel < channels; ++channel) {
                    const auto binding =
                        TreeChannelBinding::make(count, channel, channels);
                    uint64_t end = binding.element_offset;
                    for (uint64_t index = 0;
                         index < binding.tileCount(tile_elements); ++index) {
                        const auto tile = binding.tile(index, tile_elements);
                        EXPECT_EQ(tile.element_offset, end);
                        EXPECT_GT(tile.element_count, 0u);
                        EXPECT_LE(tile.element_count, tile_elements);
                        end += tile.element_count;
                    }
                    EXPECT_EQ(end,
                              binding.element_offset + binding.element_count);
                    EXPECT_EQ(binding
                                  .tile(binding.tileCount(tile_elements),
                                        tile_elements)
                                  .element_count,
                              0u);
                    EXPECT_EQ(
                        binding.tile(UINT64_MAX, tile_elements).element_count,
                        0u);
                    EXPECT_EQ(binding.tileCount(0), 0u);
                }
            }
        }
    }
}

TEST(TreeLayoutTest, ExtremeSizesDoNotOverflowCapacityOrTileArithmetic) {
    const auto layout =
        TreeBufferLayout::make(UINT64_MAX, UINT32_MAX, UINT64_MAX);
    ASSERT_GT(layout.chunk_bytes, 0u);
    EXPECT_LE(layout.bufferBytes(), UINT64_MAX);
    const auto last_slot =
        layout.slot(UINT32_MAX - 1, TreeReceiveRole::Parent, 1);
    ASSERT_LE(last_slot.offset, UINT64_MAX - last_slot.bytes);
    EXPECT_EQ(last_slot.offset + last_slot.bytes, layout.bufferBytes());
    const auto binding = TreeChannelBinding::make(UINT64_MAX, 1, 2);
    const auto last = binding.tile(binding.tileCount(7) - 1, 7);
    ASSERT_LE(last.element_offset, UINT64_MAX - last.element_count);
    EXPECT_EQ(last.element_offset + last.element_count, UINT64_MAX);
    EXPECT_EQ(TreeChannelBinding{}.tileCount(1), 0u);
}

}  // namespace
}  // namespace mooncake
