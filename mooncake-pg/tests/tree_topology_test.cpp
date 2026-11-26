#include "device_comm/device_collective/algorithms/tree/tree_topology.h"

#include <array>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "common_types.h"

namespace mooncake {
namespace {

std::vector<TreeTopologyIndices> buildAll(uint32_t count) {
    std::vector<TreeTopologyIndices> result;
    for (uint32_t index = 0; index < count; ++index) {
        const auto node = buildTreeTopology(count, static_cast<int32_t>(index));
        EXPECT_TRUE(node.has_value());
        if (!node.has_value()) return {};
        result.push_back(*node);
    }
    return result;
}

TEST(TreeTopologyTest, SingletonHasNoEdges) {
    const auto result = buildTreeTopology(1, 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->parent, kInvalidTreeActiveIndex);
    EXPECT_EQ(result->children[0], kInvalidTreeActiveIndex);
    EXPECT_EQ(result->children[1], kInvalidTreeActiveIndex);
    EXPECT_EQ(result->parent_child_slot, kInvalidTreeSlot);
}

TEST(TreeTopologyTest, RootChildRemainsInLogicalSlotOne) {
    for (uint32_t count = 2; count <= kMaxNumRanks; ++count) {
        SCOPED_TRACE(count);
        const auto nodes = buildAll(count);
        ASSERT_EQ(nodes.size(), count);
        EXPECT_EQ(nodes[0].children[0], kInvalidTreeActiveIndex);
        const auto child = nodes[0].children[1];
        ASSERT_GT(child, 0);
        ASSERT_LT(static_cast<uint32_t>(child), count);
        EXPECT_EQ(nodes[child].parent, 0);
        EXPECT_EQ(nodes[child].parent_child_slot, 1u);
    }
}

TEST(TreeTopologyTest, MatchesReferenceFourteenParticipantTree) {
    // NCCL's documented incomplete-level example, with root 0 -> child 8.
    const std::array<TreeTopologyIndices, 14> expected = {{
        {-1, {-1, 8}, kInvalidTreeSlot},
        {2, {-1, -1}, 0},
        {4, {1, 3}, 0},
        {2, {-1, -1}, 1},
        {8, {2, 6}, 0},
        {6, {-1, -1}, 0},
        {4, {5, 7}, 1},
        {6, {-1, -1}, 1},
        {0, {4, 12}, 1},
        {10, {-1, -1}, 0},
        {12, {9, 11}, 0},
        {10, {-1, -1}, 1},
        {8, {10, 13}, 1},
        {12, {-1, -1}, 1},
    }};
    const auto nodes = buildAll(expected.size());
    ASSERT_EQ(nodes.size(), expected.size());
    for (size_t index = 0; index < expected.size(); ++index) {
        SCOPED_TRACE(index);
        EXPECT_EQ(nodes[index].parent, expected[index].parent);
        EXPECT_EQ(nodes[index].parent_child_slot,
                  expected[index].parent_child_slot);
        for (uint32_t slot = 0; slot < kTreeChildCount; ++slot)
            EXPECT_EQ(nodes[index].children[slot],
                      expected[index].children[slot]);
    }
}

TEST(TreeTopologyTest, EverySupportedSizeIsConnectedAcyclicAndReciprocal) {
    // Exhaust the PG capacity, including odd sizes and incomplete levels.
    for (uint32_t count = 1; count <= kMaxNumRanks; ++count) {
        SCOPED_TRACE(count);
        const auto nodes = buildAll(count);
        ASSERT_EQ(nodes.size(), count);
        uint32_t roots = 0;
        uint32_t edges = 0;
        std::vector<uint32_t> incoming(count, 0);
        for (uint32_t index = 0; index < count; ++index) {
            SCOPED_TRACE(index);
            const auto& node = nodes[index];
            if (node.parent == kInvalidTreeActiveIndex) {
                ++roots;
                EXPECT_EQ(node.parent_child_slot, kInvalidTreeSlot);
            } else {
                ASSERT_GE(node.parent, 0);
                ASSERT_LT(static_cast<uint32_t>(node.parent), count);
                ASSERT_LT(node.parent_child_slot, kTreeChildCount);
                EXPECT_EQ(nodes[node.parent].children[node.parent_child_slot],
                          static_cast<int32_t>(index));
            }
            for (uint32_t slot = 0; slot < kTreeChildCount; ++slot) {
                const auto child = node.children[slot];
                if (child == kInvalidTreeActiveIndex) continue;
                ASSERT_GE(child, 0);
                ASSERT_LT(static_cast<uint32_t>(child), count);
                EXPECT_NE(child, static_cast<int32_t>(index));
                EXPECT_EQ(nodes[child].parent, static_cast<int32_t>(index));
                EXPECT_EQ(nodes[child].parent_child_slot, slot);
                ++incoming[child];
                ++edges;
            }
            // Every parent walk terminates at the same root without cycles.
            int32_t current = static_cast<int32_t>(index);
            std::vector<bool> visited(count, false);
            while (current != kInvalidTreeActiveIndex) {
                ASSERT_GE(current, 0);
                ASSERT_LT(static_cast<uint32_t>(current), count);
                ASSERT_FALSE(visited[current]);
                visited[current] = true;
                const auto parent = nodes[current].parent;
                if (parent == kInvalidTreeActiveIndex) {
                    EXPECT_EQ(current, 0);
                }
                current = parent;
            }
        }
        EXPECT_EQ(roots, 1u);
        EXPECT_EQ(edges, count - 1);
        EXPECT_EQ(incoming[0], 0u);
        for (uint32_t index = 1; index < count; ++index)
            EXPECT_EQ(incoming[index], 1u);
    }
}

TEST(TreeTopologyTest, SparseRankOrderUsesDenseIndicesAndPreservesSlots) {
    // A resolved active subset can have holes in both rank namespaces, and
    // its first participant need not have the smallest actual rank number.
    const std::array<GlobalRank, 5> global_ranks = {54, 3, 27, 11, 61};
    const std::array<InGroupRank, 5> group_ranks = {7, 2, 12, 5, 15};
    const auto nodes = buildAll(global_ranks.size());
    ASSERT_EQ(nodes.size(), global_ranks.size());
    EXPECT_EQ(global_ranks[nodes[0].children[1]], 61);
    EXPECT_EQ(group_ranks[nodes[0].children[1]], 15);
    for (size_t index = 1; index < nodes.size(); ++index) {
        const auto parent_index = nodes[index].parent;
        const auto parent_slot = nodes[index].parent_child_slot;
        ASSERT_GE(parent_index, 0);
        const auto remote_child_index =
            nodes[parent_index].children[parent_slot];
        ASSERT_EQ(remote_child_index, static_cast<int32_t>(index));
        EXPECT_EQ(global_ranks[remote_child_index], global_ranks[index]);
        EXPECT_EQ(group_ranks[remote_child_index], group_ranks[index]);
    }
}

TEST(TreeTopologyTest, RejectsInvalidSizeAndSelfIndex) {
    for (const auto count : {0u, kMaxNumRanks + 1, UINT32_MAX}) {
        const auto result = buildTreeTopology(count, 0);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code, PGErrorCode::InvalidArgument);
    }
    for (const auto index : {-1, 5, INT32_MAX}) {
        const auto result = buildTreeTopology(5, index);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code, PGErrorCode::InvalidArgument);
    }
}

TEST(TreeTopologyTest, ReservedSecondTopologyStartsInvalid) {
    std::array<TreeTopologyIndices, kTreeTopologyCapacity> topology;
    static_assert(kTreeTopologyCapacity == 2);
    const auto first = buildTreeTopology(5, 4);
    ASSERT_TRUE(first.has_value());
    topology[0] = *first;
    EXPECT_NE(topology[0].parent, kInvalidTreeActiveIndex);
    EXPECT_EQ(topology[1].parent, kInvalidTreeActiveIndex);
    EXPECT_EQ(topology[1].children[0], kInvalidTreeActiveIndex);
    EXPECT_EQ(topology[1].children[1], kInvalidTreeActiveIndex);
    EXPECT_EQ(topology[1].parent_child_slot, kInvalidTreeSlot);
}

}  // namespace
}  // namespace mooncake
