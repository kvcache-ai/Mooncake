// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <memory>

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "buffer_range_index.h"
#include "common.h"
#include "memory_location.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace {

using SegmentDesc = TransferMetadata::SegmentDesc;
using BufferDesc = TransferMetadata::BufferDesc;

BufferDesc MakeBuffer(uint64_t addr, uint64_t length, int index) {
    BufferDesc buffer;
    buffer.name = "cpu:0";
    buffer.addr = addr;
    buffer.length = length;
    buffer.lkey = {static_cast<BufferDesc::mr_key_t>(index + 1)};
    buffer.rkey = {static_cast<BufferDesc::mr_key_t>(index + 1)};
    return buffer;
}

SegmentDesc MakeSegment(const std::vector<BufferDesc> &buffers) {
    SegmentDesc desc;
    desc.name = "unit-test-server:1234";
    desc.protocol = "rdma";
    desc.buffers = buffers;
    EXPECT_EQ(desc.topology.parse(R"({"cpu:0": [["mlx5_unit_test"], []]})"), 0);
    desc.rebuildBufferRangeIndex();
    return desc;
}

// The rule selectDevice() has to reproduce: lowest buffer index that is
// selectable, covers the range, and resolves to a device.
int LinearFirstMatch(SegmentDesc *desc, uint64_t offset, size_t length,
                     int &buffer_id, int &device_id) {
    if (!desc) return ERR_ADDRESS_NOT_REGISTERED;
    for (int i = 0; i < static_cast<int>(desc->buffers.size()); ++i) {
        const auto &buffer = desc->buffers[static_cast<size_t>(i)];
#ifdef ENABLE_MULTI_PROTOCOL
        if (!buffer.protocol.empty() && buffer.protocol != "rdma") continue;
#endif
        if (!bufferCoversRange(buffer.addr, buffer.length, offset, length)) {
            continue;
        }
        int selected = desc->topology.selectDevice(buffer.name, 0);
        if (selected < 0) {
            selected = desc->topology.selectDevice(kWildcardLocation, 0);
        }
        if (selected < 0) continue;
        buffer_id = i;
        device_id = selected;
        return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

}  // namespace

TEST(BufferRangeIndex, DisjointLookupMatchesLinearScan) {
    constexpr int kRegions = 256;
    constexpr uint64_t kStride = 0x100000;
    constexpr uint64_t kLength = 0x80000;
    std::vector<BufferDesc> buffers;
    buffers.reserve(kRegions);
    for (int i = 0; i < kRegions; ++i) {
        buffers.push_back(MakeBuffer(0x100000000ULL + i * kStride, kLength, i));
    }
    auto desc = MakeSegment(buffers);
    ASSERT_FALSE(desc.buffer_range_index.overlaps());
    ASSERT_EQ(desc.buffer_range_index.size(), static_cast<size_t>(kRegions));

    for (int i = 0; i < kRegions; ++i) {
        const uint64_t addr = buffers[i].addr + 32;
        const size_t length = 32256;
        int linear_buffer = -1, linear_device = -1;
        int indexed_buffer = -1, indexed_device = -1;
        ASSERT_EQ(
            LinearFirstMatch(&desc, addr, length, linear_buffer, linear_device),
            0);
        ASSERT_EQ(RdmaTransport::selectDevice(&desc, addr, length,
                                              indexed_buffer, indexed_device),
                  0);
        EXPECT_EQ(indexed_buffer, linear_buffer);
        EXPECT_EQ(indexed_buffer, i);
        EXPECT_EQ(indexed_device, linear_device);

        EXPECT_EQ(desc.buffer_range_index.findCovering(addr, length), i);
        EXPECT_EQ(desc.buffer_range_index.findCovering(addr, 0), i);
    }

    int missing_buffer = -1, missing_device = -1;
    EXPECT_EQ(RdmaTransport::selectDevice(
                  &desc, buffers.back().addr + buffers.back().length, 16,
                  missing_buffer, missing_device),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST(BufferRangeIndex, LastHitHintSkipsScanWhenStillValid) {
    std::vector<BufferDesc> buffers = {
        MakeBuffer(0x1000, 0x1000, 0),
        MakeBuffer(0x3000, 0x1000, 1),
        MakeBuffer(0x5000, 0x1000, 2),
    };
    auto desc = MakeSegment(buffers);

    int buffer_id = -1, device_id = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x3080, 16, buffer_id, device_id),
        0);
    EXPECT_EQ(buffer_id, 1);

    int hinted = -1;
    ASSERT_EQ(RdmaTransport::selectDevice(&desc, 0x3100, 32, hinted, device_id,
                                          0, buffer_id),
              0);
    EXPECT_EQ(hinted, 1);

    int jumped = -1;
    ASSERT_EQ(RdmaTransport::selectDevice(&desc, 0x5080, 16, jumped, device_id,
                                          0, buffer_id),
              0);
    EXPECT_EQ(jumped, 2);
}

TEST(BufferRangeIndex, OverlapLeavesLookupsToTheLinearScan) {
    // Buffer 0 comes first in the list and overlaps buffer 1. A covering MR is
    // no longer unique, so the index declines to answer and selectDevice keeps
    // the original first-match rule.
    std::vector<BufferDesc> buffers = {
        MakeBuffer(0x1000, 0x1000, 0),  // [0x1000, 0x2000)
        MakeBuffer(0x1800, 0x1000, 1),  // [0x1800, 0x2800)
    };
    auto desc = MakeSegment(buffers);
    ASSERT_TRUE(desc.buffer_range_index.overlaps());
    EXPECT_EQ(desc.buffer_range_index.findCovering(0x1900, 0x80), -1);

    int buffer_id = -1, device_id = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x1900, 0x80, buffer_id, device_id),
        0);
    EXPECT_EQ(buffer_id, 0);
}

TEST(BufferRangeIndex, HintDoesNotOverrideEarlierOverlappingBuffer) {
    std::vector<BufferDesc> buffers = {
        MakeBuffer(0x1000, 0x1000, 0),  // [0x1000, 0x2000)
        MakeBuffer(0x1800, 0x1000, 1),  // [0x1800, 0x2800)
    };
    auto desc = MakeSegment(buffers);

    int buffer_id = -1, device_id = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x2100, 16, buffer_id, device_id),
        0);
    EXPECT_EQ(buffer_id, 1);

    int hinted = -1;
    ASSERT_EQ(RdmaTransport::selectDevice(&desc, 0x1900, 0x80, hinted,
                                          device_id, 0, buffer_id),
              0);
    EXPECT_EQ(hinted, 0);
}

TEST(BufferRangeIndex, RebuildSeesMetadataUpdates) {
    std::vector<BufferDesc> buffers = {MakeBuffer(0x1000, 0x1000, 0)};
    auto desc = MakeSegment(buffers);

    int buffer_id = -1, device_id = -1;
    EXPECT_EQ(
        RdmaTransport::selectDevice(&desc, 0x2100, 16, buffer_id, device_id),
        ERR_ADDRESS_NOT_REGISTERED);

    desc.buffers.push_back(MakeBuffer(0x2000, 0x1000, 1));
    desc.rebuildBufferRangeIndex();
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x2100, 16, buffer_id, device_id),
        0);
    EXPECT_EQ(buffer_id, 1);
}

TEST(BufferRangeIndex, StaleIndexStillAnswersThroughTheLinearScan) {
    // A descriptor that mutates `buffers` without rebuilding must degrade to
    // the linear scan, not to a per-lookup rebuild.
    std::vector<BufferDesc> buffers = {MakeBuffer(0x1000, 0x1000, 0)};
    auto desc = MakeSegment(buffers);
    desc.buffers.push_back(MakeBuffer(0x2000, 0x1000, 1));
    ASSERT_NE(desc.buffer_range_index.size(), desc.buffers.size());

    int linear_buffer = -1, linear_device = -1;
    int actual_buffer = -1, actual_device = -1;
    ASSERT_EQ(LinearFirstMatch(&desc, 0x2100, 16, linear_buffer, linear_device),
              0);
    ASSERT_EQ(RdmaTransport::selectDevice(&desc, 0x2100, 16, actual_buffer,
                                          actual_device),
              0);
    EXPECT_EQ(actual_buffer, linear_buffer);
    EXPECT_EQ(actual_device, linear_device);
}

TEST(BufferRangeIndex, AdjacentRegionsAreNotOverlapping) {
    std::vector<BufferDesc> buffers = {
        MakeBuffer(0x1000, 0x1000, 0),
        MakeBuffer(0x2000, 0x1000, 1),
    };
    auto desc = MakeSegment(buffers);
    const auto &index = desc.buffer_range_index;
    EXPECT_FALSE(index.overlaps());

    EXPECT_EQ(index.findCovering(0x1fff, 1), 0);
    EXPECT_EQ(index.findCovering(0x2000, 1), 1);
    // A zero length is point containment, so the shared boundary belongs to
    // the second region only. The linear scan has to agree.
    EXPECT_EQ(index.findCovering(0x2000, 0), 1);
    int linear_buffer = -1, linear_device = -1;
    ASSERT_EQ(LinearFirstMatch(&desc, 0x2000, 0, linear_buffer, linear_device),
              0);
    EXPECT_EQ(linear_buffer, 1);
    EXPECT_EQ(index.findCovering(0x3000, 0), -1);
}

TEST(BufferRangeIndex, LastHitReusesHintDevice) {
    auto desc = MakeSegment({MakeBuffer(0x1000, 0x1000, 0)});
    int buffer_id = -1, device_id = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x1080, 16, buffer_id, device_id),
        0);
    EXPECT_EQ(buffer_id, 0);
    const int first_device = device_id;

    int hinted_buffer = -1, hinted_device = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x1100, 32, hinted_buffer,
                                    hinted_device, 0, buffer_id, first_device),
        0);
    EXPECT_EQ(hinted_buffer, 0);
    EXPECT_EQ(hinted_device, first_device);
}

TEST(BufferRangeIndex, DeviceHintDoesNotFollowBufferJump) {
    auto desc = MakeSegment(
        {MakeBuffer(0x1000, 0x1000, 0), MakeBuffer(0x2000, 0x1000, 1)});
    int buffer_id = -1, device_id = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x1080, 16, buffer_id, device_id),
        0);
    EXPECT_EQ(buffer_id, 0);

    int jumped_buffer = -1, jumped_device = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&desc, 0x2080, 16, jumped_buffer,
                                    jumped_device, 0, buffer_id, device_id),
        0);
    EXPECT_EQ(jumped_buffer, 1);
}

TEST(BufferRangeIndex, InternedNicPathIsStable) {
    auto desc = MakeSegment({MakeBuffer(0x1000, 0x1000, 0)});
    desc.devices.push_back({"mlx5_unit_test", 1, "", ""});
    desc.rebuildInternedNicPaths();
    const auto &first = desc.internedNicPath(0);
    const auto &second = desc.internedNicPath(0);
    EXPECT_EQ(&first, &second);
    EXPECT_EQ(first, MakeNicPath(desc.nicPathServerName(), "mlx5_unit_test"));
}

TEST(BufferRangeIndex, InternedNicPathOutlivesSegmentDesc) {
    auto desc = std::make_unique<SegmentDesc>(
        MakeSegment({MakeBuffer(0x1000, 0x1000, 0)}));
    desc->devices.push_back({"mlx5_unit_test", 1, "", ""});
    desc->rebuildInternedNicPaths();
    const std::string *interned = &desc->internedNicPath(0);
    const std::string expected = *interned;
    desc.reset();
    EXPECT_EQ(*interned, expected);
    EXPECT_EQ(expected, MakeNicPath("unit-test-server:1234", "mlx5_unit_test"));
}

TEST(BufferRangeIndex, InternedNicPathWorksWithoutRebuild) {
    auto desc = MakeSegment({MakeBuffer(0x1000, 0x1000, 0)});
    desc.devices.push_back({"mlx5_unit_test", 1, "", ""});
    EXPECT_TRUE(desc.interned_nic_paths.empty());
    const auto &first = desc.internedNicPath(0);
    const auto &second = desc.internedNicPath(0);
    EXPECT_TRUE(desc.interned_nic_paths.empty());
    EXPECT_EQ(&first, &second);
    EXPECT_EQ(first, MakeNicPath(desc.nicPathServerName(), "mlx5_unit_test"));
}

TEST(BufferRangeIndex, SelectDeviceMatchesLinearFirstMatch) {
    std::mt19937 rng(2262008);
    std::uniform_int_distribution<int> region_dist(1, 64);
    std::uniform_int_distribution<uint64_t> gap_dist(0x1000, 0x100000);
    std::uniform_int_distribution<uint64_t> len_dist(0x100, 0x8000);
    std::uniform_int_distribution<int> overlap_coin(0, 4);
    std::uniform_int_distribution<int> query_kind(0, 9);

    int compared = 0;
    for (int trial = 0; trial < 64; ++trial) {
        const int n = region_dist(rng);
        std::vector<BufferDesc> buffers;
        buffers.reserve(static_cast<size_t>(n));
        uint64_t cursor = 0x100000000ULL;
        for (int i = 0; i < n; ++i) {
            const uint64_t length = len_dist(rng);
            uint64_t addr = cursor;
            if (i > 0 && overlap_coin(rng) == 0) {
                addr = buffers.back().addr + buffers.back().length / 2;
            }
            buffers.push_back(MakeBuffer(addr, length, i));
            cursor = addr + length + gap_dist(rng);
        }
        auto desc = MakeSegment(buffers);
        int hint = -1;
        for (int q = 0; q < 2048; ++q) {
            const int src = static_cast<int>(rng() % static_cast<unsigned>(n));
            const int kind = query_kind(rng);
            uint64_t addr = 0;
            size_t length = 0;
            if (kind == 0) {
                // Zero length on an exact region boundary, where point and
                // range containment disagree if they are not kept in sync.
                addr = buffers[src].addr + buffers[src].length;
            } else if (kind == 1) {
                addr = buffers[src].addr;
                length = static_cast<size_t>(buffers[src].length);
            } else {
                const uint64_t offset_in =
                    rng() % std::max<uint64_t>(1, buffers[src].length / 2);
                addr = buffers[src].addr + offset_in;
                length = static_cast<size_t>(
                    std::min<uint64_t>(64, buffers[src].length - offset_in));
            }
            int linear_buffer = -1, linear_device = -1;
            int indexed_buffer = -1, indexed_device = -1;
            const int linear_ret = LinearFirstMatch(
                &desc, addr, length, linear_buffer, linear_device);
            const int indexed_ret = RdmaTransport::selectDevice(
                &desc, addr, length, indexed_buffer, indexed_device, 0, hint);
            ASSERT_EQ(indexed_ret, linear_ret)
                << "trial=" << trial << " q=" << q << " addr=" << addr
                << " length=" << length;
            if (linear_ret == 0) {
                ASSERT_EQ(indexed_buffer, linear_buffer)
                    << "trial=" << trial << " q=" << q << " addr=" << addr
                    << " length=" << length;
                EXPECT_EQ(indexed_device, linear_device);
                hint = indexed_buffer;
            } else {
                hint = -1;
            }
            ++compared;
        }
    }
    EXPECT_GE(compared, 100000);
}
