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

#include "common.h"
#include "transport/rdma_transport/rdma_batch_cache.h"

using namespace mooncake;
namespace {
using Desc = TransferMetadata::SegmentDesc;
using Buffer = TransferMetadata::BufferDesc;
std::shared_ptr<Desc> segment(const std::string &location = "cpu:0",
                              bool multi_rail = false) {
    auto d = std::make_shared<Desc>();
    d->name = "test:1234";
    d->protocol = "rdma";
    Buffer b;
    b.addr = 4096;
    b.length = 8192;
    b.name = location;
    b.lkey = {1, 2};
    b.rkey = {3, 4};
    d->buffers.push_back(b);
    d->rebuildBufferRangeIndex();
    EXPECT_EQ(
        d->topology.parse(multi_rail ? R"({"cpu:0":[["mlx5_0","mlx5_1"],[]]})"
                                     : R"({"cpu:0":[["mlx5_0"],[]]})"),
        0);
    return d;
}
int select(BatchRdmaDeviceCache &cache, const std::shared_ptr<Desc> &d,
           uint64_t addr, size_t size, int &b, int &dev, int &calls) {
    return cache.select(d, addr, size, b, dev, [&] {
        ++calls;
        return RdmaTransport::selectDevice(d.get(), addr, size, b, dev);
    });
}
// Mirrors WorkerPool::selectPeerDevice's post-select rkey / devices bounds
// checks. Cache hits must skip this resolve, including those checks.
int selectPeer(BatchRdmaDeviceCache &cache, const std::shared_ptr<Desc> &d,
               uint64_t addr, size_t size, int &b, int &dev, int &calls) {
    return cache.select(d, addr, size, b, dev, [&] {
        ++calls;
        const int rc = RdmaTransport::selectDevice(d.get(), addr, size, b, dev);
        if (rc) return rc;
        if (b < 0 || static_cast<size_t>(b) >= d->buffers.size() || dev < 0 ||
            static_cast<size_t>(dev) >= d->buffers[b].rkey.size())
            return ERR_ADDRESS_NOT_REGISTERED;
        if (static_cast<size_t>(dev) >= d->devices.size())
            return ERR_ADDRESS_NOT_REGISTERED;
        return 0;
    });
}
TEST(BatchRdmaDeviceCache, ReusesSingleRailAcrossOffsets) {
    auto d = segment();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    for (int i = 0; i < 16; ++i) {
        ASSERT_EQ(select(cache, d, 4096 + i * 32, 32, b, dev, calls), 0);
        EXPECT_EQ(b, 0);
        EXPECT_EQ(dev, 0);
    }
    EXPECT_EQ(calls, 1);
}
TEST(BatchRdmaDeviceCache, DoesNotReuseAcrossRegionOrGap) {
    auto d = segment();
    auto b2 = d->buffers[0];
    b2.addr = 16384;
    d->buffers.push_back(b2);
    d->rebuildBufferRangeIndex();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    ASSERT_EQ(select(cache, d, 16384, 32, b, dev, calls), 0);
    EXPECT_EQ(b, 1);
    EXPECT_NE(select(cache, d, 12288, 32, b, dev, calls), 0);
    EXPECT_EQ(calls, 3);
}
TEST(BatchRdmaDeviceCache, RejectsCrossBoundaryAndOverflow) {
    auto d = segment();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    EXPECT_NE(select(cache, d, 12280, 32, b, dev, calls), 0);
    EXPECT_NE(select(cache, d, UINT64_MAX - 8, 32, b, dev, calls), 0);
    EXPECT_EQ(calls, 3);
}
TEST(BatchRdmaDeviceCache, MultiRailRetainsPerRequestSelection) {
    auto d = segment("cpu:0", true);
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    for (int i = 0; i < 16; ++i)
        ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    EXPECT_EQ(calls, 16);
}
TEST(BatchRdmaDeviceCache, SegmentedLocationRetainsOffsetResolution) {
    auto d = segment("segments:4096:0,1");
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    ASSERT_EQ(select(cache, d, 8192, 32, b, dev, calls), 0);
    EXPECT_EQ(calls, 2);
}
TEST(BatchRdmaDeviceCache, OverlapRetainsFirstMatch) {
    auto d = segment();
    auto narrow = d->buffers[0];
    narrow.addr = 8192;
    narrow.length = 32;
    d->buffers.insert(d->buffers.begin(), narrow);
    d->rebuildBufferRangeIndex();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    EXPECT_EQ(b, 1);
    ASSERT_EQ(select(cache, d, 8192, 32, b, dev, calls), 0);
    EXPECT_EQ(b, 0);
    EXPECT_EQ(calls, 2);
}
TEST(BatchRdmaDeviceCache, RefreshUsesNewSnapshot) {
    auto d = segment();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    d = segment();
    d->buffers[0].name = "unknown";
    ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    EXPECT_EQ(calls, 2);
}
TEST(BatchRdmaDeviceCache, FailureIsNotCached) {
    auto d = segment();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(cache.select(d, 4096, 32, b, dev,
                               [&] {
                                   ++calls;
                                   return ERR_ADDRESS_NOT_REGISTERED;
                               }),
                  ERR_ADDRESS_NOT_REGISTERED);
    }
    EXPECT_EQ(calls, 2);
}
TEST(BatchRdmaDeviceCache, PeerDeviceBoundsFailureIsNotCached) {
    auto d = segment();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(selectPeer(cache, d, 4096, 32, b, dev, calls),
                  ERR_ADDRESS_NOT_REGISTERED);
    }
    EXPECT_EQ(calls, 2);
}
TEST(BatchRdmaDeviceCache, InvalidateDropsHit) {
    auto d = segment();
    BatchRdmaDeviceCache cache;
    int calls = 0, b = -1, dev = -1;
    ASSERT_EQ(select(cache, d, 4096, 32, b, dev, calls), 0);
    cache.invalidate();
    ASSERT_EQ(select(cache, d, 4128, 32, b, dev, calls), 0);
    EXPECT_EQ(calls, 2);
}
}  // namespace
