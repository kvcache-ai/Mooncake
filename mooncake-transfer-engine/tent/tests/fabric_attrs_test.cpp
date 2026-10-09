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

#include "tent/transport/fabric/fabric_attrs.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

TEST(FabricAttrsTest, HexRoundTrip) {
    const std::string raw(
        "\x00\x01\xfe\xff"
        "abc",
        7);
    const std::string hex = hexEncode(raw);
    EXPECT_EQ(hex, "0001feff616263");
    std::string back;
    ASSERT_TRUE(hexDecode(hex, back));
    EXPECT_EQ(back, raw);
    EXPECT_FALSE(hexDecode("abc", back));
    EXPECT_FALSE(hexDecode("zz", back));
}

TEST(FabricAttrsTest, PeerAttrRoundTrip) {
    FabricPeerAttr attr;
    attr.provider = "efa";
    attr.virt_addr = false;
    attr.nics.push_back({"rdmap0s31-rdm", std::string("\x01\x00\x02", 3), 0});
    attr.nics.push_back({"rdmap1s31-rdm", "xyz", 1});
    FabricPeerAttr decoded;
    ASSERT_TRUE(decodeFabricPeerAttr(encodeFabricPeerAttr(attr), decoded).ok());
    EXPECT_EQ(decoded.provider, "efa");
    EXPECT_FALSE(decoded.virt_addr);
    ASSERT_EQ(decoded.nics.size(), 2u);
    EXPECT_EQ(decoded.nics[0].address, attr.nics[0].address);
    EXPECT_EQ(decoded.nics[1].name, "rdmap1s31-rdm");
    EXPECT_EQ(decoded.nics[1].numa_node, 1);
}

TEST(FabricAttrsTest, PeerAttrRejectsBadInput) {
    FabricPeerAttr decoded;
    EXPECT_FALSE(decodeFabricPeerAttr("not json", decoded).ok());
    EXPECT_FALSE(decodeFabricPeerAttr(
                     R"({"version":99,"provider":"efa","virt_addr":true,)"
                     R"("nics":[]})",
                     decoded)
                     .ok());
    EXPECT_FALSE(decodeFabricPeerAttr(
                     R"({"version":1,"provider":"efa","virt_addr":true,)"
                     R"("nics":[{"name":"a","addr":"xyz"}]})",
                     decoded)
                     .ok());
}

TEST(FabricAttrsTest, BufferAttrRoundTripAndValidation) {
    FabricBufferAttr attr;
    attr.chunks.push_back({0, 100, {0, 1}, {11, 12}});
    attr.chunks.push_back({100, 50, {1}, {0xffffffffffffull}});
    const std::string text = encodeFabricBufferAttr(attr);

    FabricBufferAttr decoded;
    ASSERT_TRUE(decodeFabricBufferAttr(text, 150, decoded).ok());
    ASSERT_EQ(decoded.chunks.size(), 2u);
    uint64_t key = 0;
    EXPECT_TRUE(decoded.chunks[1].keyFor(1, key));
    EXPECT_EQ(key, 0xffffffffffffull);
    EXPECT_FALSE(decoded.chunks[1].keyFor(0, key));

    // Chunks must cover exactly the buffer.
    EXPECT_FALSE(decodeFabricBufferAttr(text, 160, decoded).ok());
    // Gaps, empty NIC lists and nics/keys mismatches are rejected.
    EXPECT_FALSE(decodeFabricBufferAttr(
                     R"({"chunks":[{"off":10,"len":5,"nics":[0],"keys":[1]}]})",
                     15, decoded)
                     .ok());
    EXPECT_FALSE(
        decodeFabricBufferAttr(
            R"({"chunks":[{"off":0,"len":5,"nics":[],"keys":[]}]})", 5, decoded)
            .ok());
    EXPECT_FALSE(decodeFabricBufferAttr(
                     R"({"chunks":[{"off":0,"len":5,"nics":[0],"keys":[]}]})",
                     5, decoded)
                     .ok());
}

TEST(FabricAttrsTest, PlanChunks) {
    auto one = planFabricChunks(100, 0);
    ASSERT_EQ(one.size(), 1u);
    EXPECT_EQ(one[0].length, 100u);

    auto chunks = planFabricChunks(250, 100);
    ASSERT_EQ(chunks.size(), 3u);
    EXPECT_EQ(chunks[2].offset, 200u);
    EXPECT_EQ(chunks[2].length, 50u);
}

TEST(FabricAttrsTest, AssignFullCoverageWithinBudget) {
    auto chunks = planFabricChunks(4 * 4096, 2 * 4096);
    std::vector<std::vector<int>> assignment;
    ASSERT_TRUE(assignFabricChunkNics(chunks, 3, 4096, 4, assignment).ok());
    ASSERT_EQ(assignment.size(), 2u);
    EXPECT_EQ(assignment[0], (std::vector<int>{0, 1, 2}));
    EXPECT_EQ(assignment[1], (std::vector<int>{0, 1, 2}));
}

TEST(FabricAttrsTest, AssignDisjointPartitionOverBudget) {
    auto chunks = planFabricChunks(4 * 4096, 2 * 4096);
    std::vector<std::vector<int>> assignment;
    ASSERT_TRUE(assignFabricChunkNics(chunks, 5, 4096, 2, assignment).ok());
    EXPECT_EQ(assignment[0], (std::vector<int>{0, 1, 2}));
    EXPECT_EQ(assignment[1], (std::vector<int>{3, 4}));
}

TEST(FabricAttrsTest, AssignRoundRobinAndRejectOverflow) {
    auto chunks = planFabricChunks(4 * 4096, 4096);
    std::vector<std::vector<int>> assignment;
    ASSERT_TRUE(assignFabricChunkNics(chunks, 2, 4096, 2, assignment).ok());
    EXPECT_EQ(assignment[0], (std::vector<int>{0}));
    EXPECT_EQ(assignment[1], (std::vector<int>{1}));
    EXPECT_EQ(assignment[2], (std::vector<int>{0}));
    EXPECT_FALSE(assignFabricChunkNics(chunks, 2, 4096, 1, assignment).ok());
    EXPECT_FALSE(assignFabricChunkNics(chunks, 0, 4096, 0, assignment).ok());
}

TEST(FabricAttrsTest, CutSpansAtBothChunkBoundaries) {
    // Local chunks every 100 bytes, remote every 70.
    auto local = planFabricChunks(1000, 100);
    auto remote = planFabricChunks(1000, 70);
    std::vector<FabricSpan> spans;
    ASSERT_TRUE(cutFabricSpans(200, 50, local, 20, remote, 0, spans));
    // Local boundaries at +50, +150; remote at +50, +120, +190.
    std::vector<uint64_t> offsets, lengths;
    for (auto& span : spans) {
        offsets.push_back(span.offset);
        lengths.push_back(span.length);
    }
    EXPECT_EQ(offsets, (std::vector<uint64_t>{0, 50, 120, 150, 190}));
    EXPECT_EQ(lengths, (std::vector<uint64_t>{50, 70, 30, 40, 10}));
    EXPECT_EQ(spans[1].local_chunk, 1u);
    EXPECT_EQ(spans[1].remote_chunk, 1u);
    EXPECT_EQ(spans[4].local_chunk, 2u);
    EXPECT_EQ(spans[4].remote_chunk, 3u);
}

TEST(FabricAttrsTest, CutSpansHonoursMaxSpanAndBounds) {
    auto single = planFabricChunks(1000, 0);
    std::vector<FabricSpan> spans;
    ASSERT_TRUE(cutFabricSpans(1000, 0, single, 0, single, 300, spans));
    ASSERT_EQ(spans.size(), 4u);
    EXPECT_EQ(spans[3].length, 100u);
    EXPECT_FALSE(cutFabricSpans(100, 950, single, 0, single, 0, spans));
    EXPECT_FALSE(cutFabricSpans(100, 0, single, 901, single, 0, spans));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
