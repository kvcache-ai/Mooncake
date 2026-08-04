// Copyright 2026 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
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

#include <cstddef>
#include <cstring>

#include "shared_segment/shared_segment.h"
#include "shared_segment_internal.h"

namespace mooncake {
namespace {
constexpr uint64_t kSegmentSize = 61 * (4096 + 8192);

SharedSegmentOptions KvOptions(uint32_t rank_id = 0, uint32_t world_size = 2) {
    SharedSegmentOptions options;
    options.world_size = world_size;
    options.rank_id = rank_id;
    options.owner_rank = 0;
    return options;
}
}  // namespace

TEST(SharedSegmentFingerprintTest, ChangesWithEveryDeclaredField) {
    SharedSegmentOptions options = KvOptions();
    const auto base = ComputeSegmentFingerprint("kv", kSegmentSize, options);

    EXPECT_NE(ComputeSegmentFingerprint("other", kSegmentSize, options), base);
    EXPECT_NE(ComputeSegmentFingerprint("kv", kSegmentSize + 1, options), base);

    auto different_world = options;
    different_world.world_size = 8;
    EXPECT_NE(ComputeSegmentFingerprint("kv", kSegmentSize, different_world),
              base);

    auto different_owner = options;
    different_owner.owner_rank = 1;
    EXPECT_NE(ComputeSegmentFingerprint("kv", kSegmentSize, different_owner),
              base);

    // rank_id and device_id are local and must not affect the digest.
    auto different_rank = options;
    different_rank.rank_id = 7;
    different_rank.device_id = 3;
    EXPECT_EQ(ComputeSegmentFingerprint("kv", kSegmentSize, different_rank),
              base);
}

TEST(SharedSegmentBlobTest, RoundTripsHeaderAndHandle) {
    SegmentBlobHeader header{};
    header.backend_id = 1;
    header.rank_id = 5;
    header.fingerprint = 0xDEADBEEFCAFEULL;
    header.alloc_size = 1ULL << 30;
    const std::vector<uint8_t> handle(kMaxHandleBytes, 0xA5);

    std::string blob;
    ASSERT_TRUE(EncodeSegmentBlob(header, handle, blob).ok());
    EXPECT_EQ(blob.size(), kSegmentBlobBytes);

    SegmentBlobHeader decoded{};
    std::vector<uint8_t> decoded_handle;
    ASSERT_TRUE(DecodeSegmentBlob(blob, decoded, decoded_handle).ok());
    EXPECT_EQ(decoded.backend_id, header.backend_id);
    EXPECT_EQ(decoded.rank_id, header.rank_id);
    EXPECT_EQ(decoded.fingerprint, header.fingerprint);
    EXPECT_EQ(decoded.alloc_size, header.alloc_size);
    EXPECT_EQ(decoded_handle, handle);
}

TEST(SharedSegmentBlobTest, NonOwnerBlobHasTheSameLengthButNoHandle) {
    SegmentBlobHeader header{};
    header.rank_id = 2;
    std::string blob;
    ASSERT_TRUE(EncodeSegmentBlob(header, {}, blob).ok());
    EXPECT_EQ(blob.size(), kSegmentBlobBytes);

    SegmentBlobHeader decoded{};
    std::vector<uint8_t> decoded_handle;
    ASSERT_TRUE(DecodeSegmentBlob(blob, decoded, decoded_handle).ok());
    EXPECT_TRUE(decoded_handle.empty());
}

TEST(SharedSegmentBlobTest, RejectsHandlesThatDoNotFit) {
    std::string blob;
    EXPECT_TRUE(
        EncodeSegmentBlob({}, std::vector<uint8_t>(kMaxHandleBytes + 1), blob)
            .IsInvalidArgument());
}

TEST(SharedSegmentBlobTest, RejectsCorruptedBlobs) {
    std::vector<uint8_t> decoded_handle;
    SegmentBlobHeader decoded{};

    EXPECT_TRUE(DecodeSegmentBlob("short", decoded, decoded_handle)
                    .IsInvalidArgument());

    std::string blob;
    ASSERT_TRUE(EncodeSegmentBlob({}, {}, blob).ok());
    auto corrupted = blob;
    corrupted[0] = static_cast<char>(corrupted[0] + 1);
    EXPECT_TRUE(DecodeSegmentBlob(corrupted, decoded, decoded_handle)
                    .IsInvalidArgument());

    auto wrong_version = blob;
    wrong_version[offsetof(SegmentBlobHeader, version)] += 1;
    EXPECT_TRUE(DecodeSegmentBlob(wrong_version, decoded, decoded_handle)
                    .IsInvalidArgument());

    auto oversized_handle = blob;
    const uint32_t too_many = kMaxHandleBytes + 1;
    memcpy(oversized_handle.data() + offsetof(SegmentBlobHeader, handle_bytes),
           &too_many, sizeof(too_many));
    EXPECT_TRUE(DecodeSegmentBlob(oversized_handle, decoded, decoded_handle)
                    .IsInvalidArgument());
}

TEST(SharedSegmentTest, RejectsInvalidOptions) {
    std::string blob;
    std::shared_ptr<SharedSegment> segment;
    SharedSegmentOptions options;
    options.world_size = 0;
    EXPECT_TRUE(
        SharedSegment::Create("kv", kSegmentSize, options, segment, blob)
            .IsInvalidArgument());

    options.world_size = 2;
    options.rank_id = 2;
    EXPECT_TRUE(
        SharedSegment::Create("kv", kSegmentSize, options, segment, blob)
            .IsInvalidArgument());

    options.rank_id = 0;
    options.owner_rank = 5;
    EXPECT_TRUE(
        SharedSegment::Create("kv", kSegmentSize, options, segment, blob)
            .IsInvalidArgument());

    options.owner_rank = 0;
    EXPECT_TRUE(SharedSegment::Create("kv", 0, options, segment, blob)
                    .IsInvalidArgument());
}

TEST(SharedSegmentTest, ReportsWhenNoBackendIsCompiledIn) {
    if (SharedSegment::Supported()) {
        GTEST_SKIP() << "This build has a working VMM backend";
    }
    std::string blob;
    std::shared_ptr<SharedSegment> segment;
    EXPECT_TRUE(SharedSegment::Create("kv", kSegmentSize, KvOptions(0, 1),
                                      segment, blob)
                    .IsNotImplemented());
}

TEST(SharedSegmentMmapTest, SupportedWhenHostRegisterRuntimeIsPresent) {
#if defined(USE_CUDA) || defined(USE_ASCEND_DIRECT)
    EXPECT_TRUE(SharedSegment::Supported(/*mmap=*/true));
#else
    EXPECT_FALSE(SharedSegment::Supported(/*mmap=*/true));
#endif
}

TEST(SharedSegmentMmapTest, SingleRankRoundTrip) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment needs CUDA or Ascend";
    }
    SharedSegmentOptions options = KvOptions(0, 1);
    options.mmap = true;

    std::string blob;
    std::shared_ptr<SharedSegment> segment;
    auto status =
        SharedSegment::Create("mmap-kv", kSegmentSize, options, segment, blob);
    if (!status.ok()) {
        GTEST_SKIP() << "HostRegister unavailable: " << status.ToString();
    }
    ASSERT_TRUE(segment->Complete({blob}).ok());
    ASSERT_TRUE(segment->ready());
    ASSERT_NE(segment->base_addr(), 0u);

    auto* bytes = reinterpret_cast<uint8_t*>(segment->base_addr());
    bytes[0] = 0x3C;
    bytes[kSegmentSize - 1] = 0xC3;
    EXPECT_EQ(bytes[0], 0x3C);
    EXPECT_EQ(bytes[kSegmentSize - 1], 0xC3);
}

TEST(SharedSegmentMmapTest, TwoRanksSharePagesInOneProcess) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment needs CUDA or Ascend";
    }
    SharedSegmentOptions owner_opts = KvOptions(0, 2);
    owner_opts.mmap = true;
    SharedSegmentOptions peer_opts = KvOptions(1, 2);
    peer_opts.mmap = true;

    std::string owner_blob;
    std::string peer_blob;
    std::shared_ptr<SharedSegment> owner;
    std::shared_ptr<SharedSegment> peer;
    auto status = SharedSegment::Create("mmap-tp", kSegmentSize, owner_opts,
                                        owner, owner_blob);
    if (!status.ok()) {
        GTEST_SKIP() << "HostRegister unavailable: " << status.ToString();
    }
    ASSERT_TRUE(SharedSegment::Create("mmap-tp", kSegmentSize, peer_opts, peer,
                                      peer_blob)
                    .ok());

    const std::vector<std::string> blobs = {owner_blob, peer_blob};
    ASSERT_TRUE(owner->Complete(blobs).ok());
    ASSERT_TRUE(peer->Complete(blobs).ok());

    auto* owner_bytes = reinterpret_cast<uint8_t*>(owner->base_addr());
    auto* peer_bytes = reinterpret_cast<uint8_t*>(peer->base_addr());
    owner_bytes[0] = 0xAB;
    owner_bytes[4095] = 0xCD;
    EXPECT_EQ(peer_bytes[0], 0xAB);
    EXPECT_EQ(peer_bytes[4095], 0xCD);
    // Rank-local VAs need not match.
    EXPECT_NE(owner->base_addr(), peer->base_addr());
}

TEST(SharedSegmentMmapTest, RejectsMixedBackendBlobs) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment needs CUDA or Ascend";
    }
    SharedSegmentOptions mmap_opts = KvOptions(0, 1);
    mmap_opts.mmap = true;

    std::string mmap_blob;
    std::shared_ptr<SharedSegment> mmap_segment;
    auto status = SharedSegment::Create("mmap-mix", kSegmentSize, mmap_opts,
                                        mmap_segment, mmap_blob);
    if (!status.ok()) {
        GTEST_SKIP() << "HostRegister unavailable: " << status.ToString();
    }

    // Forge a peer blob that claims a different backend id.
    SegmentBlobHeader header{};
    std::vector<uint8_t> handle;
    ASSERT_TRUE(DecodeSegmentBlob(mmap_blob, header, handle).ok());
    header.backend_id = 1;  // Ascend VMM id
    std::string forged;
    ASSERT_TRUE(EncodeSegmentBlob(header, handle, forged).ok());
    EXPECT_TRUE(mmap_segment->Complete({forged}).IsInvalidArgument());
}

}  // namespace mooncake
