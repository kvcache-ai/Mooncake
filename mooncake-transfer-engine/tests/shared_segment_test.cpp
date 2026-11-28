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
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "shared_segment/host_memfd.h"
#include "shared_segment/shared_segment.h"
#include "shared_segment_internal.h"

#if defined(USE_ASCEND_DIRECT)
#include <acl/acl.h>
#endif

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

bool WriteAll(int fd, const void* buf, size_t len) {
    const auto* p = static_cast<const uint8_t*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, p + done, len - done);
        if (n <= 0) {
            return false;
        }
        done += static_cast<size_t>(n);
    }
    return true;
}

bool ReadAll(int fd, void* buf, size_t len) {
    auto* p = static_cast<uint8_t*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, p + done, len - done);
        if (n <= 0) {
            return false;
        }
        done += static_cast<size_t>(n);
    }
    return true;
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

    // Explicit single-element owner_ranks={0} matches default owner_rank=0.
    auto explicit_single_owner = options;
    explicit_single_owner.owner_ranks = {0};
    EXPECT_EQ(
        ComputeSegmentFingerprint("kv", kSegmentSize, explicit_single_owner),
        base);

    // Multiple owners and owner order must change the fingerprint.
    auto multi_owner = options;
    multi_owner.owner_ranks = {0, 1};
    const auto multi_fp =
        ComputeSegmentFingerprint("kv", kSegmentSize, multi_owner);
    EXPECT_NE(multi_fp, base);

    auto reversed_multi_owner = options;
    reversed_multi_owner.owner_ranks = {1, 0};
    EXPECT_NE(
        ComputeSegmentFingerprint("kv", kSegmentSize, reversed_multi_owner),
        multi_fp);

    // rank_id and device_id are local and must not affect the digest.
    auto different_rank = options;
    different_rank.rank_id = 7;
    different_rank.device_id = 3;
    EXPECT_EQ(ComputeSegmentFingerprint("kv", kSegmentSize, different_rank),
              base);

    auto different_mmap = options;
    different_mmap.mmap = !options.mmap;
    EXPECT_NE(ComputeSegmentFingerprint("kv", kSegmentSize, different_mmap),
              base);

    auto different_host_register = options;
    different_host_register.host_register = !options.host_register;
    EXPECT_NE(
        ComputeSegmentFingerprint("kv", kSegmentSize, different_host_register),
        base);
}

TEST(SharedSegmentChunkTest, PartitionsEvenlyAndUnevenlyByGranularity) {
    constexpr uint64_t kGran = 2ULL * 1024 * 1024;
    uint64_t total = 0;

    // Single owner matches AlignUp(size, granularity).
    auto single = ComputeOwnerChunks(kSegmentSize, kGran, {0}, total);
    ASSERT_EQ(single.size(), 1u);
    EXPECT_EQ(single[0].owner_rank, 0u);
    EXPECT_EQ(single[0].offset, 0u);
    EXPECT_EQ(single[0].size, kGran);
    EXPECT_EQ(total, kGran);

    // Uneven granule count: 5 granules across 3 owners -> [2, 2, 1] granules.
    auto uneven = ComputeOwnerChunks(5 * kGran - 123, kGran, {2, 0, 3}, total);
    ASSERT_EQ(uneven.size(), 3u);
    EXPECT_EQ(uneven[0].owner_rank, 2u);
    EXPECT_EQ(uneven[0].offset, 0u);
    EXPECT_EQ(uneven[0].size, 2 * kGran);
    EXPECT_EQ(uneven[1].owner_rank, 0u);
    EXPECT_EQ(uneven[1].offset, 2 * kGran);
    EXPECT_EQ(uneven[1].size, 2 * kGran);
    EXPECT_EQ(uneven[2].owner_rank, 3u);
    EXPECT_EQ(uneven[2].offset, 4 * kGran);
    EXPECT_EQ(uneven[2].size, 1 * kGran);
    EXPECT_EQ(total, 5 * kGran);

    // Small size (< K granules): each owner still gets at least 1 granule.
    auto small_multi = ComputeOwnerChunks(4096, kGran, {0, 1, 2}, total);
    ASSERT_EQ(small_multi.size(), 3u);
    EXPECT_EQ(small_multi[0].offset, 0u);
    EXPECT_EQ(small_multi[0].size, kGran);
    EXPECT_EQ(small_multi[1].offset, kGran);
    EXPECT_EQ(small_multi[1].size, kGran);
    EXPECT_EQ(small_multi[2].offset, 2 * kGran);
    EXPECT_EQ(small_multi[2].size, kGran);
    EXPECT_EQ(total, 3 * kGran);
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
    options.owner_ranks = {0, 0};
    EXPECT_TRUE(
        SharedSegment::Create("kv", kSegmentSize, options, segment, blob)
            .IsInvalidArgument());

    options.owner_ranks = {0, 2};
    EXPECT_TRUE(
        SharedSegment::Create("kv", kSegmentSize, options, segment, blob)
            .IsInvalidArgument());

    options.owner_ranks = {0};
    EXPECT_TRUE(SharedSegment::Create("kv", 0, options, segment, blob)
                    .IsInvalidArgument());

    options.mmap = false;
    options.host_register = true;
    EXPECT_TRUE(
        SharedSegment::Create("kv", kSegmentSize, options, segment, blob)
            .IsInvalidArgument());
}

TEST(SharedSegmentBackendFactoryTest, RejectsUnknownImplementation) {
    EXPECT_EQ(CreateSharedSegmentBackend("unknown"), nullptr);
}

TEST(SharedSegmentTest, ReportsWhenNoBackendIsCompiledIn) {
    if (SharedSegment::Supported(/*mmap=*/false)) {
        GTEST_SKIP() << "This build has a working VMM backend";
    }
    std::string blob;
    std::shared_ptr<SharedSegment> segment;
    SharedSegmentOptions options = KvOptions(0, 1);
    options.mmap = false;
    EXPECT_TRUE(
        SharedSegment::Create("kv", kSegmentSize, options, segment, blob)
            .IsNotImplemented());
}

TEST(SharedSegmentMmapTest, SupportedForMmapAndOptionalHostRegister) {
    EXPECT_TRUE(SharedSegment::Supported(/*mmap=*/true));
#if defined(USE_ASCEND_DIRECT)
    EXPECT_TRUE(
        SharedSegment::Supported(/*mmap=*/true, /*host_register=*/true));
#else
    EXPECT_FALSE(
        SharedSegment::Supported(/*mmap=*/true, /*host_register=*/true));
#endif
    EXPECT_FALSE(
        SharedSegment::Supported(/*mmap=*/false, /*host_register=*/true));
}

TEST(SharedSegmentMmapTest, SingleRankRoundTrip) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment unavailable";
    }
    SharedSegmentOptions options = KvOptions(0, 1);
    options.mmap = true;

    std::string blob;
    std::shared_ptr<SharedSegment> segment;
    auto status =
        SharedSegment::Create("mmap-kv", kSegmentSize, options, segment, blob);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(segment->Complete({blob}).ok());
    ASSERT_TRUE(segment->ready());
    EXPECT_NE(segment->base_addr(), 0u);
    EXPECT_EQ(segment->owner_ranks(), std::vector<uint32_t>({0}));

    auto* bytes = reinterpret_cast<uint8_t*>(segment->base_addr());
    bytes[0] = 0x3C;
    bytes[kSegmentSize - 1] = 0xC3;
    EXPECT_EQ(bytes[0], 0x3C);
    EXPECT_EQ(bytes[kSegmentSize - 1], 0xC3);
}

TEST(SharedSegmentMmapTest, TwoRanksSharePagesWithProcessLocalAddresses) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment unavailable";
    }
    // Without a global GVA constraint, two ranks in the same process map the
    // shared physical pages at distinct local VAs and still see each other's
    // writes.
    SharedSegmentOptions owner_opts = KvOptions(0, 2);
    owner_opts.mmap = true;
    SharedSegmentOptions peer_opts = KvOptions(1, 2);
    peer_opts.mmap = true;

    std::string owner_blob;
    std::string peer_blob;
    std::shared_ptr<SharedSegment> owner;
    std::shared_ptr<SharedSegment> peer;
    ASSERT_TRUE(SharedSegment::Create("mmap-same-proc", kSegmentSize,
                                      owner_opts, owner, owner_blob)
                    .ok());
    ASSERT_TRUE(SharedSegment::Create("mmap-same-proc", kSegmentSize, peer_opts,
                                      peer, peer_blob)
                    .ok());
    EXPECT_NE(owner->base_addr(), peer->base_addr());

    const std::vector<std::string> blobs = {owner_blob, peer_blob};
    ASSERT_TRUE(owner->Complete(blobs).ok());
    ASSERT_TRUE(peer->Complete(blobs).ok());

    auto* owner_bytes = reinterpret_cast<uint8_t*>(owner->base_addr());
    auto* peer_bytes = reinterpret_cast<uint8_t*>(peer->base_addr());
    owner_bytes[0] = 0x5A;
    peer_bytes[kSegmentSize - 1] = 0xA5;
    EXPECT_EQ(peer_bytes[0], 0x5A);
    EXPECT_EQ(owner_bytes[kSegmentSize - 1], 0xA5);
}

TEST(SharedSegmentMmapTest, RejectsMixedBackendBlobs) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment unavailable";
    }
    SharedSegmentOptions mmap_opts = KvOptions(0, 1);
    mmap_opts.mmap = true;

    std::string mmap_blob;
    std::shared_ptr<SharedSegment> mmap_segment;
    auto status = SharedSegment::Create("mmap-mix", kSegmentSize, mmap_opts,
                                        mmap_segment, mmap_blob);
    if (!status.ok()) {
        GTEST_SKIP() << "mmap shared segment unavailable: "
                     << status.ToString();
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

TEST(SharedSegmentMmapTest, OwnerHandleIsProcFd) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment unavailable";
    }
    SharedSegmentOptions options = KvOptions(0, 1);
    options.mmap = true;

    std::string blob;
    std::shared_ptr<SharedSegment> segment;
    auto status =
        SharedSegment::Create("mmap-fd", kSegmentSize, options, segment, blob);
    ASSERT_TRUE(status.ok()) << status.ToString();

    SegmentBlobHeader header{};
    std::vector<uint8_t> handle;
    ASSERT_TRUE(DecodeSegmentBlob(blob, header, handle).ok());
    const std::string path(handle.begin(), handle.end());
    int pid = -1;
    int fd = -1;
    ASSERT_EQ(std::sscanf(path.c_str(), "/proc/%d/fd/%d", &pid, &fd), 2);
    EXPECT_EQ(pid, static_cast<int>(getpid()));
    EXPECT_GE(fd, 0);
}

TEST(SharedSegmentMmapTest, TwoProcessesSharePages) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment unavailable";
    }

    int to_peer[2];
    int to_owner[2];
    ASSERT_EQ(pipe(to_peer), 0);
    ASSERT_EQ(pipe(to_owner), 0);

    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(to_peer[1]);
        close(to_owner[0]);
        SharedSegmentOptions peer_opts = KvOptions(1, 2);
        peer_opts.mmap = true;
        std::string peer_blob;
        std::shared_ptr<SharedSegment> peer;
        auto status = SharedSegment::Create("mmap-fork", kSegmentSize,
                                            peer_opts, peer, peer_blob);
        if (!status.ok() ||
            !WriteAll(to_owner[1], peer_blob.data(), peer_blob.size())) {
            _exit(2);
        }
        std::string owner_blob(kSegmentBlobBytes, '\0');
        if (!ReadAll(to_peer[0], owner_blob.data(), owner_blob.size())) {
            _exit(3);
        }
        const std::vector<std::string> blobs = {owner_blob, peer_blob};
        if (!peer->Complete(blobs).ok()) {
            _exit(4);
        }
        uint64_t peer_gva = static_cast<uint64_t>(peer->base_addr());
        if (!WriteAll(to_owner[1], &peer_gva, sizeof(peer_gva))) {
            _exit(5);
        }
        uint8_t ready = 0;
        if (!ReadAll(to_peer[0], &ready, 1)) {
            _exit(6);
        }
        auto* bytes = reinterpret_cast<uint8_t*>(peer->base_addr());
        const int ok = (bytes[0] == 0xA5 && bytes[4095] == 0x5A) ? 0 : 7;
        _exit(ok);
    }

    close(to_peer[0]);
    close(to_owner[1]);
    SharedSegmentOptions owner_opts = KvOptions(0, 2);
    owner_opts.mmap = true;
    std::string owner_blob;
    std::shared_ptr<SharedSegment> owner;
    auto status = SharedSegment::Create("mmap-fork", kSegmentSize, owner_opts,
                                        owner, owner_blob);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(WriteAll(to_peer[1], owner_blob.data(), owner_blob.size()));

    std::string peer_blob(kSegmentBlobBytes, '\0');
    ASSERT_TRUE(ReadAll(to_owner[0], peer_blob.data(), peer_blob.size()));
    const std::vector<std::string> blobs = {owner_blob, peer_blob};
    ASSERT_TRUE(owner->Complete(blobs).ok());

    uint64_t peer_base = 0;
    ASSERT_TRUE(ReadAll(to_owner[0], &peer_base, sizeof(peer_base)));
    EXPECT_NE(peer_base, 0u);
    EXPECT_NE(owner->base_addr(), 0u);

    auto* bytes = reinterpret_cast<uint8_t*>(owner->base_addr());
    bytes[0] = 0xA5;
    bytes[4095] = 0x5A;
    uint8_t ready = 1;
    ASSERT_TRUE(WriteAll(to_peer[1], &ready, 1));

    int wstatus = 0;
    ASSERT_EQ(waitpid(child, &wstatus, 0), child);
    ASSERT_TRUE(WIFEXITED(wstatus));
    EXPECT_EQ(WEXITSTATUS(wstatus), 0);
}

TEST(SharedSegmentMmapTest, MultiOwnerThreeProcessesShareContiguousSpan) {
    if (!SharedSegment::Supported(/*mmap=*/true)) {
        GTEST_SKIP() << "mmap shared segment unavailable";
    }
    // 3 granules across 2 owners {0, 2}: owner 0 holds [0, 2*G), owner 2 holds
    // [2*G, 3*G); rank 1 is a non-owner reader/writer mapping both shards.
    const uint64_t gran = host_memfd::Granularity();
    const uint64_t seg_size = 3 * gran;
    const std::vector<uint32_t> owners = {0, 2};

    int to_child[2][2];
    int from_child[2][2];
    for (int c = 0; c < 2; ++c) {
        ASSERT_EQ(pipe(to_child[c]), 0);
        ASSERT_EQ(pipe(from_child[c]), 0);
    }

    pid_t children[2] = {-1, -1};
    for (int c = 0; c < 2; ++c) {
        const uint32_t child_rank = static_cast<uint32_t>(c + 1);
        children[c] = fork();
        ASSERT_GE(children[c], 0);
        if (children[c] == 0) {
            for (int j = 0; j < 2; ++j) {
                if (j != c) {
                    close(to_child[j][0]);
                    close(to_child[j][1]);
                    close(from_child[j][0]);
                    close(from_child[j][1]);
                }
            }
            close(to_child[c][1]);
            close(from_child[c][0]);

            SharedSegmentOptions opts;
            opts.world_size = 3;
            opts.rank_id = child_rank;
            opts.owner_ranks = owners;
            opts.mmap = true;

            std::string local_blob;
            std::shared_ptr<SharedSegment> seg;
            if (!SharedSegment::Create("mmap-multi-owner", seg_size, opts, seg,
                                       local_blob)
                     .ok()) {
                _exit(10);
            }
            if (!WriteAll(from_child[c][1], local_blob.data(),
                          local_blob.size())) {
                _exit(11);
            }
            std::vector<std::string> blobs(
                3, std::string(kSegmentBlobBytes, '\0'));
            for (int r = 0; r < 3; ++r) {
                if (!ReadAll(to_child[c][0], blobs[r].data(),
                             blobs[r].size())) {
                    _exit(12);
                }
            }
            if (!seg->Complete(blobs).ok()) {
                _exit(13);
            }
            uint64_t gva = static_cast<uint64_t>(seg->base_addr());
            if (!WriteAll(from_child[c][1], &gva, sizeof(gva))) {
                _exit(14);
            }
            // Wait for phase-write signal.
            uint8_t sig = 0;
            if (!ReadAll(to_child[c][0], &sig, 1)) {
                _exit(15);
            }
            auto* bytes = reinterpret_cast<uint8_t*>(seg->base_addr());
            if (child_rank == 2) {
                // Owner 2 writes into its shard [2*G, 3*G).
                bytes[2 * gran] = 0xCC;
                bytes[3 * gran - 1] = 0xDD;
            }
            if (!WriteAll(from_child[c][1], &sig, 1)) {
                _exit(16);
            }
            // Wait for phase-verify signal.
            if (!ReadAll(to_child[c][0], &sig, 1)) {
                _exit(17);
            }
            const bool ok = bytes[0] == 0xAA && bytes[2 * gran - 1] == 0xBB &&
                            bytes[2 * gran] == 0xCC &&
                            bytes[3 * gran - 1] == 0xDD;
            _exit(ok ? 0 : 18);
        }
        close(to_child[c][0]);
        close(from_child[c][1]);
    }

    SharedSegmentOptions opts0;
    opts0.world_size = 3;
    opts0.rank_id = 0;
    opts0.owner_ranks = owners;
    opts0.mmap = true;

    std::string blob0;
    std::shared_ptr<SharedSegment> seg0;
    auto status =
        SharedSegment::Create("mmap-multi-owner", seg_size, opts0, seg0, blob0);
    ASSERT_TRUE(status.ok()) << status.ToString();

    std::vector<std::string> blobs(3, std::string(kSegmentBlobBytes, '\0'));
    blobs[0] = blob0;
    for (int c = 0; c < 2; ++c) {
        ASSERT_TRUE(ReadAll(from_child[c][0], blobs[c + 1].data(),
                            blobs[c + 1].size()));
    }
    for (int c = 0; c < 2; ++c) {
        for (int r = 0; r < 3; ++r) {
            ASSERT_TRUE(
                WriteAll(to_child[c][1], blobs[r].data(), blobs[r].size()));
        }
    }
    ASSERT_TRUE(seg0->Complete(blobs).ok());
    EXPECT_NE(seg0->base_addr(), 0u);

    for (int c = 0; c < 2; ++c) {
        uint64_t child_base = 0;
        ASSERT_TRUE(ReadAll(from_child[c][0], &child_base, sizeof(child_base)));
        EXPECT_NE(child_base, 0u);
    }

    // Owner 0 writes into its shard [0, 2*G); signal children to write theirs.
    auto* bytes0 = reinterpret_cast<uint8_t*>(seg0->base_addr());
    bytes0[0] = 0xAA;
    bytes0[2 * gran - 1] = 0xBB;
    uint8_t sig = 1;
    for (int c = 0; c < 2; ++c) {
        ASSERT_TRUE(WriteAll(to_child[c][1], &sig, 1));
    }
    for (int c = 0; c < 2; ++c) {
        ASSERT_TRUE(ReadAll(from_child[c][0], &sig, 1));
    }
    // Verify owner 0 sees owner 2's writes across the shard boundary.
    EXPECT_EQ(bytes0[2 * gran], 0xCC);
    EXPECT_EQ(bytes0[3 * gran - 1], 0xDD);

    for (int c = 0; c < 2; ++c) {
        ASSERT_TRUE(WriteAll(to_child[c][1], &sig, 1));
    }
    for (int c = 0; c < 2; ++c) {
        int wstatus = 0;
        ASSERT_EQ(waitpid(children[c], &wstatus, 0), children[c]);
        ASSERT_TRUE(WIFEXITED(wstatus));
        EXPECT_EQ(WEXITSTATUS(wstatus), 0) << "child " << (c + 1);
    }
}

#if defined(USE_ASCEND_DIRECT)
// Runs a two-process Ascend shared-segment test by forking both ranks as child
// processes so the gtest parent process never calls aclInit() (Ascend ACL
// breaks across fork()).
void RunTwoProcessAscendMultiOwnerTest(const char* name, uint64_t seg_size,
                                       bool mmap, bool host_register) {
    const std::vector<uint32_t> owners = {0, 1};
    int to_child[2][2];
    int from_child[2][2];
    for (int r = 0; r < 2; ++r) {
        ASSERT_EQ(pipe(to_child[r]), 0);
        ASSERT_EQ(pipe(from_child[r]), 0);
    }

    pid_t pids[2] = {-1, -1};
    for (int r = 0; r < 2; ++r) {
        pids[r] = fork();
        ASSERT_GE(pids[r], 0);
        if (pids[r] == 0) {
            for (int j = 0; j < 2; ++j) {
                if (j != r) {
                    close(to_child[j][0]);
                    close(to_child[j][1]);
                    close(from_child[j][0]);
                    close(from_child[j][1]);
                }
            }
            close(to_child[r][1]);
            close(from_child[r][0]);

            if (aclInit(nullptr) != ACL_ERROR_NONE) {
                uint8_t skip = 1;
                (void)WriteAll(from_child[r][1], &skip, 1);
                _exit(0);
            }
            uint32_t dev_count = 0;
            int32_t dev = (aclrtGetDeviceCount(&dev_count) == ACL_ERROR_NONE &&
                           dev_count >= 2)
                              ? r
                              : 0;
            if (aclrtSetDevice(dev) != ACL_ERROR_NONE) {
                uint8_t skip = 1;
                (void)WriteAll(from_child[r][1], &skip, 1);
                _exit(0);
            }
            uint8_t skip = 0;
            if (!WriteAll(from_child[r][1], &skip, 1)) {
                _exit(2);
            }

            SharedSegmentOptions opts;
            opts.world_size = 2;
            opts.rank_id = static_cast<uint32_t>(r);
            opts.owner_ranks = owners;
            opts.device_id = dev;
            opts.mmap = mmap;
            opts.host_register = host_register;

            std::string local_blob;
            std::shared_ptr<SharedSegment> seg;
            auto status =
                SharedSegment::Create(name, seg_size, opts, seg, local_blob);
            if (!status.ok()) {
                fprintf(stderr, "rank %d Create failed: %s\n", r,
                        status.ToString().c_str());
                _exit(3);
            }
            if (!WriteAll(from_child[r][1], local_blob.data(),
                          local_blob.size())) {
                _exit(4);
            }
            std::vector<std::string> blobs(
                2, std::string(kSegmentBlobBytes, '\0'));
            for (int k = 0; k < 2; ++k) {
                if (!ReadAll(to_child[r][0], blobs[k].data(),
                             blobs[k].size())) {
                    _exit(5);
                }
            }
            status = seg->Complete(blobs);
            if (!status.ok()) {
                fprintf(stderr, "rank %d Complete failed: %s\n", r,
                        status.ToString().c_str());
                _exit(6);
            }
            if (seg->base_addr() == 0) {
                _exit(7);
            }
            if (host_register && seg->device_addr() == 0) {
                _exit(7);
            }
            if (!mmap && seg->device_addr() != 0 &&
                seg->device_addr() != seg->base_addr()) {
                _exit(8);
            }
            uint64_t gva = static_cast<uint64_t>(seg->base_addr());
            if (!WriteAll(from_child[r][1], &gva, sizeof(gva))) {
                _exit(9);
            }
            const uint64_t half = seg_size / 2;
            uint8_t sig = 1;
            bool ok = false;
            if (mmap || seg->device_addr() == 0) {
                auto* bytes = reinterpret_cast<uint8_t*>(seg->base_addr());
                if (r == 0) {
                    bytes[0] = 0x11;
                    bytes[half - 1] = 0x22;
                } else {
                    bytes[half] = 0x33;
                    bytes[seg_size - 1] = 0x44;
                }
                if (!WriteAll(from_child[r][1], &sig, 1) ||
                    !ReadAll(to_child[r][0], &sig, 1)) {
                    _exit(10);
                }
                ok = bytes[0] == 0x11 && bytes[half - 1] == 0x22 &&
                     bytes[half] == 0x33 && bytes[seg_size - 1] == 0x44;
            } else {
                // Imported Ascend VMM shards are mapped onto the calling NPU's
                // device side; verify cross-shard visibility via NPU D2D copies
                // against the shared GVA.
                void* scratch = nullptr;
                aclrtStream stream = nullptr;
                if (aclrtMalloc(&scratch, 64, ACL_MEM_MALLOC_HUGE_FIRST) !=
                        ACL_ERROR_NONE ||
                    aclrtCreateStream(&stream) != ACL_ERROR_NONE) {
                    _exit(13);
                }
                auto write_byte = [&](uint64_t off, uint8_t val) -> bool {
                    aclError r1 = aclrtMemcpy(scratch, 1, &val, 1,
                                              ACL_MEMCPY_HOST_TO_DEVICE);
                    if (r1 != ACL_ERROR_NONE) {
                        fprintf(stderr, "rank %d H2D scratch failed: %d\n", r,
                                r1);
                        fflush(stderr);
                        return false;
                    }
                    // Use async D2D on an NPU stream so the NPU SDMA engine
                    // accesses both owned and imported VMM shards through
                    // device page tables without touching host PTEs.
                    aclError r2 = aclrtMemcpyAsync(
                        reinterpret_cast<uint8_t*>(seg->device_addr()) + off, 1,
                        scratch, 1, ACL_MEMCPY_DEVICE_TO_DEVICE, stream);
                    if (r2 == ACL_ERROR_NONE) {
                        r2 = aclrtSynchronizeStream(stream);
                    }
                    if (r2 != ACL_ERROR_NONE) {
                        fprintf(stderr,
                                "rank %d D2D write vmm off=%lu failed: %d\n", r,
                                static_cast<unsigned long>(off), r2);
                        fflush(stderr);
                        return false;
                    }
                    return true;
                };
                auto read_byte = [&](uint64_t off, uint8_t& out) -> bool {
                    aclError r1 = aclrtMemcpyAsync(
                        scratch, 1,
                        reinterpret_cast<uint8_t*>(seg->device_addr()) + off, 1,
                        ACL_MEMCPY_DEVICE_TO_DEVICE, stream);
                    if (r1 == ACL_ERROR_NONE) {
                        r1 = aclrtSynchronizeStream(stream);
                    }
                    if (r1 != ACL_ERROR_NONE) {
                        fprintf(stderr,
                                "rank %d D2D read vmm off=%lu failed: %d\n", r,
                                static_cast<unsigned long>(off), r1);
                        fflush(stderr);
                        return false;
                    }
                    aclError r2 = aclrtMemcpy(&out, 1, scratch, 1,
                                              ACL_MEMCPY_DEVICE_TO_HOST);
                    if (r2 != ACL_ERROR_NONE) {
                        fprintf(stderr, "rank %d D2H scratch failed: %d\n", r,
                                r2);
                        fflush(stderr);
                        return false;
                    }
                    return true;
                };
                bool write_ok = false;
                if (r == 0) {
                    write_ok =
                        write_byte(0, 0x11) && write_byte(half - 1, 0x22);
                } else {
                    write_ok = write_byte(half, 0x33) &&
                               write_byte(seg_size - 1, 0x44);
                }
                if (!write_ok) {
                    (void)aclrtDestroyStream(stream);
                    (void)aclrtFree(scratch);
                    _exit(14);
                }
                if (!WriteAll(from_child[r][1], &sig, 1) ||
                    !ReadAll(to_child[r][0], &sig, 1)) {
                    (void)aclrtDestroyStream(stream);
                    (void)aclrtFree(scratch);
                    _exit(10);
                }
                uint8_t b0 = 0, b1 = 0, b2 = 0, b3 = 0;
                ok = read_byte(0, b0) && read_byte(half - 1, b1) &&
                     read_byte(half, b2) && read_byte(seg_size - 1, b3) &&
                     b0 == 0x11 && b1 == 0x22 && b2 == 0x33 && b3 == 0x44;
                (void)aclrtDestroyStream(stream);
                (void)aclrtFree(scratch);
            }
            if (!WriteAll(from_child[r][1], &sig, 1) ||
                !ReadAll(to_child[r][0], &sig, 1)) {
                _exit(11);
            }
            seg.reset();
            _exit(ok ? 0 : 12);
        }
        close(to_child[r][0]);
        close(from_child[r][1]);
    }

    uint8_t skipped[2] = {0, 0};
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(ReadAll(from_child[r][0], &skipped[r], 1));
    }
    if (skipped[0] || skipped[1]) {
        for (int r = 0; r < 2; ++r) {
            int wstatus = 0;
            waitpid(pids[r], &wstatus, 0);
        }
        GTEST_SKIP() << "Ascend ACL device unavailable";
    }

    std::vector<std::string> blobs(2, std::string(kSegmentBlobBytes, '\0'));
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(
            ReadAll(from_child[r][0], blobs[r].data(), blobs[r].size()));
    }
    for (int r = 0; r < 2; ++r) {
        for (int k = 0; k < 2; ++k) {
            ASSERT_TRUE(
                WriteAll(to_child[r][1], blobs[k].data(), blobs[k].size()));
        }
    }

    uint64_t bases[2] = {0, 0};
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(ReadAll(from_child[r][0], &bases[r], sizeof(bases[r])));
        EXPECT_NE(bases[r], 0u);
    }

    uint8_t sig = 1;
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(ReadAll(from_child[r][0], &sig, 1));
    }
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(WriteAll(to_child[r][1], &sig, 1));
    }
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(ReadAll(from_child[r][0], &sig, 1));
    }
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(WriteAll(to_child[r][1], &sig, 1));
    }
    for (int r = 0; r < 2; ++r) {
        int wstatus = 0;
        ASSERT_EQ(waitpid(pids[r], &wstatus, 0), pids[r]);
        ASSERT_TRUE(WIFEXITED(wstatus));
        EXPECT_EQ(WEXITSTATUS(wstatus), 0) << "rank " << r;
    }
}

TEST(SharedSegmentMmapTest, HostRegisterMultiOwnerTwoProcessesSharePages) {
    if (!SharedSegment::Supported(/*mmap=*/true, /*host_register=*/true)) {
        GTEST_SKIP() << "HostRegister unavailable in this build";
    }
    RunTwoProcessAscendMultiOwnerTest(
        "mmap-hr-multi", 2 * host_memfd::Granularity(), /*mmap=*/true,
        /*host_register=*/true);
}

TEST(SharedSegmentAscendVmmTest, MultiOwnerTwoProcessesSharePages) {
    if (!SharedSegment::Supported(/*mmap=*/false)) {
        GTEST_SKIP() << "Ascend VMM shared segment unavailable in this build";
    }
    RunTwoProcessAscendMultiOwnerTest("vmm-multi-owner", 2ULL * 2 * 1024 * 1024,
                                      /*mmap=*/false,
                                      /*host_register=*/false);
}
#endif

}  // namespace mooncake
