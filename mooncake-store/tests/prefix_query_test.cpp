// Copyright 2025 Alibaba Cloud and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Tests for Forge RL Design 01 — chained-prefix LPM lookup
// (`MasterService::QueryPrefixMatch`).
//
// We exercise the *server-side* contract directly through MasterService rather
// than the wrapped RPC handler, mirroring the style of master_service_test.cpp
// — this keeps the test self-contained and fast (no network setup).

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "master_config.h"
#include "prefix_key.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake::test {

namespace {

// Use the canonical encoding shared with the server (and bench / Python
// smoke test) so the test exercises the production code path.
inline std::string HashToKey(uint64_t h) { return MakePrefixHashKey(h); }

// Common test scaffolding: mount a memory segment and expose its id/name.
struct MountedSegmentContext {
    UUID segment_id;
    UUID client_id;
    std::string name;
};

constexpr size_t kSegmentBase = 0x300000000;
constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16 MiB

UUID GenerateNonZeroUuid() {
    UUID u = generate_uuid();
    if (u.first == 0 && u.second == 0) {
        u.first = 1;
    }
    return u;
}

MountedSegmentContext MountSegment(MasterService& service,
                                   const std::string& name, size_t base) {
    Segment segment;
    segment.id = GenerateNonZeroUuid();
    segment.name = name;
    segment.base = base;
    segment.size = kSegmentSize;
    segment.te_endpoint = name;
    UUID client_id = generate_uuid();
    auto mount_result = service.MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result.has_value());
    return {segment.id, client_id, segment.name};
}

// Insert a single completed memory replica under the given key, on the
// segment named `preferred_segment`.
void PutKeyOnSegment(MasterService& service, const UUID& client_id,
                     const std::string& key,
                     const std::string& preferred_segment,
                     uint64_t value_size = 64 * 1024) {
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.preferred_segment = preferred_segment;
    auto put_start = service.PutStart(client_id, key, value_size, cfg);
    ASSERT_TRUE(put_start.has_value())
        << "PutStart failed for key=" << key << " on " << preferred_segment;
    auto put_end = service.PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value())
        << "PutEnd failed for key=" << key << " on " << preferred_segment;
}

// We deliberately do NOT init/shutdown glog inside SetUp/TearDown: glog's
// `InitGoogleLogging` is a one-shot per process and would CHECK-fail on the
// second `TEST_F` instance. The shared `main()` below initializes it once.
class PrefixQueryTest : public ::testing::Test {};

}  // namespace

TEST_F(PrefixQueryTest, RejectsEmptyChain) {
    auto service = std::make_unique<MasterService>();
    QueryPrefixMatchRequest req;
    auto result = service->QueryPrefixMatch(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::PREFIX_CHAIN_EMPTY);
}

TEST_F(PrefixQueryTest, RejectsOverlongChain) {
    auto service = std::make_unique<MasterService>();
    QueryPrefixMatchRequest req;
    // Master caps at kMaxPrefixChainLength = 1024.
    req.chain.assign(2048, 0xdead);
    auto result = service->QueryPrefixMatch(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::PREFIX_CHAIN_TOO_LONG);
}

TEST_F(PrefixQueryTest, ReturnsDisabledWhenFlagOff) {
    auto service = std::make_unique<MasterService>();
    service->SetPrefixQueryEnabledForTesting(false);

    QueryPrefixMatchRequest req;
    req.chain = {1, 2, 3};
    auto result = service->QueryPrefixMatch(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::PREFIX_QUERY_DISABLED);
}

TEST_F(PrefixQueryTest, ReturnsZeroMatchedWhenChainNotIndexed) {
    auto service = std::make_unique<MasterService>();
    MountSegment(*service, "seg_a", kSegmentBase);

    QueryPrefixMatchRequest req;
    req.chain = {0xfeedface, 0xdeadbeef};
    auto result = service->QueryPrefixMatch(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->matched_blocks, 0u);
    EXPECT_TRUE(result->candidates.empty());
    EXPECT_GT(result->query_lease_ms, 0u);
}

TEST_F(PrefixQueryTest, MatchesIndexedPrefixOnSingleSegment) {
    auto service = std::make_unique<MasterService>();
    auto seg = MountSegment(*service, "seg_a", kSegmentBase);

    const std::vector<uint64_t> chain = {0x1, 0x2, 0x3, 0x4};
    // Index the first three blocks, leave the 4th unindexed → matched=3.
    PutKeyOnSegment(*service, seg.client_id, HashToKey(chain[0]), seg.name);
    PutKeyOnSegment(*service, seg.client_id, HashToKey(chain[1]), seg.name);
    PutKeyOnSegment(*service, seg.client_id, HashToKey(chain[2]), seg.name);

    QueryPrefixMatchRequest req;
    req.chain = chain;
    auto result = service->QueryPrefixMatch(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->matched_blocks, 3u);
    ASSERT_EQ(result->candidates.size(), 1u);
    EXPECT_EQ(result->candidates[0].segment_name, seg.name);
    // NOTE: segment_id is intentionally left as the zero UUID by the master
    // (see master_service.cpp QueryPrefixMatch comment): resolving name -> id
    // would force taking segment_mutex_ inside the metadata-shard read lock
    // and break the metadata_shards_ -> segment_mutex_ acquisition order.
    // Routing callers key on segment_name, so we only assert on the name.
    EXPECT_EQ(result->candidates[0].replica_type,
              static_cast<int32_t>(ReplicaType::MEMORY));
    EXPECT_GT(result->query_lease_ms, 0u);
}

TEST_F(PrefixQueryTest, FullChainMatchHitsDeepestKey) {
    auto service = std::make_unique<MasterService>();
    auto seg = MountSegment(*service, "seg_a", kSegmentBase);
    const std::vector<uint64_t> chain = {0x10, 0x20, 0x30};
    for (auto h : chain) {
        PutKeyOnSegment(*service, seg.client_id, HashToKey(h), seg.name);
    }
    QueryPrefixMatchRequest req;
    req.chain = chain;
    auto result = service->QueryPrefixMatch(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->matched_blocks, chain.size());
    ASSERT_EQ(result->candidates.size(), 1u);
    EXPECT_EQ(result->candidates[0].segment_name, seg.name);
}

// The deepest matched key may live on a different segment than shallower
// keys in the chain. The reverse-walk LPM must report whichever segment owns
// the *deepest* indexed entry, not the most recently indexed one.
TEST_F(PrefixQueryTest, DeepestMatchReportsOwningSegment) {
    auto service = std::make_unique<MasterService>();

    auto seg_a = MountSegment(*service, "seg_a", kSegmentBase);
    auto seg_b = MountSegment(*service, "seg_b", kSegmentBase + kSegmentSize);

    // Layer 0 indexed on seg_a; layer 1 indexed on seg_b. The reverse-walk
    // LPM lands on layer 1 first → candidates must contain seg_b.
    PutKeyOnSegment(*service, seg_a.client_id, HashToKey(0xa1), seg_a.name);
    PutKeyOnSegment(*service, seg_b.client_id, HashToKey(0xa1b2), seg_b.name);

    QueryPrefixMatchRequest req;
    req.chain = {0xa1, 0xa1b2};
    auto result = service->QueryPrefixMatch(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->matched_blocks, 2u);
    ASSERT_EQ(result->candidates.size(), 1u);
    EXPECT_EQ(result->candidates[0].segment_name, seg_b.name)
        << "deepest matched key must own the reported segment";
    // segment_id is left as zero UUID by design (see note above).
}

// A key whose only replica was revoked (status != COMPLETE) must not count
// toward the LPM length. We simulate this by calling PutStart but skipping
// PutEnd, leaving the key in INITIALIZED state.
TEST_F(PrefixQueryTest, GhostKeyDoesNotInflateMatch) {
    auto service = std::make_unique<MasterService>();
    auto seg = MountSegment(*service, "seg_a", kSegmentBase);

    const uint64_t h0 = 0xabc;
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.preferred_segment = seg.name;
    ASSERT_TRUE(service
                    ->PutStart(seg.client_id, HashToKey(h0),
                               /*slice_length=*/4096, cfg)
                    .has_value());
    // Intentionally skip PutEnd → no COMPLETE replica.

    QueryPrefixMatchRequest req;
    req.chain = {h0};
    auto result = service->QueryPrefixMatch(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->matched_blocks, 0u)
        << "INITIALIZED replicas must not satisfy LPM";
    EXPECT_TRUE(result->candidates.empty());
}

// LPM must skip a "hole" in the chain — if a shallower hash has no replica
// but a deeper one does, the deepest hit still wins (the chain is built
// monotonically by the caller, so any indexed key implies all shallower
// keys were indexed at some point in the past).
TEST_F(PrefixQueryTest, ReverseWalkSkipsHoles) {
    auto service = std::make_unique<MasterService>();
    auto seg = MountSegment(*service, "seg_a", kSegmentBase);

    PutKeyOnSegment(*service, seg.client_id, HashToKey(0x1), seg.name);
    // Deliberately do NOT index 0x2.
    PutKeyOnSegment(*service, seg.client_id, HashToKey(0x3), seg.name);

    QueryPrefixMatchRequest req;
    req.chain = {0x1, 0x2, 0x3};
    auto result = service->QueryPrefixMatch(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->matched_blocks, 3u)
        << "reverse-walk should land on the deepest indexed entry";
    ASSERT_EQ(result->candidates.size(), 1u);
    EXPECT_EQ(result->candidates[0].segment_name, seg.name);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging("PrefixQueryTest");
    FLAGS_logtostderr = true;
    return RUN_ALL_TESTS();
}
