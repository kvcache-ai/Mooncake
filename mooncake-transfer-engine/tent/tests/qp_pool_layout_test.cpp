// Copyright 2025 KVCache.AI
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

// Unit tests for computeQpPoolSegments — the pure QP-pool layout resolver used
// by RdmaEndPoint::construct() (RFC #2568 step 2). Kept free of RDMA handles so
// the layout math is testable without a device.

#include <gtest/gtest.h>

#include "tent/transport/rdma/params.h"
#include "tent/transport/rdma/qp_pool_routing.h"

namespace mooncake {
namespace tent {
namespace {

// Default path: no pools configured => a single homogeneous run of
// qp_mul_factor QPs, no explicit segments (poolForQp will return nullptr and
// callers fall back to the global SL/TC — byte-for-byte the prior behavior).
TEST(QpPoolLayoutTest, EmptyPoolsKeepsFlatQpMulFactor) {
    auto layout = computeQpPoolSegments({}, 6);
    EXPECT_TRUE(layout.valid);
    EXPECT_EQ(layout.total_qp, 6);
    EXPECT_TRUE(layout.segments.empty());
}

// A non-positive total (e.g. qp_mul_factor <= 0 with no pools) is rejected so
// construct() can fail cleanly instead of allocating a zero-length QP array.
TEST(QpPoolLayoutTest, EmptyPoolsWithNonPositiveFactorIsInvalid) {
    auto layout = computeQpPoolSegments({}, 0);
    EXPECT_FALSE(layout.valid);
    EXPECT_EQ(layout.total_qp, 0);
}

// Multiple pools lay out contiguous, non-overlapping segments; total is the
// sum of per-pool num_qp; qp_mul_factor is ignored once pools are set.
TEST(QpPoolLayoutTest, MultiplePoolsLayoutContiguousSegments) {
    std::vector<QpPoolSegment> pools;
    QpPoolSegment kv;
    kv.name = "kv";
    kv.num_qp = 4;
    kv.service_level = 5;
    kv.traffic_class = 96;
    pools.push_back(kv);
    QpPoolSegment ctrl;
    ctrl.name = "ctrl";
    ctrl.num_qp = 2;
    pools.push_back(ctrl);

    auto layout = computeQpPoolSegments(pools, /*qp_mul_factor=*/6);
    ASSERT_TRUE(layout.valid);
    EXPECT_EQ(layout.total_qp, 6);  // 4 + 2, not qp_mul_factor
    ASSERT_EQ(layout.segments.size(), 2u);

    EXPECT_EQ(layout.segments[0].name, "kv");
    EXPECT_EQ(layout.segments[0].begin, 0);
    EXPECT_EQ(layout.segments[0].num_qp, 4);
    EXPECT_EQ(layout.segments[0].service_level, 5);
    EXPECT_EQ(layout.segments[0].traffic_class, 96);

    EXPECT_EQ(layout.segments[1].name, "ctrl");
    EXPECT_EQ(layout.segments[1].begin, 4);  // starts after kv's 4 QPs
    EXPECT_EQ(layout.segments[1].num_qp, 2);
    // ctrl left SL/TC unset -> sentinel -1 (setupOneQP falls back to global).
    EXPECT_EQ(layout.segments[1].service_level, -1);
    EXPECT_EQ(layout.segments[1].traffic_class, -1);
}

// Segments partition [0, total_qp): every QP index maps to exactly one pool,
// mirroring RdmaEndPoint::poolForQp's linear scan.
TEST(QpPoolLayoutTest, SegmentsPartitionAllQpIndices) {
    std::vector<QpPoolSegment> pools;
    QpPoolSegment a;
    a.name = "a";
    a.num_qp = 3;
    pools.push_back(a);
    QpPoolSegment b;
    b.name = "b";
    b.num_qp = 1;
    pools.push_back(b);

    auto layout = computeQpPoolSegments(pools, 6);
    ASSERT_TRUE(layout.valid);
    ASSERT_EQ(layout.total_qp, 4);

    auto pool_of = [&](int qp_index) -> const QpPoolSegment* {
        for (const auto& seg : layout.segments) {
            if (qp_index >= seg.begin && qp_index < seg.begin + seg.num_qp)
                return &seg;
        }
        return nullptr;
    };
    ASSERT_NE(pool_of(0), nullptr);
    EXPECT_EQ(pool_of(0)->name, "a");
    EXPECT_EQ(pool_of(2)->name, "a");
    ASSERT_NE(pool_of(3), nullptr);
    EXPECT_EQ(pool_of(3)->name, "b");
    // Out of range => no pool (default single-pool fallback in poolForQp).
    EXPECT_EQ(pool_of(4), nullptr);
}

TEST(QpPoolLayoutTest, PerPoolLinkLayerQosOverridesDefaults) {
    auto layout = computeQpPoolSegments(
        {{"latency", 2, 0, 3, 96}, {"bulk", 2, 0, 7, 128}}, 4);
    ASSERT_TRUE(layout.valid);

    auto latency0 = resolveQpLinkLayerQos(
        layout.segments, /*qp_index=*/0, /*default_service_level=*/1,
        /*default_traffic_class=*/2);
    EXPECT_EQ(latency0.service_level, 3);
    EXPECT_EQ(latency0.traffic_class, 96);

    auto latency1 = resolveQpLinkLayerQos(
        layout.segments, /*qp_index=*/1, /*default_service_level=*/1,
        /*default_traffic_class=*/2);
    EXPECT_EQ(latency1.service_level, 3);
    EXPECT_EQ(latency1.traffic_class, 96);

    auto bulk0 = resolveQpLinkLayerQos(
        layout.segments, /*qp_index=*/2, /*default_service_level=*/1,
        /*default_traffic_class=*/2);
    EXPECT_EQ(bulk0.service_level, 7);
    EXPECT_EQ(bulk0.traffic_class, 128);

    auto bulk1 = resolveQpLinkLayerQos(
        layout.segments, /*qp_index=*/3, /*default_service_level=*/1,
        /*default_traffic_class=*/2);
    EXPECT_EQ(bulk1.service_level, 7);
    EXPECT_EQ(bulk1.traffic_class, 128);
}

TEST(QpPoolLayoutTest, MissingPoolQosFieldsFallBackIndependently) {
    auto layout = computeQpPoolSegments(
        {{"sl-only", 1, 0, 5, -1}, {"tc-only", 1, 0, -1, 144}}, 2);
    ASSERT_TRUE(layout.valid);

    auto sl_only = resolveQpLinkLayerQos(
        layout.segments, /*qp_index=*/0, /*default_service_level=*/1,
        /*default_traffic_class=*/2);
    EXPECT_EQ(sl_only.service_level, 5);
    EXPECT_EQ(sl_only.traffic_class, 2);

    auto tc_only = resolveQpLinkLayerQos(
        layout.segments, /*qp_index=*/1, /*default_service_level=*/1,
        /*default_traffic_class=*/2);
    EXPECT_EQ(tc_only.service_level, 1);
    EXPECT_EQ(tc_only.traffic_class, 144);
}

// --- selectQpInPool: the step-3 router (slice pool -> QP index)
// ---------------

// Helper: a two-pool layout kv=[0,4), ctrl=[4,6).
static std::vector<QpPoolSegment> twoPools() {
    auto layout = computeQpPoolSegments(
        {{"kv", 4, 0, -1, -1}, {"ctrl", 2, 0, -1, -1}}, 6);
    return layout.segments;
}

// Empty pool name => pass through, folded into the whole QP range. This is the
// default (no pool selected) behavior — identical to the pre-step-3 spray.
TEST(SelectQpInPoolTest, EmptyPoolNameSpraysAcrossAllQps) {
    auto segs = twoPools();
    EXPECT_EQ(selectQpInPool(segs, "", 0, 6), 0);
    EXPECT_EQ(selectQpInPool(segs, "", 5, 6), 5);
    EXPECT_EQ(selectQpInPool(segs, "", 7, 6), 1);  // 7 % 6
}

// No pools configured at all => also pass through (single default pool).
TEST(SelectQpInPoolTest, NoSegmentsSpraysAcrossAllQps) {
    std::vector<QpPoolSegment> none;
    EXPECT_EQ(selectQpInPool(none, "kv", 3, 6), 3);
    EXPECT_EQ(selectQpInPool(none, "", 8, 6), 2);  // 8 % 6
}

// A named pool folds the candidate into that pool's segment only.
TEST(SelectQpInPoolTest, NamedPoolFoldsIntoItsSegment) {
    auto segs = twoPools();  // kv=[0,4), ctrl=[4,6)
    // kv: begin 0, num 4 -> indices 0..3
    EXPECT_EQ(selectQpInPool(segs, "kv", 0, 6), 0);
    EXPECT_EQ(selectQpInPool(segs, "kv", 3, 6), 3);
    EXPECT_EQ(selectQpInPool(segs, "kv", 4, 6), 0);  // 4 % 4 -> begin+0
    EXPECT_EQ(selectQpInPool(segs, "kv", 6, 6), 2);  // 6 % 4 -> begin+2
    // ctrl: begin 4, num 2 -> indices 4..5
    EXPECT_EQ(selectQpInPool(segs, "ctrl", 0, 6), 4);
    EXPECT_EQ(selectQpInPool(segs, "ctrl", 1, 6), 5);
    EXPECT_EQ(selectQpInPool(segs, "ctrl", 3, 6), 5);  // 3 % 2 -> begin+1
}

// Unknown pool name => fall back to the whole range (don't drop the transfer).
TEST(SelectQpInPoolTest, UnknownPoolFallsBackToWholeRange) {
    auto segs = twoPools();
    EXPECT_EQ(selectQpInPool(segs, "nope", 5, 6), 5);
    EXPECT_EQ(selectQpInPool(segs, "nope", 9, 6), 3);  // 9 % 6
}

// Negative candidate is clamped to 0 before folding.
TEST(SelectQpInPoolTest, NegativeCandidateClampsToZero) {
    auto segs = twoPools();
    EXPECT_EQ(selectQpInPool(segs, "ctrl", -1, 6), 4);  // begin+0
    EXPECT_EQ(selectQpInPool(segs, "", -1, 6), 0);
}

// Every result stays in [0, total_qp) regardless of pool/candidate.
TEST(SelectQpInPoolTest, ResultAlwaysInRange) {
    auto segs = twoPools();
    for (int c = 0; c < 20; ++c) {
        for (const char* name : {"", "kv", "ctrl", "nope"}) {
            int idx = selectQpInPool(segs, name, c, 6);
            EXPECT_GE(idx, 0);
            EXPECT_LT(idx, 6);
        }
    }
}

TEST(QpPoolWorkerRoutingTest, NamedPoolSelectsOwningWorker) {
    auto segs = twoPools();  // kv=[0,4), ctrl=[4,6)

    auto ctrl_from_worker0 = selectQpPoolRoute(
        segs, "ctrl", /*candidate=*/0, /*total_qp=*/6,
        /*num_workers=*/6, /*fallback_worker=*/0);
    EXPECT_EQ(ctrl_from_worker0.qp_index, 4);
    EXPECT_EQ(ctrl_from_worker0.worker_id, 4);

    auto ctrl_from_worker1 = selectQpPoolRoute(
        segs, "ctrl", /*candidate=*/1, /*total_qp=*/6,
        /*num_workers=*/6, /*fallback_worker=*/1);
    EXPECT_EQ(ctrl_from_worker1.qp_index, 5);
    EXPECT_EQ(ctrl_from_worker1.worker_id, 5);

    auto kv_wrap = selectQpPoolRoute(segs, "kv", /*candidate=*/5,
                                     /*total_qp=*/6, /*num_workers=*/6,
                                     /*fallback_worker=*/5);
    EXPECT_EQ(kv_wrap.qp_index, 1);
    EXPECT_EQ(kv_wrap.worker_id, 1);

    auto no_workers = selectQpPoolRoute(segs, "ctrl", /*candidate=*/0,
                                        /*total_qp=*/6, /*num_workers=*/0,
                                        /*fallback_worker=*/0);
    EXPECT_EQ(no_workers.qp_index, 4);
    EXPECT_EQ(no_workers.worker_id, 0);
}

TEST(QpPoolWorkerRoutingTest, MixedBatchGroupsSlicesByPool) {
    RdmaTask kv_task{};
    kv_task.qp_pool = "kv";
    RdmaTask ctrl_task{};
    ctrl_task.qp_pool = "ctrl";
    RdmaTask default_task{};

    RdmaSlice kv0{};
    kv0.task = &kv_task;
    RdmaSlice ctrl0{};
    ctrl0.task = &ctrl_task;
    RdmaSlice kv1{};
    kv1.task = &kv_task;
    RdmaSlice default0{};
    default0.task = &default_task;
    RdmaSlice ctrl1{};
    ctrl1.task = &ctrl_task;

    auto groups =
        groupSlicesByQpPool({&kv0, &ctrl0, &kv1, &default0, &ctrl1});

    ASSERT_EQ(groups.size(), static_cast<size_t>(3));
    EXPECT_EQ(groups[0].pool, "kv");
    EXPECT_EQ(groups[0].slices, (std::vector<RdmaSlice*>{&kv0, &kv1}));
    EXPECT_EQ(groups[1].pool, "ctrl");
    EXPECT_EQ(groups[1].slices, (std::vector<RdmaSlice*>{&ctrl0, &ctrl1}));
    EXPECT_EQ(groups[2].pool, "");
    EXPECT_EQ(groups[2].slices, (std::vector<RdmaSlice*>{&default0}));
}

// A pool with a non-positive num_qp would create an empty/negative QP span and
// break the router, so the whole layout is rejected (falls back to default).
TEST(QpPoolLayoutTest, PoolWithZeroQpIsInvalid) {
    std::vector<QpPoolSegment> pools;
    QpPoolSegment ok;
    ok.name = "kv";
    ok.num_qp = 4;
    pools.push_back(ok);
    QpPoolSegment bad;
    bad.name = "ctrl";
    bad.num_qp = 0;  // invalid
    pools.push_back(bad);

    auto layout = computeQpPoolSegments(pools, 6);
    EXPECT_FALSE(layout.valid);
    EXPECT_EQ(layout.total_qp, 0);
    EXPECT_TRUE(layout.segments.empty());
}

TEST(QpPoolLayoutTest, PoolWithNegativeQpIsInvalid) {
    std::vector<QpPoolSegment> pools;
    QpPoolSegment bad;
    bad.name = "kv";
    bad.num_qp = -1;  // invalid
    pools.push_back(bad);

    auto layout = computeQpPoolSegments(pools, 6);
    EXPECT_FALSE(layout.valid);
    EXPECT_EQ(layout.total_qp, 0);
    EXPECT_TRUE(layout.segments.empty());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
