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

// Unit tests for the TENT per-NIC inflight quota accounting: the DeviceSelector
// charge/release primitives and the slice-level reconcile
// (Workers::chargeSliceQuota / releaseSliceQuota). These lock in the invariants
// behind the device-selector scheduling-accuracy fix:
//   * allocate charges a per-slice estimate; the reconcile converts it to the
//     slice's real length once the routing NIC is final;
//   * same-NIC reconcile, A->B fallback, and retry all leave inflight at zero;
//   * cancel/timeout/failed completions (latency <= 0) do not move EWMA, while
//     a successful completion updates it with charged_bytes / attempt_latency;
//   * baseline (round-robin) mode never touches inflight, so it cannot
//   underflow;
//   * a charge against an untracked device leaves the slice uncharged (the data
//     transfer still proceeds) rather than corrupting the counter.
//
// The reconcile helpers are private static members of Workers (they only need a
// DeviceSelector, not a Workers/RdmaTransport instance);
// WorkersQuotaTestAccessor is a declared friend that forwards to them so the
// reconcile logic can be exercised directly and hardware-independently.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/topology.h"
#include "tent/transport/rdma/quota.h"
#include "tent/transport/rdma/slice.h"
#include "tent/transport/rdma/workers.h"

namespace mooncake {
namespace tent {

// Test-only shim: forwards to Workers' private static reconcile helpers (see
// the friend declaration in workers.h).
class WorkersQuotaTestAccessor {
   public:
    static void charge(DeviceSelector* sel, RdmaSlice* slice) {
        Workers::chargeSliceQuota(sel, slice);
    }
    static void release(DeviceSelector* sel, RdmaSlice* slice, double latency) {
        Workers::releaseSliceQuota(sel, slice, latency);
    }
};

namespace {

using Accessor = WorkersQuotaTestAccessor;
constexpr uint64_t kNotFound = std::numeric_limits<uint64_t>::max();

// Two RDMA NICs (dev 0, 1) reachable from host location "cpu:0".
std::shared_ptr<Topology> MakeTopology() {
    static const char* kJson = R"json(
    {
      "nics": [
        {"name": "mlx5_0", "pci_bus_id": "0000:01:00.0", "type": 0,
         "numa_node": 0},
        {"name": "mlx5_1", "pci_bus_id": "0000:02:00.0", "type": 0,
         "numa_node": 0}
      ],
      "mems": [
        {"name": "cpu:0", "numa_node": 0,
         "device_list": {"rank0": [0, 1]}}
      ]
    })json";
    auto topo = std::make_shared<Topology>();
    EXPECT_TRUE(topo->parse(kJson).ok());
    return topo;
}

std::unique_ptr<DeviceSelector> MakeSelector(bool smart) {
    auto sel = std::make_unique<DeviceSelector>();
    auto topo = MakeTopology();
    EXPECT_TRUE(sel->loadTopology(topo).ok());
    sel->setSmartSelection(smart);
    return sel;
}

uint64_t InflightOf(DeviceSelector& sel, const std::string& name) {
    std::vector<NicLoadStats> stats;
    sel.getNicLoadStats(stats);
    for (const auto& s : stats)
        if (s.device_name == name) return s.inflight_bytes;
    return kNotFound;
}

uint64_t TotalInflight(DeviceSelector& sel) {
    std::vector<NicLoadStats> stats;
    sel.getNicLoadStats(stats);
    uint64_t total = 0;
    for (const auto& s : stats) total += s.inflight_bytes;
    return total;
}

double EwmaOf(DeviceSelector& sel, const std::string& name) {
    std::vector<NicLoadStats> stats;
    sel.getNicLoadStats(stats);
    for (const auto& s : stats)
        if (s.device_name == name) return s.ewma_bandwidth_bps;
    return -1.0;
}

// A slice already charged `charged` bytes of inflight against device `dev`,
// i.e. the post-allocate state before the worker reconciles it in
// generatePostPath.
RdmaSlice ChargedSlice(DeviceSelector& sel, int dev, uint64_t charged,
                       uint64_t length) {
    EXPECT_TRUE(sel.chargeDevice(dev, charged).ok());
    RdmaSlice slice;
    slice.length = length;
    slice.source_dev_id = dev;
    slice.charged_dev_id = dev;
    slice.charged_bytes = charged;
    slice.quota_charged = true;
    return slice;
}

}  // namespace

// ---- allocate charges an estimate, reconcile converts it to real length ----

TEST(QuotaAccounting, AllocateChargesPerSliceEstimate) {
    auto sel = MakeSelector(/*smart=*/true);
    std::vector<int> dev_ids;
    std::vector<uint64_t> charged;
    // total 1000 over 4 slices -> estimate ceil(1000/4) = 250 per slice.
    ASSERT_TRUE(sel->allocate(1000, /*num_slices=*/4, /*slice_bytes=*/250,
                              "cpu:0", dev_ids, PRIO_HIGH, ~0ULL, &charged)
                    .ok());
    ASSERT_EQ(dev_ids.size(), 4u);
    ASSERT_EQ(charged.size(), 4u);
    for (uint64_t c : charged) EXPECT_EQ(c, 250u);  // estimate, not real length
    EXPECT_EQ(TotalInflight(*sel), 1000u);          // charged == sum estimates
}

TEST(QuotaAccounting, ReconcileConvertsEstimateToRealLengthSameNic) {
    auto sel = MakeSelector(/*smart=*/true);
    // Charged the 250-byte estimate on dev 0, but the slice is really 256
    // bytes.
    RdmaSlice slice = ChargedSlice(*sel, /*dev=*/0, /*charged=*/250,
                                   /*length=*/256);
    ASSERT_EQ(InflightOf(*sel, "mlx5_0"), 250u);

    Accessor::charge(sel.get(), &slice);  // as generatePostPath would

    EXPECT_EQ(slice.charged_bytes, 256u);  // now the real length
    EXPECT_EQ(slice.charged_dev_id, 0);
    EXPECT_TRUE(slice.quota_charged);
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 256u);  // inflight == real length

    Accessor::release(sel.get(), &slice, 0.0);
    EXPECT_FALSE(slice.quota_charged);
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

// ---- the three reconcile paths all return inflight to zero ----

TEST(QuotaAccounting, SameNicReconcileReturnsToZero) {
    auto sel = MakeSelector(/*smart=*/true);
    RdmaSlice slice = ChargedSlice(*sel, 0, 250, 256);
    Accessor::charge(sel.get(), &slice);
    Accessor::release(sel.get(), &slice, 0.0);
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

TEST(QuotaAccounting, FallbackMigrationMovesChargeAndReturnsToZero) {
    auto sel = MakeSelector(/*smart=*/true);
    // Charged on dev 0, but a fallback re-routed the slice to dev 1.
    RdmaSlice slice = ChargedSlice(*sel, /*dev=*/0, /*charged=*/256,
                                   /*length=*/256);
    slice.source_dev_id = 1;  // fallback rewrote the routing NIC

    Accessor::charge(sel.get(), &slice);
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);    // original NIC unwound
    EXPECT_EQ(InflightOf(*sel, "mlx5_1"), 256u);  // migrated to fallback NIC
    EXPECT_EQ(slice.charged_dev_id, 1);

    Accessor::release(sel.get(), &slice, 0.0);
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

TEST(QuotaAccounting, RetryReEntersInflightThenReturnsToZero) {
    auto sel = MakeSelector(/*smart=*/true);
    RdmaSlice slice = ChargedSlice(*sel, 0, 256, 256);

    // Retry path: the failed attempt releases the quota before resubmitting.
    Accessor::release(sel.get(), &slice, 0.0);
    EXPECT_FALSE(slice.quota_charged);
    EXPECT_EQ(TotalInflight(*sel), 0u);  // not double-counted, not leaked

    // Resubmit re-selects a NIC and reconciles -> re-enters the inflight view.
    slice.source_dev_id = 1;
    Accessor::charge(sel.get(), &slice);
    EXPECT_TRUE(slice.quota_charged);
    EXPECT_EQ(InflightOf(*sel, "mlx5_1"), 256u);

    Accessor::release(sel.get(), &slice, 0.0);
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

// ---- EWMA is only learned from successful completions ----

TEST(QuotaAccounting, NonSuccessCompletionDoesNotMoveEwma) {
    auto sel = MakeSelector(/*smart=*/true);
    const double before = EwmaOf(*sel, "mlx5_0");
    RdmaSlice slice = ChargedSlice(*sel, 0, 4096, 4096);

    // cancel / timeout / failed CQ all release with latency <= 0.
    Accessor::release(sel.get(), &slice, 0.0);

    EXPECT_EQ(EwmaOf(*sel, "mlx5_0"), before);  // unchanged
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);
}

TEST(QuotaAccounting, SuccessUpdatesEwmaWithLengthOverLatency) {
    auto sel = MakeSelector(/*smart=*/true);
    const double before = EwmaOf(*sel, "mlx5_0");

    const uint64_t length = 100'000'000;  // 100 MB
    const double latency = 0.001;         // seconds (attempt latency)
    RdmaSlice slice = ChargedSlice(*sel, 0, length, length);

    Accessor::release(sel.get(), &slice, latency);

    // observed_bw = length / latency; EWMA = a*old + (1-a)*observed, a = 0.01.
    const double observed = static_cast<double>(length) / latency;  // 1e11 B/s
    const double alpha = sel->getSchedulingParams().bandwidth_learning_rate;
    const double expected = alpha * before + (1.0 - alpha) * observed;
    EXPECT_NEAR(EwmaOf(*sel, "mlx5_0"), expected, expected * 1e-6);
    EXPECT_NE(EwmaOf(*sel, "mlx5_0"), before);
}

// ---- baseline (round-robin) mode never tracks inflight ----

TEST(QuotaAccounting, BaselineModeKeepsInflightZero) {
    auto sel = MakeSelector(/*smart=*/false);
    std::vector<int> dev_ids;
    std::vector<uint64_t> charged;
    ASSERT_TRUE(sel->allocate(4096, 8, 512, "cpu:0", dev_ids, PRIO_HIGH, ~0ULL,
                              &charged)
                    .ok());
    ASSERT_EQ(charged.size(), dev_ids.size());
    for (uint64_t c : charged) EXPECT_EQ(c, 0u);  // baseline charges nothing
    EXPECT_EQ(TotalInflight(*sel), 0u);

    // Even the reconcile/release path must not move (or underflow) inflight.
    RdmaSlice slice;
    slice.length = 512;
    slice.source_dev_id = 0;
    Accessor::charge(sel.get(), &slice);
    EXPECT_EQ(TotalInflight(*sel), 0u);
    Accessor::release(sel.get(), &slice, 0.001);
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);  // not ~2^64 (no underflow)
    EXPECT_EQ(InflightOf(*sel, "mlx5_1"), 0u);
}

// ---- charge against an untracked device: proceed, but do not corrupt state
// ----

TEST(QuotaAccounting, ChargeFailureLeavesSliceUncharged) {
    auto sel = MakeSelector(/*smart=*/true);
    RdmaSlice slice;
    slice.length = 4096;
    slice.source_dev_id = 999;  // not present in the topology

    Accessor::charge(sel.get(), &slice);

    EXPECT_FALSE(slice.quota_charged);  // not marked charged
    EXPECT_EQ(slice.charged_dev_id, -1);
    EXPECT_EQ(slice.charged_bytes, 0u);
    EXPECT_EQ(TotalInflight(*sel), 0u);

    // A later release must be a harmless no-op (no crash, no underflow).
    Accessor::release(sel.get(), &slice, 0.0);
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

TEST(QuotaAccounting,
     MigrationToUntrackedDeviceUnwindsOriginalWithoutUnderflow) {
    auto sel = MakeSelector(/*smart=*/true);
    RdmaSlice slice = ChargedSlice(*sel, /*dev=*/0, 256, 256);
    slice.source_dev_id = 999;  // fallback picked an untracked device

    Accessor::charge(sel.get(), &slice);

    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);  // original NIC still unwound
    EXPECT_FALSE(slice.quota_charged);  // new charge failed -> uncharged
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

}  // namespace tent
}  // namespace mooncake
