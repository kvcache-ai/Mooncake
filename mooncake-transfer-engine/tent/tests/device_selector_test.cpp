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
//
// DeviceSelector bandwidth calibration: the per-device link speed must drive
// the EWMA seed and clamp instead of a one-size-fits-all constant.

#include "tent/transport/rdma/quota.h"


#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <utility>
#include <algorithm>
#include <vector>

namespace mooncake {
namespace tent {
namespace {

constexpr int kDev = 0;
constexpr uint64_t kMiB = 1ull << 20;

std::shared_ptr<Topology> oneRdmaNic() {
    auto topo = std::make_shared<Topology>();
    Topology::NicEntry nic;
    nic.name = "mlx5_0";
    nic.type = Topology::NIC_RDMA;
    nic.numa_node = 0;
    topo->nic_list_.push_back(nic);
    return topo;
}

// A selector over one RDMA NIC, with scheduling params applied the way
// Workers does it (topology first, then params).
std::unique_ptr<DeviceSelector> makeSelector(
    const DeviceSelector::SchedulingParams& params = {}) {
    auto topo = oneRdmaNic();
    auto sel = std::make_unique<DeviceSelector>();
    EXPECT_TRUE(sel->loadTopology(topo).ok());
    sel->setSchedulingParams(params);
    return sel;
}

// Feed one completion whose observed bandwidth is `bytes_per_sec`.
void observe(DeviceSelector& sel, double bytes_per_sec) {
    ASSERT_TRUE(sel.release(kDev, kMiB, kMiB / bytes_per_sec).ok());
}

TEST(DeviceSelectorBandwidthTest, LinkSpeedSeedsEwma) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    // 25 Gbps = 3.125 GB/s, before a single byte has moved.
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 3.125e9);
}

TEST(DeviceSelectorBandwidthTest, UnknownLinkSpeedUsesConfiguredDefault) {
    DeviceSelector::SchedulingParams params;
    params.default_bandwidth_gbps = 100.0;
    auto sel = makeSelector(params);
    // 0 = the transport could not read the port speed.
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 0.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
}

TEST(DeviceSelectorBandwidthTest, OutOfRangeLinkSpeedUsesConfiguredDefault) {
    DeviceSelector::SchedulingParams params;
    params.default_bandwidth_gbps = 100.0;
    params.min_bandwidth_gbps = 10.0;
    params.max_bandwidth_gbps = 800.0;
    auto sel = makeSelector(params);
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 5.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 1600.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
}

TEST(DeviceSelectorBandwidthTest, UnknownDeviceIsRejected) {
    auto sel = makeSelector();
    EXPECT_FALSE(sel->setDeviceBandwidth(7, 100.0).ok());
}

// The bug this file exists for: with theoretical bandwidth fixed at 400 Gbps
// the EWMA floor was 5 GB/s everywhere, so a 25G link (3.1 GB/s measured)
// could never learn its real rate.
TEST(DeviceSelectorBandwidthTest, EwmaLearnsRealRateOn25GLink) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    const double measured = 3.1e9;
    for (int i = 0; i < 64; ++i) observe(*sel, measured);
    EXPECT_NEAR(sel->getAggregateEwmaBandwidth(), measured, measured * 0.02);
}

TEST(DeviceSelectorBandwidthTest, EwmaClampScalesWithLinkSpeed) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    // One absurd sample: the ceiling must be 10x the 25G link, not 10x 400G.
    observe(*sel, 1e13);
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 31.25e9);
    // And absurdly slow ones: the floor is 0.1x the 25G link.
    observe(*sel, 1e3);
    observe(*sel, 1e3);
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 0.3125e9);
}

// ---------------------------------------------------------------------------
// Availability: a NIC that cannot carry traffic (context never constructed,
// construct() failed, port down) must not be a selection candidate and must
// not count toward the aggregate the admission queue reads.
// ---------------------------------------------------------------------------

constexpr int kDev0 = 0;
constexpr int kDev1 = 1;

// Two RDMA NICs, both rank-0 for "cpu:0".
std::shared_ptr<Topology> twoRdmaNics() {
    auto topo = std::make_shared<Topology>();
    for (const char* name : {"mlx5_0", "mlx5_1"}) {
        Topology::NicEntry nic;
        nic.name = name;
        nic.type = Topology::NIC_RDMA;
        nic.numa_node = 0;
        topo->nic_list_.push_back(nic);
    }
    Topology::MemEntry mem;
    mem.name = "cpu:0";
    mem.type = Topology::MEM_HOST;
    mem.numa_node = 0;
    mem.device_list[0] = {kDev0, kDev1};
    topo->mem_list_.push_back(mem);
    return topo;
}

std::unique_ptr<DeviceSelector> makeTwoNicSelector(
    const DeviceSelector::SchedulingParams& params = {}) {
    auto topo = twoRdmaNics();
    auto sel = std::make_unique<DeviceSelector>();
    EXPECT_TRUE(sel->loadTopology(topo).ok());
    sel->setSchedulingParams(params);
    EXPECT_TRUE(sel->setDeviceBandwidth(kDev0, 100.0).ok());
    EXPECT_TRUE(sel->setDeviceBandwidth(kDev1, 100.0).ok());
    return sel;
}

// Allocate `rounds` single-slice requests and return how many landed on each
// device, releasing each so inflight never skews the next choice.
std::pair<int, int> countAllocations(DeviceSelector& sel, int rounds) {
    std::pair<int, int> hits{0, 0};
    for (int i = 0; i < rounds; ++i) {
        int dev = -1;
        if (!sel.allocate(kMiB, "cpu:0", dev).ok()) continue;
        if (dev == kDev0) ++hits.first;
        if (dev == kDev1) ++hits.second;
        sel.release(dev, kMiB, 0.0);
    }
    return hits;
}

TEST(DeviceSelectorAvailabilityTest, DevicesStartAvailable) {
    auto sel = makeTwoNicSelector();
    EXPECT_TRUE(sel->isDeviceAvailable(kDev0));
    EXPECT_TRUE(sel->isDeviceAvailable(kDev1));
    EXPECT_FALSE(sel->isDeviceAvailable(7));  // unknown device
    EXPECT_FALSE(sel->setDeviceAvailable(7, false).ok());
}

TEST(DeviceSelectorAvailabilityTest, UnavailableDeviceLeavesAggregate) {
    auto sel = makeTwoNicSelector();
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 25e9);
    EXPECT_DOUBLE_EQ(sel->getAggregateTransmitBandwidth(), 25e9);
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
    // The admission queue reads this series: a dead NIC must leave it too.
    EXPECT_DOUBLE_EQ(sel->getAggregateTransmitBandwidth(), 12.5e9);
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, true).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 25e9);
}

// getNicLoadStats() is the load signal upper layers score NICs with
// (#2996, for #2516). A NIC that cannot carry traffic must not appear in it
// at all: its seed is meaningless and, with zero inflight, would look like
// the best NIC of the lot -- the same bad pick the aggregate avoids.
TEST(DeviceSelectorAvailabilityTest, UnavailableDeviceLeavesLoadStats) {
    auto sel = makeTwoNicSelector();
    std::vector<NicLoadStats> stats;
    ASSERT_TRUE(sel->getNicLoadStats(stats).ok());
    ASSERT_EQ(stats.size(), 2u);

    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    stats.clear();
    ASSERT_TRUE(sel->getNicLoadStats(stats).ok());
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].device_name, "mlx5_0");

    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, true).ok());
    stats.clear();
    ASSERT_TRUE(sel->getNicLoadStats(stats).ok());
    EXPECT_EQ(stats.size(), 2u);
}

TEST(DeviceSelectorAvailabilityTest, UnavailableDeviceIsNeverAllocated) {
    auto sel = makeTwoNicSelector();
    auto both = countAllocations(*sel, 64);
    ASSERT_GT(both.first, 0);
    ASSERT_GT(both.second, 0);  // sanity: both are picked when available

    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    auto smart = countAllocations(*sel, 64);
    EXPECT_EQ(smart.first, 64);
    EXPECT_EQ(smart.second, 0);

    sel->setSmartSelection(false);  // round-robin path has its own loop
    auto rr = countAllocations(*sel, 64);
    EXPECT_EQ(rr.first, 64);
    EXPECT_EQ(rr.second, 0);
}

TEST(DeviceSelectorAvailabilityTest, AllDevicesUnavailable) {
    auto sel = makeTwoNicSelector();
    ASSERT_TRUE(sel->setDeviceAvailable(kDev0, false).ok());
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    int dev = -1;
    EXPECT_FALSE(sel->allocate(kMiB, "cpu:0", dev).ok());
    // No usable bandwidth: the admission queue must not drop on it.
    EXPECT_LT(sel->getAggregateEwmaBandwidth(), 0.0);
}

TEST(DeviceSelectorAvailabilityTest, PriorityFallbackKeepsUnavailableOut) {
    // Priority filtering can reject every candidate, after which
    // buildCandidates falls back to "all devices". That fallback must still
    // honor availability.
    DeviceSelector::SchedulingParams params;
    params.enable_priority_filtering = true;
    params.local_rotation_interval_us = 0;  // fixed priorities: dev0=0, dev1=1
    auto sel = makeTwoNicSelector(params);
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    // Device priorities are 0 and 1, so request priority 2 rejects both in
    // the first pass; the fallback must then offer dev0 only.
    for (int i = 0; i < 16; ++i) {
        std::vector<int> dev_ids;
        ASSERT_TRUE(sel->allocate(kMiB, 1, kMiB, "cpu:0", dev_ids, 2).ok());
        ASSERT_EQ(dev_ids, std::vector<int>{kDev0});
        sel->release(kDev0, kMiB, 0.0);
    }
    ASSERT_TRUE(sel->setDeviceAvailable(kDev0, false).ok());
    std::vector<int> dev_ids;
    EXPECT_FALSE(sel->allocate(kMiB, 1, kMiB, "cpu:0", dev_ids, 2).ok());
}

// The reviewer's scenario for a link that renegotiates lower: the learned
// EWMA and the clamp derived from the old speed must both be replaced.
TEST(DeviceSelectorBandwidthTest, ReseedOnLinkSpeedChangeReleasesStaleClamp) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 400.0).ok());
    for (int i = 0; i < 64; ++i) observe(*sel, 45e9);
    ASSERT_NEAR(sel->getAggregateEwmaBandwidth(), 45e9, 45e9 * 0.02);

    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 3.125e9);
    for (int i = 0; i < 64; ++i) observe(*sel, 3.1e9);
    // With the 400G clamp still in place this would sit at the 5 GB/s floor.
    EXPECT_NEAR(sel->getAggregateEwmaBandwidth(), 3.1e9, 3.1e9 * 0.02);
}

// ---------------------------------------------------------------------------
// The transmit estimate: the series the admission queue's deadline-drop
// predictor and the NIC arbitration read. It is metered from bytes completed
// over the NIC's busy time -- never from a slice's own latency, which grows
// with the batch it travelled in -- and smoothed hard enough that one odd
// interval cannot flip a drop decision.
// ---------------------------------------------------------------------------

constexpr uint64_t kT0 = 1'000'000'000;  // 1s in ns

// A selector whose meter samples on every call and adopts each sample
// outright, so one interval is one observation. 25 Gbps seed = 3.125 GB/s.
std::unique_ptr<DeviceSelector> makeMeteredSelector(double alpha = 0.0) {
    DeviceSelector::SchedulingParams params;
    params.transmit_bandwidth_learning_rate = alpha;
    params.transmit_meter_interval_ns = 0;  // sample whenever asked
    auto sel = makeSelector(params);
    EXPECT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    return sel;
}

// Drive the meter the way the RDMA workers do: `n` slices of 1 MiB are
// posted together (one submitSlices call, one timestamp), the NIC serves
// them at `per_slice_ns` each, and completions are polled `poll_batch` at a
// time (one poll pass timestamps its whole group alike).
// Returns the timestamp of the last completion, so batches can be chained
// on a clock that only moves forward (a sample whose timestamp is not past
// the previous one is ignored, as the meter ignores it in production).
uint64_t runBatch(DeviceSelector& sel, uint32_t n, uint64_t per_slice_ns,
                  uint32_t poll_batch, uint64_t t0 = kT0) {
    for (uint32_t i = 0; i < n; ++i) sel.notePosted(kDev, kMiB, t0);
    sel.maybeSampleTransmit(kDev, t0);  // baseline: the NIC has a backlog
    uint32_t done = 0;
    uint64_t poll_ts = t0;
    while (done < n) {
        const uint32_t group = std::min(poll_batch, n - done);
        poll_ts = t0 + (done + group) * per_slice_ns;
        for (uint32_t i = 0; i < group; ++i) {
            sel.notePostEnded(kDev, kMiB, poll_ts);
            sel.noteCompleted(kDev, kMiB);
        }
        done += group;
        sel.maybeSampleTransmit(kDev, poll_ts);
    }
    return poll_ts;
}

// The NIC arbitration needs "bytes already charged to this NIC" as the
// queue-ahead term of its deadline prediction.
TEST(DeviceSelectorTransmitTest, InflightBytesPerDevice) {
    auto topo = oneRdmaNic();
    Topology::MemEntry mem;
    mem.name = "cpu:0";
    mem.type = Topology::MEM_HOST;
    mem.numa_node = 0;
    mem.device_list[0].push_back(kDev);
    topo->mem_list_.push_back(mem);
    DeviceSelector sel;
    ASSERT_TRUE(sel.loadTopology(topo).ok());

    EXPECT_EQ(sel.getInflightBytes(kDev), 0u);
    int chosen = -1;
    ASSERT_TRUE(sel.allocate(kMiB, "cpu:0", chosen).ok());
    ASSERT_EQ(chosen, kDev);
    EXPECT_EQ(sel.getInflightBytes(kDev), kMiB);
    EXPECT_EQ(sel.getInflightBytes(7), 0u);  // unknown device
    ASSERT_TRUE(sel.release(kDev, kMiB, 0.0).ok());
    EXPECT_EQ(sel.getInflightBytes(kDev), 0u);
}

// Smart-mode multi-path allocation must charge each device the bytes of the
// slices it was assigned -- the caller cuts slices as min(block, remaining),
// and release() returns exactly that -- not ceil(total / n) per slice, which
// would drift per device (and wrap a device that receives less than it
// released).
TEST(DeviceSelectorTransmitTest, MultiPathChargesActualSliceBytes) {
    auto topo = oneRdmaNic();
    Topology::NicEntry nic;
    nic.name = "mlx5_1";
    nic.type = Topology::NIC_RDMA;
    nic.numa_node = 0;
    topo->nic_list_.push_back(nic);
    Topology::MemEntry mem;
    mem.name = "cpu:0";
    mem.type = Topology::MEM_HOST;
    mem.numa_node = 0;
    mem.device_list[0].push_back(0);
    mem.device_list[0].push_back(1);
    topo->mem_list_.push_back(mem);
    DeviceSelector sel;
    ASSERT_TRUE(sel.loadTopology(topo).ok());
    DeviceSelector::SchedulingParams params;
    params.score_jitter_range = 0.0;
    sel.setSchedulingParams(params);

    // 1,000,001 B in 64 KiB blocks: 15 full slices and one of 16,961 B;
    // ceil(total / 16) = 62,501 would charge 1,000,016 in total.
    constexpr uint64_t kTotal = 1'000'001;
    constexpr uint64_t kBlock = 65536;
    constexpr uint32_t kSlices = 16;
    std::vector<int> ids;
    ASSERT_TRUE(
        sel.allocate(kTotal, kSlices, kBlock, "cpu:0", ids, PRIO_HIGH, ~0ULL)
            .ok());
    ASSERT_EQ(ids.size(), kSlices);

    uint64_t expected[2] = {0, 0};
    for (uint32_t i = 0; i < kSlices; ++i) {
        ASSERT_TRUE(ids[i] == 0 || ids[i] == 1);
        expected[ids[i]] += std::min<uint64_t>(kBlock, kTotal - i * kBlock);
    }
    EXPECT_EQ(sel.getInflightBytes(0), expected[0]);
    EXPECT_EQ(sel.getInflightBytes(1), expected[1]);

    for (uint32_t i = 0; i < kSlices; ++i) {
        const uint64_t len = std::min<uint64_t>(kBlock, kTotal - i * kBlock);
        ASSERT_TRUE(sel.release(ids[i], len, 0.0).ok());
    }
    EXPECT_EQ(sel.getInflightBytes(0), 0u);
    EXPECT_EQ(sel.getInflightBytes(1), 0u);
}

// A learning rate is a weight on the old value; anything outside [0, 1]
// makes the EWMA update meaningless, so setSchedulingParams clamps both.
TEST(DeviceSelectorTransmitTest, LearningRatesAreClampedToUnitInterval) {
    DeviceSelector::SchedulingParams params;
    params.bandwidth_learning_rate = 1.5;
    params.transmit_bandwidth_learning_rate = -0.5;
    params.transmit_meter_interval_ns = 0;
    auto sel = makeSelector(params);
    EXPECT_DOUBLE_EQ(sel->getSchedulingParams().bandwidth_learning_rate, 1.0);
    EXPECT_DOUBLE_EQ(
        sel->getSchedulingParams().transmit_bandwidth_learning_rate, 0.0);

    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    ASSERT_TRUE(sel->release(kDev, kMiB, kMiB / 3.1e9).ok());
    runBatch(*sel, 1, kMiB, 1);  // one interval at 1 GB/s
    // alpha 1.0: the selection EWMA never learns; alpha 0.0: the transmit
    // estimate adopts each interval outright.
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 3.125e9);
    EXPECT_NEAR(sel->getAggregateTransmitBandwidth(), 1e9, 1.0);
}

TEST(DeviceSelectorTransmitTest, SeededFromLinkSpeed) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    EXPECT_DOUBLE_EQ(sel->getTransmitBandwidth(kDev), 3.125e9);
    EXPECT_DOUBLE_EQ(sel->getAggregateTransmitBandwidth(), 3.125e9);
    EXPECT_LT(sel->getTransmitBandwidth(7), 0.0);  // unknown device
}

// The point of metering bytes over time rather than timing each slice: work
// requested together is posted with one timestamp and completed under one
// poll timestamp, so per-slice "post to completion" grows with the batch
// while the NIC's rate does not.
TEST(DeviceSelectorTransmitTest, RateIsIndependentOfBatchDepth) {
    // 1 MiB every kMiB/8 nanoseconds == 8 bytes/ns == 8 GB/s. Even the
    // 64-slice batch then fits well inside one meter window.
    for (uint32_t n : {1u, 8u, 64u}) {
        for (uint32_t poll_batch : {1u, 64u}) {
            auto sel = makeMeteredSelector();
            runBatch(*sel, n, kMiB / 8, poll_batch);
            EXPECT_NEAR(sel->getTransmitBandwidth(kDev), 8e9, 1.0)
                << "n=" << n << " poll_batch=" << poll_batch;
        }
    }
}

// Selection and transmit answer different questions from the same traffic:
// each MiB queues on the NIC behind earlier work as long as it spends on
// the wire.
TEST(DeviceSelectorTransmitTest, SelectionKeepsQueueingThatTransmitDrops) {
    auto sel = makeMeteredSelector();
    const double wire_s = kMiB / 1e9;
    for (int i = 0; i < 2000; ++i)
        ASSERT_TRUE(sel->release(kDev, kMiB, 2.0 * wire_s).ok());
    runBatch(*sel, 8, kMiB, 8);
    // A backed-up NIC should look slow to device selection...
    EXPECT_NEAR(sel->getAggregateEwmaBandwidth(), 0.5e9, 0.5e9 * 0.02);
    // ...while the deadline predictors get the rate it moves bytes at.
    EXPECT_NEAR(sel->getAggregateTransmitBandwidth(), 1e9, 1.0);
}

// The accumulator itself: time is banked only for stretches during which
// something was posted, and a stretch already open is not restarted.
TEST(DeviceSelectorTransmitTest, BusyTimeCountsOnlyStretchesWithWorkPosted) {
    auto sel = makeSelector();
    EXPECT_EQ(sel->getBusyNs(kDev), 0u);

    sel->notePosted(kDev, kMiB, kT0);
    sel->notePosted(kDev, kMiB, kT0 + 5);      // already busy: not a restart
    sel->notePostEnded(kDev, kMiB, kT0 + 40);  // still busy
    EXPECT_EQ(sel->getBusyNs(kDev), 0u);       // nothing banked until idle
    sel->notePostEnded(kDev, kMiB, kT0 + 100);
    EXPECT_EQ(sel->getBusyNs(kDev), 100u);

    // The gap to the next stretch is not counted.
    sel->notePosted(kDev, kMiB, kT0 + 1000);
    sel->notePostEnded(kDev, kMiB, kT0 + 1010);
    EXPECT_EQ(sel->getBusyNs(kDev), 110u);
}

// Two workers racing on one NIC can open the next stretch before the ending
// one is banked, leaving busy_since ahead of the timestamp closing it. Bank
// nothing rather than an underflowed interval.
TEST(DeviceSelectorTransmitTest, OutOfOrderTransitionBanksNothing) {
    auto sel = makeSelector();
    sel->notePosted(kDev, kMiB, kT0 + 500);
    sel->notePostEnded(kDev, kMiB, kT0 + 100);
    EXPECT_EQ(sel->getBusyNs(kDev), 0u);
}

// An interval that opens with an empty queue pair still holds a usable
// measurement: whatever the NIC did while it had work. Only the idle part is
// dropped, not the whole interval.
TEST(DeviceSelectorTransmitTest, OnlyTheBusyPartOfAnIntervalIsMeasured) {
    auto sel = makeMeteredSelector();
    sel->maybeSampleTransmit(kDev, kT0);  // opens idle
    // Idle for 3 * kMiB ns, then 1 MiB moved in kMiB ns: 1 byte/ns.
    sel->notePosted(kDev, kMiB, kT0 + 3 * kMiB);
    sel->notePostEnded(kDev, kMiB, kT0 + 4 * kMiB);
    sel->noteCompleted(kDev, kMiB);
    sel->maybeSampleTransmit(kDev, kT0 + 4 * kMiB);
    EXPECT_NEAR(sel->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// A sample spanning more wall time than transmit_meter_max_interval_ns is
// dropped whatever its busy time says: the link it describes is too old to
// attribute to the link as it is now.
TEST(DeviceSelectorTransmitTest, StaleIntervalsAreNotSamples) {
    auto sel = makeMeteredSelector();
    sel->notePosted(kDev, kMiB, kT0);
    sel->maybeSampleTransmit(kDev, kT0);
    sel->notePostEnded(kDev, kMiB, kT0 + 1'000'000'000);
    sel->noteCompleted(kDev, kMiB);
    sel->maybeSampleTransmit(kDev, kT0 + 1'000'000'000);
    EXPECT_DOUBLE_EQ(sel->getTransmitBandwidth(kDev), 3.125e9);
}

TEST(DeviceSelectorTransmitTest, MeterHonoursItsInterval) {
    DeviceSelector::SchedulingParams params;
    params.transmit_bandwidth_learning_rate = 0.0;
    auto sel = makeSelector(params);  // default 10 ms interval
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());

    sel->notePosted(kDev, 64 * kMiB, kT0);
    sel->maybeSampleTransmit(kDev, kT0);
    sel->noteCompleted(kDev, kMiB);
    sel->maybeSampleTransmit(kDev, kT0 + 1'000'000);  // 1 ms: too soon
    EXPECT_DOUBLE_EQ(sel->getTransmitBandwidth(kDev), 3.125e9);
    sel->maybeSampleTransmit(kDev, kT0 + 10'000'000);  // 10 ms: sampled
    EXPECT_NE(sel->getTransmitBandwidth(kDev), 3.125e9);
}

// One fast interval is one observation among many, not the new estimate.
TEST(DeviceSelectorTransmitTest, SmoothsSingleOutlier) {
    auto sel = makeMeteredSelector(0.9);  // the shipped learning rate
    uint64_t t = kT0;
    for (int i = 0; i < 200; ++i) t = runBatch(*sel, 8, kMiB, 8, t);
    ASSERT_NEAR(sel->getTransmitBandwidth(kDev), 1e9, 1e9 * 0.02);

    // An interval that moved 30x as fast moves the estimate by ~(1 - alpha).
    sel->notePosted(kDev, kMiB, t);
    sel->maybeSampleTransmit(kDev, t);
    sel->notePostEnded(kDev, kMiB, t + kMiB / 30);
    sel->noteCompleted(kDev, kMiB);
    sel->maybeSampleTransmit(kDev, t + kMiB / 30);
    EXPECT_LT(sel->getTransmitBandwidth(kDev), 5e9);
    EXPECT_GT(sel->getTransmitBandwidth(kDev), 3e9);
}

// release() feeds the selection EWMA only; the transmit estimate has one
// source, the meter.
TEST(DeviceSelectorTransmitTest, ReleaseDoesNotFeedTheTransmitEstimate) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    ASSERT_TRUE(sel->release(kDev, kMiB, kMiB / 1e9).ok());
    EXPECT_NE(sel->getAggregateEwmaBandwidth(), 3.125e9);
    EXPECT_DOUBLE_EQ(sel->getAggregateTransmitBandwidth(), 3.125e9);
}

TEST(DeviceSelectorTransmitTest, ClampScalesWithLinkSpeed) {
    auto sel = makeMeteredSelector();
    // 1 MiB in a nanosecond: capped at 10x the 3.125 GB/s link rate.
    runBatch(*sel, 1, 1, 1);
    EXPECT_DOUBLE_EQ(sel->getTransmitBandwidth(kDev), 31.25e9);
    // 1 MiB in 40 ms: floored at 0.1x.
    sel->notePosted(kDev, kMiB, kT0 + 1);
    sel->maybeSampleTransmit(kDev, kT0 + 1);
    sel->notePostEnded(kDev, kMiB, kT0 + 1 + 40'000'000);
    sel->noteCompleted(kDev, kMiB);
    sel->maybeSampleTransmit(kDev, kT0 + 1 + 40'000'000);
    EXPECT_DOUBLE_EQ(sel->getTransmitBandwidth(kDev), 0.3125e9);
}

// ---------------------------------------------------------------------------
// How closely the meter recovers a NIC's real rate in the SHIPPED
// configuration -- 10 ms interval, alpha 0.9 -- rather than the
// one-sample-per-call setup the mechanism tests above use. Each records the
// figure it measured and pins it with a bound just above it, so a change in
// what the meter is worth as a number shows up here rather than in
// production. Every figure is the worst of ten readings taken over
// successive stretches of the same traffic, not the value one run happened
// to stop on.
//
//   steady 8 GB/s, polled 1, 16 or 64     4.8e-16, the last bit of a double
//   steady 8 GB/s, 64 MiB slices          7.0e-13   (slice size cancels)
//   steady 2 GB/s, polled 16 deep         5.8e-13   (one pass fills most of
//                                                    a meter interval)
//   bursty 4/12 GB/s, mean 8              1.2%      (the swing between the
//                                                    halves, not an offset)
//   21% duty cycle, 16 MiB per 10 ms      6.0e-16   (was -78% before the
//                                                    meter charged busy time)
//   8 -> 2 GB/s renegotiation             within 5% after 478 ms
//   a real 50 GB/s wire                   pinned at 31.25 GB/s, the clamp
//
// Once the sample is taken at the end of a poll pass, how deep the polling
// is does not enter the steady-state error at all. What it does change is
// how often a sample can be taken, and therefore how fast the estimate
// follows a change -- which is what the renegotiation figure measures.
// ---------------------------------------------------------------------------

// Default scheduling params (10 ms meter interval, alpha 0.9, 50 ms staleness
// cut-off) over a 25 Gbps link: seed and clamp centre are 3.125 GB/s.
std::unique_ptr<DeviceSelector> shippedSelector() {
    auto sel = makeSelector();
    EXPECT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    return sel;
}

// A NIC transmitting steadily at `bytes_per_ns`, driven the way
// handleCompletion drives the meter: a slice leaves the wire every
// slice/rate nanoseconds, one poll pass picks up `poll_batch` of them and
// stamps the whole group with a single timestamp, each completion offers the
// meter a sample, and the queue pair is refilled so the NIC never goes idle.
// Returns the estimate after `duration_ns` of traffic.
double runWire(DeviceSelector& sel, double bytes_per_ns, uint64_t slice,
               uint64_t poll_batch, uint64_t duration_ns, uint64_t& clock) {
    const uint64_t per_slice_ns = static_cast<uint64_t>(slice / bytes_per_ns);
    // Whole slices only, and the clock is advanced by exactly what was
    // driven: a stretch left over at the end would be busy time with no
    // completions in it, which is the harness lying to the meter rather
    // than the meter being wrong.
    const uint64_t n = duration_ns / per_slice_ns;
    const uint64_t start_ns = clock;
    for (uint64_t i = 0; i < poll_batch; ++i)
        sel.notePosted(kDev, slice, start_ns);
    sel.maybeSampleTransmit(kDev, start_ns);  // open an interval, NIC busy

    uint64_t done = 0;
    while (done < n) {
        const uint64_t group = std::min<uint64_t>(poll_batch, n - done);
        // One poll pass timestamps everything it reaps alike.
        const uint64_t poll_ts = start_ns + (done + group) * per_slice_ns;
        for (uint64_t i = 0; i < group; ++i) {
            sel.notePostEnded(kDev, slice, poll_ts);
            sel.noteCompleted(kDev, slice);
            // Refill: the queue pair never drains, so the NIC stays busy.
            sel.notePosted(kDev, slice, poll_ts);
        }
        sel.maybeSampleTransmit(kDev, poll_ts);  // once per pass, at its end
        done += group;
    }
    clock = start_ns + n * per_slice_ns;
    return sel.getTransmitBandwidth(kDev);
}

double relErr(double got, double want) { return std::abs(got - want) / want; }

// The worst reading in a run, and where the readings sat. Each scenario is
// judged over many meter intervals rather than at one endpoint: an estimate
// that lands on the right number once but wanders between samples is not
// worth what the deadline predictors spend it on.
struct Spread {
    double worst_rel_err = 0.0;
    double lo = 0.0;
    double hi = 0.0;

    void add(double got, double want) {
        if (lo == 0.0 || got < lo) lo = got;
        if (got > hi) hi = got;
        worst_rel_err = std::max(worst_rel_err, relErr(got, want));
    }
    std::string str() const {
        std::ostringstream out;
        out.precision(3);
        out << "lo=" << std::fixed << lo << " hi=" << hi << std::scientific
            << " worst_rel_err=" << worst_rel_err;
        return out.str();
    }
};

// A wire held at exactly 8 GB/s. Both ends of every meter interval are
// completion timestamps, so each window measures the rate over a whole
// number of slices -- there is no partial slice to misattribute, and the
// EWMA of a constant is that constant. The residual is the poll group split
// across an interval boundary, and it is ~1e-6 even at the deepest polling.
TEST(DeviceSelectorTransmitAccuracyTest, SteadyRateIsRecovered) {
    // Long enough for the seed to have washed out of the EWMA at every one
    // of these depths: the deeper the polling the fewer samples a second of
    // traffic offers, so a shorter warm-up leaves the shallowest converged
    // and the deepest still on its way, which is a difference in how fast
    // the estimate moves and not in what it settles on.
    constexpr uint64_t kWarmUp = 8'000'000'000ull;
    constexpr uint64_t kChunk = 200'000'000ull;  // ~20 meter intervals each
    for (uint64_t poll_batch : {1ull, 16ull, 64ull}) {
        auto sel = shippedSelector();
        uint64_t at = kT0;
        runWire(*sel, 8.0, kMiB, poll_batch, kWarmUp, at);
        Spread spread;
        for (int chunk = 0; chunk < 10; ++chunk)
            spread.add(runWire(*sel, 8.0, kMiB, poll_batch, kChunk, at), 8e9);
        RecordProperty("poll_batch_" + std::to_string(poll_batch),
                       spread.str());
        EXPECT_LT(spread.worst_rel_err, 1e-12)
            << "poll_batch=" << poll_batch << " lo=" << spread.lo
            << " hi=" << spread.hi;
    }
}

// Slice size does not enter the error. 64 MiB slices leave barely one
// completion per interval, but since an interval can only begin and end on a
// completion, the window still holds whole slices: no phase error to average
// out. This is what makes the estimate independent of how the caller carved
// its transfer up.
TEST(DeviceSelectorTransmitAccuracyTest, SliceSizeDoesNotSkewTheEstimate) {
    auto sel = shippedSelector();  // 8 GB/s, an interval holds 76 MiB
    uint64_t at = kT0;
    runWire(*sel, 8.0, 64 * kMiB, 1, 4'000'000'000ull, at);
    Spread spread;
    for (int chunk = 0; chunk < 10; ++chunk)
        spread.add(runWire(*sel, 8.0, 64 * kMiB, 1, 400'000'000ull, at), 8e9);
    RecordProperty("estimate", spread.str());
    EXPECT_LT(spread.worst_rel_err, 1e-9)
        << "lo=" << spread.lo << " hi=" << spread.hi;
}

// The realistic case: the wire does not hold one rate. Alternating 5 ms
// bursts of 4 and 12 GB/s carry the same bytes per 10 ms as a steady 8 GB/s
// link, and each burst is short enough to fall inside a meter window. What
// the estimate has to recover is the mean, not the burst it last saw.
TEST(DeviceSelectorTransmitAccuracyTest, BurstyWireConvergesOnItsMean) {
    auto sel = shippedSelector();
    uint64_t at = kT0;
    Spread spread;
    for (int i = 0; i < 1200; ++i) {
        runWire(*sel, (i % 2) ? 12.0 : 4.0, kMiB, 8, 5'000'000ull, at);
        // Read it back on every burst boundary past the warm-up, so the
        // swing between the fast and the slow half is inside the bound and
        // not just the endpoint the run happens to stop on.
        if (i >= 400) spread.add(sel->getTransmitBandwidth(kDev), 8e9);
    }
    RecordProperty("estimate", spread.str());
    EXPECT_LT(spread.worst_rel_err, 0.015)
        << "lo=" << spread.lo << " hi=" << spread.hi;
}

// A link that renegotiates down to a quarter of its rate. alpha 0.9 closes
// 10% of the gap per SAMPLE, and ~39 samples are needed to get within 5%.
// What that costs in wall time is set by how often a sample can be taken,
// which is not the 10 ms interval but the poll rhythm: a sample only lands
// on a completion timestamp, so the effective interval here is the first
// poll pass at or after 10 ms. This measures the window in which the
// admission queue's drop predicate still believes the old, faster link.
TEST(DeviceSelectorTransmitAccuracyTest, FollowsALinkThatSlowsDown) {
    auto sel = shippedSelector();
    uint64_t at = kT0;
    // Polled four at a time, so one pass is a fifth of a meter interval and
    // the coarse-polling bias below stays out of the measurement.
    ASSERT_LT(relErr(runWire(*sel, 8.0, kMiB, 4, 2'000'000'000ull, at), 8e9),
              0.02);

    constexpr uint64_t kStep = 10'000'000ull;  // 10 ms of traffic per step
    const uint64_t slowdown_at = at;
    while (relErr(sel->getTransmitBandwidth(kDev), 2e9) > 0.05 &&
           at - slowdown_at < 5'000'000'000ull) {
        runWire(*sel, 2.0, kMiB, 4, kStep, at);
    }
    const uint64_t elapsed_ns = at - slowdown_at;
    RecordProperty("ms_to_within_5pct", std::to_string(elapsed_ns / 1'000'000));
    // Both ends matter: too slow and the drop predicate believes the old
    // link for longer, too fast and one odd interval could flip it.
    EXPECT_GT(elapsed_ns, 300'000'000ull) << "ms=" << elapsed_ns / 1'000'000;
    EXPECT_LT(elapsed_ns, 700'000'000ull) << "ms=" << elapsed_ns / 1'000'000;
}

// Nothing calls the meter while a NIC is idle: handleCompletion is its only
// caller. A workload that bursts and then waits therefore does not close an
// interval at the end of its burst -- it closes it at the NEXT burst, with
// the idle gap inside the elapsed time. Drives only completions, the way
// production does.
double runDutyCycle(DeviceSelector& sel, double bytes_per_ns, uint64_t slice,
                    uint64_t burst_slices, uint64_t period_ns, int bursts,
                    uint64_t start_ns = kT0) {
    const uint64_t per_slice_ns = static_cast<uint64_t>(slice / bytes_per_ns);
    uint64_t at = start_ns;
    for (int b = 0; b < bursts; ++b) {
        for (uint64_t i = 0; i < burst_slices; ++i)
            sel.notePosted(kDev, slice, at);
        for (uint64_t i = 0; i < burst_slices; ++i) {
            // One completion per poll pass here: the bursts are what is
            // being measured, not the polling.
            const uint64_t done_ts = at + (i + 1) * per_slice_ns;
            sel.notePostEnded(kDev, slice, done_ts);
            sel.noteCompleted(kDev, slice);
            sel.maybeSampleTransmit(kDev, done_ts);
        }
        at += period_ns;
    }
    return sel.getTransmitBandwidth(kDev);
}

// A NIC that bursts and then waits. The gap between bursts is not the link
// being slow, it is the link having nothing to do, so only the time the NIC
// actually held work may be charged for the bytes it moved. Metered against
// elapsed time this read 1.68 GB/s -- the period average, 4.8x below the
// wire -- and the admission layer's absolute drop threshold was wrong by
// that factor.
TEST(DeviceSelectorTransmitAccuracyTest, IdleTimeIsNotChargedToTheLink) {
    auto sel = shippedSelector();
    // 16 MiB at 8 GB/s == 2 ms of wire time every 10 ms: 21% duty cycle.
    uint64_t at = kT0;
    runDutyCycle(*sel, 8.0, kMiB, 16, 10'000'000ull, 400, at);
    at += 400ull * 10'000'000ull;
    Spread spread;
    for (int chunk = 0; chunk < 20; ++chunk) {
        spread.add(runDutyCycle(*sel, 8.0, kMiB, 16, 10'000'000ull, 20, at),
                   8e9);
        at += 20ull * 10'000'000ull;
    }
    RecordProperty("estimate", spread.str());
    EXPECT_LT(spread.worst_rel_err, 1e-6)
        << "lo=" << spread.lo << " hi=" << spread.hi;
}

// Widen the gap past transmit_meter_max_interval_ns and the same workload is
// rejected instead: the estimate keeps its seed. The protection is a cliff at
// 50 ms, not a correction.
TEST(DeviceSelectorTransmitAccuracyTest, DutyCyclePastTheCutOffIsRejected) {
    auto sel = shippedSelector();
    const double got = runDutyCycle(*sel, 8.0, kMiB, 16, 60'000'000ull, 100);
    RecordProperty("estimate", std::to_string(got));
    EXPECT_DOUBLE_EQ(got, 3.125e9);  // the 25 Gbps seed, never updated
}

// The worst case for the poll cadence: sixteen 1 MiB slices at 2 GB/s put one
// pass at 8.4 ms against a 10 ms meter interval, so almost every interval
// ends on the pass that fills it. Taking the sample at the end of a pass
// rather than partway through is what makes this exact -- sampled from inside
// a pass, the interval that closed lost the rest of the pass's bytes and the
// next one got them for free, and since the estimate averages per-interval
// rates over intervals of unequal length the two did not cancel: this read 8%
// high and a 4x slowdown never converged at all.
TEST(DeviceSelectorTransmitAccuracyTest, CoarsePollingDoesNotSkewTheEstimate) {
    auto sel = shippedSelector();
    uint64_t at = kT0;
    runWire(*sel, 2.0, kMiB, 16, 4'000'000'000ull, at);
    Spread spread;
    for (int chunk = 0; chunk < 10; ++chunk)
        spread.add(runWire(*sel, 2.0, kMiB, 16, 400'000'000ull, at), 2e9);
    RecordProperty("estimate", spread.str());
    EXPECT_LT(spread.worst_rel_err, 1e-9)
        << "lo=" << spread.lo << " hi=" << spread.hi;
}

// The clamp is the hard ceiling on what can be measured at all: a link whose
// real rate is more than ewma_max_multiplier times the seed reads as the
// clamp, not as itself. Reachable when the seed came from
// default_bandwidth_gbps because the port speed could not be read.
TEST(DeviceSelectorTransmitAccuracyTest, RateAboveTheClampIsNotMeasurable) {
    auto sel = shippedSelector();  // seed 3.125 GB/s, ceiling 31.25 GB/s
    uint64_t at = kT0;
    for (int chunk = 0; chunk < 10; ++chunk) {
        const double got = runWire(*sel, 50.0, kMiB, 16, 200'000'000ull, at);
        // Pinned at 10x, not the real 50 GB/s, and it stays pinned: more
        // intervals of the same evidence never lift it past the clamp.
        EXPECT_DOUBLE_EQ(got, 31.25e9) << "chunk=" << chunk;
    }
    RecordProperty("estimate", std::to_string(sel->getTransmitBandwidth(kDev)));
}

// A reset abandons the interval in progress without learning from it; the
// sample after it rebuilds both baselines from scratch, so the bytes and
// busy time it eventually divides come from the same window.
TEST(DeviceSelectorTransmitTest, ResetStartsTheNextIntervalFresh) {
    auto sel = makeMeteredSelector();
    sel->notePosted(kDev, 2 * kMiB, kT0);  // one busy stretch, two slices
    sel->maybeSampleTransmit(kDev, kT0);   // baseline
    sel->noteCompleted(kDev, kMiB);
    sel->resetTransmitMeter(kDev);
    // Rebuilds the baselines (1 MiB done, 1 MiB ns busy) and learns nothing.
    sel->maybeSampleTransmit(kDev, kT0 + kMiB);
    EXPECT_DOUBLE_EQ(sel->getTransmitBandwidth(kDev), 3.125e9);
    sel->noteCompleted(kDev, kMiB);
    sel->notePostEnded(kDev, 2 * kMiB, kT0 + 2 * kMiB);
    // 1 MiB over the 1 MiB ns of busy time since those baselines: 1 GB/s.
    sel->maybeSampleTransmit(kDev, kT0 + 2 * kMiB);
    EXPECT_NEAR(sel->getTransmitBandwidth(kDev), 1e9, 1.0);
}

// The padding after each hot counter is only worth anything if the struct
// itself starts on a cache line; otherwise two lanes' posts and completions
// contend on the same line.
TEST(DeviceSelectorLayoutTest, HotCountersSitOnTheirOwnCacheLines) {
    DeviceSelector::DeviceInfo info;
    const auto base = reinterpret_cast<uintptr_t>(&info);
    auto line_of = [&](const void* p) {
        return (reinterpret_cast<uintptr_t>(p) - base) / 64;
    };
    EXPECT_EQ(alignof(DeviceSelector::DeviceInfo), 64u);
    EXPECT_EQ(sizeof(DeviceSelector::DeviceInfo) % 64, 0u);
    const std::vector<const void*> hot = {
        &info.inflight_bytes, &info.ewma_bandwidth_bps, &info.ewma_transmit_bps,
        &info.total_bytes,    &info.posted_bytes,       &info.completed_bytes,
        &info.meter_ts};
    for (size_t i = 0; i < hot.size(); ++i) {
        EXPECT_EQ((reinterpret_cast<uintptr_t>(hot[i]) - base) % 64, 0u)
            << "field " << i;
        for (size_t j = i + 1; j < hot.size(); ++j)
            EXPECT_NE(line_of(hot[i]), line_of(hot[j])) << i << " vs " << j;
    }
}

// The NIC's posted backlog is what a slice about to be posted waits behind:
// charged when a WR reaches the hardware, returned when it leaves.
TEST(DeviceSelectorTransmitTest, PostedBytesTrackTheHardwareBacklog) {
    auto sel = makeSelector();
    EXPECT_EQ(sel->getPostedBytes(kDev), 0u);
    sel->notePosted(kDev, kMiB, kT0);
    sel->notePosted(kDev, kMiB, kT0);
    EXPECT_EQ(sel->getPostedBytes(kDev), 2 * kMiB);
    sel->notePostEnded(kDev, kMiB, kT0);
    EXPECT_EQ(sel->getPostedBytes(kDev), kMiB);
    sel->notePostEnded(kDev, kMiB, kT0);
    EXPECT_EQ(sel->getPostedBytes(kDev), 0u);
    EXPECT_EQ(sel->getPostedBytes(7), 0u);  // unknown device
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
