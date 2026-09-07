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

// The admission queue's deadline drop and the DeviceSelector's transmit
// estimate form a loop: the drop decides what is sent, what is sent is all
// the meter can learn from, and the estimate is what the next drop is judged
// against. These tests close that loop with the real queue and the real
// meter over a queue-depth-bound link model -- a NIC that reaches line rate
// only with enough work in flight -- and ask whether the loop returns to
// saturation after the estimate has been depressed, and which parameters
// that depends on.

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tent/runtime/admission_queue.h"
#include "tent/runtime/topology.h"
#include "tent/transport/rdma/quota.h"

namespace mooncake {
namespace tent {
namespace {

constexpr int kDev = 0;
constexpr double kLineRateBps = 2e9;  // 16 Gbps: 2 bytes per ns
constexpr int kSaturatingOwners = 8;  // in flight to reach line rate
constexpr size_t kOwnerBytes = 2'000'000;
constexpr uint64_t kNow = 1'000'000'000;
constexpr uint64_t kWindowNs = 10'000'000;  // 10 ms deadline window

QueueOwnerInput eligibleOwner(size_t task_id) {
    QueueOwnerInput owner;
    owner.owner_task_id = task_id;
    owner.request.opcode = Request::WRITE;
    owner.request.target_id = 1;
    owner.request.length = kOwnerBytes;
    owner.request.deadline_ns = kNow + kWindowNs;
    owner.degradation_eligible = true;
    return owner;
}

// One RDMA NIC whose transmit meter samples whenever asked and follows each
// sample with `alpha` weight on the old value, seeded at line rate.
std::unique_ptr<DeviceSelector> makeMeteredNic(double alpha) {
    auto topo = std::make_shared<Topology>();
    Topology::NicEntry nic;
    nic.name = "mlx5_0";
    nic.type = Topology::NIC_RDMA;
    nic.numa_node = 0;
    topo->nic_list_.push_back(nic);
    auto sel = std::make_unique<DeviceSelector>();
    EXPECT_TRUE(sel->loadTopology(topo).ok());
    DeviceSelector::SchedulingParams params;
    params.transmit_bandwidth_learning_rate = alpha;
    params.transmit_meter_interval_ns = 0;
    sel->setSchedulingParams(params);
    // 16 Gbps, above min_bandwidth_gbps so it seeds rather than falling
    // back to the 400 Gbps default.
    EXPECT_TRUE(sel->setDeviceBandwidth(kDev, 16.0).ok());  // 2 GB/s
    return sel;
}

// The closed loop. Each round offers `kSaturatingOwners` equal, eligible
// owners; whatever the queue dispatches is posted to the NIC together, served
// at the rate that many in flight achieve (line rate times min(n, N) / N,
// capped by an external `served_cap` when another tenant holds the link),
// completed under one poll timestamp, and metered. The queue's bandwidth
// provider reads the meter, so the next round is judged by what this one
// taught.
class FeedbackLoop {
   public:
    FeedbackLoop(double theta_local, double alpha)
        : nic_(makeMeteredNic(alpha)), clock_(kNow) {
        QueueLimits limits{16, 1 << 30, 0, 0};
        limits.deadline_aware = true;
        limits.mlu_local_threshold = theta_local;
        queue_ = std::make_unique<LocalTransferAdmissionQueue>(limits);
        DegradationHooks hooks;
        hooks.on_local_decode_suggested = [this](const Request&) {
            ++drops_signalled_;
        };
        queue_->setDegradationPolicy(
            [this] { return nic_->getAggregateTransmitBandwidth(); }, hooks,
            [] { return kNow; });
    }

    struct Round {
        int admitted;
        int dropped;
        double estimate_after;
    };

    // `served_cap` < N models the link being shared with traffic the queue
    // does not see: only that many of the admitted owners move at once.
    Round run(int served_cap = kSaturatingOwners) {
        const uint64_t token = ++token_;
        std::vector<QueueOwnerInput> inputs;
        for (int i = 0; i < kSaturatingOwners; ++i)
            inputs.push_back(eligibleOwner(i));
        QueueSubmit submit;
        submit.batch_token = token;
        submit.batch_slots_left = kSaturatingOwners;
        submit.owners = std::move(inputs);
        std::vector<QueueOwnerId> ids;
        EXPECT_EQ(queue_->tryAdmit(submit, ids).code(), Status::Code::kOk);

        std::vector<QueueOwnerId> dropped;
        auto picked =
            queue_->pickForDispatch(kSaturatingOwners, 1 << 30, &dropped);
        const int n = static_cast<int>(picked.size());
        if (n > 0) serve(n, std::min(n, served_cap));
        for (auto id : picked)
            EXPECT_EQ(
                queue_->complete(id, TransferStatusEnum::COMPLETED).code(),
                Status::Code::kOk);
        EXPECT_EQ(queue_->retireBatch(token).code(), Status::Code::kOk);
        return {n, static_cast<int>(dropped.size()),
                nic_->getAggregateTransmitBandwidth()};
    }

    double estimate() const { return nic_->getAggregateTransmitBandwidth(); }
    int dropsSignalled() const { return drops_signalled_; }

   private:
    // Post `n` owners at once, let `concurrent` of them share the wire, and
    // reap everything in one poll pass -- the shape the RDMA workers give the
    // meter. Busy time is the span; bytes are n * kOwnerBytes.
    void serve(int n, int concurrent) {
        const double achieved = kLineRateBps *
                                std::min(concurrent, kSaturatingOwners) /
                                kSaturatingOwners;
        const uint64_t span_ns = static_cast<uint64_t>(
            static_cast<double>(n) * kOwnerBytes / achieved * 1e9);
        const uint64_t t0 = clock_;
        for (int i = 0; i < n; ++i) nic_->notePosted(kDev, kOwnerBytes, t0);
        nic_->maybeSampleTransmit(kDev, t0);
        const uint64_t poll_ts = t0 + span_ns;
        for (int i = 0; i < n; ++i) {
            nic_->notePostEnded(kDev, kOwnerBytes, poll_ts);
            nic_->noteCompleted(kDev, kOwnerBytes);
        }
        nic_->maybeSampleTransmit(kDev, poll_ts);
        clock_ = poll_ts + 1'000'000;  // 1 ms idle before the next round
    }

    std::unique_ptr<DeviceSelector> nic_;
    std::unique_ptr<LocalTransferAdmissionQueue> queue_;
    uint64_t clock_;
    uint64_t token_ = 0;
    int drops_signalled_ = 0;
};

double achievedBy(int concurrent) {
    return kLineRateBps * std::min(concurrent, kSaturatingOwners) /
           kSaturatingOwners;
}

// ---- step 2: the real meter behaves as the model did --------------------

// From the line-rate seed the meter measures line rate every round and the
// queue never drops.
TEST(AdmissionFeedbackTest, CleanStartStaysSaturated) {
    FeedbackLoop loop(/*theta=*/0.95, /*alpha=*/0.9);
    for (int round = 1; round <= 30; ++round) {
        auto r = loop.run();
        ASSERT_EQ(r.admitted, kSaturatingOwners) << "round " << round;
        ASSERT_EQ(r.dropped, 0) << "round " << round;
        ASSERT_NEAR(r.estimate_after, kLineRateBps, 1.0) << "round " << round;
    }
    EXPECT_EQ(loop.dropsSignalled(), 0);
}

// Another tenant holds half the link for a while: only 4 of the 8 admitted
// owners move at once, the meter learns 1 GB/s. Then the tenant leaves.
// The queue now admits 4 (the 5th scores MLU 1.0 against 1 GB/s), those 4
// achieve exactly 1 GB/s, and the meter has nothing to learn from that
// would say otherwise: the loop stays on the plateau for good.
TEST(AdmissionFeedbackTest, ContentionLeavesAPlateauTheLoopNeverLeaves) {
    FeedbackLoop loop(/*theta=*/0.95, /*alpha=*/0.9);
    for (int round = 0; round < 60; ++round) loop.run(/*served_cap=*/4);
    ASSERT_NEAR(loop.estimate(), achievedBy(4), 0.02 * achievedBy(4));
    const int signalled_during_contention = loop.dropsSignalled();

    for (int round = 1; round <= 60; ++round) {
        auto r = loop.run();  // contention gone: the link could do 2 GB/s
        ASSERT_EQ(r.admitted, 4) << "round " << round;
        ASSERT_EQ(r.dropped, 4) << "round " << round;
    }
    EXPECT_NEAR(loop.estimate(), achievedBy(4), 0.02 * achievedBy(4));
    // Every drop after the contention lifted was a transfer the link could
    // have carried, and each one was signalled for local recompute.
    EXPECT_EQ(loop.dropsSignalled() - signalled_during_contention, 4 * 60);
}

// A shallower episode -- 6 of 8 moving -- leaves an estimate the survivors
// can outrun: 7 are admitted, they achieve 1.75 GB/s, the estimate climbs,
// and admission is back at 8.
TEST(AdmissionFeedbackTest, ShallowContentionRecovers) {
    FeedbackLoop loop(/*theta=*/0.95, /*alpha=*/0.9);
    for (int round = 0; round < 60; ++round) loop.run(/*served_cap=*/6);
    ASSERT_NEAR(loop.estimate(), achievedBy(6), 0.02 * achievedBy(6));

    int recovered_at = -1;
    for (int round = 1; round <= 60; ++round) {
        auto r = loop.run();
        ASSERT_GE(r.admitted, 7) << "round " << round;
        if (r.admitted == kSaturatingOwners && recovered_at < 0)
            recovered_at = round;
        if (recovered_at > 0)
            ASSERT_EQ(r.admitted, kSaturatingOwners) << "round " << round;
    }
    EXPECT_GT(recovered_at, 0);
    EXPECT_LE(recovered_at, 30);
    EXPECT_NEAR(loop.estimate(), kLineRateBps, 0.02 * kLineRateBps);
}

// ---- step 3: what the plateau depends on ---------------------------------

// Whether the loop recovers from the 4-of-8 plateau within 100 rounds. The
// contention episode lasts until the estimate has settled within 1% of what
// 4 achieve, however slowly this alpha gets there.
bool recoversFromHalfPlateau(double theta, double alpha) {
    FeedbackLoop loop(theta, alpha);
    const double plateau = achievedBy(4);
    for (int round = 0; round < 5000; ++round) {
        loop.run(/*served_cap=*/4);
        if (std::abs(loop.estimate() - plateau) <= 0.01 * plateau) break;
    }
    EXPECT_NEAR(loop.estimate(), plateau, 0.01 * plateau) << "alpha=" << alpha;
    for (int round = 0; round < 100; ++round)
        if (loop.run().admitted == kSaturatingOwners) return true;
    return false;
}

// The smoothing rate only sets how fast the estimate moves; it does not
// create or remove the fixed point. Every alpha stays stuck.
TEST(AdmissionFeedbackTest, PlateauDoesNotDependOnLearningRate) {
    for (double alpha : {0.0, 0.5, 0.9, 0.99}) {
        EXPECT_FALSE(recoversFromHalfPlateau(/*theta=*/0.95, alpha))
            << "alpha=" << alpha;
    }
}

// The threshold sets the basin. At an estimate of n/N line rate the
// (n+1)-th owner scores MLU = (n+1) * length / ((n/N) * line_rate * window)
// and is admitted only below theta; with N * length / (line_rate * window)
// = 16 MB / 20 MB = 0.8 here that is theta > 0.8 * (n+1) / n, so a 4-of-8
// estimate admits a
// 5th owner -- and climbs from there -- only when theta > 1.0. Loosening
// theta buys recovery by dropping less in the first place, which is not a
// fix for the loop but a different drop policy.
TEST(AdmissionFeedbackTest, PlateauDependsOnTheDropThreshold) {
    EXPECT_FALSE(recoversFromHalfPlateau(/*theta=*/0.8, /*alpha=*/0.9));
    EXPECT_FALSE(recoversFromHalfPlateau(/*theta=*/0.95, /*alpha=*/0.9));
    EXPECT_FALSE(recoversFromHalfPlateau(/*theta=*/0.99, /*alpha=*/0.9));
    EXPECT_TRUE(recoversFromHalfPlateau(/*theta=*/1.05, /*alpha=*/0.9));
    EXPECT_TRUE(recoversFromHalfPlateau(/*theta=*/1.2, /*alpha=*/0.9));
    EXPECT_TRUE(recoversFromHalfPlateau(/*theta=*/1.5, /*alpha=*/0.9));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
