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

// The routing and aggregation contract of warmupSegment(): the local segment
// is a no-op, every installed transport is offered the target, NotImplemented
// means nothing-to-warm rather than failure, and a transport that genuinely
// fails is reported even when another succeeded.
//
// These use fake transports. Enumerating RDMA endpoint pairs needs a NIC and a
// peer, so that part is not exercised here; the rule that turns a pass over
// those pairs into a result is split out as RdmaTransport::warmupPassResult()
// and covered at the bottom of this file.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/rdma/rdma_transport.h"

namespace mooncake {
namespace tent {
namespace {

// No real transport installed: the fakes below are swapped into the slots, so
// nothing else can answer a warmup and change what these tests observe.
std::shared_ptr<Config> makeConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    config->set("transports/tcp/enable", true);
    config->set("transports/shm/enable", false);
    config->set("transports/rdma/enable", false);
    config->set("transports/io_uring/enable", false);
    config->set("transports/nvlink/enable", false);
    config->set("transports/mnnvl/enable", false);
    config->set("transports/gds/enable", false);
    config->set("transports/ascend_direct/enable", false);
    config->set("transports/sunrise_link/enable", false);
    config->set("transports/mpcomm/enable", false);
    config->set("transports/tpu/enable", false);
    config->set("transports/ub/enable", false);
    return config;
}

// A transport whose only interesting behaviour is what warmupSegment returns.
class WarmupProbeTransport : public Transport {
   public:
    explicit WarmupProbeTransport(Status result) : result_(result) {
        caps.dram_to_dram = true;
    }

    int warmup_calls = 0;
    SegmentID last_target = 0;

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        return Status::OK();
    }

    Status warmupSegment(SegmentID target_id) override {
        ++warmup_calls;
        last_target = target_id;
        return result_;
    }

    const char* getName() const override { return "<warmup-probe>"; }

   private:
    Status result_;
};

std::shared_ptr<WarmupProbeTransport> install(TransferEngineImpl& engine,
                                              TransportType slot,
                                              Status result) {
    auto probe = std::make_shared<WarmupProbeTransport>(result);
    std::string seg_name = engine.getSegmentName();
    EXPECT_TRUE(probe->install(seg_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(slot, probe);
    return probe;
}

// A segment id the engine has never opened stands in for "some remote target":
// these tests only care about routing, and no fake looks the id up.
constexpr SegmentID kRemote = 42;

// The local segment has no connection state to establish, so warming it must
// succeed without reaching a transport at all.
TEST(WarmupSegmentTest, LocalSegmentIsANoOp) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());
    auto probe = install(engine, RDMA, Status::OK());

    EXPECT_TRUE(engine.warmupSegment(LOCAL_SEGMENT_ID).ok());
    EXPECT_EQ(probe->warmup_calls, 0);
}

// Every installed transport is offered the target - the data path may route a
// later request over any of them, so stopping at the first would leave the
// others cold.
TEST(WarmupSegmentTest, EveryInstalledTransportIsOffered) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());
    auto first = install(engine, RDMA, Status::OK());
    auto second = install(engine, TCP, Status::OK());

    EXPECT_TRUE(engine.warmupSegment(kRemote).ok());
    EXPECT_EQ(first->warmup_calls, 1);
    EXPECT_EQ(second->warmup_calls, 1);
    EXPECT_EQ(first->last_target, kRemote);
    EXPECT_EQ(second->last_target, kRemote);
}

// NotImplemented is how a transport says it does not warm, and also how it
// says it has nothing to warm towards this target (a peer that advertises no
// device of its kind). Neither is a failure.
TEST(WarmupSegmentTest, NotImplementedIsNotAFailure) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());
    auto probe =
        install(engine, RDMA, Status::NotImplemented("nothing to warm here"));

    auto status = engine.warmupSegment(kRemote);
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(probe->warmup_calls, 1);
}

// A transport that genuinely failed to warm must be reported even when
// another one succeeded. The caller asked for the setup cost to be paid up
// front; silently returning OK would hand back a target whose first real
// transfer still stalls, with only a log line to explain it.
TEST(WarmupSegmentTest, AFailureIsReportedEvenIfAnotherTransportSucceeded) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());
    auto failing =
        install(engine, RDMA, Status::InternalError("endpoints unreachable"));
    auto succeeding = install(engine, TCP, Status::OK());

    auto status = engine.warmupSegment(kRemote);
    EXPECT_FALSE(status.ok());
    // The succeeding transport was still offered the target: one failure does
    // not abandon the rest.
    EXPECT_EQ(succeeding->warmup_calls, 1);
    EXPECT_EQ(failing->warmup_calls, 1);
}

// With more than one failure the first is what the caller sees.
TEST(WarmupSegmentTest, FirstFailureIsReported) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());
    install(engine, RDMA, Status::InternalError("first failure"));
    install(engine, TCP, Status::InternalError("second failure"));

    auto status = engine.warmupSegment(kRemote);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.ToString().find("first failure"), std::string::npos)
        << status.ToString();
}

// The per-pair rule inside RDMA, which has to match the per-transport rule
// above: a pass that leaves any pair cold is a failure, because the data path
// may route a later slice over any of them.
//
// warmupPassResult() takes the counts the enumeration produces, so these cases
// are "one pair already ready and one failing", "both failing", and so on,
// without needing a NIC to produce them.

// Every pair came up: nothing is left cold, so the caller got what it paid
// for.
TEST(RdmaWarmupPassResultTest, AllPairsReadyIsSuccess) {
    EXPECT_TRUE(RdmaTransport::warmupPassResult(3, 3, Status::OK()).ok());
}

// One pair ready, the rest failed. This used to return OK, because only an
// all-failed pass was reported - which handed back a target whose first
// transfer could still stall on one of the cold pairs.
TEST(RdmaWarmupPassResultTest, PartialWarmupIsNotReportedAsComplete) {
    auto status = RdmaTransport::warmupPassResult(
        3, 1, Status::InternalError("endpoint 2 unreachable"));
    ASSERT_FALSE(status.ok()) << "a partly warmed target must not report OK";
    EXPECT_NE(status.ToString().find("endpoint 2 unreachable"),
              std::string::npos)
        << status.ToString();
}

// One cold pair out of many is still a cold pair.
TEST(RdmaWarmupPassResultTest, ASingleColdPairIsReported) {
    auto status = RdmaTransport::warmupPassResult(
        8, 7, Status::InternalError("last pair refused"));
    EXPECT_FALSE(status.ok()) << status.ToString();
}

// With several failures the first is what the caller sees, matching how the
// engine aggregates transports.
TEST(RdmaWarmupPassResultTest, FirstPairFailureIsReported) {
    auto status = RdmaTransport::warmupPassResult(
        4, 0, Status::InternalError("first pair failure"));
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.ToString().find("first pair failure"), std::string::npos)
        << status.ToString();
}

// No enabled local context means there was no pair to warm at all, which is
// the same "nothing to warm" answer a peer advertising no RDMA device gets -
// not a failure.
TEST(RdmaWarmupPassResultTest, NothingAttemptedIsNotAFailure) {
    auto status = RdmaTransport::warmupPassResult(0, 0, Status::OK());
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
}

// Defensive: a cold pair that somehow recorded no error is still a cold pair,
// so the result must not be OK just because the error slot is empty.
TEST(RdmaWarmupPassResultTest, ColdPairWithoutARecordedErrorStillFails) {
    EXPECT_FALSE(RdmaTransport::warmupPassResult(2, 1, Status::OK()).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
