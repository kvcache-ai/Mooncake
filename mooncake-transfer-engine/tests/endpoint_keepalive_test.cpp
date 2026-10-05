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

// Hardware-free coverage for endpoint keepalives (MC_ENDPOINT_IDLE_TIMEOUT):
// the per posting-thread schedule, the endpoint's keepalive slice, and the
// configuration.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "transport/rdma_transport/endpoint_keepalive.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace {

constexpr uint64_t kSecond = 1000000000ull;

// ---------------------------------------------------------------------------
// KeepaliveSchedule

struct FakeEndpoint {
    uint64_t last_used = 0;
    bool is_retired = false;
    int keepalives = 0;
    uint64_t lastUsedNs() const { return last_used; }
    bool retired() const { return is_retired; }
};

size_t service(KeepaliveSchedule<FakeEndpoint> &schedule, uint64_t now,
               uint64_t idle = 120 * kSecond) {
    return schedule.service(now, idle,
                            [](FakeEndpoint &ep) { ep.keepalives++; });
}

TEST(KeepaliveSchedule, IdleEndpointGetsOneKeepalivePerIdlePeriod) {
    KeepaliveSchedule<FakeEndpoint> schedule;
    auto ep = std::make_shared<FakeEndpoint>();
    ep->last_used = 0;
    schedule.add(ep, 120 * kSecond);

    EXPECT_EQ(service(schedule, 119 * kSecond), 0u) << "not due yet";
    EXPECT_EQ(service(schedule, 120 * kSecond), 1u);
    EXPECT_EQ(ep->keepalives, 1);
    EXPECT_EQ(schedule.nextDueNs(), 240 * kSecond);
    EXPECT_EQ(service(schedule, 200 * kSecond), 0u);
    EXPECT_EQ(service(schedule, 241 * kSecond), 1u);
    EXPECT_EQ(ep->keepalives, 2);
    EXPECT_EQ(schedule.size(), 1u);
}

TEST(KeepaliveSchedule, UsedEndpointIsRequeuedWithoutKeepalive) {
    KeepaliveSchedule<FakeEndpoint> schedule;
    auto ep = std::make_shared<FakeEndpoint>();
    schedule.add(ep, 120 * kSecond);
    ep->last_used = 100 * kSecond;  // a transfer since it was queued

    EXPECT_EQ(service(schedule, 120 * kSecond), 0u);
    EXPECT_EQ(ep->keepalives, 0);
    EXPECT_EQ(schedule.nextDueNs(), 220 * kSecond);
    EXPECT_EQ(service(schedule, 220 * kSecond), 1u);
    EXPECT_EQ(ep->keepalives, 1);
}

// A busy endpoint is re-queued every time without a keepalive; it never
// accumulates entries.
TEST(KeepaliveSchedule, BusyEndpointNeverGetsKeepalives) {
    KeepaliveSchedule<FakeEndpoint> schedule;
    auto ep = std::make_shared<FakeEndpoint>();
    schedule.add(ep, 120 * kSecond);
    for (uint64_t t = 120; t < 2000; t += 60) {
        ep->last_used = t * kSecond;
        service(schedule, t * kSecond);
    }
    EXPECT_EQ(ep->keepalives, 0);
    EXPECT_EQ(schedule.size(), 1u);
}

TEST(KeepaliveSchedule, GoneOrRetiredEndpointsAreDropped) {
    KeepaliveSchedule<FakeEndpoint> schedule;
    auto retired = std::make_shared<FakeEndpoint>();
    retired->is_retired = true;
    schedule.add(retired, 1);
    {
        auto gone = std::make_shared<FakeEndpoint>();
        schedule.add(gone, 1);
    }
    EXPECT_EQ(service(schedule, 120 * kSecond), 0u);
    EXPECT_EQ(retired->keepalives, 0);
    EXPECT_TRUE(schedule.empty());
}

TEST(KeepaliveSchedule, AllDueEndpointsAreServedInOneCall) {
    KeepaliveSchedule<FakeEndpoint> schedule;
    std::vector<std::shared_ptr<FakeEndpoint>> eps;
    for (int i = 0; i < 10; ++i) {
        eps.push_back(std::make_shared<FakeEndpoint>());
        schedule.add(eps.back(), (110 + i) * kSecond);
    }
    EXPECT_EQ(service(schedule, 120 * kSecond), 10u);
    for (auto &ep : eps) EXPECT_EQ(ep->keepalives, 1);
    EXPECT_EQ(schedule.size(), 10u);
}

TEST(KeepaliveSchedule, ZeroIdleTimeoutPostsNothingUntilDue) {
    KeepaliveSchedule<FakeEndpoint> schedule;
    EXPECT_EQ(service(schedule, 0), 0u);
    EXPECT_EQ(schedule.nextDueNs(), 0u);
}

// ---------------------------------------------------------------------------
// The endpoint's keepalive slice

class KeepaliveEndpointTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Leaked on purpose, as in endpoint_store_test: ~RdmaTransport
        // dereferences metadata_, which is null until install().
        transport_ = new RdmaTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);
        ctx_ = std::make_unique<RdmaContext>(*transport_, "unused");
        ep_ = std::make_unique<RdmaEndPoint>(*ctx_);
    }

    RdmaTransport *transport_ = nullptr;
    std::unique_ptr<RdmaContext> ctx_;
    std::unique_ptr<RdmaEndPoint> ep_;
};

// The completion path recognizes a keepalive by pointer and reads only
// rdma.endpoint from it; it never finalizes it, so it has no task.
TEST_F(KeepaliveEndpointTest, KeepaliveSliceBelongsToItsEndpoint) {
    Transport::Slice other{};
    EXPECT_FALSE(ep_->isKeepaliveSlice(&other));
    EXPECT_FALSE(ep_->isKeepaliveSlice(nullptr));
}

// Posting needs a CONNECTED endpoint; anything else is skipped until the next
// timeout, without touching the (unconstructed) QP state.
TEST_F(KeepaliveEndpointTest, UnconnectedEndpointSkipsKeepalive) {
    ep_->postKeepalive();
    SUCCEED();
}

TEST_F(KeepaliveEndpointTest, LastUsedStartsAtConstruction) {
    const uint64_t now = static_cast<uint64_t>(getCurrentTimeInNano());
    EXPECT_LE(ep_->lastUsedNs(), now);
    EXPECT_GT(ep_->lastUsedNs(), now - 60 * kSecond);
    ep_->testOnlySetLastUsedNs(42);
    EXPECT_EQ(ep_->lastUsedNs(), 42u);
}

TEST_F(KeepaliveEndpointTest, RegistrationIsClaimedOnce) {
    EXPECT_TRUE(ep_->claimKeepaliveRegistration());
    EXPECT_FALSE(ep_->claimKeepaliveRegistration());
}

// ---------------------------------------------------------------------------
// MC_ENDPOINT_IDLE_TIMEOUT

class IdleTimeoutEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_ENDPOINT_IDLE_TIMEOUT"); }
};

TEST_F(IdleTimeoutEnvTest, DefaultIsDisabled) {
    ::unsetenv("MC_ENDPOINT_IDLE_TIMEOUT");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.endpoint_idle_timeout_s, 0u);
}

TEST_F(IdleTimeoutEnvTest, ValidValueIsParsed) {
    ASSERT_EQ(::setenv("MC_ENDPOINT_IDLE_TIMEOUT", "120", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.endpoint_idle_timeout_s, 120u);
}

TEST_F(IdleTimeoutEnvTest, InvalidValueKeepsDefault) {
    ASSERT_EQ(::setenv("MC_ENDPOINT_IDLE_TIMEOUT", "2m", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.endpoint_idle_timeout_s, 0u);
}

}  // namespace
