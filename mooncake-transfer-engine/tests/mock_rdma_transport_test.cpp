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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <thread>
#include <chrono>

#include "multi_transport.h"
#include "transport/rdma_transport/mock_rdma_transport.h"
#include "transport/transport.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "topology.h"
#include "config.h"

using namespace mooncake;

namespace mooncake {

class MockRdmaTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MockRdmaTransportTest");
        FLAGS_logtostderr = 1;

        // Enable mock RDMA mode
        setenv("MC_RDMA_MOCK", "1", 1);

        // Create minimal metadata and topology
        // Note: Full integration requires TransferMetadata with a running
        // metadata server, so we test the basic API surface here.
    }

    void TearDown() override {
        unsetenv("MC_RDMA_MOCK");
        unsetenv("MC_RDMA_MOCK_FAIL_RATE");
        unsetenv("MC_RDMA_MOCK_FAIL_AFTER");
        google::ShutdownGoogleLogging();
    }
};

// Test: isMockRdmaEnabled reads MC_RDMA_MOCK correctly
TEST_F(MockRdmaTransportTest, MockRdmaEnabledCheck) {
    setenv("MC_RDMA_MOCK", "1", 1);
    EXPECT_TRUE(isMockRdmaEnabled());

    setenv("MC_RDMA_MOCK", "0", 1);
    EXPECT_FALSE(isMockRdmaEnabled());

    unsetenv("MC_RDMA_MOCK");
    EXPECT_FALSE(isMockRdmaEnabled());
}

// Test: MockRdmaTransport constructor loads fault injection params
TEST_F(MockRdmaTransportTest, FaultInjectionParams) {
    setenv("MC_RDMA_MOCK_FAIL_RATE", "0.5", 1);
    setenv("MC_RDMA_MOCK_FAIL_AFTER", "3", 1);

    MockRdmaTransport mock;
    // Constructor should parse env vars without crash
    // (full verification requires submitTransfer which needs metadata)

    unsetenv("MC_RDMA_MOCK_FAIL_RATE");
    unsetenv("MC_RDMA_MOCK_FAIL_AFTER");
}

// Test: shouldInjectFailure with fail_after counter
TEST_F(MockRdmaTransportTest, ShouldInjectFailureCountBased) {
    setenv("MC_RDMA_MOCK_FAIL_AFTER", "2", 1);
    unsetenv("MC_RDMA_MOCK_FAIL_RATE");

    MockRdmaTransport mock;
    // First two should succeed, third should fail
    EXPECT_FALSE(mock.shouldInjectFailure());  // count 0
    EXPECT_FALSE(mock.shouldInjectFailure());  // count 1
    EXPECT_TRUE(mock.shouldInjectFailure());   // count 2 >= fail_after
    EXPECT_TRUE(mock.shouldInjectFailure());   // count 3 >= fail_after

    unsetenv("MC_RDMA_MOCK_FAIL_AFTER");
}

// Test: shouldInjectFailure with fail_rate
TEST_F(MockRdmaTransportTest, ShouldInjectFailureRateBased) {
    setenv("MC_RDMA_MOCK_FAIL_RATE", "1.0", 1);  // 100% failure
    unsetenv("MC_RDMA_MOCK_FAIL_AFTER");

    MockRdmaTransport mock;
    // With 100% rate, should always fail (statistically)
    bool any_failure = false;
    for (int i = 0; i < 10; i++) {
        if (mock.shouldInjectFailure()) {
            any_failure = true;
            break;
        }
    }
    EXPECT_TRUE(any_failure);

    unsetenv("MC_RDMA_MOCK_FAIL_RATE");
}

// Test: MultiTransport::isTransportHealthy defaults to healthy
TEST_F(MockRdmaTransportTest, TransportHealthDefault) {
    std::string server_name = "127.0.0.1:12345";
    auto meta = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    MultiTransport mt(meta, server_name);

    // Without any tracking, all transports should be healthy
    EXPECT_TRUE(mt.isTransportHealthy("rdma"));
    EXPECT_TRUE(mt.isTransportHealthy("tcp"));
}

// Test: markTransportUnhealthy / markTransportHealthy cycle
TEST_F(MockRdmaTransportTest, TransportHealthMarkUnhealthy) {
    std::string server_name = "127.0.0.1:12345";
    auto meta = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    MultiTransport mt(meta, server_name);

    mt.markTransportUnhealthy("rdma", 10);
    EXPECT_FALSE(mt.isTransportHealthy("rdma"));

    mt.markTransportHealthy("rdma");
    EXPECT_TRUE(mt.isTransportHealthy("rdma"));
}

// Test: TransportHealth struct operations
TEST_F(MockRdmaTransportTest, TransportHealthStruct) {
    TransportHealth health;
    EXPECT_TRUE(health.healthy);
    EXPECT_EQ(health.consecutive_failures, 0);

    health.markUnhealthy(30);
    EXPECT_FALSE(health.healthy);
    EXPECT_TRUE(health.isCoolingDown());
    EXPECT_EQ(health.consecutive_failures, 1);

    health.markHealthy();
    EXPECT_TRUE(health.healthy);
    EXPECT_EQ(health.consecutive_failures, 0);
    EXPECT_FALSE(health.isCoolingDown());
}

// Test: TransportHealth cooldown expiration
TEST_F(MockRdmaTransportTest, TransportHealthCooldown) {
    TransportHealth health;
    health.markUnhealthy(1);  // 1 second cooldown
    EXPECT_TRUE(health.isCoolingDown());

    // Wait for cooldown to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(health.isCoolingDown());
}

}  // namespace mooncake
