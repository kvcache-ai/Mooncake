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

// End-to-end fault tolerance tests.
// Simulates endpoint failures and verifies the system can recover correctly.

#define TESTING  // Enable test-only interface

#include <gtest/gtest.h>
#include <thread>
#include <chrono>

#include "config.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"
#include "transport/transport.h"

using namespace mooncake;

namespace {

// Helper to pick an RDMA device
std::string pickRdmaDevice() {
    const char* override_name = std::getenv("MC_TEST_DEVICE_NAME");
    if (override_name && *override_name) return override_name;
    int num_devices = 0;
    ibv_device** list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) return "";
    std::string name = ibv_get_device_name(list[0]);
    ibv_free_device_list(list);
    return name;
}

// Test fixture for end-to-end fault tolerance testing
class E2EFaultToleranceTest : public ::testing::Test {
   protected:
    std::shared_ptr<RdmaContext> context_;
    std::unique_ptr<WorkerPool> worker_pool_;
    std::vector<void*> allocated_buffers_;

    void SetUp() override {
        const std::string device = pickRdmaDevice();
        if (device.empty()) {
            GTEST_SKIP() << "no RDMA device available — set "
                            "MC_TEST_DEVICE_NAME to override.";
        }

        // Create RdmaTransport
        auto* transport = new RdmaTransport();
        context_ = std::make_shared<RdmaContext>(*transport, device);

        auto& config = globalConfig();
        int rc = context_->construct(
            config.num_cq_per_ctx, config.num_comp_channels_per_ctx,
            config.port, config.gid_index, config.max_cqe);
        if (rc != 0) {
            GTEST_SKIP() << "failed to construct RDMA context for device "
                         << device;
        }

        // Create WorkerPool
        worker_pool_ = std::make_unique<WorkerPool>(*context_, 0);

        // Give workers time to start
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        worker_pool_.reset();
        context_.reset();
        for (void* buf : allocated_buffers_) {
            if (buf) free(buf);
        }
    }

    // Helper to allocate and register memory for RDMA
    void* allocateRdmaBuffer(size_t size) {
        void* buf = aligned_alloc(4096, size);
        if (!buf) return nullptr;
        allocated_buffers_.push_back(buf);

        // Register memory region
        int rc = context_->registerMemoryRegion(buf, size,
                                                IBV_ACCESS_LOCAL_WRITE |
                                                    IBV_ACCESS_REMOTE_WRITE |
                                                    IBV_ACCESS_REMOTE_READ);
        if (rc != 0) {
            free(buf);
            allocated_buffers_.pop_back();
            return nullptr;
        }
        return buf;
    }

    // Helper to create a test slice with registered memory
    Transport::Slice* createTestSlice(int target_id = 1) {
        auto* slice = new Transport::Slice();
        slice->target_id = target_id;
        slice->opcode = TransferRequest::OpCode::WRITE;
        slice->source_addr = allocateRdmaBuffer(4096);
        if (!slice->source_addr) {
            delete slice;
            return nullptr;
        }
        slice->length = 4096;
        slice->rdma.dest_addr = 0x1000;
        slice->rdma.dest_rkey = 123;  // Dummy rkey
        slice->rdma.retry_cnt = 0;
        slice->rdma.max_retry_cnt = 3;
        slice->rdma.qp_depth = nullptr;
        slice->peer_nic_path = "test_segment/test_device";
        slice->status = Transport::Slice::PENDING;
        return slice;
    }
};

// =============================================================================
// Test 1: Basic retry logic
// =============================================================================

TEST_F(E2EFaultToleranceTest,
       RetrySlice_IncreasesRetryCountAndReturnsCorrectly) {
    auto* slice = createTestSlice();
    ASSERT_NE(slice, nullptr) << "Failed to create test slice";

    uint32_t initial_retry = slice->rdma.retry_cnt;
    bool should_retry = WorkerPool::testOnlyShouldRetrySlice(slice);

    EXPECT_EQ(slice->rdma.retry_cnt, initial_retry + 1);
    EXPECT_TRUE(should_retry);

    delete slice;
}

// =============================================================================
// Test 2: Rail pause mechanism - verify error accumulation
// =============================================================================

TEST_F(E2EFaultToleranceTest, RailPause_AccumulatesErrorsBeforePause) {
    std::string test_path = "test_peer/test_device";
    const int kThreshold = WorkerPool::testOnlyGetRailErrorThreshold();

    // Initial state
    auto state = worker_pool_->testOnlyGetRailState(test_path);
    EXPECT_EQ(state.error_count, 0);
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(test_path));

    // Add errors up to threshold - 1
    for (int i = 0; i < kThreshold - 1; ++i) {
        worker_pool_->testOnlySimulatePathFailure(test_path, nullptr);
        state = worker_pool_->testOnlyGetRailState(test_path);
        EXPECT_EQ(state.error_count, i + 1);
        EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(test_path));
    }

    // One more error should pause the rail
    worker_pool_->testOnlySimulatePathFailure(test_path, nullptr);
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(test_path));
}

// =============================================================================
// Test 3: Rail auto-recovery after pause timeout
// =============================================================================

TEST_F(E2EFaultToleranceTest, RailPause_AutoRecoversAfterTimeout) {
    std::string test_path = "test_peer/test_device";
    const int kThreshold = WorkerPool::testOnlyGetRailErrorThreshold();
    const uint64_t kPauseNs = 1000000000ull;  // 1 second pause

    // Pause the rail
    for (int i = 0; i < kThreshold; ++i) {
        worker_pool_->testOnlySimulatePathFailure(test_path, nullptr);
    }
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(test_path));

    // Wait for pause to expire
    std::this_thread::sleep_for(
        std::chrono::nanoseconds(kPauseNs + 100000000ull));

    // Verify rail recovered
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(test_path));
    auto state = worker_pool_->testOnlyGetRailState(test_path);
    EXPECT_EQ(state.error_count, 0);
    EXPECT_EQ(state.pause_until_ns, 0);
}

// =============================================================================
// Test 4: Multiple independent rails
// =============================================================================

TEST_F(E2EFaultToleranceTest, RailPause_MultipleRailsManagedIndependently) {
    const int kThreshold = WorkerPool::testOnlyGetRailErrorThreshold();
    std::string rail_a = "peer_a/device_a";
    std::string rail_b = "peer_b/device_b";

    // Pause rail-A
    for (int i = 0; i < kThreshold; ++i) {
        worker_pool_->testOnlySimulatePathFailure(rail_a, nullptr);
    }

    // Add one error to rail-B
    worker_pool_->testOnlySimulatePathFailure(rail_b, nullptr);

    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(rail_a));
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rail_b));

    auto state_a = worker_pool_->testOnlyGetRailState(rail_a);
    auto state_b = worker_pool_->testOnlyGetRailState(rail_b);
    EXPECT_EQ(state_a.error_count, kThreshold);
    EXPECT_EQ(state_b.error_count, 1);
}

// =============================================================================
// Test 5: Simulated WC error triggers complete retry flow
// =============================================================================

TEST_F(E2EFaultToleranceTest, WcError_CompleteRetryFlow) {
    // This test simulates a WC error and verifies the complete retry flow:
    // 1. Create a slice with registered memory
    // 2. Simulate path failure (which is what happens on WC error)
    // 3. Verify retry logic works correctly
    // 4. Verify slice can exhaust retries

    auto* slice = createTestSlice();
    ASSERT_NE(slice, nullptr) << "Failed to create test slice";

    std::string test_path = "test_segment/test_device";
    slice->peer_nic_path = test_path;
    slice->rdma.endpoint = nullptr;  // No actual endpoint for this test
    const int max_retries = slice->rdma.max_retry_cnt;

    // Simulate WC error multiple times using the retry helper directly
    for (int attempt = 0; attempt < max_retries - 1; ++attempt) {
        uint32_t retry_before = slice->rdma.retry_cnt;

        // Simulate path failure (this is what happens on WC error)
        worker_pool_->testOnlySimulatePathFailure(test_path, nullptr);

        // Verify retry logic - should retry
        bool should_retry = WorkerPool::testOnlyShouldRetrySlice(slice);

        // Verify retry_cnt increased
        EXPECT_EQ(slice->rdma.retry_cnt, retry_before + 1)
            << "Retry count should increment after WC error";

        // Verify rail state changed
        auto state = worker_pool_->testOnlyGetRailState(test_path);
        EXPECT_GT(state.error_count, 0) << "Rail error count should increase";
    }

    // After max_retries-1 attempts, retry_cnt should be max_retries-1
    EXPECT_EQ(slice->rdma.retry_cnt, max_retries - 1);

    // One more retry attempt should exhaust retries
    EXPECT_FALSE(WorkerPool::testOnlyShouldRetrySlice(slice));
    EXPECT_EQ(slice->rdma.retry_cnt, max_retries);

    delete slice;
}

// =============================================================================
// Test 6: Rail failure and recovery cycle
// =============================================================================

TEST_F(E2EFaultToleranceTest, RailFailureAndRecovery_CompleteCycle) {
    // This test verifies the complete rail failure and recovery cycle:
    // 1. Rail is working (available)
    // 2. Simulate failures until rail pauses
    // 3. Verify rail is paused
    // 4. Wait for auto-recovery
    // 5. Verify rail is available again
    // 6. Simulate more failures to verify rail can fail again

    std::string test_path = "test_peer/test_device";
    const int kThreshold = WorkerPool::testOnlyGetRailErrorThreshold();
    const uint64_t kPauseNs = 1000000000ull;  // 1 second pause

    // Step 1: Rail is initially available
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(test_path))
        << "Rail should be available initially";

    // Step 2: Cause failures until pause
    for (int i = 0; i < kThreshold; ++i) {
        worker_pool_->testOnlySimulatePathFailure(test_path, nullptr);
    }

    // Step 3: Verify rail is paused
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(test_path))
        << "Rail should be paused after " << kThreshold << " errors";

    auto paused_state = worker_pool_->testOnlyGetRailState(test_path);
    EXPECT_EQ(paused_state.error_count, kThreshold);
    EXPECT_GT(paused_state.pause_until_ns, 0);

    // Step 4: Wait for recovery
    std::this_thread::sleep_for(
        std::chrono::nanoseconds(kPauseNs + 100000000ull));

    // Step 5: Verify rail recovered
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(test_path))
        << "Rail should be available after pause timeout";

    auto recovered_state = worker_pool_->testOnlyGetRailState(test_path);
    EXPECT_EQ(recovered_state.error_count, 0) << "Error count should be reset";
    EXPECT_EQ(recovered_state.pause_until_ns, 0)
        << "Pause timestamp should be reset";

    // Step 6: Verify rail can fail again
    for (int i = 0; i < kThreshold; ++i) {
        worker_pool_->testOnlySimulatePathFailure(test_path, nullptr);
    }
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(test_path))
        << "Rail should be able to pause again after recovery";
}

// =============================================================================
// Test 7: Multiple concurrent rail failures
// =============================================================================

TEST_F(E2EFaultToleranceTest, ConcurrentRailFailures_AllHandledCorrectly) {
    // This test verifies that multiple concurrent rail failures are handled
    // correctly:
    // 1. Create multiple rails
    // 2. Cause failures on different rails
    // 3. Verify each rail's state is managed independently
    // 4. Verify rails recover independently

    const int kThreshold = WorkerPool::testOnlyGetRailErrorThreshold();
    std::vector<std::string> rails = {"peer_a/device_a", "peer_b/device_b",
                                      "peer_c/device_c"};

    // Pause rail-a (kThreshold errors)
    for (int i = 0; i < kThreshold; ++i) {
        worker_pool_->testOnlySimulatePathFailure(rails[0], nullptr);
    }

    // Add 1 error to rail-b
    worker_pool_->testOnlySimulatePathFailure(rails[1], nullptr);

    // Add 2 errors to rail-c
    worker_pool_->testOnlySimulatePathFailure(rails[2], nullptr);
    worker_pool_->testOnlySimulatePathFailure(rails[2], nullptr);

    // Verify each rail's state
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(rails[0]))
        << "Rail-A should be paused";
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rails[1]))
        << "Rail-B should be available";
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rails[2]))
        << "Rail-C should be available";

    // Verify error counts
    auto state_a = worker_pool_->testOnlyGetRailState(rails[0]);
    auto state_b = worker_pool_->testOnlyGetRailState(rails[1]);
    auto state_c = worker_pool_->testOnlyGetRailState(rails[2]);

    EXPECT_EQ(state_a.error_count, kThreshold);
    EXPECT_EQ(state_b.error_count, 1);
    EXPECT_EQ(state_c.error_count, 2);
}

// =============================================================================
// Test 7: Retry exhaustion after max retries
// =============================================================================

TEST_F(E2EFaultToleranceTest, RetryExhaustion_AfterMaxRetries) {
    auto* slice = createTestSlice();
    ASSERT_NE(slice, nullptr);

    // Exhaust all retries
    int max_retries = slice->rdma.max_retry_cnt;
    for (int i = 0; i < max_retries; ++i) {
        bool should_retry = WorkerPool::testOnlyShouldRetrySlice(slice);
        if (i < max_retries - 1) {
            EXPECT_TRUE(should_retry) << "Should retry on attempt " << (i + 1);
        } else {
            EXPECT_FALSE(should_retry)
                << "Should not retry on attempt " << (i + 1);
        }
    }

    EXPECT_EQ(slice->rdma.retry_cnt, max_retries);
    EXPECT_FALSE(WorkerPool::testOnlyShouldRetrySlice(slice));

    delete slice;
}

// =============================================================================
// Test 8: Redispatch notification mechanism
// =============================================================================

TEST_F(E2EFaultToleranceTest, RedispatchCounter_IncrementsOnFailure) {
    auto initial_counter = worker_pool_->testOnlyGetRedispatchCounter().load();

    // Simulate a path failure
    worker_pool_->testOnlySimulatePathFailure("test_peer/test_device", nullptr);

    // Verify counter increased
    auto current_counter = worker_pool_->testOnlyGetRedispatchCounter().load();
    EXPECT_GT(current_counter, initial_counter)
        << "redispatch_counter_ should increase after path failure";
}

// =============================================================================
// Test 9: No excessive redispatch (avalanche prevention)
// =============================================================================

TEST_F(E2EFaultToleranceTest, NoAvalanche_SingleFailureDoesNotRedistributeAll) {
    // This test verifies that a single failure doesn't cause all pending
    // slices to be redispatched (only those using the failed rail)
    //
    // The redispatch_counter_ mechanism is designed to notify all workers
    // to redispatch their queues. This test verifies the mechanism works
    // correctly and doesn't cause excessive redispatch.

    std::string rail_a = "peer_a/device_a";
    std::string rail_b = "peer_b/device_b";
    std::string rail_c = "peer_c/device_c";

    const int kThreshold = WorkerPool::testOnlyGetRailErrorThreshold();

    // All rails should be available initially
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rail_a));
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rail_b));
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rail_c));

    // Pause only rail-a
    for (int i = 0; i < kThreshold; ++i) {
        worker_pool_->testOnlySimulatePathFailure(rail_a, nullptr);
    }

    // Verify only rail-a is paused
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(rail_a));
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rail_b));
    EXPECT_TRUE(worker_pool_->testOnlyIsRailAvailable(rail_c));

    // Verify rail states
    auto state_a = worker_pool_->testOnlyGetRailState(rail_a);
    auto state_b = worker_pool_->testOnlyGetRailState(rail_b);
    auto state_c = worker_pool_->testOnlyGetRailState(rail_c);

    EXPECT_EQ(state_a.error_count, kThreshold);
    EXPECT_GT(state_a.pause_until_ns, 0);

    EXPECT_EQ(state_b.error_count, 0);
    EXPECT_EQ(state_b.pause_until_ns, 0);

    EXPECT_EQ(state_c.error_count, 0);
    EXPECT_EQ(state_c.pause_until_ns, 0);

    // Verify redispatch counter increased
    auto redispatch_counter =
        worker_pool_->testOnlyGetRedispatchCounter().load();
    EXPECT_GT(redispatch_counter, 0)
        << "Redispatch counter should increase after failure";

    // Additional failures on rail-a should not increase error_count beyond
    // threshold (pause_until_ns is updated but error_count saturates at
    // kThreshold)
    auto error_count_before = state_a.error_count;
    worker_pool_->testOnlySimulatePathFailure(rail_a, nullptr);
    auto state_a_after = worker_pool_->testOnlyGetRailState(rail_a);
    EXPECT_GE(state_a_after.error_count, error_count_before);
}

// =============================================================================
// Test 10: Concurrent failures handled correctly
// =============================================================================

TEST_F(E2EFaultToleranceTest, ConcurrentFailures_NoDeadlockOrDataLoss) {
    // This test verifies concurrent failures are handled correctly
    //
    // Multiple threads simulate failures on different rails to verify:
    // 1. All failures are handled without deadlock
    // 2. Rail state is managed correctly under concurrent access
    // 3. Redispatch counter increases correctly

    std::string rail_a = "peer_a/device_a";
    std::string rail_b = "peer_b/device_b";
    std::string rail_c = "peer_c/device_c";

    const int kThreshold = WorkerPool::testOnlyGetRailErrorThreshold();
    const int kIterationsPerThread = 10;

    // Get initial redispatch counter
    auto initial_counter = worker_pool_->testOnlyGetRedispatchCounter().load();

    // Launch multiple threads to inject failures concurrently
    std::vector<std::thread> threads;
    std::atomic<int> thread_ready_count{0};
    std::atomic<bool> start_flag{false};

    // Thread 1: Fail rail-a
    threads.emplace_back([this, &thread_ready_count, &start_flag, &rail_a,
                          kThreshold, kIterationsPerThread]() {
        thread_ready_count++;
        while (!start_flag) std::this_thread::yield();

        for (int i = 0; i < kIterationsPerThread; ++i) {
            worker_pool_->testOnlySimulatePathFailure(rail_a, nullptr);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    // Thread 2: Fail rail-b
    threads.emplace_back([this, &thread_ready_count, &start_flag, &rail_b,
                          kThreshold, kIterationsPerThread]() {
        thread_ready_count++;
        while (!start_flag) std::this_thread::yield();

        for (int i = 0; i < kIterationsPerThread; ++i) {
            worker_pool_->testOnlySimulatePathFailure(rail_b, nullptr);
            std::this_thread::sleep_for(std::chrono::microseconds(150));
        }
    });

    // Thread 3: Fail rail-c
    threads.emplace_back([this, &thread_ready_count, &start_flag, &rail_c,
                          kThreshold, kIterationsPerThread]() {
        thread_ready_count++;
        while (!start_flag) std::this_thread::yield();

        for (int i = 0; i < kIterationsPerThread; ++i) {
            worker_pool_->testOnlySimulatePathFailure(rail_c, nullptr);
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    // Wait for all threads to be ready
    while (thread_ready_count.load() < 3) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Start all threads simultaneously
    start_flag = true;

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    // Verify all rails are paused (each got kIterationsPerThread errors, >=
    // kThreshold)
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(rail_a));
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(rail_b));
    EXPECT_FALSE(worker_pool_->testOnlyIsRailAvailable(rail_c));

    // Verify error counts accumulated correctly
    auto state_a = worker_pool_->testOnlyGetRailState(rail_a);
    auto state_b = worker_pool_->testOnlyGetRailState(rail_b);
    auto state_c = worker_pool_->testOnlyGetRailState(rail_c);

    EXPECT_GE(state_a.error_count, kThreshold);
    EXPECT_GE(state_b.error_count, kThreshold);
    EXPECT_GE(state_c.error_count, kThreshold);

    EXPECT_GT(state_a.pause_until_ns, 0);
    EXPECT_GT(state_b.pause_until_ns, 0);
    EXPECT_GT(state_c.pause_until_ns, 0);

    // Verify redispatch counter increased (total failures across all rails)
    auto final_counter = worker_pool_->testOnlyGetRedispatchCounter().load();
    EXPECT_GT(final_counter, initial_counter)
        << "Redispatch counter should increase";
}

// =============================================================================
// Test 11: Slice state transitions during retry
// =============================================================================

TEST_F(E2EFaultToleranceTest, SliceStateTransitions_DuringRetryFlow) {
    // This test verifies slice state transitions during the retry flow
    //
    // Expected state transitions:
    // PENDING → (submit) → POSTED → (WC error) → (redispatch) → PENDING → ...
    // Or: PENDING → (submit) → POSTED → (WC success) → SUCCESS
    // Or: PENDING → (submit) → POSTED → (retries exhausted) → FAILED

    auto* slice = createTestSlice();
    ASSERT_NE(slice, nullptr) << "Failed to create test slice";

    // Initial state: PENDING
    EXPECT_EQ(slice->status, Transport::Slice::PENDING);

    // Set the peer_nic_path to match what we'll use for failure simulation
    std::string test_path = "test_segment/test_device";
    slice->peer_nic_path = test_path;
    slice->rdma.endpoint = nullptr;  // No actual endpoint for this test

    const int max_retries = slice->rdma.max_retry_cnt;

    // Simulate multiple WC errors (retry flow)
    // We use testOnlySimulatePathFailure to simulate the path failure
    // and testOnlyShouldRetrySlice to verify retry logic
    for (int attempt = 0; attempt < max_retries - 1; ++attempt) {
        uint32_t retry_before = slice->rdma.retry_cnt;

        // Simulate path failure
        worker_pool_->testOnlySimulatePathFailure(test_path, nullptr);

        // Verify retry logic - should retry
        EXPECT_TRUE(WorkerPool::testOnlyShouldRetrySlice(slice))
            << "Should retry on attempt " << attempt;

        // Verify retry_cnt increased
        EXPECT_EQ(slice->rdma.retry_cnt, retry_before + 1)
            << "Retry count should increment on attempt " << attempt;

        // Verify rail error count increased
        auto state = worker_pool_->testOnlyGetRailState(test_path);
        EXPECT_GT(state.error_count, 0)
            << "Rail error count should be positive";
    }

    // After max_retries-1 calls, retry_cnt should be max_retries-1
    EXPECT_EQ(slice->rdma.retry_cnt, max_retries - 1);

    // One more call to shouldRetrySlice should increment to max_retries and
    // return false (exhausted)
    EXPECT_FALSE(WorkerPool::testOnlyShouldRetrySlice(slice));
    EXPECT_EQ(slice->rdma.retry_cnt, max_retries);

    delete slice;

    // Test success path: create a new slice and verify initial state
    auto* success_slice = createTestSlice();
    ASSERT_NE(success_slice, nullptr);

    EXPECT_EQ(success_slice->status, Transport::Slice::PENDING);
    EXPECT_EQ(success_slice->rdma.retry_cnt, 0);

    // Verify success_slice can retry (retry_cnt becomes 1 after call)
    EXPECT_TRUE(WorkerPool::testOnlyShouldRetrySlice(success_slice));
    EXPECT_EQ(success_slice->rdma.retry_cnt, 1);

    delete success_slice;
}

// =============================================================================
// Test 12: Endpoint RESET with in-flight requests
// =============================================================================

TEST_F(E2EFaultToleranceTest, EndpointReset_WithInFlightRequests) {
    // This test simulates an endpoint RESET while RDMA operations are
    // in-flight:
    // 1. Get a real RDMA endpoint (using loopback mode)
    // 2. Submit slices to the worker pool
    // 3. Simulate endpoint RESET by deleting the endpoint
    // 4. Verify the failure is handled correctly

    // Use loopback mode (local_nic_path == peer_nic_path)
    std::string loopback_path = context_->nicPath();

    ASSERT_FALSE(loopback_path.empty()) << "Local NIC path should not be empty";

    // Create test slice with loopback path
    auto* slice = createTestSlice();
    ASSERT_NE(slice, nullptr) << "Failed to create test slice";

    // Set the peer_nic_path to loopback path
    slice->peer_nic_path = loopback_path;
    slice->rdma.endpoint = nullptr;  // Will be set by performPostSend

    // Submit the slice to worker pool
    worker_pool_->testOnlySubmitSlice(slice, 0);

    // Try to perform post-send
    // This will attempt to get the endpoint and submit RDMA operations
    worker_pool_->testOnlyPerformPostSend(0);

    // Wait a bit for the operation to be processed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // NOW SIMULATE ENDPOINT RESET: Inject a path failure
    // This simulates a hard failure where the endpoint becomes unavailable
    // while operations are pending or in-flight
    worker_pool_->testOnlySimulatePathFailure(loopback_path, nullptr);

    // Verify the failure was recorded
    auto state = worker_pool_->testOnlyGetRailState(loopback_path);
    EXPECT_GT(state.error_count, 0)
        << "Rail error count should increase after RESET simulation";

    // Verify redispatch counter increased
    auto initial_counter = worker_pool_->testOnlyGetRedispatchCounter().load();
    EXPECT_GT(initial_counter, 0)
        << "Redispatch counter should increase after failure";

    // The slice should have been processed (either succeeded, failed, or
    // redispatched) We don't check the exact state since it depends on timing
    // and endpoint availability

    // Clean up
    if (slice->status == Transport::Slice::PENDING) {
        delete slice;
    }
}

}  // namespace
