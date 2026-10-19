// Regression test: close() while a prefetch job is blocked in the pool must
// drain and join the pool before teardown releases segment memory.
//
// Background: prefetch pool jobs capture shared_ptrs locked at submission
// time, so a job blocked on its metadata RPC keeps Client alive past
// client_.reset(). Without draining, teardown would release segment memory
// (and leave it registered with TE / published on the master) while the
// blocked job still runs. This test covers close() with a blocked prefetch
// job: a client with DRAM segments but no local SSD offload enqueues a
// prefetch job held on a test latch, and close() must not return until the
// latch releases and the job finishes.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <latch>
#include <memory>
#include <thread>

#include "real_client.h"
#include "replica.h"
#include "test_server_helpers.h"

namespace mooncake::test {

class PrefetchTeardownTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_rpc_port(0)
                                      .set_http_metrics_port(0)
                                      .set_http_metadata_port(0)
                                      .build()));
    }
    void TearDown() override { master_.Stop(); }

    testing::InProcMaster master_;
};

TEST_F(PrefetchTeardownTest, CloseJoinsBlockedPrefetchJob) {
    ConfigDict config = {
        {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
        {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
        // His scenario: DRAM segments, but no local SSD offload.
        {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "16777216"},
        {CONFIG_KEY_LOCAL_BUFFER_SIZE, "16777216"},
        {CONFIG_KEY_PROTOCOL, "tcp"},
        {CONFIG_KEY_MASTER_SERVER_ADDR, master_.master_address()},
        {CONFIG_KEY_ENABLE_SSD_PREFETCH, "true"},
    };
    auto client = RealClient::create();
    ASSERT_TRUE(client->setup_internal(config).has_value());
    ASSERT_NE(client->prefetcher_, nullptr)
        << "prefetch enabled must construct the prefetcher";

    // Every pool job now blocks on this latch before running.
    auto gate = std::make_shared<std::latch>(1);
    client->prefetcher_->SetJobGateForTest(gate);

    // Enqueue a job through the real path: a probe with prefetch_to_memory.
    // The key need not exist; the trigger fires on the probe itself.
    ExistOptions options;
    options.prefetch_to_memory = true;
    (void)client->isExist("prefetch_teardown_probe_key", options);

    std::atomic<bool> closed{false};
    std::thread closer([&] {
        EXPECT_EQ(client->tearDownAll(), 0);
        closed.store(true, std::memory_order_release);
    });

    // While the job is held, close() must be stuck in the pool join.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_FALSE(closed.load(std::memory_order_acquire))
        << "close() returned while a prefetch job was still blocked";

    gate->count_down();
    closer.join();
    EXPECT_TRUE(closed.load(std::memory_order_acquire));
}

TEST_F(PrefetchTeardownTest, ShutdownIsIdempotentAndStopsNewWork) {
    ConfigDict config = {
        {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
        {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
        {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "16777216"},
        {CONFIG_KEY_LOCAL_BUFFER_SIZE, "16777216"},
        {CONFIG_KEY_PROTOCOL, "tcp"},
        {CONFIG_KEY_MASTER_SERVER_ADDR, master_.master_address()},
        {CONFIG_KEY_ENABLE_SSD_PREFETCH, "true"},
    };
    auto client = RealClient::create();
    ASSERT_TRUE(client->setup_internal(config).has_value());
    ASSERT_NE(client->prefetcher_, nullptr);

    client->prefetcher_->Shutdown();
    client->prefetcher_->Shutdown();  // must not hang or crash
    EXPECT_FALSE(client->prefetcher_->initialized());

    // After Shutdown the trigger is a no-op: no enqueue, no throw.
    ExistOptions options;
    options.prefetch_to_memory = true;
    (void)client->isExist("prefetch_teardown_late_probe", options);

    EXPECT_EQ(client->tearDownAll(), 0);
}

}  // namespace mooncake::test
