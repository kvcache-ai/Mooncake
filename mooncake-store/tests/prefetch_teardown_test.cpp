// Regression test: close() while a prefetch job is blocked in the pool must
// drain and join the pool before teardown releases segment memory.
//
// Background: prefetch pool jobs capture shared_ptrs locked at submission
// time, so a job blocked on its metadata RPC keeps Client alive past
// client_.reset(). Without draining, teardown would release segment memory
// (and leave it registered with TE / published on the master) while the
// blocked job still runs. This test covers close() with a blocked prefetch
// job: a client with DRAM segments but no local SSD offload enqueues a
// prefetch job held on a test gate, and close() must not return until the
// gate releases and the job finishes.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
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
        ConfigDict config = {
            {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
            {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
            // The review scenario: DRAM segments, but no local SSD offload.
            {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "16777216"},
            {CONFIG_KEY_LOCAL_BUFFER_SIZE, "16777216"},
            {CONFIG_KEY_PROTOCOL, "tcp"},
            {CONFIG_KEY_MASTER_SERVER_ADDR, master_.master_address()},
            {CONFIG_KEY_ENABLE_SSD_PREFETCH, "true"},
        };
        client_ = RealClient::create();
        ASSERT_TRUE(client_->setup_internal(config).has_value());
    }
    void TearDown() override { master_.Stop(); }

    // TEST_F bodies run in generated subclasses, which do NOT inherit this
    // fixture's friendship with RealClient — every touch of a private member
    // goes through these fixture helpers.
    SsdPrefetcher* PrefetcherOrNull() { return client_->prefetcher_.get(); }
    void ArmJobGate(std::shared_ptr<SsdPrefetcher::JobGateForTest> gate) {
        client_->prefetcher_->SetJobGateForTest(std::move(gate));
    }
    void ShutdownPrefetcher() { client_->prefetcher_->Shutdown(); }
    bool PrefetcherInitialized() { return client_->prefetcher_->initialized(); }

    testing::InProcMaster master_;
    std::shared_ptr<RealClient> client_;
};

TEST_F(PrefetchTeardownTest, CloseJoinsBlockedPrefetchJob) {
    ASSERT_NE(PrefetcherOrNull(), nullptr)
        << "prefetch enabled must construct the prefetcher";

    // isExist() only triggers prefetch for keys that EXIST, so create the
    // probe key first.
    const std::string key = "prefetch_teardown_probe_key";
    const std::string value(1024, 'x');
    ASSERT_EQ(client_->put(key, std::span<const char>(value)), 0);

    auto gate = std::make_shared<SsdPrefetcher::JobGateForTest>();
    ArmJobGate(gate);

    ExistOptions options;
    options.prefetch_to_memory = true;
    EXPECT_EQ(client_->isExist(key, options), 1)
        << "probe must see the existing key (this is what triggers prefetch)";

    // Wait until a worker actually reaches the gate before closing — without
    // this acknowledgement the test could pass with no blocked job at all.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!gate->arrived.try_wait()) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "no prefetch job reached the gate within 10s";
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    std::atomic<bool> closed{false};
    std::thread closer([&] {
        EXPECT_EQ(client_->tearDownAll(), 0);
        closed.store(true, std::memory_order_release);
    });

    // While the job is held, close() must be stuck in the pool join.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_FALSE(closed.load(std::memory_order_acquire))
        << "close() returned while a prefetch job was still blocked";

    gate->release.count_down();
    closer.join();
    EXPECT_TRUE(closed.load(std::memory_order_acquire));
}

TEST_F(PrefetchTeardownTest, ShutdownIsIdempotentAndStopsNewWork) {
    ASSERT_NE(PrefetcherOrNull(), nullptr);

    ShutdownPrefetcher();
    ShutdownPrefetcher();  // must not hang or crash
    EXPECT_FALSE(PrefetcherInitialized());

    // After Shutdown the trigger is a no-op: no enqueue, no throw. The key
    // exists so the trigger path is reached and exercised.
    const std::string key = "prefetch_teardown_late_probe";
    const std::string value(1024, 'y');
    ASSERT_EQ(client_->put(key, std::span<const char>(value)), 0);
    ExistOptions options;
    options.prefetch_to_memory = true;
    EXPECT_EQ(client_->isExist(key, options), 1);

    EXPECT_EQ(client_->tearDownAll(), 0);
}

}  // namespace mooncake::test
