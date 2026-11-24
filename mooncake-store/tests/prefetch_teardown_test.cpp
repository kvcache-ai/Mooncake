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
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

#include "dummy_client.h"

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
            {"ipc_socket_path", IpcPath()},
        };
        client_ = RealClient::create();
        ASSERT_TRUE(client_->setup_internal(config).has_value());
    }
    void TearDown() override {
        if (dummy_) dummy_->tearDownAll();
        if (server_) server_->stop();
        if (client_) client_->tearDownAll();
        master_.Stop();
    }

    std::string IpcPath() const {
        return "@prefetch_rpc_test_" + std::to_string(getpid()) + ".sock";
    }

    void ResetClient() {
        ASSERT_EQ(client_->tearDownAll(), 0);
        client_ = RealClient::create();
    }

    tl::expected<void, ErrorCode> SetupPositional(
        bool enabled = false,
        int64_t cooldown = DEFAULT_SSD_PREFETCH_COOLDOWN_SEC,
        int64_t dedup = DEFAULT_SSD_PREFETCH_DEDUP_TTL_SEC) {
        return client_->setup_internal(
            "127.0.0.1", "P2PHANDSHAKE", 16 * 1024 * 1024, 16 * 1024 * 1024,
            "tcp", "", master_.master_address(), nullptr, IpcPath(), 50052,
            false, false, "", "default", false, DEFAULT_CLIENT_HTTP_PORT, false,
            enabled, cooldown, dedup);
    }

    bool StartDummy(bool with_options_rpc = true) {
        server_ =
            std::make_unique<coro_rpc::coro_rpc_server>(1, 0, "127.0.0.1");
        server_->register_handler<&RealClient::service_ready_internal>(
            client_.get());
        server_->register_handler<&RealClient::ping>(client_.get());
        server_->register_handler<&RealClient::map_shm_internal>(client_.get());
        server_->register_handler<&RealClient::unmap_shm_internal>(
            client_.get());
        server_->register_handler<&RealClient::unregister_shm_buffer_internal>(
            client_.get());
        server_->register_handler<&RealClient::isExist_internal>(client_.get());
        server_->register_handler<&RealClient::batchIsExist_internal>(
            client_.get());
        if (with_options_rpc) {
            server_->register_handler<&RealClient::isExistWithOptions_internal>(
                client_.get());
            server_->register_handler<
                &RealClient::batchIsExistWithOptions_internal>(client_.get());
        }
        server_->async_start();
        if (server_->get_errc()) return false;
        dummy_ = std::make_shared<DummyClient>();
        return dummy_->setup_dummy(
                   16 * 1024 * 1024, 16 * 1024 * 1024,
                   "127.0.0.1:" + std::to_string(server_->port()),
                   IpcPath()) == 0;
    }

    bool WaitForJob(
        const std::shared_ptr<SsdPrefetcher::JobGateForTest>& gate) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!gate->arrived.try_wait() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        const bool arrived = gate->arrived.try_wait();
        // Release before any assertion so teardown can always join the pool.
        gate->release.count_down();
        return arrived;
    }

    int64_t CooldownSeconds() { return client_->ssd_prefetch_cooldown_sec_; }
    int64_t DedupSeconds() { return client_->ssd_prefetch_dedup_ttl_sec_; }

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
    std::shared_ptr<DummyClient> dummy_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
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

TEST_F(PrefetchTeardownTest, PositionalSetupDefaultsDisablePrefetch) {
    // Reusing an enabled client must not keep the previous setup's switch.
    ASSERT_EQ(client_->tearDownAll(), 0);
    ASSERT_TRUE(SetupPositional().has_value());
    EXPECT_FALSE(PrefetcherInitialized());
}

TEST_F(PrefetchTeardownTest, PositionalSetupAcceptsExplicitZeroDurations) {
    ResetClient();
    ASSERT_TRUE(SetupPositional(true, 0, 0).has_value());
    EXPECT_TRUE(PrefetcherInitialized());
    EXPECT_EQ(CooldownSeconds(), 0);
    EXPECT_EQ(DedupSeconds(), 0);
}

TEST_F(PrefetchTeardownTest, ConfigDictPreservesPrefetchConfiguration) {
    ResetClient();
    ConfigDict config = {
        {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
        {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
        {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "16777216"},
        {CONFIG_KEY_LOCAL_BUFFER_SIZE, "16777216"},
        {CONFIG_KEY_PROTOCOL, "tcp"},
        {CONFIG_KEY_MASTER_SERVER_ADDR, master_.master_address()},
        {CONFIG_KEY_ENABLE_SSD_PREFETCH, "true"},
        {CONFIG_KEY_SSD_PREFETCH_COOLDOWN_SEC, "7"},
        {CONFIG_KEY_SSD_PREFETCH_DEDUP_TTL_SEC, "11"},
    };
    ASSERT_TRUE(client_->setup_internal(config).has_value());
    EXPECT_TRUE(PrefetcherInitialized());
    EXPECT_EQ(CooldownSeconds(), 7);
    EXPECT_EQ(DedupSeconds(), 11);
}

TEST_F(PrefetchTeardownTest, RejectsInvalidPrefetchDurationsBeforeSetup) {
    ResetClient();
    for (const int64_t invalid :
         std::vector<int64_t>{-1, std::numeric_limits<int64_t>::max()}) {
        for (const bool bad_cooldown : {false, true}) {
            auto result = SetupPositional(true, bad_cooldown ? invalid : 5,
                                          bad_cooldown ? 30 : invalid);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            EXPECT_FALSE(PrefetcherInitialized());
        }
    }
}

TEST_F(PrefetchTeardownTest, ConfigDictRejectsInvalidDurationRanges) {
    ResetClient();
    for (const auto* knob : {CONFIG_KEY_SSD_PREFETCH_COOLDOWN_SEC,
                             CONFIG_KEY_SSD_PREFETCH_DEDUP_TTL_SEC}) {
        for (const auto* value : {"-1", "9223372036854775807"}) {
            SCOPED_TRACE(std::string(knob) + "=" + value);
            ConfigDict config = {
                {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
                {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
                {CONFIG_KEY_MASTER_SERVER_ADDR, master_.master_address()},
                {knob, value},
            };
            auto result = client_->setup_internal(config);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            EXPECT_FALSE(PrefetcherInitialized());
        }
    }
}

TEST_F(PrefetchTeardownTest, DummySingleExistForwardsPrefetchOptions) {
    ASSERT_TRUE(StartDummy());
    ASSERT_EQ(client_->put("dummy_single", std::span<const char>("value", 5)),
              0);
    auto gate = std::make_shared<SsdPrefetcher::JobGateForTest>();
    ArmJobGate(gate);
    ExistOptions options;
    options.prefetch_to_memory = true;
    EXPECT_EQ(dummy_->isExist("dummy_single", options), 1);
    EXPECT_TRUE(WaitForJob(gate));
    EXPECT_EQ(dummy_->isExist("missing", options), 0);
}

TEST_F(PrefetchTeardownTest, DummyBatchExistForwardsPrefetchOptions) {
    ASSERT_TRUE(StartDummy());
    ASSERT_EQ(client_->put("dummy_batch", std::span<const char>("value", 5)),
              0);
    auto gate = std::make_shared<SsdPrefetcher::JobGateForTest>();
    ArmJobGate(gate);
    ExistOptions options;
    options.prefetch_to_memory = true;
    EXPECT_EQ(dummy_->batchIsExist({"dummy_batch", "missing", "dummy_batch"},
                                   options),
              (std::vector<int>{1, 0, 1}));
    EXPECT_TRUE(WaitForJob(gate));
    EXPECT_TRUE(dummy_->batchIsExist({}, options).empty());
}

TEST_F(PrefetchTeardownTest, DummyPlainExistDoesNotTriggerPrefetch) {
    ASSERT_TRUE(StartDummy());
    ASSERT_EQ(client_->put("dummy_plain", std::span<const char>("value", 5)),
              0);
    auto gate = std::make_shared<SsdPrefetcher::JobGateForTest>();
    ArmJobGate(gate);
    EXPECT_EQ(dummy_->isExist("dummy_plain"), 1);
    EXPECT_EQ(dummy_->isExist("dummy_plain", ExistOptions{}), 1);
    EXPECT_EQ(dummy_->batchIsExist({"dummy_plain"}, ExistOptions{}),
              (std::vector<int>{1}));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(gate->arrived.try_wait());
    gate->release.count_down();
}

TEST_F(PrefetchTeardownTest, DummyExistFallsBackToLegacyServer) {
    ASSERT_TRUE(StartDummy(false));
    ASSERT_EQ(client_->put("legacy", std::span<const char>("value", 5)), 0);
    ExistOptions options;
    options.prefetch_to_memory = true;
    EXPECT_EQ(dummy_->isExist("legacy", options), 1);
    EXPECT_EQ(dummy_->isExist("missing", options), 0);
    EXPECT_EQ(dummy_->batchIsExist({"legacy", "missing"}, options),
              (std::vector<int>{1, 0}));
}

}  // namespace mooncake::test
