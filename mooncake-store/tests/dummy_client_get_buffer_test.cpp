// dummy_client_get_buffer_test.cpp
// Verifies DummyClient::get_buffer / batch_get_buffer correctness,
// including hot-cache shm path and allocator fallback path.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "dummy_client.h"
#include "real_client.h"
#include "default_config.h"
#include "test_server_helpers.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

// Register all RPC handlers that DummyClient needs
static void RegisterRpcHandlers(coro_rpc::coro_rpc_server &server,
                                RealClient &rc) {
    server.register_handler<&RealClient::service_ready_internal>(&rc);
    server.register_handler<&RealClient::ping>(&rc);
    server.register_handler<&RealClient::map_shm_internal>(&rc);
    server.register_handler<&RealClient::unmap_shm_internal>(&rc);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(&rc);
    server.register_handler<&RealClient::put_dummy_helper>(&rc);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&rc);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&rc);
    server.register_handler<&RealClient::remove_internal>(&rc);
    server.register_handler<&RealClient::removeAll_internal>(&rc);
    server.register_handler<&RealClient::isExist_internal>(&rc);
    server.register_handler<&RealClient::getSize_internal>(&rc);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(&rc);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(&rc);
    server.register_handler<&RealClient::acquire_hot_cache>(&rc);
    server.register_handler<&RealClient::release_hot_cache>(&rc);
    server.register_handler<&RealClient::batch_acquire_hot_cache>(&rc);
    server.register_handler<&RealClient::batch_release_hot_cache>(&rc);
    server.register_handler<&RealClient::acquire_buffer_dummy>(&rc);
    server.register_handler<&RealClient::release_buffer_dummy>(&rc);
    server.register_handler<&RealClient::batch_acquire_buffer_dummy>(&rc);
}

static constexpr size_t kMB = 1024ULL * 1024;
static constexpr size_t kGB = 1024ULL * kMB;
static constexpr size_t kPayloadSize = 500 * kMB;  // single key: 500 MB
static constexpr size_t kHotBlockSize =
    512 * kMB;  // hot cache block: 512 MB (>= payload)
static constexpr size_t kSegmentSize =
    8 * kGB;  // global segment: 8 GB (holds batch)
static constexpr size_t kLocalBufSize = 8 * kGB;  // local transfer buffer: 8 GB
static constexpr size_t kPoolSize = 8 * kGB;      // dummy mem pool: 8 GB
static constexpr size_t kHotCacheSize =
    4 * kGB;  // hot cache: 4 GB (8 x 512 MB blocks)
static constexpr size_t kBatchKeyCount = 14;  // 14 x 500 MB ~ 7 GB total
static constexpr size_t kHotWarmCount = 4;    // keys to warm into hot cache

class DummyClientGetBufferTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("DummyClientGetBufferTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
    }

    void TearDown() override {
        // Mute ylt/coro_rpc "operation canceled" noise during shutdown
        easylog::set_min_severity(easylog::Severity::FATAL);
        if (dummy_client_) dummy_client_->tearDownAll();
        if (rpc_server_) rpc_server_->stop();
        if (real_client_) real_client_->tearDownAll();
        master_.Stop();
        easylog::set_min_severity(easylog::Severity::WARN);

        // Restore env
        if (saved_hot_cache_env_.has_value()) {
            setenv("LOCAL_HOT_CACHE_SIZE", saved_hot_cache_env_->c_str(), 1);
        } else {
            unsetenv("LOCAL_HOT_CACHE_SIZE");
        }
        if (saved_hot_block_env_.has_value()) {
            setenv("LOCAL_HOT_BLOCK_SIZE", saved_hot_block_env_->c_str(), 1);
        } else {
            unsetenv("LOCAL_HOT_BLOCK_SIZE");
        }
    }

    // Bring up the full real+dummy stack.
    // Returns true on success.
    bool SetupStack() {
        // Enable hot cache with production-scale block size
        const char *prev = std::getenv("LOCAL_HOT_CACHE_SIZE");
        if (prev) saved_hot_cache_env_ = std::string(prev);
        setenv("LOCAL_HOT_CACHE_SIZE", std::to_string(kHotCacheSize).c_str(),
               1);

        const char *prev_block = std::getenv("LOCAL_HOT_BLOCK_SIZE");
        if (prev_block) saved_hot_block_env_ = std::string(prev_block);
        setenv("LOCAL_HOT_BLOCK_SIZE", std::to_string(kHotBlockSize).c_str(),
               1);

        // Start in-proc master
        if (!master_.Start(InProcMasterConfigBuilder().build())) return false;

        // Create RealClient
        real_client_ = RealClient::create();
        const std::string rdma_devices =
            (FLAGS_protocol == "rdma") ? FLAGS_device_name : "";
        ipc_path_ = "@dummy_test_" + std::to_string(getpid()) + ".sock";
        if (real_client_->setup_real(
                "localhost:17815", "P2PHANDSHAKE", kSegmentSize, kLocalBufSize,
                FLAGS_protocol, rdma_devices, master_.master_address(), nullptr,
                ipc_path_) != 0) {
            return false;
        }

        // Start RPC server for DummyClient to connect to
        rpc_port_ = getFreeTcpPort();
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/2, /*port=*/rpc_port_, /*address=*/"127.0.0.1",
            std::chrono::seconds(0), /*tcp_no_delay=*/true);
        RegisterRpcHandlers(*rpc_server_, *real_client_);
        auto ec = rpc_server_->async_start();
        if (ec.hasResult()) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        // Create DummyClient
        dummy_client_ = std::make_shared<DummyClient>();
        std::string rpc_addr = "127.0.0.1:" + std::to_string(rpc_port_);
        if (dummy_client_->setup_dummy(kPoolSize, kLocalBufSize, rpc_addr,
                                       ipc_path_) != 0) {
            return false;
        }

        return true;
    }

    // Put test data via RealClient
    void PutData(const std::string &key, const std::string &data) {
        std::span<const char> span(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;
        ASSERT_EQ(real_client_->put(key, span, config), 0);
    }

    // Read via RealClient to populate hot cache, then wait for async fill
    void WarmHotCache(const std::string &key) {
        auto buf = real_client_->get_buffer(key);
        ASSERT_NE(buf, nullptr);
        // Async hot cache fill — larger payloads need more time
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }

    mooncake::testing::InProcMaster master_;
    std::shared_ptr<RealClient> real_client_;
    std::shared_ptr<DummyClient> dummy_client_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    int rpc_port_ = 0;
    std::string ipc_path_;
    std::optional<std::string> saved_hot_cache_env_;
    std::optional<std::string> saved_hot_block_env_;
};

// ---- Test: get_buffer via allocator fallback (no hot cache hit) ----
TEST_F(DummyClientGetBufferTest, GetBuffer_AllocatorFallback) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "fallback_key";
    const std::string data(kPayloadSize, 'F');
    PutData(key, data);

    // DummyClient get_buffer without warming hot cache → allocator fallback
    auto buf = dummy_client_->get_buffer(key);
    ASSERT_NE(buf, nullptr) << "get_buffer should succeed via fallback";
    EXPECT_EQ(buf->size(), data.size());
    EXPECT_FALSE(dummy_client_->is_hot_cache_ptr(buf->ptr()))
        << "Should NOT be in hot cache region (allocator fallback)";

    std::string got(static_cast<const char *>(buf->ptr()), buf->size());
    EXPECT_EQ(got, data) << "Data mismatch on allocator fallback path";
}

// ---- Test: get_buffer via hot cache shm zero-copy path ----
TEST_F(DummyClientGetBufferTest, GetBuffer_HotCachePath) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "hot_key";
    const std::string data(kPayloadSize, 'H');
    PutData(key, data);

    // Warm hot cache: RealClient reads → async fill
    WarmHotCache(key);

    // DummyClient should hit hot cache path
    auto buf = dummy_client_->get_buffer(key);
    ASSERT_NE(buf, nullptr) << "get_buffer should succeed via hot cache";
    EXPECT_EQ(buf->size(), data.size());
    EXPECT_TRUE(dummy_client_->is_hot_cache_ptr(buf->ptr()))
        << "Should be in hot cache shm region (zero-copy path)";

    std::string got(static_cast<const char *>(buf->ptr()), buf->size());
    EXPECT_EQ(got, data) << "Data mismatch on hot cache path";
}

// ---- Test: batch_get_buffer mixed hot-cache + fallback ----
TEST_F(DummyClientGetBufferTest, BatchGetBuffer) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const size_t num_keys = kBatchKeyCount;
    std::vector<std::string> keys;
    std::vector<std::string> payloads;
    keys.reserve(num_keys);
    payloads.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i) {
        keys.push_back("batch_key_" + std::to_string(i));
        payloads.push_back(
            std::string(kPayloadSize, 'A' + static_cast<char>(i % 26)));
        PutData(keys.back(), payloads.back());
    }

    // Warm first kHotWarmCount keys into hot cache
    for (size_t i = 0; i < kHotWarmCount; ++i) {
        WarmHotCache(keys[i]);
    }

    // Batch get all keys via DummyClient
    auto results = dummy_client_->batch_get_buffer(keys);
    ASSERT_EQ(results.size(), num_keys);

    size_t hot_hits = 0;
    size_t fallback_hits = 0;
    for (size_t i = 0; i < num_keys; ++i) {
        ASSERT_NE(results[i], nullptr)
            << "batch_get_buffer returned nullptr for key " << keys[i];
        EXPECT_EQ(results[i]->size(), payloads[i].size());
        std::string got(static_cast<const char *>(results[i]->ptr()),
                        results[i]->size());
        EXPECT_EQ(got, payloads[i]) << "Data mismatch for key " << keys[i];

        if (dummy_client_->is_hot_cache_ptr(results[i]->ptr()))
            hot_hits++;
        else
            fallback_hits++;
    }
    // 4 blocks in hot cache, so at most 4 hot hits; rest go through fallback
    EXPECT_GT(hot_hits, 0) << "Should have at least one hot cache hit";
    EXPECT_GT(fallback_hits, 0) << "Should have at least one fallback hit";
}

// ---- Test: hot cache shm is properly mapped in DummyClient ----
TEST_F(DummyClientGetBufferTest, ShmMappingEstablished) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    // After setup_dummy, hot_cache_base_ should be non-null
    // We verify indirectly: put + warm + get via hot cache should work
    const std::string key = "shm_verify";
    const std::string data(kPayloadSize, 'S');
    PutData(key, data);
    WarmHotCache(key);

    auto buf = dummy_client_->get_buffer(key);
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(buf->size(), data.size());
    EXPECT_TRUE(dummy_client_->is_hot_cache_ptr(buf->ptr()))
        << "Warmed key should resolve via hot cache shm";

    // Verify shm mapping is valid via memcmp (byte-by-byte too slow at 500 MB)
    EXPECT_EQ(std::memcmp(buf->ptr(), data.data(), data.size()), 0)
        << "Shm content mismatch";
}

// ---- Perf: hot cache vs allocator fallback latency comparison ----
TEST_F(DummyClientGetBufferTest, Perf_HotCacheVsFallback) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const size_t payload_size = kPayloadSize;
    const int iterations = 20;
    const std::string hot_key = "perf_hot";
    const std::string cold_key = "perf_cold";
    const std::string data(payload_size, 'P');
    PutData(hot_key, data);
    PutData(cold_key, data);
    WarmHotCache(hot_key);

    // Benchmark: hot cache path
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto buf = dummy_client_->get_buffer(hot_key);
        ASSERT_NE(buf, nullptr);
    }
    auto t1 = std::chrono::steady_clock::now();
    double hot_ms =
        std::chrono::duration<double, std::milli>(t1 - t0).count() / iterations;

    // Benchmark: allocator fallback path
    auto t2 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto buf = dummy_client_->get_buffer(cold_key);
        ASSERT_NE(buf, nullptr);
    }
    auto t3 = std::chrono::steady_clock::now();
    double fallback_ms =
        std::chrono::duration<double, std::milli>(t3 - t2).count() / iterations;

    LOG(INFO) << "=== get_buffer latency (avg over " << iterations
              << " iterations, payload=" << payload_size / kMB << " MB) ===";
    LOG(INFO) << "  Hot cache path : " << std::fixed << std::setprecision(2)
              << hot_ms << " ms";
    LOG(INFO) << "  Fallback path  : " << std::fixed << std::setprecision(2)
              << fallback_ms << " ms";
    LOG(INFO) << "  Speedup        : " << std::fixed << std::setprecision(2)
              << fallback_ms / hot_ms << "x";

    // Hot cache should be faster (no data copy, no allocator round-trip)
    EXPECT_LT(hot_ms, fallback_ms)
        << "Hot cache path should be faster than fallback";
}

// ---- Perf: batch_get_buffer hot vs cold ----
TEST_F(DummyClientGetBufferTest, Perf_BatchHotVsCold) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    // Hot cache has 4 blocks of 1 GB; use 2 keys so both fit
    const size_t payload_size = kPayloadSize;
    const int iterations = 10;

    std::vector<std::string> hot_keys = {"batch_perf_hot_0",
                                         "batch_perf_hot_1"};
    std::vector<std::string> cold_keys = {"batch_perf_cold_0",
                                          "batch_perf_cold_1"};
    const std::string data(payload_size, 'B');

    for (auto &k : hot_keys) PutData(k, data);
    for (auto &k : cold_keys) PutData(k, data);
    for (auto &k : hot_keys) WarmHotCache(k);

    // Benchmark: batch hot cache path
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto results = dummy_client_->batch_get_buffer(hot_keys);
        for (auto &r : results) ASSERT_NE(r, nullptr);
    }
    auto t1 = std::chrono::steady_clock::now();
    double hot_ms =
        std::chrono::duration<double, std::milli>(t1 - t0).count() / iterations;

    // Benchmark: batch fallback path
    auto t2 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        auto results = dummy_client_->batch_get_buffer(cold_keys);
        for (auto &r : results) ASSERT_NE(r, nullptr);
    }
    auto t3 = std::chrono::steady_clock::now();
    double fallback_ms =
        std::chrono::duration<double, std::milli>(t3 - t2).count() / iterations;

    LOG(INFO) << "=== batch_get_buffer latency (avg over " << iterations
              << " iterations, 2 keys x " << payload_size / kMB << " MB) ===";
    LOG(INFO) << "  Hot cache path : " << std::fixed << std::setprecision(2)
              << hot_ms << " ms";
    LOG(INFO) << "  Fallback path  : " << std::fixed << std::setprecision(2)
              << fallback_ms << " ms";
    LOG(INFO) << "  Speedup        : " << std::fixed << std::setprecision(2)
              << fallback_ms / hot_ms << "x";

    EXPECT_LT(hot_ms, fallback_ms)
        << "Batch hot cache path should be faster than fallback";
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}