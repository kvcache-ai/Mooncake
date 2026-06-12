// dummy_client_get_buffer_test.cpp
// Verifies DummyClient::get_buffer / batch_get_buffer correctness,
// including hot-cache shm path and allocator fallback path.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
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
static constexpr size_t kPayloadSize = 16 * kMB;  // single key: 16 MB
static constexpr size_t kHotBlockSize =
    32 * kMB;  // hot cache block: 32 MB (>= payload)
static constexpr size_t kSegmentSize =
    512 * kMB;  // global segment: 512 MB (holds batch)
static constexpr size_t kLocalBufSize = 512 * kMB;  // local transfer buffer
static constexpr size_t kPoolSize = 512 * kMB;      // dummy mem pool
static constexpr size_t kHotCacheSize =
    256 * kMB;  // hot cache: 256 MB (8 x 32 MB blocks)
static constexpr size_t kBatchKeyCount = 14;  // 14 x 16 MB ~ 224 MB total
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
            setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE",
                   saved_hot_cache_env_->c_str(), 1);
        } else {
            unsetenv("MC_STORE_LOCAL_HOT_CACHE_SIZE");
        }
        if (saved_hot_block_env_.has_value()) {
            setenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE",
                   saved_hot_block_env_->c_str(), 1);
        } else {
            unsetenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE");
        }
        if (saved_hot_shm_env_.has_value()) {
            setenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM",
                   saved_hot_shm_env_->c_str(), 1);
        } else {
            unsetenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM");
        }
    }

    // Bring up the full real+dummy stack.
    // Returns true on success.
    bool SetupStack() {
        // Enable hot cache with production-scale block size
        const char *prev = std::getenv("MC_STORE_LOCAL_HOT_CACHE_SIZE");
        if (prev) saved_hot_cache_env_ = std::string(prev);
        setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE",
               std::to_string(kHotCacheSize).c_str(), 1);

        const char *prev_block = std::getenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE");
        if (prev_block) saved_hot_block_env_ = std::string(prev_block);
        setenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE",
               std::to_string(kHotBlockSize).c_str(), 1);

        // Enable shm mode so hot cache is shared with dummy client
        const char *prev_shm = std::getenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM");
        if (prev_shm) saved_hot_shm_env_ = std::string(prev_shm);
        setenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1", 1);

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

    // Read via RealClient to populate hot cache, then wait for async fill.
    // CMS frequency admission requires admission_threshold_ (default 2) reads
    // before a key is admitted, so we read multiple times.
    void WarmHotCache(const std::string &key) {
        for (int i = 0; i < 3; ++i) {
            auto buf = real_client_->get_buffer(key);
            ASSERT_NE(buf, nullptr);
        }
        // Async hot cache fill — larger payloads need more time
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }

    // Trigger hot-cache admission via DummyClient fallback reads (CMS threshold
    // default 2) but do NOT wait for async PutHotKey to publish.
    void TriggerAdmissionWithoutWaiting(const std::string &key) {
        for (int i = 0; i < 2; ++i) {
            auto buf = dummy_client_->get_buffer(key);
            ASSERT_NE(buf, nullptr)
                << "get_buffer failed while triggering admission";
        }
    }

    mooncake::testing::InProcMaster master_;
    std::shared_ptr<RealClient> real_client_;
    std::shared_ptr<DummyClient> dummy_client_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    int rpc_port_ = 0;
    std::string ipc_path_;
    std::optional<std::string> saved_hot_cache_env_;
    std::optional<std::string> saved_hot_block_env_;
    std::optional<std::string> saved_hot_shm_env_;
};
TEST_F(DummyClientGetBufferTest,
       RePutSameKeyAfterHotCacheRemoveShouldNotUseStaleHotCache) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "reput_same_key_hotcache_regression";

    // Run multiple rounds to simulate a realistic workload.
    // With default admission_threshold_=2, hot-cache paths often appear after
    // rounds 2-3.
    constexpr int kRounds = 8;

    for (int round = 0; round < kRounds; ++round) {
        SCOPED_TRACE("round=" + std::to_string(round));

        // Use a different payload each round. Distinct data makes stale hot
        // cache from a prior round show up as a data mismatch without crashing.
        // Fill with non-printable bytes (starting at 1) to avoid log noise.
        const char fill = static_cast<char>(1 + (round % 250));
        const std::string data(kPayloadSize, fill);

        // 1. Write the target key.
        PutData(key, data);

        // 2. Warm the hot cache.
        //
        // WarmHotCache calls real_client_->get_buffer(key) multiple times to
        // reach the admission_sketch_ threshold and waits for async fill.
        WarmHotCache(key);

        // 3. Read via DummyClient.
        //
        // This should hit the hot cache shm path.
        auto hot_buf = dummy_client_->get_buffer(key);
        ASSERT_NE(hot_buf, nullptr) << "dummy get_buffer should succeed";

        EXPECT_EQ(hot_buf->size(), data.size()) << "hot buffer size mismatch";

        // Key assertion: pointer must be in the hot cache region.
        // If not, this round did not exercise the hot cache path.
        EXPECT_TRUE(dummy_client_->is_hot_cache_ptr(hot_buf->ptr()))
            << "warmed key should resolve via hot cache shm path";

        // 4. Verify hot cache contents match this round's payload.
        {
            std::string got(static_cast<char *>(hot_buf->ptr()),
                            hot_buf->size());

            // Check size first, then compare byte-by-byte with std::equal.
            ASSERT_EQ(got.size(), data.size());
            bool is_equal = std::equal(got.begin(), got.end(), data.begin());

            // Report mismatch without printing binary payload.
            EXPECT_TRUE(is_equal) << "Data mismatch detected in hot cache, but "
                                     "text payload is hidden.";
        }

        // 5. Remove the target key.
        //
        // force=true avoids lease/task state blocking remove.
        int remove_rc = real_client_->remove(key, true);
        ASSERT_EQ(remove_rc, 0) << "remove failed";

        // 6. After remove, dummy get_buffer must not return stale hot cache.
        //
        // A non-null buffer here means hot cache or metadata was not cleaned
        // up.
        auto after_remove_buf = dummy_client_->get_buffer(key);
        EXPECT_EQ(after_remove_buf, nullptr)
            << "dummy get_buffer should return nullptr after remove; "
            << "hot cache may still hold stale key";
    }
}
// ---- Regression: same key remove + re-put should not reuse stale hot-cache
// buffer ----
//
// Covers residual hot-cache state across repeated put/remove on the same key:
// 1. Fixed key for read/write across rounds;
// 2. After first put, RealClient::get_buffer warms admission_sketch_ to
// threshold;
// 3. DummyClient::get_buffer should use hot cache shm zero-copy when admitted;
// 4. remove the key;
// 5. After remove, DummyClient::get_buffer must not read stale hot cache;
// 6. Re-put new data for the same key (intended scenario in design comments);
// 7. DummyClient::get_buffer must read new data, not leftover hot cache;
// 8. Repeat for many rounds to catch hot-cache state leaking across cycles.
TEST_F(DummyClientGetBufferTest,
       RePutSameKeyAfterRemoveWithoutWarmHotCacheFindFailRound) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "reput_same_key_hotcache_regression";

    // Skip WarmHotCache to observe natural admission under load.
    // Surfaces which round hits hot cache and whether remove leaves stale
    // entries.
    constexpr int kRounds = 20;

    for (int round = 0; round < kRounds; ++round) {
        SCOPED_TRACE("round=" + std::to_string(round));

        // Different payload each round; non-zero binary fill avoids log
        // pollution.
        const char fill = static_cast<char>(1 + (round % 250));
        const std::string data(kPayloadSize, fill);

        // 1. Write the target key.
        PutData(key, data);

        // 2. Intentionally omit WarmHotCache(key).
        //
        // Normal warm-up drives admission_sketch_ via real_client_->get_buffer
        // and waits for async hot cache fill. Without it we observe:
        // - whether natural access admits keys to hot cache;
        // - which round admission happens;
        // - whether stale hot cache is visible after remove.

        // 3. DummyClient read attempt.
        auto hot_buf = dummy_client_->get_buffer(key);

        // Do not ASSERT_NE before remove: early rounds may not be admitted yet,
        // so nullptr is valid before hot cache is populated.
        if (hot_buf == nullptr) {
            std::cerr << "[round=" << round
                      << "] dummy get_buffer returned nullptr before remove; "
                      << "hot cache may not be admitted yet." << std::endl;
        } else {
            const bool is_hot_ptr =
                dummy_client_->is_hot_cache_ptr(hot_buf->ptr());

            std::cerr << "[round=" << round
                      << "] dummy get_buffer succeeded before remove; "
                      << "size=" << hot_buf->size()
                      << ", is_hot_cache_ptr=" << is_hot_ptr << std::endl;

            // 4. If a buffer was returned, verify contents strictly.
            //
            // Failure here means stale data before remove (hot cache/metadata
            // bug).
            if (hot_buf->size() != data.size()) {
                FAIL() << "FAILED at round=" << round
                       << ": buffer size mismatch before remove. "
                       << "got=" << hot_buf->size()
                       << ", expected=" << data.size();
            }

            std::string got(static_cast<char *>(hot_buf->ptr()),
                            hot_buf->size());

            // Byte-by-byte compare; do not print binary payload in logs.
            const bool same_data =
                std::equal(got.begin(), got.end(), data.begin());

            if (!same_data) {
                FAIL() << "FAILED at round=" << round
                       << ": data mismatch before remove. "
                       << "This indicates stale data was returned.";
            }
        }

        // 5. Remove the target key (force=true avoids lease/task blocking
        // remove).
        int remove_rc = real_client_->remove(key, true);
        ASSERT_EQ(remove_rc, 0) << "remove failed at round=" << round;

        // 6. After remove, dummy get_buffer must not return any buffer.
        //
        // Core check: non-null after successful remove indicates incomplete
        // hot cache / metadata cleanup.
        auto after_remove_buf = dummy_client_->get_buffer(key);

        if (after_remove_buf != nullptr) {
            const bool is_hot_ptr =
                dummy_client_->is_hot_cache_ptr(after_remove_buf->ptr());

            std::cerr << "[round=" << round
                      << "] dummy get_buffer still succeeded after remove; "
                      << "size=" << after_remove_buf->size()
                      << ", is_hot_cache_ptr=" << is_hot_ptr << std::endl;

            FAIL()
                << "FAILED at round=" << round
                << ": dummy get_buffer should return nullptr after remove, "
                << "but got non-null buffer. "
                << "This strongly indicates stale hot cache / stale metadata.";
        } else {
            std::cerr << "[round=" << round
                      << "] dummy get_buffer returned nullptr after remove."
                      << std::endl;
        }
    }
}

// ---- Regression: a stable hot-cache entry must be invalidated by remove ----
TEST_F(DummyClientGetBufferTest,
       StableHotCacheEntryShouldBeInvalidatedByRemove) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "stable_hot_cache_remove_invalidation";

    const char fill = static_cast<char>(7);
    const std::string data(kPayloadSize, fill);

    PutData(key, data);
    WarmHotCache(key);

    auto hot_buf_1 = dummy_client_->get_buffer(key);
    ASSERT_NE(hot_buf_1, nullptr)
        << "dummy get_buffer should succeed after WarmHotCache";
    ASSERT_EQ(hot_buf_1->size(), data.size())
        << "hot cache buffer size mismatch before remove";
    ASSERT_TRUE(dummy_client_->is_hot_cache_ptr(hot_buf_1->ptr()))
        << "buffer should be in hot cache shm before remove";

    {
        std::string got(static_cast<char *>(hot_buf_1->ptr()),
                        hot_buf_1->size());
        ASSERT_EQ(got.size(), data.size());
        ASSERT_TRUE(std::equal(got.begin(), got.end(), data.begin()))
            << "hot cache data mismatch before remove";
    }

    auto hot_buf_2 = dummy_client_->get_buffer(key);
    ASSERT_NE(hot_buf_2, nullptr)
        << "second dummy get_buffer should still succeed before remove";
    ASSERT_TRUE(dummy_client_->is_hot_cache_ptr(hot_buf_2->ptr()))
        << "second buffer should still be in hot cache shm before remove";
    ASSERT_EQ(hot_buf_2->size(), data.size())
        << "second hot cache buffer size mismatch before remove";

    {
        std::string got(static_cast<char *>(hot_buf_2->ptr()),
                        hot_buf_2->size());
        ASSERT_EQ(got.size(), data.size());
        ASSERT_TRUE(std::equal(got.begin(), got.end(), data.begin()))
            << "second hot cache data mismatch before remove";
    }

    int remove_rc = real_client_->remove(key, true);
    ASSERT_EQ(remove_rc, 0) << "remove failed";

    auto after_remove_buf = dummy_client_->get_buffer(key);
    if (after_remove_buf != nullptr) {
        const bool after_remove_is_hot =
            dummy_client_->is_hot_cache_ptr(after_remove_buf->ptr());
        std::cerr << "[StableHotCacheEntryShouldBeInvalidatedByRemove] "
                  << "after remove get_buffer returned non-null; "
                  << "size=" << after_remove_buf->size()
                  << ", is_hot_cache_ptr=" << after_remove_is_hot << std::endl;
        FAIL() << "remove(key, true) succeeded, but dummy get_buffer(key) "
               << "still returned a non-null buffer";
    }
}

// ---- Diagnosis: removed key must not be resurrected by async hot-cache fill
// ----
//
// Compare against pre-fix behavior:
//   mooncake-store/tests/scripts/compare_hot_cache_fix.sh all
TEST_F(DummyClientGetBufferTest,
       RemoveShouldNotBeResurrectedByAsyncHotCacheFill) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "async_fill_resurrect_removed_key";
    constexpr int kRounds = 8;

    for (int round = 0; round < kRounds; ++round) {
        SCOPED_TRACE("round=" + std::to_string(round));

        const char fill = static_cast<char>(1 + (round % 250));
        const std::string data(kPayloadSize, fill);

        PutData(key, data);

        // Two fallback reads reach CMS admission threshold (default 2) and
        // submit async fill, but we do not wait for worker PutHotKey.
        TriggerAdmissionWithoutWaiting(key);

        int remove_rc = real_client_->remove(key, true);
        ASSERT_EQ(remove_rc, 0) << "remove failed at round=" << round;

        auto immediately_after_remove = dummy_client_->get_buffer(key);
        const bool immediate_non_null = (immediately_after_remove != nullptr);
        bool immediate_is_hot = false;
        size_t immediate_size = 0;
        if (immediate_non_null) {
            immediate_is_hot = dummy_client_->is_hot_cache_ptr(
                immediately_after_remove->ptr());
            immediate_size = immediately_after_remove->size();
            std::cerr << "[round=" << round
                      << "] immediately after remove: NON_NULL"
                      << ", size=" << immediate_size
                      << ", is_hot_cache_ptr=" << immediate_is_hot << std::endl;
        } else {
            std::cerr << "[round=" << round
                      << "] immediately after remove: nullptr" << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(2000));

        auto delayed_after_remove = dummy_client_->get_buffer(key);
        const bool delayed_non_null = (delayed_after_remove != nullptr);
        bool delayed_is_hot = false;
        size_t delayed_size = 0;
        if (delayed_non_null) {
            delayed_is_hot =
                dummy_client_->is_hot_cache_ptr(delayed_after_remove->ptr());
            delayed_size = delayed_after_remove->size();
            std::cerr << "[round=" << round
                      << "] delayed after remove: NON_NULL"
                      << ", size=" << delayed_size
                      << ", is_hot_cache_ptr=" << delayed_is_hot << std::endl;
        } else {
            std::cerr << "[round=" << round << "] delayed after remove: nullptr"
                      << std::endl;
        }

        if (immediate_non_null || delayed_non_null) {
            if (!immediate_non_null && delayed_non_null) {
                FAIL() << "FAILED at round=" << round
                       << ": key was nullptr immediately after remove, "
                       << "but became non-null after waiting. "
                       << "This strongly suggests async hot-cache fill "
                       << "resurrected a removed key. "
                       << "delayed_is_hot_cache_ptr=" << delayed_is_hot
                       << ", delayed_size=" << delayed_size;
            }
            if (immediate_non_null && delayed_non_null) {
                FAIL() << "FAILED at round=" << round
                       << ": key was non-null immediately after remove "
                       << "and remained non-null after waiting. "
                       << "immediate_is_hot_cache_ptr=" << immediate_is_hot
                       << ", delayed_is_hot_cache_ptr=" << delayed_is_hot;
            }
            if (immediate_non_null && !delayed_non_null) {
                FAIL() << "FAILED at round=" << round
                       << ": key was non-null immediately after remove, "
                       << "but became nullptr after waiting. "
                       << "immediate_is_hot_cache_ptr=" << immediate_is_hot;
            }
        }
    }
}

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