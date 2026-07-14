#pragma once

// Shared fixture and helpers for DummyClient buffer tests.

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
        const int local_port = getFreeTcpPort();
        if (local_port < 0) return false;
        const std::string local_hostname =
            "localhost:" + std::to_string(local_port);
        ipc_path_ = "@dummy_test_" + std::to_string(getpid()) + ".sock";
        if (real_client_->setup_real(
                local_hostname, "P2PHANDSHAKE", kSegmentSize, kLocalBufSize,
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
// ---- Regression: a stable hot-cache entry must be invalidated by remove ----
}  // namespace testing
}  // namespace mooncake
