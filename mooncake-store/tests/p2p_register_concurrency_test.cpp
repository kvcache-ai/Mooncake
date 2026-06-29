/**
 * @file p2p_register_concurrency_test.cpp
 * @brief Concurrency-safety tests for P2PClientService register / unregister /
 *        Stop, driven through the /register and /unregister HTTP endpoints.
 *
 * Covers: concurrent /register must not crash (B1/B2); interleaved
 * register+unregister must not deadlock and must stay state-consistent (B3);
 * Stop() racing in-flight /register must not use-after-free nor restart the
 * heartbeat after shutdown (B4). Run under TSan/ASan; a deadlock shows up as a
 * ctest timeout.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>

// Read private heartbeat_running_ / registered_ for state assertions.
#define private public
#define protected public
#include "p2p_client_service.h"
#undef protected
#undef private

#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

class P2PRegisterConcurrencyTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("P2PRegisterConcurrencyTest");
        FLAGS_logtostderr = 1;
        ASSERT_TRUE(master_.Start()) << "Failed to start P2P master";
        master_address_ = master_.master_address();
    }

    static void TearDownTestSuite() {
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    // A fresh, fully-initialized client per test so register/unregister/Stop
    // cannot leak across tests.
    static std::shared_ptr<P2PClientService> CreateClient() {
        const uint32_t te_port = static_cast<uint32_t>(getFreeTcpPort());
        const uint32_t rpc_port = static_cast<uint32_t>(getFreeTcpPort());
        const uint16_t http_port = static_cast<uint16_t>(getFreeTcpPort());
        const std::string host_name = "localhost:" + std::to_string(te_port);

        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port,
            /*rpc_thread_num=*/2, /*lock_shard_count=*/1024,
            /*route_cache_max_memory_bytes=*/300 * 1024 * 1024,
            /*route_cache_ttl_ms=*/60 * 1000,
            /*local_transfer_mode=*/"te",
            /*local_memcpy_async_worker_num=*/32, http_port,
            /*enable_http_server=*/true);

        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);
        EXPECT_EQ(client->Init(config), ErrorCode::OK);
        return client;
    }

    static std::string Url(const std::shared_ptr<P2PClientService>& c,
                           const std::string& path) {
        return "http://127.0.0.1:" + std::to_string(c->GetHttpPort()) + path;
    }

    static int HttpPost(const std::string& url) {
        coro_http::coro_http_client client;
        return client.post(url, "", coro_http::req_content_type::octet_stream)
            .status;
    }

    template <typename Pred>
    static bool WaitFor(Pred pred, std::chrono::milliseconds timeout =
                                       std::chrono::seconds(5)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (pred()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return pred();
    }

    static InProcP2PMaster master_;
    static std::string master_address_;
};

InProcP2PMaster P2PRegisterConcurrencyTest::master_;
std::string P2PRegisterConcurrencyTest::master_address_;

// B1/B2: a burst of concurrent /register after LOCAL_ONLY (heartbeat stopped)
// must not std::terminate / race heartbeat_thread_; the client ends registered
// with the heartbeat running.
TEST_F(P2PRegisterConcurrencyTest, ConcurrentRegisterNoCrash) {
    auto client = CreateClient();

    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(WaitFor([&] { return !client->heartbeat_running_.load(); }));
    EXPECT_FALSE(client->registered_.load());
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    constexpr int kThreads = 16;
    std::atomic<bool> go{false};
    std::atomic<int> ok_count{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            while (!go.load(std::memory_order_acquire)) {
            }
            if (HttpPost(Url(client, "/register")) == 200) {
                ok_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();

    // Registers serialize on registration_mutex_, so exactly one re-registers
    // the client (HTTP 200); the rest hit the master with the same client_id
    // and get CLIENT_ALREADY_EXISTS -> non-200. The point of the test is no
    // crash and a consistent terminal state, not that every concurrent register
    // succeeds.
    EXPECT_EQ(ok_count.load(), 1);
    ASSERT_TRUE(WaitFor([&] { return client->heartbeat_running_.load(); }));
    EXPECT_TRUE(client->registered_.load());

    client->Stop();
}

// B3: hammer /register and /unregister concurrently. Must not deadlock nor
// crash, and the client must be drivable to a consistent terminal state.
TEST_F(P2PRegisterConcurrencyTest, RegisterUnregisterInterleaving) {
    auto client = CreateClient();

    std::atomic<bool> stop{false};
    std::thread reg([&] {
        while (!stop.load(std::memory_order_acquire)) {
            HttpPost(Url(client, "/register"));
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::thread unreg([&] {
        while (!stop.load(std::memory_order_acquire)) {
            HttpPost(Url(client, "/unregister"));
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    stop.store(true, std::memory_order_release);
    reg.join();
    unreg.join();

    // Terminal: unregistered + heartbeat stopped + LOCAL_ONLY all agree.
    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(WaitFor([&] { return !client->heartbeat_running_.load(); }));
    EXPECT_FALSE(client->registered_.load());
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    client->Stop();
}

// B4: Stop() while /register is in flight must not use-after-free nor restart
// the heartbeat once shutdown has begun.
TEST_F(P2PRegisterConcurrencyTest, StopDuringRegister) {
    auto client = CreateClient();

    std::atomic<bool> stop{false};
    std::thread spam([&] {
        while (!stop.load(std::memory_order_acquire)) {
            HttpPost(Url(client, "/register"));
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    client->Stop();

    // Heartbeat is stopped and stays stopped while /register keeps being tried.
    EXPECT_FALSE(client->heartbeat_running_.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(client->heartbeat_running_.load());

    stop.store(true, std::memory_order_release);
    spam.join();
    client.reset();  // ~P2PClientService after Stop must not double-throw.
}

// Unregister must always land in LOCAL_ONLY with the heartbeat stopped, and a
// repeated unregister stays there (idempotent).
TEST_F(P2PRegisterConcurrencyTest, UnregisterAlwaysLocalOnly) {
    auto client = CreateClient();

    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(WaitFor([&] { return !client->heartbeat_running_.load(); }));
    EXPECT_FALSE(client->registered_.load());
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    // Second unregister: still OK, still local-only, heartbeat still stopped.
    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    EXPECT_FALSE(client->registered_.load());
    EXPECT_FALSE(client->heartbeat_running_.load());
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    client->Stop();
}

// Register from LOCAL_ONLY restarts the heartbeat and drives recovery to FULL.
TEST_F(P2PRegisterConcurrencyTest, RegisterFromLocalOnlyRecovers) {
    auto client = CreateClient();

    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(WaitFor([&] { return !client->heartbeat_running_.load(); }));
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    ASSERT_EQ(HttpPost(Url(client, "/register")), 200);
    EXPECT_TRUE(client->registered_.load());
    ASSERT_TRUE(WaitFor([&] { return client->heartbeat_running_.load(); }));
    EXPECT_TRUE(WaitFor([&] { return client->GetHealthStatus() == "FULL"; },
                        std::chrono::seconds(10)))
        << "health=" << client->GetHealthStatus();

    client->Stop();
}

// A redundant /register while already registered (master returns
// CLIENT_ALREADY_EXISTS) must not crash nor change state.
TEST_F(P2PRegisterConcurrencyTest, DuplicateRegisterIsNoop) {
    auto client = CreateClient();
    ASSERT_TRUE(WaitFor([&] { return client->registered_.load(); }));
    const bool hb_before = client->heartbeat_running_.load();

    // Status is expected non-200 (ALREADY_EXISTS); we only care that state
    // holds.
    HttpPost(Url(client, "/register"));
    EXPECT_TRUE(client->registered_.load());
    EXPECT_EQ(client->heartbeat_running_.load(), hb_before);

    client->Stop();
}

}  // namespace testing
}  // namespace mooncake
