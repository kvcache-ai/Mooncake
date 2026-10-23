/**
 * @brief Exercise concurrent register/unregister and shutdown through HTTP.
 * Check state consistency, heartbeat lifetime and completion without deadlock.
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

// Read private heartbeat thread / Service state for state assertions.
#define private public
#define protected public
#include "p2p/client/p2p_client_config_builder.h"
#include "p2p/client/p2p_client_service.h"
#undef protected
#undef private

#include "default_config.h"
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
        // Let each listener own its port as soon as it is allocated. Choosing
        // and releasing free ports first races with concurrent test servers.
        const std::string host_name = "localhost:0";

        auto config = P2PClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", /*client_rpc_port=*/0,
            /*rpc_thread_num=*/2, /*lock_shard_count=*/1024,
            /*route_cache_max_memory_bytes=*/300 * 1024 * 1024,
            /*route_cache_ttl_ms=*/60 * 1000,
            /*local_transfer_mode=*/"te",
            /*local_memcpy_async_worker_num=*/32, /*http_port=*/0,
            /*enable_http_server=*/true);

        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);
        EXPECT_EQ(client->Init(config), ErrorCode::OK);
        EXPECT_TRUE(WaitForRoutableClient(client->GetMasterClient(),
                                          client->GetClientID()));
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

// Concurrent registration from LOCAL_ONLY must reuse the heartbeat thread.
TEST_F(P2PRegisterConcurrencyTest, ConcurrentRegisterNoCrash) {
    auto client = CreateClient();

    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_FALSE((client->GetServiceState() == P2PClientServiceState::ONLINE));
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

    // The first request creates the client; later requests may observe the
    // existing client and complete as idempotent re-registers.
    EXPECT_GE(ok_count.load(), 1);
    ASSERT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_TRUE((client->GetServiceState() == P2PClientServiceState::ONLINE));

    client->Stop();
}

// Concurrent register/unregister must finish in a consistent state.
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

    // Terminal: unregistered + heartbeat parked + LOCAL_ONLY all agree.
    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_FALSE((client->GetServiceState() == P2PClientServiceState::ONLINE));
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    client->Stop();
}

// Shutdown must not race registration into use-after-free or restart heartbeat.
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
    EXPECT_FALSE(client->heartbeat_thread_.joinable());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(client->heartbeat_thread_.joinable());

    stop.store(true, std::memory_order_release);
    spam.join();
    client.reset();  // ~P2PClientService after Stop must not double-throw.
}

// Unregister must always land in LOCAL_ONLY with the heartbeat parked, and a
// repeated unregister stays there (idempotent).
TEST_F(P2PRegisterConcurrencyTest, UnregisterAlwaysLocalOnly) {
    auto client = CreateClient();

    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_FALSE((client->GetServiceState() == P2PClientServiceState::ONLINE));
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    // Second unregister: still OK, still local-only, heartbeat still parked.
    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    EXPECT_FALSE((client->GetServiceState() == P2PClientServiceState::ONLINE));
    EXPECT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    client->Stop();
}

// Register from LOCAL_ONLY resumes the heartbeat and starts metadata recovery
// while ONLINE.
TEST_F(P2PRegisterConcurrencyTest, RegisterFromLocalOnlyRecovers) {
    auto client = CreateClient();

    ASSERT_EQ(HttpPost(Url(client, "/unregister")), 200);
    ASSERT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_EQ(client->GetHealthStatus(), "LOCAL_ONLY");

    ASSERT_EQ(HttpPost(Url(client, "/register")), 200);
    EXPECT_TRUE((client->GetServiceState() == P2PClientServiceState::ONLINE));
    ASSERT_TRUE(client->heartbeat_thread_.joinable());
    EXPECT_TRUE(WaitFor([&] { return client->GetHealthStatus() == "ONLINE"; },
                        std::chrono::seconds(10)))
        << "health=" << client->GetHealthStatus();

    client->Stop();
}

// A redundant /register while already registered must not crash nor change
// state.
TEST_F(P2PRegisterConcurrencyTest, DuplicateRegisterIsNoop) {
    auto client = CreateClient();
    ASSERT_TRUE(WaitFor([&] {
        return (client->GetServiceState() == P2PClientServiceState::ONLINE);
    }));
    const auto hb_before = client->heartbeat_thread_.get_id();

    EXPECT_EQ(HttpPost(Url(client, "/register")), 200);
    EXPECT_TRUE((client->GetServiceState() == P2PClientServiceState::ONLINE));
    EXPECT_EQ(client->heartbeat_thread_.get_id(), hb_before);

    client->Stop();
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    mooncake::init_ylt_log_level();
    return RUN_ALL_TESTS();
}
