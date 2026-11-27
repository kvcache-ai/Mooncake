#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <string_view>
#include <thread>

#include "dummy_client.h"
#include "real_client.h"
#include "test_server_helpers.h"

namespace mooncake {
class RealClientHeartbeatTestPeer {
   public:
    using RegistrationGuard = RealClient::DummyRegistrationGuard;
    static auto& Mutex(RealClient& c) { return c.dummy_heartbeat_mutex_; }
    static auto& Heartbeats(RealClient& c) { return c.dummy_heartbeats_; }
    static auto& PingClient(RealClient& c) { return c.dummy_ping_client_; }
    static bool TryExpire(RealClient& c, const UUID& id) {
        return c.try_expire_dummy_client(id);
    }
};
namespace {
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

template <class Predicate>
bool Eventually(Predicate predicate) {
    const auto deadline = Clock::now() + 5s;
    do {
        if (predicate()) return true;
        std::this_thread::sleep_for(5ms);
    } while (Clock::now() < deadline);
    return false;
}

void Expire(RealClient& client, const UUID& id) {
    std::lock_guard lock(RealClientHeartbeatTestPeer::Mutex(client));
    RealClientHeartbeatTestPeer::Heartbeats(client)[id] = {
        Clock::now() - std::chrono::seconds(DEFAULT_CLIENT_LIVE_TTL_SEC + 1),
        false};
}

bool NeedsRemount(RealClient& client, const UUID& id) {
    std::lock_guard lock(RealClientHeartbeatTestPeer::Mutex(client));
    auto it = RealClientHeartbeatTestPeer::Heartbeats(client).find(id);
    return it == RealClientHeartbeatTestPeer::Heartbeats(client).end() ||
           it->second.needs_remount;
}

class ReconnectLogSink : public google::LogSink {
   public:
    ReconnectLogSink() { google::AddLogSink(this); }
    ~ReconnectLogSink() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity, const char*, const char*, int,
              const struct ::tm*, const char* message,
              size_t message_len) override {
        if (std::string_view(message, message_len) ==
            "RealClient lost, entering reconnection loop...") {
            reconnect_started.store(true);
        }
    }

    std::atomic<bool> reconnect_started{false};
};

TEST(RealClientHeartbeatTest, RegistrationProtectsOnlyItsOwnClient) {
    RealClient client;
    const UUID registering{1, 1}, stale{1, 2};
    Expire(client, registering);
    Expire(client, stale);
    {
        RealClientHeartbeatTestPeer::RegistrationGuard registration(
            client, registering);
        EXPECT_FALSE(
            RealClientHeartbeatTestPeer::TryExpire(client, registering));
        EXPECT_TRUE(RealClientHeartbeatTestPeer::TryExpire(client, stale));
    }
    EXPECT_TRUE(RealClientHeartbeatTestPeer::TryExpire(client, registering));
}

TEST(RealClientHeartbeatTest, InvalidRegistrationDoesNotTrackClient) {
    RealClient client;
    const UUID id{1, 3};
    auto result = client.map_shm_internal(-1, 1, 4096, true, id);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(RealClientHeartbeatTestPeer::Heartbeats(client).empty());
}

TEST(RealClientHeartbeatTest, DuplicateRegistrationDoesNotRenewExpiry) {
    testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto client = RealClient::create();
    ASSERT_EQ(
        client->setup_real("127.0.0.1:" + std::to_string(getFreeTcpPort()),
                           "P2PHANDSHAKE", 16 * 1024 * 1024, 16 * 1024 * 1024,
                           "tcp", "", master.master_address(), nullptr, ""),
        0);
    const UUID id{3, 1};
    auto* helper = ShmHelper::getInstance();
    void* buffer = helper->allocate(4096);
    auto shm = helper->get_shm(buffer);
    ASSERT_TRUE(shm);
    const auto address = reinterpret_cast<uint64_t>(buffer);
    ASSERT_TRUE(
        client->map_shm_internal(dup(shm->fd), address, shm->size, true, id));
    EXPECT_FALSE(NeedsRemount(*client, id));
    Expire(*client, id);
    ASSERT_TRUE(
        client->map_shm_internal(dup(shm->fd), address, shm->size, true, id));
    ASSERT_TRUE(RealClientHeartbeatTestPeer::TryExpire(*client, id));
    EXPECT_TRUE(RealClientHeartbeatTestPeer::Heartbeats(*client).empty());

    ASSERT_TRUE(
        client->map_shm_internal(dup(shm->fd), address, shm->size, false, id));
    EXPECT_TRUE(NeedsRemount(*client, id));
    EXPECT_FALSE(RealClientHeartbeatTestPeer::TryExpire(*client, id));
    EXPECT_EQ(client->tearDownAll(), 0);
    EXPECT_EQ(helper->free(buffer), 0);
}

TEST(RealClientHeartbeatTest, ExplicitUnmapDoesNotRequestRemount) {
    RealClient client;
    const UUID id{4, 1};
    Expire(client, id);
    client.shm_contexts_.try_emplace(id);
    ASSERT_TRUE(client.unmap_shm_internal(id).has_value());
    EXPECT_FALSE(NeedsRemount(client, id));
}

TEST(RealClientHeartbeatTest, ZeroBufferClientCanAcknowledgeRemount) {
    testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto client = RealClient::create();
    const auto setup = [&] {
        return client->setup_real(
            "127.0.0.1:" + std::to_string(getFreeTcpPort()), "P2PHANDSHAKE",
            16 * 1024 * 1024, 16 * 1024 * 1024, "tcp", "",
            master.master_address(), nullptr, "");
    };
    const UUID id{5, 1};
    EXPECT_FALSE(client->ping(id));
    EXPECT_TRUE(RealClientHeartbeatTestPeer::Heartbeats(*client).empty());
    ASSERT_EQ(setup(), 0);
    ASSERT_FALSE(RealClientHeartbeatTestPeer::PingClient(*client).expired());
    auto initial = client->ping(id);
    ASSERT_TRUE(initial);
    EXPECT_EQ(initial->client_status, ClientStatus::NEED_REMOUNT);

    const auto port = getFreeTcpPort();
    const auto endpoint = "127.0.0.1:" + std::to_string(port);
    coro_rpc::coro_rpc_server server(1, port, "127.0.0.1");
    server.register_handler<&RealClient::service_ready_internal>(client.get());
    server.register_handler<&RealClient::ping>(client.get());
    server.register_handler<&RealClient::unmap_shm_internal>(client.get());
    auto running = server.async_start();
    ASSERT_FALSE(running.hasResult());
    ReconnectLogSink reconnect_log;
    DummyClient dummy;
    ASSERT_EQ(dummy.setup_dummy(16 * 1024 * 1024, 0, endpoint, ""), 0);
    EXPECT_EQ(dummy.health_check(), HC_HEALTHY);
    EXPECT_EQ(dummy.tearDownAll(), 0);
    EXPECT_FALSE(reconnect_log.reconnect_started.load());
    server.stop();

    // A fresh successful heartbeat invalidates a previously stale candidate.
    Expire(*client, id);
    ASSERT_TRUE(client->ping(id).has_value());
    EXPECT_FALSE(RealClientHeartbeatTestPeer::TryExpire(*client, id));
    {
        std::lock_guard lock(RealClientHeartbeatTestPeer::Mutex(*client));
        RealClientHeartbeatTestPeer::Heartbeats(*client).at(id).needs_remount =
            true;
    }
    auto first = client->ping(id);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->client_status, ClientStatus::NEED_REMOUNT);
    auto acknowledged = client->ping(id);
    ASSERT_TRUE(acknowledged.has_value());
    EXPECT_EQ(acknowledged->client_status, ClientStatus::OK);
    EXPECT_EQ(client->tearDownAll(), 0);
    EXPECT_TRUE(RealClientHeartbeatTestPeer::PingClient(*client).expired());
    EXPECT_FALSE(client->ping(id));
    EXPECT_TRUE(RealClientHeartbeatTestPeer::Heartbeats(*client).empty());
    ASSERT_EQ(setup(), 0);
    auto after_setup = client->ping(id);
    ASSERT_TRUE(after_setup);
    EXPECT_EQ(after_setup->client_status, ClientStatus::NEED_REMOUNT);
    EXPECT_EQ(client->tearDownAll(), 0);
    master.Stop();
}

TEST(RealClientHeartbeatTest, DummyAutomaticallyReregistersAfterExpiry) {
    testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto real = RealClient::create();
    const auto port = getFreeTcpPort();
    const auto endpoint = "127.0.0.1:" + std::to_string(port);
    const auto ipc = "@heartbeat_test_" + std::to_string(port);
    ASSERT_EQ(
        real->setup_real("127.0.0.1:" + std::to_string(getFreeTcpPort()),
                         "P2PHANDSHAKE", 16 * 1024 * 1024, 16 * 1024 * 1024,
                         "tcp", "", master.master_address(), nullptr, ipc),
        0);
    coro_rpc::coro_rpc_server server(2, port, "127.0.0.1");
    server.register_handler<&RealClient::service_ready_internal>(real.get());
    server.register_handler<&RealClient::ping>(real.get());
    server.register_handler<&RealClient::unmap_shm_internal>(real.get());
    auto running = server.async_start();
    ASSERT_FALSE(running.hasResult());
    DummyClient dummy;
    ASSERT_EQ(dummy.setup_dummy(16 * 1024 * 1024, 4096, endpoint, ipc), 0);
    auto* payload = reinterpret_cast<void*>(dummy.alloc_from_mem_pool(64));
    ASSERT_NE(payload, nullptr);
    const std::array<unsigned char, 64> expected{0x5a, 0x32, 0x71};
    std::memcpy(payload, expected.data(), expected.size());
    ASSERT_EQ(dummy.register_buffer(payload, expected.size()), 0);
    ASSERT_TRUE(Eventually([&] { return dummy.health_check() == HC_HEALTHY; }));
    UUID id;
    size_t mappings;
    {
        std::shared_lock lock(real->dummy_client_mutex_);
        ASSERT_EQ(real->shm_contexts_.size(), 1);
        id = real->shm_contexts_.begin()->first;
        mappings = real->shm_contexts_.at(id).mapped_shms.size();
    }
    ASSERT_GT(mappings, 0);
    ASSERT_TRUE(Eventually([&] {
        Expire(*real, id);
        return RealClientHeartbeatTestPeer::TryExpire(*real, id);
    }));
    ASSERT_TRUE(Eventually([&] {
        std::shared_lock lock(real->dummy_client_mutex_);
        auto it = real->shm_contexts_.find(id);
        if (it == real->shm_contexts_.end() ||
            it->second.mapped_shms.size() != mappings)
            return false;
        for (const auto& mapping : it->second.mapped_shms) {
            if (mapping.dummy_base_addr ==
                reinterpret_cast<uintptr_t>(payload)) {
                return std::memcmp(mapping.shm_buffer, expected.data(),
                                   expected.size()) == 0;
            }
        }
        return false;
    }));
    ASSERT_TRUE(Eventually([&] { return dummy.health_check() == HC_HEALTHY; }));
    EXPECT_EQ(dummy.tearDownAll(), 0);
    EXPECT_EQ(ShmHelper::getInstance()->free(payload), 0);
    server.stop();
    EXPECT_EQ(real->tearDownAll(), 0);
    master.Stop();
}

}  // namespace
}  // namespace mooncake
