#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstring>
#include <future>
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
    static auto& Registrations(RealClient& c) {
        return c.dummy_registrations_in_progress_;
    }
    static auto& LastRegistration(RealClient& c) {
        return c.last_dummy_registration_;
    }
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

TEST(RealClientHeartbeatTest, InitialControlRegistrationDoesNotRequestRemount) {
    RealClient client;
    const UUID id{1, 3};
    {
        RealClientHeartbeatTestPeer::RegistrationGuard control(client, id,
                                                               true);
    }
    {
        RealClientHeartbeatTestPeer::RegistrationGuard data(client, id, false);
    }
    EXPECT_FALSE(NeedsRemount(client, id));
}

TEST(RealClientHeartbeatTest, RegistrationAndGraceProtectQueuedClients) {
    RealClient client;
    const UUID registering{1, 1}, queued{1, 2};
    Expire(client, registering);
    Expire(client, queued);
    {
        RealClientHeartbeatTestPeer::RegistrationGuard registration(
            client, registering);
        RealClientHeartbeatTestPeer::TryExpire(client, queued);
        EXPECT_FALSE(NeedsRemount(client, registering));
        EXPECT_FALSE(NeedsRemount(client, queued));
    }
    RealClientHeartbeatTestPeer::TryExpire(client, queued);
    EXPECT_EQ(RealClientHeartbeatTestPeer::Registrations(client), 0u);
    EXPECT_FALSE(NeedsRemount(client, queued));
}

TEST(RealClientHeartbeatTest, ExpiryNotificationSurvivesLateRegistration) {
    RealClient client;
    const UUID id{3, 1};
    Expire(client, id);
    RealClientHeartbeatTestPeer::TryExpire(client, id);
    ASSERT_TRUE(NeedsRemount(client, id));
    EXPECT_TRUE(RealClientHeartbeatTestPeer::Heartbeats(client).empty());
    {
        RealClientHeartbeatTestPeer::RegistrationGuard late_registration(client,
                                                                         id);
        EXPECT_TRUE(NeedsRemount(client, id));
    }
    EXPECT_TRUE(NeedsRemount(client, id));
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
    ASSERT_EQ(
        client->setup_real("127.0.0.1:" + std::to_string(getFreeTcpPort()),
                           "P2PHANDSHAKE", 16 * 1024 * 1024, 16 * 1024 * 1024,
                           "tcp", "", master.master_address(), nullptr, ""),
        0);
    ASSERT_FALSE(RealClientHeartbeatTestPeer::PingClient(*client).expired());
    const UUID id{5, 1};
    ASSERT_TRUE(Eventually([&] {
        auto result = client->ping(id);
        return result && result->client_status == ClientStatus::OK;
    }));
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
        {
            std::lock_guard lock(RealClientHeartbeatTestPeer::Mutex(*real));
            RealClientHeartbeatTestPeer::LastRegistration(*real) =
                Clock::now() -
                std::chrono::seconds(DEFAULT_CLIENT_LIVE_TTL_SEC + 1);
        }
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
