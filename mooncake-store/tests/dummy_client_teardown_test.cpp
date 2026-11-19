#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

#include "dummy_client.h"

namespace mooncake {

// Own a blocked background reader without depending on RPC reconnect timing.
class DummyClientTeardownTestPeer {
   public:
    static void StartReader(DummyClient& client, void* primary,
                            std::promise<void>& stopped,
                            std::shared_future<void> release) {
        client.shm_helper_ = ShmHelper::getInstance();
        client.local_buffer_base_ = primary;
        client.ping_running_ = true;
        client.ping_thread_ = std::thread([&client, &stopped, release] {
            while (client.ping_running_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            stopped.set_value();
            release.wait();
        });
    }

    static void RequestStop(DummyClient& client) {
        client.ping_running_ = false;
    }

    static void StartStoppedThread(DummyClient& client) {
        client.ping_thread_ = std::thread([] {});
    }

    static bool Joinable(const DummyClient& client) {
        return client.ping_thread_.joinable();
    }

    static void Join(DummyClient& client) {
        if (client.ping_thread_.joinable()) client.ping_thread_.join();
    }
};

TEST(DummyClientTeardownTest, RetainsPrimaryShmUntilBackgroundReaderExits) {
    using namespace std::chrono_literals;
    DummyClient client;
    auto* helper = ShmHelper::getInstance();
    void* primary = helper->allocate(4096);
    ASSERT_NE(primary, nullptr);
    std::promise<void> stopped;
    auto stop_seen = stopped.get_future();
    std::promise<void> release;
    DummyClientTeardownTestPeer::StartReader(client, primary, stopped,
                                             release.get_future().share());

    auto close =
        std::async(std::launch::async, [&] { return client.tearDownAll(); });
    const auto status = stop_seen.wait_for(5s);
    const bool still_mapped = helper->get_shm(primary) != nullptr;
    // Always release the reader, including when the ordering assertion fails.
    DummyClientTeardownTestPeer::RequestStop(client);
    release.set_value();
    EXPECT_EQ(close.get(), 0);
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(still_mapped);
    EXPECT_EQ(helper->get_shm(primary), nullptr);
}

TEST(DummyClientTeardownTest, JoinsAnAlreadyStoppedBackgroundThread) {
    DummyClient client;
    DummyClientTeardownTestPeer::StartStoppedThread(client);
    EXPECT_EQ(client.tearDownAll(), 0);
    const bool still_joinable = DummyClientTeardownTestPeer::Joinable(client);
    // Keep the unfixed negative control from terminating in
    // std::thread::~thread.
    DummyClientTeardownTestPeer::Join(client);
    EXPECT_FALSE(still_joinable);
    EXPECT_EQ(client.tearDownAll(), 0);
}

}  // namespace mooncake
