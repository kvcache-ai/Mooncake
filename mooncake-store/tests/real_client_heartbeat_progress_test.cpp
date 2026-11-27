#include <gtest/gtest.h>

#include <chrono>
#include <future>

#include "real_client.h"

namespace mooncake {

TEST(RealClientHeartbeatProgressTest, PingDoesNotWaitForShmRegistrationLock) {
    using namespace std::chrono_literals;
    RealClient client;
    std::unique_lock shm_lock(client.dummy_client_mutex_);
    auto ping =
        std::async(std::launch::async, [&] { return client.ping(UUID{1, 1}); });
    const auto status = ping.wait_for(2s);
    shm_lock.unlock();
    // An uninitialized client still reports an error, without waiting for SHM.
    EXPECT_FALSE(ping.get().has_value());
    EXPECT_EQ(status, std::future_status::ready);
}

}  // namespace mooncake
