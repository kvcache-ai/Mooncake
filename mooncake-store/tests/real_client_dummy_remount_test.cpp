// A dummy client that misses pings for the live TTL has its shared memory
// unmapped by the real client's monitor. Ping must then report NEED_REMOUNT
// until the dummy maps every expired segment again; otherwise the dummy keeps
// seeing OK and every later put/get fails with shm_not_mapped.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "common/network.h"
#include "real_client.h"
#include "rpc_types.h"
#include "test_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {
namespace {

constexpr size_t kMB = 1024ULL * 1024;
constexpr size_t kShmSize = 4 * kMB;
// Dummy-side addresses only key the mapping; the real client never
// dereferences them.
constexpr uint64_t kPoolAddr = 0x7f0000000000ULL;
constexpr uint64_t kBufferAddr = 0x7f1000000000ULL;
constexpr uint64_t kNewPoolAddr = 0x7f2000000000ULL;

class RealClientDummyRemountTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        client_ = RealClient::create();
        ASSERT_NE(client_, nullptr);
        const std::string hostname =
            "localhost:" + std::to_string(getFreeTcpPort());
        const std::string ipc_path =
            "@dummy_remount_test_" + std::to_string(getpid()) + ".sock";
        ASSERT_EQ(client_->setup_real(
                      hostname, "P2PHANDSHAKE", 16 * kMB, 16 * kMB, "tcp", "",
                      master_.master_address(), nullptr, ipc_path),
                  0);
        ASSERT_EQ(client_->start_dummy_client_monitor(), 0);
        // Ping only reports remounts while the real client reaches the master.
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (Ping() != ClientStatus::OK &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_EQ(Ping(), ClientStatus::OK);
    }

    void TearDown() override {
        if (client_) {
            client_->stop_dummy_client_monitor();
            client_->tearDownAll();
        }
        master_.Stop();
    }

    void Map(uint64_t dummy_addr) {
        const int fd = memfd_create("dummy_remount_test", MFD_CLOEXEC);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, kShmSize), 0);
        // map_shm_internal takes ownership of the descriptor.
        ASSERT_TRUE(client_
                        ->map_shm_internal(fd, dummy_addr, kShmSize,
                                           /*is_local_buffer=*/false,
                                           client_id_)
                        .has_value());
    }

    bool IsMapped(uint64_t dummy_addr) {
        auto mapped = client_->is_shm_mapped_internal(dummy_addr, client_id_);
        EXPECT_TRUE(mapped.has_value());
        return mapped.value_or(false);
    }

    ClientStatus Ping() {
        auto response = client_->ping(client_id_);
        EXPECT_TRUE(response.has_value());
        return response ? response->client_status : ClientStatus::UNDEFINED;
    }

    // Stop pinging until the monitor expires the dummy.
    static void MissPings() {
        std::this_thread::sleep_for(
            std::chrono::seconds(DEFAULT_CLIENT_LIVE_TTL_SEC + 3));
    }

    InProcMaster master_;
    std::shared_ptr<RealClient> client_;
    const UUID client_id_ = generate_uuid();
};

TEST_F(RealClientDummyRemountTest,
       ExpiredClientRemountsUntilEverySegmentIsMapped) {
    Map(kPoolAddr);
    Map(kBufferAddr);
    ASSERT_EQ(Ping(), ClientStatus::OK);

    MissPings();
    ASSERT_FALSE(IsMapped(kPoolAddr));
    ASSERT_FALSE(IsMapped(kBufferAddr));
    EXPECT_EQ(Ping(), ClientStatus::NEED_REMOUNT);

    Map(kPoolAddr);
    EXPECT_EQ(Ping(), ClientStatus::NEED_REMOUNT)
        << "a remount is not complete until every expired segment is mapped";

    Map(kBufferAddr);
    EXPECT_EQ(Ping(), ClientStatus::OK);
}

TEST_F(RealClientDummyRemountTest, UnregisteringAnExpiredSegmentEndsRemount) {
    Map(kPoolAddr);
    ASSERT_EQ(Ping(), ClientStatus::OK);

    MissPings();
    ASSERT_EQ(Ping(), ClientStatus::NEED_REMOUNT);

    ASSERT_TRUE(client_->unregister_shm_buffer_internal(kPoolAddr, client_id_)
                    .has_value());
    EXPECT_EQ(Ping(), ClientStatus::OK);
}

TEST_F(RealClientDummyRemountTest,
       ExplicitTeardownClearsRemountForReregisteredClient) {
    Map(kPoolAddr);
    MissPings();
    ASSERT_EQ(Ping(), ClientStatus::NEED_REMOUNT);

    // The monitor already removed the active context. An explicit teardown
    // still has to clear the expired client's remount record.
    ASSERT_EQ(client_->unmap_shm_internal(client_id_).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(Ping(), ClientStatus::OK);

    EXPECT_TRUE(
        client_->is_shm_mapped_internal(kPoolAddr, client_id_).has_value());
    Map(kNewPoolAddr);
    EXPECT_EQ(Ping(), ClientStatus::OK);
}

TEST_F(RealClientDummyRemountTest, ExpiredClientWithoutSegmentsStaysOk) {
    ASSERT_EQ(Ping(), ClientStatus::OK);
    MissPings();
    EXPECT_EQ(Ping(), ClientStatus::OK);
}

}  // namespace
}  // namespace testing
}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    return RUN_ALL_TESTS();
}
