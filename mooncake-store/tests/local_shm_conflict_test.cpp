#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <memory>
#include <vector>

#include "common/network.h"
#include "real_client.h"
#include "test_server_helpers.h"

namespace mooncake {
namespace {

class LocalShmConflictTest : public ::testing::Test {
   protected:
    static constexpr size_t kSize = facebook::cachelib::Slab::kSize;
    static constexpr uintptr_t kFirstAddress = 0x100000000ULL;
    static constexpr uintptr_t kSecondAddress = 0x200000000ULL;

    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        engine_ = std::make_shared<TransferEngine>(false);
        const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        ASSERT_EQ(engine_->init("P2PHANDSHAKE", endpoint), 0);
        ASSERT_NE(engine_->installTransport("tcp", nullptr), nullptr);
        client_ = RealClient::create();
        ASSERT_TRUE(client_->setup_internal(endpoint, "P2PHANDSHAKE", 0, 0,
                                            "tcp", "", master_.master_address(),
                                            engine_));
    }

    void TearDown() override {
        if (client_) {
            client_->unmap_shm_internal(id_);
            client_->tearDownAll();
            client_.reset();
        }
        engine_.reset();
        master_.Stop();
    }

    int NewFd() {
        const int fd = memfd_create("local-shm-conflict-test", MFD_CLOEXEC);
        if (fd < 0) return fd;
        if (ftruncate(fd, kSize) != 0) {
            close(fd);
            return -1;
        }
        return fd;
    }

    void ExpectClosed(int fd) {
        errno = 0;
        EXPECT_EQ(fcntl(fd, F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
    }

    std::vector<SegmentBufferInfo> Buffers() {
        std::vector<SegmentBufferInfo> result;
        const auto segment = engine_->openSegment(engine_->getLocalIpAndPort());
        EXPECT_EQ(engine_->getSegmentBuffers(segment, result), 0);
        return result;
    }

    testing::InProcMaster master_;
    std::shared_ptr<TransferEngine> engine_;
    std::shared_ptr<RealClient> client_;
    const UUID id_{12345, 67890};
};

TEST_F(LocalShmConflictTest, ConflictingLocalBufferKeepsOriginalRegistration) {
    const int first = NewFd();
    ASSERT_GE(first, 0);
    // Retain a sender-side view to verify that the original receiver mapping
    // remains attached to the same shared memory after rejecting the conflict.
    void* sender =
        mmap(nullptr, kSize, PROT_READ | PROT_WRITE, MAP_SHARED, first, 0);
    ASSERT_NE(sender, MAP_FAILED);
    ASSERT_TRUE(
        client_->map_shm_internal(first, kFirstAddress, kSize, true, id_));
    ExpectClosed(first);
    const auto before = Buffers();
    ASSERT_EQ(before.size(), 1);
    const int second = NewFd();
    ASSERT_GE(second, 0);
    const auto conflict =
        client_->map_shm_internal(second, kSecondAddress, kSize, true, id_);
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    ExpectClosed(second);
    const auto after = Buffers();
    EXPECT_EQ(after.size(), before.size());
    ASSERT_FALSE(after.empty());
    EXPECT_EQ(after.front().addr, before.front().addr);
    *static_cast<unsigned char*>(sender) = 42;
    EXPECT_EQ(*reinterpret_cast<unsigned char*>(before.front().addr), 42);
    EXPECT_EQ(munmap(sender, kSize), 0);
    EXPECT_TRUE(client_->unmap_shm_internal(id_));
    EXPECT_TRUE(Buffers().empty());
}

TEST_F(LocalShmConflictTest, SameAddressIsIdempotentAndConsumesFd) {
    const int first = NewFd();
    ASSERT_GE(first, 0);
    ASSERT_TRUE(
        client_->map_shm_internal(first, kFirstAddress, kSize, true, id_));
    const auto before = Buffers();
    ASSERT_EQ(before.size(), 1);
    const int second = NewFd();
    ASSERT_GE(second, 0);
    EXPECT_TRUE(
        client_->map_shm_internal(second, kFirstAddress, kSize, true, id_));
    ExpectClosed(second);
    const auto after = Buffers();
    ASSERT_EQ(after.size(), 1);
    EXPECT_EQ(after.front().addr, before.front().addr);
}

TEST_F(LocalShmConflictTest, AdditionalNonLocalBufferIsAllowed) {
    const int first = NewFd();
    ASSERT_GE(first, 0);
    ASSERT_TRUE(
        client_->map_shm_internal(first, kFirstAddress, kSize, true, id_));
    const int second = NewFd();
    ASSERT_GE(second, 0);
    EXPECT_TRUE(
        client_->map_shm_internal(second, kSecondAddress, kSize, false, id_));
    ExpectClosed(second);
    EXPECT_EQ(Buffers().size(), 2);
    EXPECT_TRUE(client_->unmap_shm_internal(id_));
    EXPECT_TRUE(Buffers().empty());
}

}  // namespace
}  // namespace mooncake
