#include <gtest/gtest.h>

#include <csignal>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "ha/snapshot/snapshot_socket_util.h"

namespace mooncake::test {

class SnapshotSocketUtilTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Suppress SIGPIPE so that writing to a closed socket returns an error
        // instead of killing the process.
        signal(SIGPIPE, SIG_IGN);

        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv_), 0)
            << "Failed to create socketpair: " << strerror(errno);
    }

    void TearDown() override {
        if (sv_[0] >= 0) close(sv_[0]);
        if (sv_[1] >= 0) close(sv_[1]);
    }

    // sv_[0] and sv_[1] are the two ends of the socketpair.
    int sv_[2] = {-1, -1};
};

TEST_F(SnapshotSocketUtilTest, ReadWriteExact_Roundtrip) {
    const std::vector<uint8_t> data = {0, 1, 2, 42, 128, 254, 255};

    ASSERT_TRUE(snapshot_socket::WriteExact(sv_[0], data.data(), data.size()));

    std::vector<uint8_t> buf(data.size());
    ASSERT_TRUE(snapshot_socket::ReadExact(sv_[1], buf.data(), buf.size()));
    EXPECT_EQ(buf, data);
}

TEST_F(SnapshotSocketUtilTest, ReadWriteExact_LargeBuffer) {
    // 4 MB — much larger than the kernel socket buffer (~200 KB), which forces
    // partial reads/writes internally.
    constexpr size_t kSize = 4 * 1024 * 1024;
    std::vector<uint8_t> data(kSize);
    std::iota(data.begin(), data.end(), uint8_t{0});

    // Writer must run in a separate thread because the socket buffer will fill
    // up and block before all bytes are written.
    std::thread writer([&] {
        EXPECT_TRUE(snapshot_socket::WriteExact(sv_[0], data.data(), data.size()));
    });

    std::vector<uint8_t> buf(kSize);
    ASSERT_TRUE(snapshot_socket::ReadExact(sv_[1], buf.data(), buf.size()));
    EXPECT_EQ(buf, data);

    writer.join();
}

TEST_F(SnapshotSocketUtilTest, ReadExact_PeerClose_ReturnsFalse) {
    // Close the write end, then try to read — should return false (EOF).
    close(sv_[0]);
    sv_[0] = -1;

    uint8_t buf[16];
    EXPECT_FALSE(snapshot_socket::ReadExact(sv_[1], buf, sizeof(buf)));
}

TEST_F(SnapshotSocketUtilTest, WriteExact_ClosedFd_ReturnsFalse) {
    // Close the read end, then try to write — should return false (EPIPE).
    close(sv_[1]);
    sv_[1] = -1;

    const uint8_t data[16] = {};
    EXPECT_FALSE(snapshot_socket::WriteExact(sv_[0], data, sizeof(data)));
}

TEST_F(SnapshotSocketUtilTest, ReadWriteExact_ZeroLength) {
    // Zero-length operations should succeed immediately without touching the
    // socket.
    EXPECT_TRUE(snapshot_socket::ReadExact(sv_[0], nullptr, 0));
    EXPECT_TRUE(snapshot_socket::WriteExact(sv_[1], nullptr, 0));
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
