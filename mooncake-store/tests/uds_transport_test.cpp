#include "uds_transport.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>

namespace mooncake {
namespace {

std::string testSocketPath(const std::string &suffix) {
    return "uds_transport_test_" + std::to_string(getpid()) + "_" + suffix;
}

int createTempFd(const char *content) {
    char path[] = "/tmp/uds_transport_test_XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return -1;
    unlink(path);
    if (write(fd, content, strlen(content)) < 0) {
        close(fd);
        return -1;
    }
    lseek(fd, 0, SEEK_SET);
    return fd;
}

std::string readFdContent(int fd) {
    char buffer[64] = {};
    lseek(fd, 0, SEEK_SET);
    ssize_t n = read(fd, buffer, sizeof(buffer) - 1);
    if (n < 0) return "";
    return std::string(buffer, static_cast<size_t>(n));
}

bool waitForFlag(const std::atomic<bool> &flag) {
    for (int i = 0; i < 100; ++i) {
        if (flag.load()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return flag.load();
}

}  // namespace

TEST(UdsTransportTest, SendsRawPayload) {
    UdsAcceptor acceptor(testSocketPath("raw"));
    std::atomic<bool> handled{false};

    acceptor.registerHandler([&](UdsConnection &connection) {
        uint32_t value = 0;
        EXPECT_EQ(connection.recvRaw(&value, sizeof(value)), 0);
        value += 1;
        EXPECT_EQ(connection.sendRaw(&value, sizeof(value)), 0);
        handled = true;
    });
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();

    UdsConnector connector(testSocketPath("raw"));
    auto connection_result = connector.connect();
    ASSERT_TRUE(connection_result) << connection_result.error();
    auto connection = std::move(connection_result.value());

    uint32_t value = 41;
    ASSERT_EQ(connection->sendRaw(&value, sizeof(value)), 0);
    ASSERT_EQ(connection->recvRaw(&value, sizeof(value)), 0);
    EXPECT_EQ(value, 42);

    acceptor.stop();
    EXPECT_TRUE(handled.load());
}

TEST(UdsTransportTest, SendsFdFromClientToServer) {
    UdsAcceptor acceptor(testSocketPath("client_fd"));
    std::atomic<bool> received{false};

    acceptor.registerHandler([&](UdsConnection &connection) {
        uint32_t marker = 0;
        int fd = connection.recvFd(&marker, sizeof(marker));
        ASSERT_GE(fd, 0);
        EXPECT_EQ(marker, 7u);
        EXPECT_EQ(readFdContent(fd), "client-to-server");
        close(fd);
        received = true;
    });
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();

    UdsConnector connector(testSocketPath("client_fd"));
    auto connection_result = connector.connect();
    ASSERT_TRUE(connection_result) << connection_result.error();
    auto connection = std::move(connection_result.value());

    int fd = createTempFd("client-to-server");
    ASSERT_GE(fd, 0);
    uint32_t marker = 7;
    ASSERT_EQ(connection->sendFd(fd, &marker, sizeof(marker)), 0);
    close(fd);

    ASSERT_TRUE(waitForFlag(received));
    acceptor.stop();
    EXPECT_TRUE(received.load());
}

TEST(UdsTransportTest, RejectsFdWithPartialPayload) {
    int sockets[2] = {-1, -1};
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets), 0);

    UdsConnection receiver(sockets[1]);
    int fd = createTempFd("partial-payload");
    ASSERT_GE(fd, 0);

    uint32_t marker = 7;
    iovec iov;
    iov.iov_base = &marker;
    iov.iov_len = sizeof(marker) - 1;

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));

    msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &fd, sizeof(int));

    ASSERT_EQ(sendmsg(sockets[0], &msg, 0),
              static_cast<ssize_t>(sizeof(marker) - 1));
    close(fd);
    close(sockets[0]);

    uint32_t received_marker = 0;
    EXPECT_LT(receiver.recvFd(&received_marker, sizeof(received_marker)), 0);
}

TEST(UdsTransportTest, SendsFdFromServerToClient) {
    UdsAcceptor acceptor(testSocketPath("server_fd"));

    acceptor.registerHandler([&](UdsConnection &connection) {
        int fd = createTempFd("server-to-client");
        ASSERT_GE(fd, 0);
        uint32_t marker = 9;
        EXPECT_EQ(connection.sendFd(fd, &marker, sizeof(marker)), 0);
        close(fd);
    });
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();

    UdsConnector connector(testSocketPath("server_fd"));
    auto connection_result = connector.connect();
    ASSERT_TRUE(connection_result) << connection_result.error();
    auto connection = std::move(connection_result.value());

    uint32_t marker = 0;
    int fd = connection->recvFd(&marker, sizeof(marker));
    ASSERT_GE(fd, 0);
    EXPECT_EQ(marker, 9u);
    EXPECT_EQ(readFdContent(fd), "server-to-client");
    close(fd);

    acceptor.stop();
}

TEST(UdsTransportTest, StopWakesAcceptLoop) {
    UdsAcceptor acceptor(testSocketPath("stop"));
    acceptor.registerHandler([](UdsConnection &) {});
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();
    acceptor.stop();

    UdsConnector connector(testSocketPath("stop"));
    EXPECT_FALSE(connector.connect());
}

TEST(UdsTransportTest, StopWakesActiveClientHandler) {
    UdsAcceptor acceptor(testSocketPath("active_stop"));
    std::atomic<bool> entered{false};
    std::atomic<bool> exited{false};

    acceptor.registerHandler([&](UdsConnection &connection) {
        entered = true;
        uint32_t value = 0;
        EXPECT_EQ(connection.recvRaw(&value, sizeof(value)), -1);
        exited = true;
    });
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();

    UdsConnector connector(testSocketPath("active_stop"));
    auto connection_result = connector.connect();
    ASSERT_TRUE(connection_result) << connection_result.error();

    ASSERT_TRUE(waitForFlag(entered));
    acceptor.stop();
    EXPECT_TRUE(exited.load());
}

TEST(UdsTransportTest, ConnectRejectsNonPositiveTimeout) {
    UdsConnector connector(testSocketPath("invalid_timeout"),
                           std::chrono::milliseconds(0));
    auto connection_result = connector.connect();
    ASSERT_FALSE(connection_result);
    EXPECT_NE(connection_result.error().find("timeout"), std::string::npos);
}

TEST(UdsTransportTest, ConnectFailureReturnsPromptlyWithShortTimeout) {
    std::string socket_name = testSocketPath("missing");
    UdsConnector connector(socket_name, std::chrono::milliseconds(10));

    auto start = std::chrono::steady_clock::now();
    auto connection_result = connector.connect();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_FALSE(connection_result);
    EXPECT_LT(elapsed, std::chrono::seconds(1));
    EXPECT_NE(connection_result.error().find(socket_name), std::string::npos);
}

TEST(UdsTransportTest, ConnectClearsSendTimeout) {
    UdsAcceptor acceptor(testSocketPath("send_timeout"));
    acceptor.registerHandler([](UdsConnection &) {});
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();

    UdsConnector connector(testSocketPath("send_timeout"),
                           std::chrono::milliseconds(10));
    auto connection_result = connector.connect();
    ASSERT_TRUE(connection_result) << connection_result.error();
    auto connection = std::move(connection_result.value());

    timeval tv;
    socklen_t tv_len = sizeof(tv);
    ASSERT_EQ(
        getsockopt(connection->fd(), SOL_SOCKET, SO_SNDTIMEO, &tv, &tv_len), 0);
    EXPECT_EQ(tv.tv_sec, 0);
    EXPECT_EQ(tv.tv_usec, 0);

    acceptor.stop();
}

TEST(UdsTransportTest, StartRequiresRegisteredHandler) {
    UdsAcceptor acceptor(testSocketPath("no_handler"));
    auto start_result = acceptor.start();
    ASSERT_FALSE(start_result);
    EXPECT_NE(start_result.error().find("handler"), std::string::npos);
}

}  // namespace mooncake
