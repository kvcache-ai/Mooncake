#include "uds_transport.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#ifdef __linux__
#include <sched.h>
#include <sys/wait.h>
#endif

namespace mooncake {
namespace {

std::string testSocketPath(const std::string &suffix) {
    return "uds_transport_test_" + std::to_string(getpid()) + "_" + suffix;
}

std::string testFilesystemSocketPath(const std::string &suffix) {
    return "/tmp/uds_transport_test_" + std::to_string(getpid()) + "_" +
           suffix + ".sock";
}

bool pathExists(const std::string &path) {
    struct stat st;
    return lstat(path.c_str(), &st) == 0;
}

std::string lockPathFor(const std::string &socket_path) {
    return socket_path + ".lock";
}

// Removes the socket file and the ownership lock file an acceptor leaves.
void removeSocketFiles(const std::string &path) {
    unlink(path.c_str());
    unlink(lockPathFor(path).c_str());
}

// Binds a socket at `path` without listening and returns its fd. Connecting
// to it fails with ECONNREFUSED, exactly like a stale file.
int bindWithoutListening(const std::string &path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Leaves a socket file with no listener behind, as a crashed owner would.
bool createStaleSocketFile(const std::string &path) {
    int fd = bindWithoutListening(path);
    if (fd < 0) return false;
    close(fd);
    return true;
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

TEST(UdsTransportTest, FilesystemSocketPassesFdAndRemovesFileOnStop) {
    std::string path = testFilesystemSocketPath("fs_fd");
    unlink(path.c_str());
    UdsAcceptor acceptor(path);
    std::atomic<bool> received{false};

    acceptor.registerHandler([&](UdsConnection &connection) {
        uint32_t marker = 0;
        int fd = connection.recvFd(&marker, sizeof(marker));
        ASSERT_GE(fd, 0);
        EXPECT_EQ(marker, 7u);
        EXPECT_EQ(readFdContent(fd), "fs-client-to-server");
        close(fd);
        received = true;
    });
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();

    struct stat st;
    ASSERT_EQ(lstat(path.c_str(), &st), 0);
    EXPECT_TRUE(S_ISSOCK(st.st_mode));
    // Only the owner's uid and gid may connect to the fd-passing endpoint.
    EXPECT_EQ(static_cast<unsigned>(st.st_mode & 0777), 0660u);

    UdsConnector connector(path);
    auto connection_result = connector.connect();
    ASSERT_TRUE(connection_result) << connection_result.error();
    auto connection = std::move(connection_result.value());

    int fd = createTempFd("fs-client-to-server");
    ASSERT_GE(fd, 0);
    uint32_t marker = 7;
    ASSERT_EQ(connection->sendFd(fd, &marker, sizeof(marker)), 0);
    close(fd);

    ASSERT_TRUE(waitForFlag(received));
    acceptor.stop();
    EXPECT_FALSE(pathExists(path));
    removeSocketFiles(path);
}

TEST(UdsTransportTest, FilesystemSocketReplacesStaleFile) {
    std::string path = testFilesystemSocketPath("fs_stale");
    unlink(path.c_str());
    ASSERT_TRUE(createStaleSocketFile(path));
    ASSERT_TRUE(pathExists(path));

    UdsAcceptor acceptor(path);
    acceptor.registerHandler([](UdsConnection &) {});
    auto start_result = acceptor.start();
    ASSERT_TRUE(start_result) << start_result.error();

    UdsConnector connector(path);
    EXPECT_TRUE(connector.connect());
    acceptor.stop();
    EXPECT_FALSE(pathExists(path));
    removeSocketFiles(path);
}

TEST(UdsTransportTest, FilesystemSocketRefusesLiveOwner) {
    std::string path = testFilesystemSocketPath("fs_live");
    unlink(path.c_str());
    UdsAcceptor owner(path);
    owner.registerHandler([](UdsConnection &) {});
    auto owner_result = owner.start();
    ASSERT_TRUE(owner_result) << owner_result.error();

    UdsAcceptor intruder(path);
    intruder.registerHandler([](UdsConnection &) {});
    auto intruder_result = intruder.start();
    ASSERT_FALSE(intruder_result);
    EXPECT_NE(intruder_result.error().find("in use"), std::string::npos);

    // The failed second acceptor must not have removed the owner's socket.
    UdsConnector connector(path);
    EXPECT_TRUE(connector.connect());
    owner.stop();
    removeSocketFiles(path);
}

TEST(UdsTransportTest, FilesystemSocketKeepsUnrelatedFile) {
    std::string path = testFilesystemSocketPath("fs_regular");
    {
        std::ofstream file(path);
        file << "not a socket";
    }

    UdsAcceptor acceptor(path);
    acceptor.registerHandler([](UdsConnection &) {});
    auto start_result = acceptor.start();
    ASSERT_FALSE(start_result);
    EXPECT_NE(start_result.error().find("not a socket"), std::string::npos);
    EXPECT_TRUE(pathExists(path));
    removeSocketFiles(path);
}

// Models a sibling starter that has bound the path but not yet called
// listen(): connecting gets ECONNREFUSED just like a stale file, so only the
// ownership lock tells them apart.
TEST(UdsTransportTest, FilesystemSocketKeepsSiblingBoundBeforeListen) {
    std::string path = testFilesystemSocketPath("fs_bound");
    removeSocketFiles(path);

    int lock_fd = open(lockPathFor(path).c_str(), O_RDWR | O_CREAT, 0660);
    ASSERT_GE(lock_fd, 0);
    ASSERT_EQ(flock(lock_fd, LOCK_EX | LOCK_NB), 0);
    int sibling_fd = bindWithoutListening(path);
    ASSERT_GE(sibling_fd, 0);
    struct stat before;
    ASSERT_EQ(lstat(path.c_str(), &before), 0);

    UdsAcceptor acceptor(path);
    acceptor.registerHandler([](UdsConnection &) {});
    auto start_result = acceptor.start();
    ASSERT_FALSE(start_result);
    EXPECT_NE(start_result.error().find("in use"), std::string::npos);

    // The sibling's socket file must be untouched.
    struct stat after;
    ASSERT_EQ(lstat(path.c_str(), &after), 0);
    EXPECT_EQ(after.st_ino, before.st_ino);

    // Once the sibling dies, the path is stale and can be taken over.
    close(sibling_fd);
    close(lock_fd);
    auto retry_result = acceptor.start();
    ASSERT_TRUE(retry_result) << retry_result.error();
    UdsConnector connector(path);
    EXPECT_TRUE(connector.connect());
    acceptor.stop();
    removeSocketFiles(path);
}

TEST(UdsTransportTest, FilesystemSocketConcurrentStartHasSingleOwner) {
    constexpr int kStarters = 8;
    std::string path = testFilesystemSocketPath("fs_race");
    // Start from a stale file so every starter sees ECONNREFUSED and would try
    // to remove it without the ownership lock.
    removeSocketFiles(path);
    ASSERT_TRUE(createStaleSocketFile(path));

    std::atomic<int> served_by{-1};
    std::vector<std::unique_ptr<UdsAcceptor>> acceptors;
    for (int i = 0; i < kStarters; ++i) {
        acceptors.push_back(std::make_unique<UdsAcceptor>(path));
        acceptors.back()->registerHandler(
            [&served_by, i](UdsConnection &) { served_by = i; });
    }

    std::atomic<bool> go{false};
    std::vector<int> started(kStarters, 0);
    std::vector<std::thread> threads;
    for (int i = 0; i < kStarters; ++i) {
        threads.emplace_back([&, i]() {
            while (!go.load()) std::this_thread::yield();
            started[i] = acceptors[i]->start() ? 1 : 0;
        });
    }
    go = true;
    for (auto &thread : threads) thread.join();

    int owner = -1;
    int owners = 0;
    for (int i = 0; i < kStarters; ++i) {
        if (started[i]) {
            owner = i;
            ++owners;
        }
    }
    ASSERT_EQ(owners, 1) << "exactly one starter may own the path";

    // The path must lead to the owner, not to a socket that was unlinked.
    UdsConnector connector(path);
    auto connection = connector.connect();
    ASSERT_TRUE(connection) << connection.error();
    for (int i = 0; i < 100 && served_by.load() < 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(served_by.load(), owner);

    // After the owner stops, another acceptor can take the path over.
    acceptors[owner]->stop();
    int next = (owner + 1) % kStarters;
    auto next_result = acceptors[next]->start();
    EXPECT_TRUE(next_result) << next_result.error();
    for (auto &acceptor : acceptors) acceptor->stop();
    removeSocketFiles(path);
}

TEST(UdsTransportTest, FilesystemSocketRejectsTooLongPath) {
    UdsAcceptor acceptor("/" + std::string(200, 'a'));
    acceptor.registerHandler([](UdsConnection &) {});
    auto start_result = acceptor.start();
    ASSERT_FALSE(start_result);
    EXPECT_NE(start_result.error().find("too long"), std::string::npos);
}

TEST(UdsTransportTest, ResolveClientIpcSocketName) {
    const char *saved = std::getenv(kIpcSocketPathEnv);
    std::string saved_value = saved ? saved : "";

    unsetenv(kIpcSocketPathEnv);
    EXPECT_EQ(resolveClientIpcSocketName("", 50052),
              "@mooncake_client_50052.sock");
    EXPECT_EQ(resolveClientIpcSocketName("/run/mooncake/ipc.sock", 50052),
              "/run/mooncake/ipc.sock");

    setenv(kIpcSocketPathEnv, "/var/run/mooncake/client.sock", 1);
    EXPECT_EQ(resolveClientIpcSocketName("", 50052),
              "/var/run/mooncake/client.sock");
    // An explicit value still wins over the environment.
    EXPECT_EQ(resolveClientIpcSocketName("@explicit.sock", 50052),
              "@explicit.sock");

    setenv(kIpcSocketPathEnv, "", 1);
    EXPECT_EQ(resolveClientIpcSocketName("", 50053),
              "@mooncake_client_50053.sock");

    if (saved) {
        setenv(kIpcSocketPathEnv, saved_value.c_str(), 1);
    } else {
        unsetenv(kIpcSocketPathEnv);
    }
}

#ifdef __linux__
// Models a dummy client in another pod: a peer in a different network
// namespace cannot reach an abstract socket, but can still reach a filesystem
// socket and pass an fd through it.
TEST(UdsTransportTest, FilesystemSocketCrossesNetworkNamespace) {
    constexpr uint8_t kAbstractReached = 1;
    constexpr uint8_t kFilesystemSentFd = 2;
    constexpr uint8_t kUnshareUnsupported = 0x80;

    const std::string abstract_name = testSocketPath("netns_abstract");
    const std::string fs_path = testFilesystemSocketPath("netns_fs");
    unlink(fs_path.c_str());

    int go_pipe[2];
    int result_pipe[2];
    ASSERT_EQ(pipe(go_pipe), 0);
    ASSERT_EQ(pipe(result_pipe), 0);

    // Fork before any acceptor thread exists: the child must stay
    // single-threaded so that unshare(CLONE_NEWUSER) is allowed and malloc is
    // safe after fork.
    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        close(go_pipe[1]);
        close(result_pipe[0]);
        uint8_t result = 0;
        if (unshare(CLONE_NEWNET) != 0 &&
            unshare(CLONE_NEWUSER | CLONE_NEWNET) != 0) {
            result = kUnshareUnsupported;
        }
        char go = 0;
        if (read(go_pipe[0], &go, 1) != 1) _exit(1);
        if (result == 0) {
            UdsConnector abstract_connector(abstract_name,
                                            std::chrono::milliseconds(200));
            if (abstract_connector.connect()) result |= kAbstractReached;

            UdsConnector fs_connector(fs_path, std::chrono::seconds(1));
            auto connection = fs_connector.connect();
            int fd = createTempFd("cross-netns");
            uint32_t marker = 5;
            if (connection && fd >= 0 &&
                (*connection)->sendFd(fd, &marker, sizeof(marker)) == 0) {
                result |= kFilesystemSentFd;
            }
            if (fd >= 0) close(fd);
        }
        if (write(result_pipe[1], &result, 1) != 1) _exit(1);
        _exit(0);
    }
    close(go_pipe[0]);
    close(result_pipe[1]);

    std::atomic<bool> fd_received{false};
    UdsAcceptor abstract_acceptor(abstract_name);
    abstract_acceptor.registerHandler([](UdsConnection &) {});
    UdsAcceptor fs_acceptor(fs_path);
    fs_acceptor.registerHandler([&](UdsConnection &connection) {
        uint32_t marker = 0;
        int fd = connection.recvFd(&marker, sizeof(marker));
        if (fd < 0) return;
        EXPECT_EQ(marker, 5u);
        EXPECT_EQ(readFdContent(fd), "cross-netns");
        close(fd);
        fd_received = true;
    });
    auto abstract_start = abstract_acceptor.start();
    auto fs_start = fs_acceptor.start();

    char go = 1;
    EXPECT_EQ(write(go_pipe[1], &go, 1), 1);
    close(go_pipe[1]);
    uint8_t result = 0;
    ssize_t n = read(result_pipe[0], &result, 1);
    close(result_pipe[0]);
    int status = 0;
    waitpid(pid, &status, 0);

    ASSERT_TRUE(abstract_start) << abstract_start.error();
    ASSERT_TRUE(fs_start) << fs_start.error();
    ASSERT_EQ(n, 1) << "child exited without reporting a result";
    if (result == kUnshareUnsupported) {
        abstract_acceptor.stop();
        fs_acceptor.stop();
        GTEST_SKIP() << "cannot create a network namespace here";
    }
    EXPECT_FALSE(result & kAbstractReached)
        << "abstract socket must not be visible from another netns";
    EXPECT_TRUE(result & kFilesystemSentFd)
        << "filesystem socket must be reachable from another netns";
    EXPECT_TRUE(waitForFlag(fd_received));

    abstract_acceptor.stop();
    fs_acceptor.stop();
    removeSocketFiles(fs_path);
}
#endif  // __linux__

}  // namespace mooncake
