#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

// Environment variable that overrides the dummy/real client IPC socket name
// when no explicit name is configured.
inline constexpr const char *kIpcSocketPathEnv = "MOONCAKE_IPC_SOCKET_PATH";

// Resolves the IPC socket name shared by a real client and its dummy clients.
// Order: `configured`, then $MOONCAKE_IPC_SOCKET_PATH, then the default
// abstract name "@mooncake_client_<port>.sock". Both sides must apply the same
// rule so that they meet on the same socket.
std::string resolveClientIpcSocketName(const std::string &configured, int port);

// Lightweight Unix domain socket helper for one-shot, performance-insensitive
// control-plane exchanges such as local fd passing. This opens short-lived
// blocking sockets and handles one connection at a time; use asio-based
// transport instead for latency-sensitive, high-throughput, or long-lived I/O.
//
// A socket name starting with '/' is a filesystem path, reachable by any
// process that can see the path (e.g. pods sharing a hostPath) and runs with
// the owner's uid or gid: the socket is created with mode 0660. The acceptor
// also holds an exclusive flock on "<path>.lock" while it runs, so only one
// acceptor at a time owns the path. Any other name is a Linux abstract socket,
// visible only inside the same network namespace.
class UdsConnection {
   public:
    UdsConnection() = default;
    explicit UdsConnection(int fd);
    ~UdsConnection();

    UdsConnection(const UdsConnection &) = delete;
    UdsConnection &operator=(const UdsConnection &) = delete;

    UdsConnection(UdsConnection &&other) noexcept;
    UdsConnection &operator=(UdsConnection &&other) noexcept;

    bool valid() const;
    int fd() const;
    int release();
    void close();
    tl::expected<void, std::string> setRecvTimeout(
        std::chrono::seconds timeout);

    int sendRaw(const void *data, size_t len);
    int recvRaw(void *data, size_t len);
    int sendFd(int fd, void *data, size_t data_len);
    int recvFd(void *data, size_t data_len);

   private:
    int fd_ = -1;
};

class UdsConnector {
   public:
    explicit UdsConnector(
        std::string socket_name,
        std::chrono::milliseconds connect_timeout = std::chrono::seconds(5));
    tl::expected<std::unique_ptr<UdsConnection>, std::string> connect();

   private:
    std::string socket_name_;
    std::chrono::milliseconds connect_timeout_;
};

class UdsAcceptor {
   public:
    using Handler = std::function<void(UdsConnection &)>;

    explicit UdsAcceptor(std::string socket_name);
    ~UdsAcceptor();

    void registerHandler(Handler handler);
    tl::expected<void, std::string> start();
    void stop();

   private:
    void acceptLoop();
    void wakeAccept();
    void unlinkBoundPath();
    void releaseOwnershipLock();

    std::string socket_name_;
    // Filesystem path bound by this acceptor and removed on stop; empty for
    // abstract sockets.
    std::string bound_socket_path_;
    Handler handler_;
    int listen_fd_ = -1;
    // Holds the flock on "<socket_name_>.lock" while running; -1 for abstract
    // sockets.
    int lock_fd_ = -1;
    std::atomic<int> active_client_fd_{-1};
    std::atomic<bool> running_{false};
    std::thread thread_;
};

}  // namespace mooncake
