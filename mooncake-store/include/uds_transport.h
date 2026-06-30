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

// Lightweight Unix domain socket helper for one-shot, performance-insensitive
// control-plane exchanges such as local fd passing. This opens short-lived
// blocking sockets and handles one connection at a time; use asio-based
// transport instead for latency-sensitive, high-throughput, or long-lived I/O.
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

    std::string socket_name_;
    Handler handler_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::jthread thread_;
};

}  // namespace mooncake
