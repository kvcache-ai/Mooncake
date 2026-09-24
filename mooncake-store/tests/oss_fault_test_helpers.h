#pragma once

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake::test {

// Loopback-only, path-routed fault injection. Replies do not depend on the
// arrival order of concurrent requests. Stop() never closes a descriptor owned
// by a server thread: handlers observe stop_ in bounded poll intervals.
class OssFaultHttpServer {
   public:
    struct Response {
        enum class Action { kReply, kReset, kStall, kGate };
        Action action = Action::kReply;
        std::string wire;
    };

    static Response Reply(int status, const std::string& body,
                          const std::string& extra_headers = "") {
        return {Response::Action::kReply,
                "HTTP/1.1 " + std::to_string(status) +
                    " Test\r\nContent-Length: " + std::to_string(body.size()) +
                    "\r\nConnection: close\r\n" + extra_headers + "\r\n" +
                    body};
    }
    static Response Raw(std::string wire) {
        return {Response::Action::kReply, std::move(wire)};
    }
    static Response Reset() { return {Response::Action::kReset, {}}; }
    static Response Stall() { return {Response::Action::kStall, {}}; }
    static Response Gate(int status, const std::string& body,
                         const std::string& extra_headers = "") {
        auto response = Reply(status, body, extra_headers);
        response.action = Response::Action::kGate;
        return response;
    }

    explicit OssFaultHttpServer(std::map<std::string, Response> routes,
                                bool concurrent = false)
        : routes_(std::move(routes)), concurrent_(concurrent) {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) throw std::runtime_error("socket failed");
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) != 0 ||
            listen(listen_fd_, 32) != 0 || !SetNonblocking(listen_fd_)) {
            close(listen_fd_);
            throw std::runtime_error("loopback listener setup failed");
        }
        socklen_t length = sizeof(address);
        if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                        &length) != 0) {
            close(listen_fd_);
            throw std::runtime_error("getsockname failed");
        }
        port_ = ntohs(address.sin_port);
        try {
            thread_ = std::thread([this] { Serve(); });
        } catch (...) {
            close(listen_fd_);
            throw;
        }
    }

    ~OssFaultHttpServer() { Stop(); }

    void Stop() {
        {
            std::lock_guard lock(mutex_);
            stop_.store(true);
        }
        condition_.notify_all();
        if (thread_.joinable()) thread_.join();
        // Only the accept thread adds handlers; join it before reading them.
        for (auto& handler : handlers_) {
            if (handler.joinable()) handler.join();
        }
        if (listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    bool WaitForRequest(
        const std::string& key,
        std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        std::unique_lock lock(mutex_);
        auto received = [&] {
            return std::find(request_keys_.begin(), request_keys_.end(), key) !=
                   request_keys_.end();
        };
        condition_.wait_for(lock, timeout,
                            [&] { return stop_.load() || received(); });
        return received();
    }

    void ReleaseResponses() {
        {
            std::lock_guard lock(mutex_);
            responses_released_ = true;
        }
        condition_.notify_all();
    }

    uint16_t port() const { return port_; }
    std::string error() const {
        std::lock_guard lock(mutex_);
        return error_;
    }
    std::vector<std::string> requests() const {
        std::lock_guard lock(mutex_);
        return requests_;
    }

   private:
    using Clock = std::chrono::steady_clock;

    static bool SetNonblocking(int fd) {
        const int flags = fcntl(fd, F_GETFL, 0);
        return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
    }

    void SetError(const char* error) {
        std::lock_guard lock(mutex_);
        if (error_.empty()) error_ = error;
    }

    bool Ready(int fd, short events, Clock::time_point deadline) {
        while (!stop_.load() && Clock::now() < deadline) {
            pollfd descriptor{fd, events, 0};
            const int ready = poll(&descriptor, 1, 25);
            if (ready > 0) return true;
            if (ready < 0 && errno != EINTR) return false;
        }
        return false;
    }

    std::string ReadRequest(int fd) {
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        std::string request;
        std::array<char, 4096> buffer{};
        std::optional<size_t> request_size;
        while (Ready(fd, POLLIN, deadline)) {
            const ssize_t count = recv(fd, buffer.data(), buffer.size(), 0);
            if (count < 0 && (errno == EAGAIN || errno == EINTR)) continue;
            if (count <= 0) return {};
            request.append(buffer.data(), static_cast<size_t>(count));
            if (request.size() > 1024 * 1024) return {};
            const size_t header_end = request.find("\r\n\r\n");
            if (!request_size && header_end != std::string::npos) {
                std::string headers = request.substr(0, header_end);
                std::transform(headers.begin(), headers.end(), headers.begin(),
                               [](unsigned char c) { return std::tolower(c); });
                constexpr const char* field = "\r\ncontent-length:";
                const size_t field_pos = headers.find(field);
                size_t body_size = 0;
                if (field_pos != std::string::npos) {
                    try {
                        body_size = std::stoull(headers.substr(field_pos + 17));
                    } catch (...) {
                        return {};
                    }
                }
                if (body_size > 1024 * 1024) return {};
                request_size = header_end + 4 + body_size;
            }
            if (request_size && request.size() >= *request_size) return request;
        }
        return {};
    }

    void SendReply(int fd, const std::string& wire) {
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        size_t sent = 0;
        while (sent < wire.size() && Ready(fd, POLLOUT, deadline)) {
            const ssize_t count =
                send(fd, wire.data() + sent, wire.size() - sent, MSG_NOSIGNAL);
            if (count < 0 && (errno == EAGAIN || errno == EINTR)) continue;
            // The client may legitimately abort an error or malformed body.
            if (count <= 0) return;
            sent += static_cast<size_t>(count);
        }
    }

    void StallUntilPeerCloses(int fd) {
        // Production currently has a 120 s request deadline. The test server
        // holds the connection beyond that deadline; Stop() still exits within
        // a poll interval, including when a test assertion fails.
        const auto deadline = Clock::now() + std::chrono::seconds(135);
        char byte;
        while (Ready(fd, POLLIN, deadline)) {
            const ssize_t count = recv(fd, &byte, 1, 0);
            if (count == 0) return;
            if (count < 0 && errno != EAGAIN && errno != EINTR) return;
        }
    }

    bool WaitForRelease() {
        std::unique_lock lock(mutex_);
        if (!condition_.wait_for(lock, std::chrono::seconds(5), [&] {
                return stop_.load() || responses_released_;
            })) {
            if (error_.empty())
                error_ = "timed out waiting to release response";
            return false;
        }
        return !stop_.load();
    }

    void HandleClient(int client) {
        // The handler owns this descriptor until it returns, including Stop.
        struct ClientGuard {
            int fd;
            ~ClientGuard() { close(fd); }
        } guard{client};
        if (!SetNonblocking(client)) {
            SetError("client nonblocking setup failed");
            return;
        }
        const std::string request = ReadRequest(client);
        if (request.empty()) {
            if (!stop_.load()) SetError("incomplete request");
            return;
        }
        const size_t first_space = request.find(' ');
        const size_t second_space = request.find(' ', first_space + 1);
        const std::string target =
            request.substr(first_space + 1, second_space - first_space - 1);
        const std::string key = target.substr(target.find_last_of('/') + 1);
        {
            std::lock_guard lock(mutex_);
            requests_.push_back(request);
            request_keys_.push_back(key);
        }
        condition_.notify_all();
        const auto response = routes_.find(key);
        if (response == routes_.end()) {
            SetError("unexpected request target");
            SendReply(client, Reply(404, "unknown").wire);
        } else if (response->second.action == Response::Action::kReset) {
            linger reset{1, 0};
            if (setsockopt(client, SOL_SOCKET, SO_LINGER, &reset,
                           sizeof(reset)) != 0)
                SetError("reset setup failed");
        } else if (response->second.action == Response::Action::kStall) {
            StallUntilPeerCloses(client);
        } else if (response->second.action != Response::Action::kGate ||
                   WaitForRelease()) {
            SendReply(client, response->second.wire);
        }
    }

    void Serve() {
        while (!stop_.load()) {
            if (!Ready(listen_fd_, POLLIN,
                       Clock::now() + std::chrono::milliseconds(50)))
                continue;
            const int client = accept(listen_fd_, nullptr, nullptr);
            if (client < 0) {
                if (errno != EAGAIN && errno != EINTR)
                    SetError("accept failed");
                continue;
            }
            if (concurrent_) {
                try {
                    handlers_.emplace_back(
                        [this, client] { HandleClient(client); });
                } catch (...) {
                    close(client);
                    SetError("client thread setup failed");
                }
            } else {
                HandleClient(client);
            }
        }
    }

    const std::map<std::string, Response> routes_;
    const bool concurrent_;
    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stop_{false};
    std::thread thread_;
    std::vector<std::thread> handlers_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<std::string> requests_;
    std::vector<std::string> request_keys_;
    bool responses_released_ = false;
    std::string error_;
};

}  // namespace mooncake::test
