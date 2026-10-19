#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/oss_adapter.h"
#include "storage_backend.h"
#include "oss_fault_test_helpers.h"
#include "oss_live_test_helpers.h"

namespace mooncake {
namespace {

class ScopedEnvironment {
   public:
    ~ScopedEnvironment() {
        for (const auto& [name, value] : original_values_) {
            if (value) {
                setenv(name.c_str(), value->c_str(), 1);
            } else {
                unsetenv(name.c_str());
            }
        }
    }

    void Set(const std::string& name, const std::string& value) {
        if (!original_values_.contains(name)) {
            const char* original = std::getenv(name.c_str());
            original_values_[name] =
                original ? std::optional<std::string>(original) : std::nullopt;
        }
        setenv(name.c_str(), value.c_str(), 1);
    }

   private:
    std::map<std::string, std::optional<std::string>> original_values_;
};

class ObjectCleanup {
   public:
    explicit ObjectCleanup(OssObjectStorageAdapter& adapter)
        : adapter_(adapter) {}

    ~ObjectCleanup() {
        for (const auto& key : keys_) adapter_.Delete(key);
    }

    void Add(std::string key) { keys_.push_back(std::move(key)); }

   private:
    OssObjectStorageAdapter& adapter_;
    std::vector<std::string> keys_;
};

class ScriptedOssServer {
   public:
    struct Response {
        long status;
        std::string body;
        std::string raw_response{};
        bool keep_alive = false;
        bool close_without_response = false;
    };

    explicit ScriptedOssServer(std::vector<Response> responses,
                               bool defer_first_response = false)
        : responses_(std::move(responses)),
          defer_first_response_(defer_first_response) {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) throw std::runtime_error("socket failed");

        int reuse = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) != 0) {
            close(listen_fd_);
            throw std::runtime_error("bind failed");
        }
        const int backlog =
            static_cast<int>(std::max<size_t>(2, responses_.size()));
        if (listen(listen_fd_, backlog) != 0) {
            close(listen_fd_);
            throw std::runtime_error("listen failed");
        }
        socklen_t address_size = sizeof(address);
        if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                        &address_size) != 0) {
            close(listen_fd_);
            throw std::runtime_error("getsockname failed");
        }
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { Serve(); });
    }

    ~ScriptedOssServer() {
        ReleaseFirstResponse();
        Wait();
    }

    uint16_t port() const { return port_; }

    void Wait() {
        if (thread_.joinable()) thread_.join();
        if (listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    const std::vector<std::string>& requests() const { return requests_; }
    const std::string& error() const { return error_; }

    bool WaitForFirstRequest() {
        return first_request_seen_.wait_for(std::chrono::seconds(5)) ==
               std::future_status::ready;
    }

    void ReleaseFirstResponse() {
        std::call_once(release_first_once_,
                       [this] { release_first_.set_value(); });
    }

   private:
    static bool SendAll(int fd, const std::string& data) {
#if defined(MSG_NOSIGNAL)
        constexpr int send_flags = MSG_NOSIGNAL;
#else
        constexpr int send_flags = 0;
#endif
        size_t offset = 0;
        while (offset < data.size()) {
            const ssize_t sent = send(fd, data.data() + offset,
                                      data.size() - offset, send_flags);
            if (sent <= 0) return false;
            offset += static_cast<size_t>(sent);
        }
        return true;
    }

    static std::string ReadRequest(int fd) {
        std::string request;
        std::array<char, 4096> buffer{};
        std::optional<size_t> request_size;
        while (!request_size || request.size() < *request_size) {
            pollfd descriptor{fd, POLLIN, 0};
            if (poll(&descriptor, 1, 5000) <= 0) break;
            const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
            if (received <= 0) break;
            request.append(buffer.data(), static_cast<size_t>(received));
            const size_t header_end = request.find("\r\n\r\n");
            if (!request_size && header_end != std::string::npos) {
                std::string headers = request.substr(0, header_end);
                std::transform(headers.begin(), headers.end(), headers.begin(),
                               [](unsigned char c) { return std::tolower(c); });
                constexpr std::string_view content_length =
                    "\r\ncontent-length:";
                const size_t length_pos = headers.find(content_length);
                const size_t body_size =
                    length_pos == std::string::npos
                        ? 0
                        : std::stoull(headers.substr(length_pos +
                                                     content_length.size()));
                request_size = header_end + 4 + body_size;
            }
        }
        return request;
    }

    void Serve() {
        // Hold A's connection open while serving B. This makes the overlap
        // test depend on request admission, not on timing or a fast server.
        struct DeferredResponse {
            int client = -1;
            std::string data;
            ~DeferredResponse() {
                if (client >= 0) close(client);
            }
        } deferred;
        struct Connection {
            int fd = -1;
            ~Connection() {
                if (fd >= 0) close(fd);
            }
        } connection;
        for (const auto& response : responses_) {
            if (connection.fd < 0) {
                pollfd descriptor{listen_fd_, POLLIN, 0};
                if (poll(&descriptor, 1, 5000) <= 0) {
                    error_ = "timed out waiting for OSS request";
                    return;
                }
                connection.fd = accept(listen_fd_, nullptr, nullptr);
                if (connection.fd < 0) {
                    error_ = "accept failed";
                    return;
                }
            }
            const int client = connection.fd;
            requests_.push_back(ReadRequest(client));
            if (response.close_without_response) {
                // Read the upload first so libcurl must rewind its source
                // when it retries this reused connection on a fresh socket.
                close(client);
                connection.fd = -1;
                continue;
            }
            std::string response_data = response.raw_response;
            if (response_data.empty()) {
                const std::string status_text =
                    response.status == 204   ? "No Content"
                    : response.status == 206 ? "Partial Content"
                                             : "OK";
                // A single-part 206 must identify its range. Unknown total size
                // is valid; this scripted server only knows the selected body.
                std::string content_range;
                if (response.status == 206 && !response.body.empty()) {
                    constexpr std::string_view range_header =
                        "\r\nRange: bytes=";
                    const auto start = requests_.back().find(range_header);
                    const auto offset =
                        start == std::string::npos
                            ? 0ULL
                            : std::stoull(requests_.back().substr(
                                  start + range_header.size()));
                    content_range =
                        "Content-Range: bytes " + std::to_string(offset) + "-" +
                        std::to_string(offset + response.body.size() - 1) +
                        "/*\r\n";
                }
                response_data =
                    "HTTP/1.1 " + std::to_string(response.status) + " " +
                    status_text +
                    "\r\nContent-Type: application/xml\r\n"
                    "Content-Length: " +
                    std::to_string(response.body.size()) +
                    (response.keep_alive ? "\r\nConnection: keep-alive\r\n"
                                         : "\r\nConnection: close\r\n") +
                    content_range + "\r\n" + response.body;
            }
            if (defer_first_response_ && deferred.client < 0) {
                deferred.client = client;
                connection.fd = -1;
                deferred.data = response_data;
                first_request_.set_value();
                continue;
            }
            if (!SendAll(client, response_data)) error_ = "send failed";
            if (!response.keep_alive) {
                close(client);
                connection.fd = -1;
            }
            if (!error_.empty()) return;
        }
        if (deferred.client >= 0) {
            if (first_response_released_.wait_for(std::chrono::seconds(5)) !=
                std::future_status::ready) {
                error_ = "timed out waiting to release the first response";
            } else if (!SendAll(deferred.client, deferred.data)) {
                error_ = "deferred send failed";
            }
        }
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::thread thread_;
    std::vector<Response> responses_;
    std::vector<std::string> requests_;
    std::string error_;
    bool defer_first_response_ = false;
    std::promise<void> first_request_;
    std::future<void> first_request_seen_{first_request_.get_future()};
    std::promise<void> release_first_;
    std::future<void> first_response_released_{release_first_.get_future()};
    std::once_flag release_first_once_;
};

bool HasOssConfiguration() {
    const auto has = [](const char* primary, const char* fallback = nullptr) {
        const char* value = std::getenv(primary);
        if ((!value || !*value) && fallback) value = std::getenv(fallback);
        return value && *value;
    };
    if (!has("MOONCAKE_OSS_ENDPOINT", "OSS_ENDPOINT") ||
        !has("MOONCAKE_OSS_BUCKET", "OSS_BUCKET") ||
        !has("MOONCAKE_OSS_REGION", "OSS_REGION")) {
        return false;
    }
    const char* anonymous = std::getenv("MOONCAKE_OSS_ANONYMOUS");
    if (anonymous) {
        std::string value(anonymous);
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (value == "1" || value == "true" || value == "on" ||
            value == "yes") {
            return true;
        }
    }
    return has("MOONCAKE_OSS_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID") &&
           has("MOONCAKE_OSS_ACCESS_KEY_SECRET", "OSS_ACCESS_KEY_SECRET");
}

std::string UniquePrefix(const std::string& suffix) {
    const auto now =
        std::chrono::steady_clock::now().time_since_epoch().count();
    return "/mooncake-oss-adapter-test/" + std::to_string(getpid()) + "-" +
           std::to_string(now) + "-" + suffix;
}

void ConfigureFaultEndpoint(ScopedEnvironment& environment, uint16_t port) {
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(port));
    environment.Set("MOONCAKE_OSS_BUCKET", "fault-bucket");
    environment.Set("MOONCAKE_OSS_REGION", "fault-region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "8");
    // Never send inherited real credentials to even a loopback test server.
    // ScopedEnvironment restores these process-local values at test teardown.
    environment.Set("MOONCAKE_OSS_ACCESS_KEY_ID", "fault-test-id");
    environment.Set("MOONCAKE_OSS_ACCESS_KEY_SECRET", "fault-test-secret");
    environment.Set("MOONCAKE_OSS_SECURITY_TOKEN", "fault-test-token");
    environment.Set("NO_PROXY", "127.0.0.1");
    environment.Set("no_proxy", "127.0.0.1");
}

// Two slots, more requests than slots, and holes left by local validation.
// The first network request stays pending while the other slot must drain
// every later request, including failures. Only then release the first one.
void CheckMixedGetBatch(bool timeout) {
    using Server = test::OssFaultHttpServer;
    Server server(
        {{"waiting", timeout ? Server::Stall() : Server::Gate(206, "first")},
         {"missing", Server::Reply(404, "")},
         {"reset", Server::Reset()},
         {"large-error", Server::Reply(404, std::string(512, 'x'))},
         {"forbidden", Server::Reply(403, "deny")},
         {"truncated", Server::Raw("HTTP/1.1 206 Test\r\nContent-Length: 5\r\n"
                                   "Connection: close\r\n\r\nva")},
         {"short", Server::Reply(206, "val")},
         {"oversized", Server::Reply(206, "too-long")},
         {"unavailable", Server::Reply(503, "busy!")},
         {"tail", Server::Reply(206, "later")}},
        true);
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "2");
    OssObjectStorageAdapter adapter("/mixed-get/");
    ASSERT_TRUE(adapter.Init());
    std::array<std::array<char, 7>, 13> buffers;
    for (auto& buffer : buffers) buffer.fill('!');
    const std::vector<ObjectGetRequest> requests{
        {"invalid-first", nullptr, 5},
        {"waiting", buffers[1].data() + 1, 5},
        {"missing", buffers[2].data() + 1, 5},
        {"empty", nullptr, 0},
        {"reset", buffers[4].data() + 1, 5},
        {"large-error", buffers[5].data() + 1, 5},
        {"forbidden", buffers[6].data() + 1, 5},
        {"truncated", buffers[7].data() + 1, 5},
        {"short", buffers[8].data() + 1, 5},
        {"oversized", buffers[9].data() + 1, 5},
        {"unavailable", buffers[10].data() + 1, 5},
        {"tail", buffers[11].data() + 1, 5},
        {"invalid-last", nullptr, 1},
    };
    const auto started = std::chrono::steady_clock::now();
    auto pending = std::async(std::launch::async,
                              [&] { return adapter.GetBatch(requests); });
    const bool tail_admitted = server.WaitForRequest("tail");
    const auto before_release = pending.wait_for(std::chrono::seconds(0));
    server.ReleaseResponses();
    const auto results = pending.get();
    if (timeout) {
        const auto elapsed = std::chrono::steady_clock::now() - started;
        EXPECT_GE(elapsed, std::chrono::seconds(110));
        EXPECT_LT(elapsed, std::chrono::seconds(130));
    }
    EXPECT_TRUE(tail_admitted);
    EXPECT_EQ(before_release, std::future_status::timeout);
    ASSERT_EQ(results.size(), requests.size());
    const std::array<std::optional<ErrorCode>, 13> errors{
        ErrorCode::INVALID_PARAMS,
        timeout ? std::optional(ErrorCode::DFS_SERVICE_UNAVAILABLE)
                : std::nullopt,
        ErrorCode::FILE_NOT_FOUND,
        std::nullopt,
        ErrorCode::DFS_SERVICE_UNAVAILABLE,
        ErrorCode::FILE_NOT_FOUND,
        ErrorCode::FILE_READ_FAIL,
        ErrorCode::DFS_SERVICE_UNAVAILABLE,
        ErrorCode::FILE_READ_FAIL,
        ErrorCode::DFS_SERVICE_UNAVAILABLE,
        ErrorCode::FILE_READ_FAIL,
        std::nullopt,
        ErrorCode::INVALID_PARAMS};
    for (size_t i = 0; i < results.size(); ++i) {
        SCOPED_TRACE(requests[i].logical_key);
        EXPECT_EQ(results[i].has_value(), !errors[i].has_value());
        if (errors[i] && !results[i]) EXPECT_EQ(results[i].error(), *errors[i]);
        if (!errors[i] && results[i]) EXPECT_EQ(*results[i], requests[i].size);
        EXPECT_EQ(buffers[i].front(), '!');
        EXPECT_EQ(buffers[i].back(), '!');
    }
    if (!timeout) EXPECT_EQ(std::string(buffers[1].data() + 1, 5), "first");
    EXPECT_EQ(std::string(buffers[11].data() + 1, 5), "later");
    // Local invalid/zero-length reads must never reach HTTP. No request may
    // be lost or accidentally submitted twice by the admission loop.
    auto received = server.requests();
    ASSERT_EQ(received.size(), 10U);
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto count = std::count_if(
            received.begin(), received.end(), [&](const auto& wire) {
                return wire.find("/" + requests[i].logical_key + " HTTP/") !=
                       std::string::npos;
            });
        EXPECT_EQ(count, i == 0 || i == 3 || i == 12 ? 0 : 1);
    }
    auto recovered = adapter.GetBatch({{"tail", buffers[0].data() + 1, 5}});
    ASSERT_EQ(recovered.size(), 1U);
    ASSERT_TRUE(recovered[0]);
    EXPECT_EQ(std::string(buffers[0].data() + 1, 5), "later");
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
}

void CheckMixedPutBatch(bool timeout) {
    using Server = test::OssFaultHttpServer;
    Server server(
        {{"waiting", timeout ? Server::Stall() : Server::Gate(200, "")},
         {"missing", Server::Reply(404, "missing")},
         {"reset", Server::Reset()},
         {"forbidden", Server::Reply(403, "deny")},
         {"unavailable", Server::Reply(503, "busy")},
         {"empty", Server::Reply(200, "")},
         {"tail", Server::Reply(200, "")}},
        true);
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "2");
    OssObjectStorageAdapter adapter("/mixed-put/");
    ASSERT_TRUE(adapter.Init());
    std::string left = "left", right = "right";
    const iovec data[] = {
        {left.data(), left.size()}, {nullptr, 0}, {right.data(), right.size()}};
    const iovec invalid{nullptr, 1};
    const iovec overflow[] = {{left.data(), std::numeric_limits<size_t>::max()},
                              {right.data(), 1}};
    const std::vector<ObjectPutRequest> requests{
        {"negative-count", nullptr, -1},
        {"waiting", data, 3},
        {"missing", data, 3},
        {"null-array", nullptr, 1},
        {"reset", data, 3},
        {"overflow", overflow, 2},
        {"forbidden", data, 3},
        {"bad-base", &invalid, 1},
        {"unavailable", data, 3},
        {"empty", nullptr, 0},
        {"tail", data, 3},
    };
    const auto started = std::chrono::steady_clock::now();
    auto pending = std::async(std::launch::async,
                              [&] { return adapter.PutBatch(requests); });
    const bool tail_admitted = server.WaitForRequest("tail");
    const auto before_release = pending.wait_for(std::chrono::seconds(0));
    server.ReleaseResponses();
    const auto results = pending.get();
    if (timeout) {
        const auto elapsed = std::chrono::steady_clock::now() - started;
        EXPECT_GE(elapsed, std::chrono::seconds(110));
        EXPECT_LT(elapsed, std::chrono::seconds(130));
    }
    EXPECT_TRUE(tail_admitted);
    EXPECT_EQ(before_release, std::future_status::timeout);
    ASSERT_EQ(results.size(), requests.size());
    const std::array<std::optional<ErrorCode>, 11> errors{
        ErrorCode::INVALID_PARAMS,
        timeout ? std::optional(ErrorCode::DFS_SERVICE_UNAVAILABLE)
                : std::nullopt,
        ErrorCode::FILE_WRITE_FAIL,
        ErrorCode::INVALID_PARAMS,
        ErrorCode::DFS_SERVICE_UNAVAILABLE,
        ErrorCode::INVALID_PARAMS,
        ErrorCode::FILE_WRITE_FAIL,
        ErrorCode::INVALID_PARAMS,
        ErrorCode::FILE_WRITE_FAIL,
        std::nullopt,
        std::nullopt};
    for (size_t i = 0; i < results.size(); ++i) {
        SCOPED_TRACE(requests[i].logical_key);
        EXPECT_EQ(results[i].has_value(), !errors[i].has_value());
        if (errors[i] && !results[i]) EXPECT_EQ(results[i].error(), *errors[i]);
    }
    const auto received = server.requests();
    ASSERT_EQ(received.size(), 7U);
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto count = std::count_if(
            received.begin(), received.end(), [&](const auto& wire) {
                return wire.find("/" + requests[i].logical_key + " HTTP/") !=
                       std::string::npos;
            });
        EXPECT_EQ(count, i == 0 || i == 3 || i == 5 || i == 7 ? 0 : 1);
    }
    for (const auto& wire : received) {
        EXPECT_EQ(wire.rfind("PUT ", 0), 0U);
        const auto body_start = wire.find("\r\n\r\n");
        ASSERT_NE(body_start, std::string::npos);
        const bool empty = wire.find("/empty HTTP/") != std::string::npos;
        EXPECT_EQ(wire.substr(body_start + 4), empty ? "" : left + right);
    }
    auto recovered = adapter.PutBatch({{"tail", data, 3}});
    ASSERT_EQ(recovered.size(), 1U);
    EXPECT_TRUE(recovered[0]);
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest, MixedGetBatchRefillsWhileFirstRequestWaits) {
    CheckMixedGetBatch(false);
}

TEST(OssObjectStorageAdapterTest, MixedPutBatchRefillsWhileFirstRequestWaits) {
    CheckMixedPutBatch(false);
}

TEST(OssObjectStorageAdapterTest, MixedGetBatchIsolatesRequestTimeout) {
    const char* enabled = std::getenv("MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS");
    if (!enabled || std::string_view(enabled) != "1")
        GTEST_SKIP() << "Set MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS=1 for the "
                        "production 120-second timeout regression";
    CheckMixedGetBatch(true);
}

TEST(OssObjectStorageAdapterTest, MixedPutBatchIsolatesRequestTimeout) {
    const char* enabled = std::getenv("MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS");
    if (!enabled || std::string_view(enabled) != "1")
        GTEST_SKIP() << "Set MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS=1 for the "
                        "production 120-second timeout regression";
    CheckMixedPutBatch(true);
}

TEST(OssObjectStorageAdapterTest, BatchConnectionLimitAppliesToGetAndPut) {
    using Server = test::OssFaultHttpServer;
    for (bool upload : {false, true}) {
        for (int limit : {1, 2, 4}) {
            SCOPED_TRACE(upload ? "PUT" : "GET");
            SCOPED_TRACE(limit);
            constexpr size_t count = 6;
            std::map<std::string, Server::Response> routes;
            std::vector<ObjectGetRequest> gets;
            std::vector<ObjectPutRequest> puts;
            std::array<char, count> buffers{};
            char payload = 'x';
            const iovec iov{&payload, 1};
            for (size_t i = 0; i < count; ++i) {
                const auto key = std::to_string(i);
                routes.emplace(
                    key, Server::Gate(upload ? 200 : 206, upload ? "" : "x"));
                gets.push_back({key, &buffers[i], 1});
                puts.push_back({key, &iov, 1});
            }
            Server server(std::move(routes), true);
            ScopedEnvironment environment;
            ConfigureFaultEndpoint(environment, server.port());
            environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS",
                            std::to_string(limit));
            OssObjectStorageAdapter adapter("/connection-limit/");
            ASSERT_TRUE(adapter.Init());
            auto pending = std::async(std::launch::async, [&] {
                if (upload) {
                    const auto results = adapter.PutBatch(puts);
                    return results.size() == count &&
                           std::all_of(results.begin(), results.end(),
                                       [](const auto& r) { return bool(r); });
                }
                const auto results = adapter.GetBatch(gets);
                return results.size() == count &&
                       std::all_of(results.begin(), results.end(),
                                   [](const auto& r) { return r && *r == 1; });
            });
            const bool window_full =
                server.WaitForRequest(std::to_string(limit - 1));
            // All admitted requests are gated: no slot can finish yet.
            const bool exceeded = server.WaitForRequest(
                std::to_string(limit), std::chrono::milliseconds(200));
            const auto admitted = server.requests().size();
            server.ReleaseResponses();
            const bool succeeded = pending.get();
            EXPECT_TRUE(window_full);
            EXPECT_FALSE(exceeded);
            EXPECT_EQ(admitted, static_cast<size_t>(limit));
            EXPECT_TRUE(succeeded);
            if (!upload)
                for (char value : buffers) EXPECT_EQ(value, 'x');
            server.Stop();
            EXPECT_EQ(server.requests().size(), count);
            EXPECT_TRUE(server.error().empty()) << server.error();
        }
    }
}

TEST(OssObjectStorageAdapterTest,
     FaultResetAndPartialHeaderFailThenSameAdapterRecovers) {
    using Server = test::OssFaultHttpServer;
    Server server(
        {{"reset", Server::Reset()},
         {"partial-header",
          Server::Raw("HTTP/1.1 206 Partial Content\r\nContent-Len")},
         {"healthy",
          Server::Reply(206, "value", "Content-Range: bytes 0-4/5\r\n")}});
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    for (const char* key : {"reset", "partial-header"}) {
        SCOPED_TRACE(key);
        std::array<char, 7> output;
        output.fill('!');
        auto failed = adapter.Get(key, output.data() + 1, 5);
        EXPECT_FALSE(failed);
        if (!failed) {
            if (std::string_view(key) == "reset") {
                EXPECT_EQ(failed.error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
            } else {
                // libcurl may report a transport error or let the adapter
                // reject the incomplete response. Neither is a valid read.
                EXPECT_TRUE(failed.error() ==
                                ErrorCode::DFS_SERVICE_UNAVAILABLE ||
                            failed.error() == ErrorCode::FILE_READ_FAIL);
            }
        }
        EXPECT_EQ(output.front(), '!');
        EXPECT_EQ(output.back(), '!');
        auto recovered = adapter.Get("healthy", output.data() + 1, 5);
        ASSERT_TRUE(recovered);
        EXPECT_EQ(*recovered, 5U);
        EXPECT_EQ(std::string(output.data() + 1, 5), "value");
        EXPECT_EQ(output.front(), '!');
        EXPECT_EQ(output.back(), '!');
    }
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest,
     FaultTruncatedBodyFailsThenSameAdapterRecovers) {
    using Server = test::OssFaultHttpServer;
    Server server({
        {"truncated",
         Server::Raw(
             "HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\n"
             "Content-Range: bytes 0-4/5\r\nConnection: close\r\n\r\nva")},
        {"healthy",
         Server::Reply(206, "value", "Content-Range: bytes 0-4/5\r\n")},
    });
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    std::array<char, 7> output;
    output.fill('!');
    auto failed = adapter.Get("truncated", output.data() + 1, 5);
    EXPECT_FALSE(failed);
    if (!failed) {
        EXPECT_EQ(failed.error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    EXPECT_EQ(output.front(), '!');
    EXPECT_EQ(output.back(), '!');
    auto recovered = adapter.Get("healthy", output.data() + 1, 5);
    ASSERT_TRUE(recovered);
    EXPECT_EQ(*recovered, 5U);
    EXPECT_EQ(std::string(output.data() + 1, 5), "value");
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest,
     FaultRangeShortReadAndHttpErrorAreRejectedThenRecovers) {
    using Server = test::OssFaultHttpServer;
    Server server({
        {"clipped-range",
         Server::Reply(206, "end", "Content-Range: bytes 5-7/8\r\n")},
        {"unsatisfiable-range", Server::Reply(416, "")},
        {"healthy",
         Server::Reply(206, "later", "Content-Range: bytes 5-9/10\r\n")},
    });
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    for (const char* key : {"clipped-range", "unsatisfiable-range"}) {
        SCOPED_TRACE(key);
        std::array<char, 7> output;
        output.fill('!');
        // Reject short reads and HTTP errors using status and actual length.
        auto failed = adapter.GetRange(key, output.data() + 1, 5, 5);
        EXPECT_FALSE(failed);
        if (!failed) {
            EXPECT_EQ(failed.error(), ErrorCode::FILE_READ_FAIL);
        }
        EXPECT_EQ(output.front(), '!');
        EXPECT_EQ(output.back(), '!');
        auto recovered = adapter.GetRange("healthy", output.data() + 1, 5, 5);
        ASSERT_TRUE(recovered);
        EXPECT_EQ(*recovered, 5U);
        EXPECT_EQ(std::string(output.data() + 1, 5), "later");
    }
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
    for (const auto& request : server.requests()) {
        EXPECT_NE(request.find("Range: bytes=5-9"), std::string::npos);
    }
}

TEST(OssObjectStorageAdapterTest,
     FaultShortReadAndHttpErrorAreRejectedByDirectAndBatchGet) {
    using Server = test::OssFaultHttpServer;
    Server server({
        {"short-200", Server::Reply(200, "val")},
        {"clipped-range",
         Server::Reply(206, "val", "Content-Range: bytes 0-2/3\r\n")},
        {"unsatisfiable-range", Server::Reply(416, "error")},
        {"full-200", Server::Reply(200, "value")},
        {"full-206", Server::Reply(206, "value")},
    });
    const std::array<const char*, 5> keys{"short-200", "clipped-range",
                                          "unsatisfiable-range", "full-200",
                                          "full-206"};
    constexpr size_t invalid_count = 4;
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    std::vector<std::array<char, 7>> buffers(keys.size());
    std::vector<ObjectGetRequest> requests;
    for (size_t i = 0; i < keys.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        buffers[i].fill('!');
        auto result = adapter.Get(keys[i], buffers[i].data() + 1, 5);
        if (i < invalid_count) {
            EXPECT_FALSE(result);
            if (!result) {
                EXPECT_EQ(result.error(), ErrorCode::FILE_READ_FAIL);
            }
        } else {
            ASSERT_TRUE(result);
            EXPECT_EQ(*result, 5U);
            EXPECT_EQ(std::string(buffers[i].data() + 1, 5), "value");
        }
        EXPECT_EQ(buffers[i].front(), '!');
        EXPECT_EQ(buffers[i].back(), '!');
        buffers[i].fill('!');
        requests.push_back({keys[i], buffers[i].data() + 1, 5});
    }
    // Check status and actual length in both paths. The 200 and 416 bodies
    // have the expected length but are not valid range responses.
    auto results = adapter.GetBatch(requests);
    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        if (i < invalid_count) {
            EXPECT_FALSE(results[i]);
            if (!results[i]) {
                EXPECT_EQ(results[i].error(), ErrorCode::FILE_READ_FAIL);
            }
        } else {
            ASSERT_TRUE(results[i]);
            EXPECT_EQ(*results[i], 5U);
            EXPECT_EQ(std::string(buffers[i].data() + 1, 5), "value");
        }
        EXPECT_EQ(buffers[i].front(), '!');
        EXPECT_EQ(buffers[i].back(), '!');
    }
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest,
     ConcurrentSyncBatchesDoNotWaitForSharedMultiHandle) {
    ScriptedOssServer server({{206, "first"}, {206, "later"}}, true);
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    // The limit is per batch, not per adapter: the first batch consumes its
    // only connection while the second batch must still make progress.
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "1");
    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    std::array<char, 7> first_buffer, later_buffer;
    first_buffer.fill('!');
    later_buffer.fill('!');
    auto first = std::async(std::launch::async, [&] {
        return adapter.GetBatch({{"first", first_buffer.data() + 1, 5}});
    });
    const bool first_received = server.WaitForFirstRequest();
    auto later = std::async(std::launch::async, [&] {
        return adapter.GetBatch({{"later", later_buffer.data() + 1, 5}});
    });
    const auto later_status = later.wait_for(std::chrono::seconds(2));
    const auto first_before_release = first.wait_for(std::chrono::seconds(0));
    // Always release and join before asserting. Wait() also closes the
    // listener after its bounded accept timeout, so the old serialized
    // implementation fails this regression without waiting for a 120 s GET.
    server.ReleaseFirstResponse();
    server.Wait();
    auto first_result = first.get();
    auto later_result = later.get();
    ASSERT_TRUE(first_received);
    ASSERT_EQ(later_status, std::future_status::ready);
    EXPECT_EQ(first_before_release, std::future_status::timeout);
    ASSERT_EQ(first_result.size(), 1U);
    ASSERT_EQ(later_result.size(), 1U);
    ASSERT_TRUE(first_result[0]);
    ASSERT_TRUE(later_result[0]);
    EXPECT_EQ(*first_result[0], 5U);
    EXPECT_EQ(*later_result[0], 5U);
    EXPECT_EQ(std::string(first_buffer.data() + 1, 5), "first");
    EXPECT_EQ(std::string(later_buffer.data() + 1, 5), "later");
    EXPECT_EQ(first_buffer.front(), '!');
    EXPECT_EQ(first_buffer.back(), '!');
    EXPECT_EQ(later_buffer.front(), '!');
    EXPECT_EQ(later_buffer.back(), '!');
    EXPECT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest,
     FaultBatchErrorsRemainIsolatedAndNextBatchRecovers) {
    using Server = test::OssFaultHttpServer;
    Server server({
        {"healthy",
         Server::Reply(206, "value", "Content-Range: bytes 0-4/5\r\n")},
        {"reset", Server::Reset()},
        {"truncated",
         Server::Raw(
             "HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\n"
             "Content-Range: bytes 0-4/5\r\nConnection: close\r\n\r\nva")},
        {"forbidden", Server::Reply(403, "deny")},
        {"unavailable", Server::Reply(503, "busy!")},
        {"recovery-a",
         Server::Reply(206, "after", "Content-Range: bytes 0-4/5\r\n")},
        {"recovery-b",
         Server::Reply(206, "again", "Content-Range: bytes 0-4/5\r\n")},
    });
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    std::array<std::array<char, 7>, 5> output;
    for (auto& buffer : output) buffer.fill('!');
    const std::array<const char*, 5> keys{"healthy", "reset", "truncated",
                                          "forbidden", "unavailable"};
    std::vector<ObjectGetRequest> requests;
    for (size_t i = 0; i < keys.size(); ++i)
        requests.push_back({keys[i], output[i].data() + 1, 5});
    auto results = adapter.GetBatch(requests);
    ASSERT_EQ(results.size(), requests.size());
    ASSERT_TRUE(results[0]);
    EXPECT_EQ(*results[0], 5U);
    EXPECT_EQ(std::string(output[0].data() + 1, 5), "value");
    for (size_t i = 1; i < keys.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        EXPECT_FALSE(results[i]);
        if (!results[i]) {
            EXPECT_EQ(results[i].error(),
                      i < 3 ? ErrorCode::DFS_SERVICE_UNAVAILABLE
                            : ErrorCode::FILE_READ_FAIL);
        }
    }
    for (const auto& buffer : output) {
        EXPECT_EQ(buffer.front(), '!');
        EXPECT_EQ(buffer.back(), '!');
    }
    auto recovered =
        adapter.GetBatch({{"recovery-a", output[0].data() + 1, 5},
                          {"recovery-b", output[1].data() + 1, 5}});
    ASSERT_EQ(recovered.size(), 2U);
    ASSERT_TRUE(recovered[0]);
    ASSERT_TRUE(recovered[1]);
    EXPECT_EQ(std::string(output[0].data() + 1, 5), "after");
    EXPECT_EQ(std::string(output[1].data() + 1, 5), "again");
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest, FaultBatchPutFailureDoesNotPoisonNextUpload) {
    using Server = test::OssFaultHttpServer;
    Server server({{"healthy", Server::Reply(200, "")},
                   {"unavailable", Server::Reply(503, "busy")},
                   {"reset", Server::Reset()},
                   {"recovered", Server::Reply(200, "")}});
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    std::string payload(16 * 1024, 'p');
    const iovec source{payload.data(), payload.size()};
    auto results = adapter.PutBatch({{"healthy", &source, 1},
                                     {"unavailable", &source, 1},
                                     {"reset", &source, 1}});
    ASSERT_EQ(results.size(), 3U);
    EXPECT_TRUE(results[0]);
    EXPECT_FALSE(results[1]);
    EXPECT_FALSE(results[2]);
    if (!results[1]) {
        EXPECT_EQ(results[1].error(), ErrorCode::FILE_WRITE_FAIL);
    }
    if (!results[2]) {
        EXPECT_EQ(results[2].error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    auto recovered = adapter.PutBatch({{"recovered", &source, 1}});
    ASSERT_EQ(recovered.size(), 1U);
    EXPECT_TRUE(recovered[0]);
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
    for (const auto& request : server.requests()) {
        EXPECT_EQ(request.rfind("PUT ", 0), 0U);
        const auto body = request.find("\r\n\r\n");
        ASSERT_NE(body, std::string::npos);
        EXPECT_EQ(request.substr(body + 4), payload);
    }
}

TEST(OssObjectStorageAdapterTest,
     BatchReusedConnectionPutRewindsIovecsForCurlRetry) {
    ScriptedOssServer server({
        {200, "", "", true},
        {0, "", "", false, true},
        {200, ""},
    });
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "1");
    OssObjectStorageAdapter adapter("/retry-test/");
    ASSERT_TRUE(adapter.Init());

    std::string left(20 * 1024, 'a');
    std::string right(12 * 1024, 'b');
    const iovec source[] = {
        {left.data(), left.size()}, {nullptr, 0}, {right.data(), right.size()}};
    // Within one batch, reuse the warmup connection, then drop the upload
    // response. libcurl must rewind the iovecs before retrying the PUT.
    auto results =
        adapter.PutBatch({{"warmup", nullptr, 0}, {"payload", source, 3}});
    ASSERT_EQ(results.size(), 2U);
    EXPECT_TRUE(results[0]);
    EXPECT_TRUE(results[1]);
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();
    ASSERT_EQ(server.requests().size(), 3U);
    EXPECT_EQ(server.requests()[0].rfind(
                  "PUT /fault-bucket/retry-test/warmup HTTP/", 0),
              0U);
    for (size_t i = 1; i < server.requests().size(); ++i) {
        const auto& received = server.requests()[i];
        EXPECT_EQ(
            received.rfind("PUT /fault-bucket/retry-test/payload HTTP/", 0),
            0U);
        const auto body = received.find("\r\n\r\n");
        ASSERT_NE(body, std::string::npos);
        EXPECT_EQ(received.substr(body + 4), left + right);
    }
}

TEST(OssObjectStorageAdapterTest, FaultRequestTimeoutThenSameAdapterRecovers) {
    const char* enabled = std::getenv("MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS");
    if (!enabled || std::string_view(enabled) != "1") {
        GTEST_SKIP() << "Set MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS=1 for the "
                        "production 120-second timeout regression";
    }
    // Slow by design: the current production request timeout is 120 s and is
    // not configurable. Run this test with an outer timeout greater than 130 s.
    using Server = test::OssFaultHttpServer;
    Server server(
        {{"stall", Server::Stall()},
         {"healthy",
          Server::Reply(206, "value", "Content-Range: bytes 0-4/5\r\n")}});
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    std::array<char, 7> output;
    output.fill('!');
    const auto started = std::chrono::steady_clock::now();
    auto failed = adapter.Get("stall", output.data() + 1, 5);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    EXPECT_FALSE(failed);
    if (!failed) {
        EXPECT_EQ(failed.error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    EXPECT_GE(elapsed, std::chrono::seconds(110));
    EXPECT_LT(elapsed, std::chrono::seconds(130));
    auto recovered = adapter.Get("healthy", output.data() + 1, 5);
    ASSERT_TRUE(recovered);
    EXPECT_EQ(*recovered, 5U);
    EXPECT_EQ(std::string(output.data() + 1, 5), "value");
    EXPECT_EQ(output.front(), '!');
    EXPECT_EQ(output.back(), '!');
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest, BatchQueueDoesNotConsumeRequestTimeout) {
    const char* enabled = std::getenv("MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS");
    if (!enabled || std::string_view(enabled) != "1") {
        GTEST_SKIP() << "Set MOONCAKE_RUN_OSS_FAULT_TIMEOUT_TESTS=1 for the "
                        "production 120-second timeout regression";
    }
    using Server = test::OssFaultHttpServer;
    Server server(
        {{"stall", Server::Stall()}, {"healthy", Server::Reply(206, "value")}});
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "1");
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());
    std::array<char, 5> stalled{};
    std::array<char, 5> healthy{};
    // The second request waits for the first one's full 120-second timeout.
    // Its own timeout must begin on admission, not while waiting for a slot.
    auto results =
        adapter.GetBatch({{"stall", stalled.data(), stalled.size()},
                          {"healthy", healthy.data(), healthy.size()}});
    ASSERT_EQ(results.size(), 2U);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    ASSERT_TRUE(results[1]);
    EXPECT_EQ(*results[1], healthy.size());
    EXPECT_EQ(std::string(healthy.data(), healthy.size()), "value");
    server.Stop();
    EXPECT_TRUE(server.error().empty()) << server.error();
    EXPECT_EQ(server.requests().size(), 2U);
}

TEST(OssObjectStorageAdapterTest, ValidatesVectorAndRangeArguments) {
    OssObjectStorageAdapter adapter("/test-prefix");

    auto invalid_count = adapter.PutV("key", nullptr, -1);
    ASSERT_FALSE(invalid_count);
    EXPECT_EQ(invalid_count.error(), ErrorCode::INVALID_PARAMS);

    iovec invalid_iov{nullptr, 1};
    auto invalid_buffer = adapter.PutV("key", &invalid_iov, 1);
    ASSERT_FALSE(invalid_buffer);
    EXPECT_EQ(invalid_buffer.error(), ErrorCode::INVALID_PARAMS);

    char value = '\0';
    std::array<iovec, 2> overflowing_iov{
        {{&value, std::numeric_limits<size_t>::max()}, {&value, 1}}};
    auto overflow = adapter.PutV("key", overflowing_iov.data(), 2);
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error(), ErrorCode::INVALID_PARAMS);

    auto invalid_offset = adapter.GetRange("key", nullptr, 1, -1);
    ASSERT_FALSE(invalid_offset);
    EXPECT_EQ(invalid_offset.error(), ErrorCode::INVALID_PARAMS);

    auto invalid_batch =
        adapter.PutBatch({{"negative-count", nullptr, -1},
                          {"null-array", nullptr, 1},
                          {"bad-pointer", &invalid_iov, 1},
                          {"overflow", overflowing_iov.data(), 2}});
    ASSERT_EQ(invalid_batch.size(), 4U);
    for (const auto& result : invalid_batch) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST(OssObjectStorageAdapterTest, RejectsIgnoredRangeResponse) {
    ScriptedOssServer server({
        {200, "abcd", ""},
    });
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    std::array<char, 4> buffer{};
    auto result = adapter.GetRange("key", buffer.data(), buffer.size(), 2);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_READ_FAIL);
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();

    ASSERT_EQ(server.requests().size(), 1U);
    EXPECT_NE(server.requests()[0].find("Range: bytes=2-5"), std::string::npos);
    EXPECT_NE(server.requests()[0].find("x-oss-range-behavior: standard"),
              std::string::npos);
}

TEST(OssObjectStorageAdapterTest,
     BatchRejectsIgnoredRangeResponseThenRecovers) {
    ScriptedOssServer server({{200, "abcd"}, {206, "efgh"}, {206, "ijkl"}});
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "1");
    OssObjectStorageAdapter adapter("/fault-test/");
    ASSERT_TRUE(adapter.Init());

    std::array<char, 4> ignored{};
    std::array<char, 4> healthy{};
    auto results =
        adapter.GetBatch({{"ignored", ignored.data(), ignored.size()},
                          {"healthy", healthy.data(), healthy.size()}});
    ASSERT_EQ(results.size(), 2U);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::FILE_READ_FAIL);
    ASSERT_TRUE(results[1]);
    EXPECT_EQ(*results[1], healthy.size());
    EXPECT_EQ(std::string(healthy.data(), healthy.size()), "efgh");

    auto recovered =
        adapter.GetBatch({{"recovered", healthy.data(), healthy.size()}});
    ASSERT_EQ(recovered.size(), 1U);
    ASSERT_TRUE(recovered[0]);
    EXPECT_EQ(*recovered[0], healthy.size());
    EXPECT_EQ(std::string(healthy.data(), healthy.size()), "ijkl");
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();
    ASSERT_EQ(server.requests().size(), 3U);
    for (const auto& request : server.requests()) {
        EXPECT_NE(request.find("Range: bytes=0-3"), std::string::npos);
        EXPECT_NE(request.find("x-oss-range-behavior: standard"),
                  std::string::npos);
    }
}

TEST(OssObjectStorageAdapterTest, ClearsHeadersAtResponseBoundaries) {
    ScriptedOssServer server({
        {200, "",
         "HTTP/1.1 103 Early Hints\r\n"
         "Content-Length: 123\r\n\r\n"
         "HTTP/1.1 200 OK\r\n"
         "Connection: close\r\n\r\n"},
    });
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    auto result = adapter.GetSize("key");
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_READ_FAIL);
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest, IgnoresDfsOnlyConfiguration) {
    DistributedStorageConfig config;
    config.fsdir = "/object-prefix";
    config.fs_adapter_type = "oss";
    config.shard_count = 0;
    config.shard_capacity = 0;
    config.alignment = 0;
    config.single_tenant = false;

    EXPECT_TRUE(config.Validate());
    EXPECT_FALSE(config.ValidateForAllocator());
}

TEST(OssObjectStorageAdapterTest, DecodesPaginatedLogicalKeys) {
    ScriptedOssServer server({
        {200,
         "<ListBucketResult>"
         "<EncodingType>url</EncodingType>"
         "<IsTruncated>true</IsTruncated>"
         "<Contents><Key>test-prefix%2Falpha%252Fbeta</Key>"
         "<Size>3</Size></Contents>"
         "<NextContinuationToken>next%2Btoken</NextContinuationToken>"
         "</ListBucketResult>"},
        {200,
         "<ListBucketResult>"
         "<EncodingType>url</EncodingType>"
         "<IsTruncated>false</IsTruncated>"
         "<Contents><Key>test-prefix%2Fpercent%2525value</Key>"
         "<Size>5</Size></Contents>"
         "</ListBucketResult>"},
    });
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    auto keys = adapter.ListKeys();
    ASSERT_TRUE(keys);
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();

    ASSERT_EQ(keys->size(), 2U);
    EXPECT_EQ((*keys)[0].logical_key, "alpha/beta");
    EXPECT_EQ((*keys)[0].size, 3U);
    EXPECT_EQ((*keys)[1].logical_key, "percent%value");
    EXPECT_EQ((*keys)[1].size, 5U);
    ASSERT_EQ(server.requests().size(), 2U);
    EXPECT_NE(server.requests()[0].find("prefix=test-prefix%2F"),
              std::string::npos);
    EXPECT_NE(server.requests()[1].find("continuation-token=next%2Btoken"),
              std::string::npos);
}

TEST(OssObjectStorageAdapterTest, HealthCheckWritesReadsAndDeletesProbe) {
    ScriptedOssServer server({
        {200, ""},
        {206, "health_check"},
        {204, ""},
    });
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    ASSERT_TRUE(adapter.CheckHealth());
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();

    ASSERT_EQ(server.requests().size(), 3U);
    EXPECT_EQ(server.requests()[0].rfind("PUT ", 0), 0U);
    EXPECT_EQ(server.requests()[1].rfind("GET ", 0), 0U);
    EXPECT_EQ(server.requests()[2].rfind("DELETE ", 0), 0U);
    EXPECT_NE(server.requests()[0].find(
                  "/bucket/test-prefix/.mooncake_health_probe_"),
              std::string::npos);
    EXPECT_NE(server.requests()[1].find("Range: bytes=0-11"),
              std::string::npos);
}

TEST(OssObjectStorageAdapterTest, HealthCheckCleansUpAfterReadMismatch) {
    ScriptedOssServer server({
        {200, ""},
        {206, "wrong_health"},
        {204, ""},
    });
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    auto result = adapter.CheckHealth();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();

    ASSERT_EQ(server.requests().size(), 3U);
    EXPECT_EQ(server.requests()[2].rfind("DELETE ", 0), 0U);
}

TEST(OssObjectStorageAdapterTest,
     DirectGetPreservesNotFoundWithOversizedErrorBody) {
    ScriptedOssServer server({
        {404,
         "<Error><Code>NoSuchKey</Code>" + std::string(512, 'x') + "</Error>"},
        {206, "value"},
    });
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    std::array<char, 7> buffer{'!', '\0', '\0', '\0', '\0', '\0', '!'};
    auto missing = adapter.Get("missing", buffer.data() + 1, 5);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::FILE_NOT_FOUND);
    EXPECT_EQ(buffer.front(), '!');
    EXPECT_EQ(buffer.back(), '!');

    // A failed request must not prevent subsequent reads on the same adapter.
    auto present = adapter.Get("present", buffer.data() + 1, 5);
    ASSERT_TRUE(present);
    EXPECT_EQ(*present, 5U);
    EXPECT_EQ(std::string(buffer.data() + 1, 5), "value");
    EXPECT_EQ(buffer.front(), '!');
    EXPECT_EQ(buffer.back(), '!');
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();
}

TEST(OssObjectStorageAdapterTest,
     GetRangeAndBatchDistinguishNotFoundFromTransferFailures) {
    using Server = test::OssFaultHttpServer;
    Server server({{"missing", Server::Reply(404, std::string(512, 'x'))},
                   {"truncated-error",
                    Server::Raw("HTTP/1.1 404 Test\r\nContent-Length: 5\r\n"
                                "Connection: close\r\n\r\nxx")},
                   {"large-error", Server::Reply(503, std::string(512, 'x'))},
                   {"oversized", Server::Reply(206, "too-long")},
                   {"present", Server::Reply(206, "value")}},
                  true);
    ScopedEnvironment environment;
    ConfigureFaultEndpoint(environment, server.port());
    OssObjectStorageAdapter adapter("/get-error-mapping/");
    ASSERT_TRUE(adapter.Init());
    const std::vector<std::pair<std::string, ErrorCode>> cases{
        {"missing", ErrorCode::FILE_NOT_FOUND},
        {"truncated-error", ErrorCode::DFS_SERVICE_UNAVAILABLE},
        {"large-error", ErrorCode::DFS_SERVICE_UNAVAILABLE},
        {"oversized", ErrorCode::DFS_SERVICE_UNAVAILABLE}};
    for (const auto& [key, error] : cases) {
        SCOPED_TRACE(key);
        std::array<char, 7> buffer;
        buffer.fill('!');
        const auto single = adapter.GetRange(key, buffer.data() + 1, 5, 1);
        ASSERT_FALSE(single);
        EXPECT_EQ(single.error(), error);
        const auto batch = adapter.GetBatch({{key, buffer.data() + 1, 5}});
        ASSERT_EQ(batch.size(), 1U);
        ASSERT_FALSE(batch[0]);
        EXPECT_EQ(batch[0].error(), error);
        EXPECT_EQ(buffer.front(), '!');
        EXPECT_EQ(buffer.back(), '!');
    }
    std::array<char, 5> buffer;
    const auto recovered = adapter.GetBatch({{"present", buffer.data(), 5}});
    ASSERT_EQ(recovered.size(), 1U);
    ASSERT_TRUE(recovered[0]);
    EXPECT_EQ(*recovered[0], buffer.size());
    EXPECT_EQ(std::string(buffer.data(), buffer.size()), "value");
}

TEST(OssObjectStorageAdapterTest,
     BatchGetPreservesRequestOrderAndReportsPartialFailures) {
    ScriptedOssServer server({
        {206, "value"},
        {404, ""},
        {206, "bad"},
    });
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "3");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    std::array<std::array<char, 5>, 3> buffers{};
    const std::vector<ObjectGetRequest> requests{
        {"first", buffers[0].data(), buffers[0].size()},
        {"invalid", nullptr, 5},
        {"second", buffers[1].data(), buffers[1].size()},
        {"empty", nullptr, 0},
        {"third", buffers[2].data(), buffers[2].size()},
    };
    auto results = adapter.GetBatch(requests);
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();
    ASSERT_EQ(results.size(), requests.size());
    ASSERT_FALSE(results[1]);
    EXPECT_EQ(results[1].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(results[3]);
    EXPECT_EQ(*results[3], 0U);
    ASSERT_EQ(server.requests().size(), 3U);

    // Match responses to observed request order rather than assuming that
    // concurrent connections arrive in the order of the input vector.
    for (size_t i = 0; i < server.requests().size(); ++i) {
        const auto& received = server.requests()[i];
        auto request = std::find_if(
            requests.begin(), requests.end(), [&](const auto& candidate) {
                return received.find("/" + candidate.logical_key + " HTTP/") !=
                       std::string::npos;
            });
        ASSERT_NE(request, requests.end());
        const auto& result = results[request - requests.begin()];
        EXPECT_NE(received.find("Range: bytes=0-4"), std::string::npos);
        if (i == 0) {
            ASSERT_TRUE(result);
            EXPECT_EQ(*result, 5U);
            EXPECT_EQ(std::string(static_cast<char*>(request->buffer), 5),
                      "value");
        } else {
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), i == 1 ? ErrorCode::FILE_NOT_FOUND
                                             : ErrorCode::FILE_READ_FAIL);
        }
    }
    EXPECT_TRUE(adapter.GetBatch({}).empty());
}

TEST(OssObjectStorageAdapterTest,
     BatchPutStreamsIovecsAndPreservesPartialResults) {
    ScriptedOssServer server({{200, ""}, {503, "unavailable"}, {200, ""}});
    ScopedEnvironment environment;
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");
    environment.Set("MOONCAKE_OSS_MAX_CONNECTIONS", "3");

    OssObjectStorageAdapter adapter("/test-prefix/");
    ASSERT_TRUE(adapter.Init());
    std::string left(20 * 1024, 'a');
    std::string right(12 * 1024, 'b');
    std::string second = "second value";
    const iovec first_iov[] = {
        {left.data(), left.size()}, {nullptr, 0}, {right.data(), right.size()}};
    const iovec invalid_iov{nullptr, 1};
    // Only the first descriptor belongs to this request. The trailing invalid
    // descriptor must not be read: iovcnt, not the backing array size, wins.
    const iovec second_iov[] = {{second.data(), second.size()}, {nullptr, 1}};
    const std::vector<ObjectPutRequest> requests{
        {"first", first_iov, 3},
        {"invalid", &invalid_iov, 1},
        {"second", second_iov, 1},
        {"empty", nullptr, 0},
    };
    auto results = adapter.PutBatch(requests);
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();
    ASSERT_EQ(results.size(), requests.size());
    ASSERT_FALSE(results[1]);
    EXPECT_EQ(results[1].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_EQ(server.requests().size(), 3U);

    const std::map<std::string, std::string> expected_bodies{
        {"first", left + right}, {"second", second}, {"empty", ""}};
    for (size_t i = 0; i < server.requests().size(); ++i) {
        const auto& received = server.requests()[i];
        auto request = std::find_if(
            requests.begin(), requests.end(), [&](const auto& candidate) {
                return received.find("/" + candidate.logical_key + " HTTP/") !=
                       std::string::npos;
            });
        ASSERT_NE(request, requests.end());
        EXPECT_EQ(received.rfind("PUT ", 0), 0U);
        const size_t body_start = received.find("\r\n\r\n");
        ASSERT_NE(body_start, std::string::npos);
        EXPECT_EQ(received.substr(body_start + 4),
                  expected_bodies.at(request->logical_key));
        const auto& result = results[request - requests.begin()];
        if (i == 1) {
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::FILE_WRITE_FAIL);
        } else {
            EXPECT_TRUE(result);
        }
    }
    EXPECT_TRUE(adapter.PutBatch({}).empty());
}

TEST(OssObjectStorageAdapterTest, LiveSyncBatchGetSmokeAndTiming) {
    mooncake::test::RunLiveOssBatchGet(
        "sync", [](OssObjectStorageAdapter& adapter,
                   const std::vector<ObjectGetRequest>& requests) {
            return adapter.GetBatch(requests);
        });
}

TEST(OssObjectStorageAdapterTest, ObjectLifecycleAndVectorIO) {
    if (!HasOssConfiguration()) {
        GTEST_SKIP() << "OSS configuration is not available";
    }

    OssObjectStorageAdapter adapter(UniquePrefix("lifecycle"));
    ASSERT_TRUE(adapter.Init());
    ObjectCleanup cleanup(adapter);

    std::string first_key = "tenant";
    first_key.push_back('\0');
    first_key += "/file%one";
    const std::string first_data = "hello oss";
    ASSERT_TRUE(adapter.Put(
        first_key,
        std::span<const char>(first_data.data(), first_data.size())));
    cleanup.Add(first_key);

    auto exists = adapter.Exists(first_key);
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
    auto size = adapter.GetSize(first_key);
    ASSERT_TRUE(size);
    EXPECT_EQ(*size, first_data.size());

    std::string read_buffer(first_data.size(), '\0');
    auto read = adapter.Get(first_key, read_buffer.data(), read_buffer.size());
    ASSERT_TRUE(read);
    EXPECT_EQ(*read, first_data.size());
    EXPECT_EQ(read_buffer, first_data);

    const std::string second_key = "file-two";
    std::array<char, 3> left{{'a', 'b', 'c'}};
    std::array<char, 3> right{{'d', 'e', 'f'}};
    std::array<iovec, 2> write_iov{
        {{left.data(), left.size()}, {right.data(), right.size()}}};
    ASSERT_TRUE(adapter.PutV(second_key, write_iov.data(), 2));
    cleanup.Add(second_key);

    std::array<char, 2> read_left{};
    std::array<char, 2> read_right{};
    std::array<iovec, 2> read_iov{{{read_left.data(), read_left.size()},
                                   {read_right.data(), read_right.size()}}};
    auto vector_read = adapter.GetV(second_key, read_iov.data(), 2, 1);
    ASSERT_TRUE(vector_read);
    EXPECT_EQ(*vector_read, 4U);
    EXPECT_EQ(std::string(read_left.data(), read_left.size()), "bc");
    EXPECT_EQ(std::string(read_right.data(), read_right.size()), "de");

    auto keys = adapter.ListKeys();
    ASSERT_TRUE(keys);
    std::map<std::string, size_t> sizes;
    for (const auto& key : *keys) sizes[key.logical_key] = key.size;
    EXPECT_EQ(sizes[first_key], first_data.size());
    EXPECT_EQ(sizes[second_key], 6U);

    EXPECT_TRUE(adapter.Delete(first_key));
    EXPECT_TRUE(adapter.Delete(second_key));
    exists = adapter.Exists(first_key);
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
}

TEST(OssObjectStorageAdapterTest, StorageBackendFactoryRunsObjectHealthCheck) {
    ScriptedOssServer server({
        {200, ""},
        {206, "health_check"},
        {204, ""},
    });
    ScopedEnvironment environment;
    const std::string root = UniquePrefix("factory");
    environment.Set("MOONCAKE_OSS_ENDPOINT",
                    "http://127.0.0.1:" + std::to_string(server.port()));
    environment.Set("MOONCAKE_OSS_BUCKET", "bucket");
    environment.Set("MOONCAKE_OSS_REGION", "region");
    environment.Set("MOONCAKE_OSS_PATH_STYLE", "true");
    environment.Set("MOONCAKE_OSS_ANONYMOUS", "true");
    environment.Set("MOONCAKE_DISTRIBUTED_FS_TYPE", "oss");
    environment.Set("MOONCAKE_DISTRIBUTED_ROOT_DIR", root);
    environment.Set("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", "true");
    environment.Set("MOONCAKE_DISTRIBUTED_HASH_BUCKET_COUNT", "4");
    environment.Set("MOONCAKE_DFS_SHARD_COUNT", "0");
    environment.Set("MOONCAKE_DFS_SHARD_CAPACITY", "0");
    environment.Set("MOONCAKE_DFS_ALIGNMENT", "0");
    environment.Set("MOONCAKE_DFS_SINGLE_TENANT", "false");

    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kDistributed;
    auto backend = CreateStorageBackend(config);
    ASSERT_TRUE(backend);
    auto distributed =
        std::dynamic_pointer_cast<DistributedStorageBackend>(*backend);
    ASSERT_NE(distributed, nullptr);
    EXPECT_TRUE(distributed->UsesObjectStorage());
    ASSERT_TRUE(distributed->Init());
    server.Wait();
    ASSERT_TRUE(server.error().empty()) << server.error();

    ASSERT_EQ(server.requests().size(), 3U);
    EXPECT_EQ(server.requests()[0].rfind("PUT ", 0), 0U);
    EXPECT_EQ(server.requests()[1].rfind("GET ", 0), 0U);
    EXPECT_EQ(server.requests()[2].rfind("DELETE ", 0), 0U);
}

}  // namespace
}  // namespace mooncake
