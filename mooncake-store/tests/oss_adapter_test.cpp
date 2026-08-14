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
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/oss_adapter.h"
#include "storage_backend.h"

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
    };

    explicit ScriptedOssServer(std::vector<Response> responses)
        : responses_(std::move(responses)) {
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
        if (listen(listen_fd_, 2) != 0) {
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
        if (listen_fd_ >= 0) close(listen_fd_);
        if (thread_.joinable()) thread_.join();
    }

    uint16_t port() const { return port_; }

    void Wait() {
        if (thread_.joinable()) thread_.join();
    }

    const std::vector<std::string>& requests() const { return requests_; }
    const std::string& error() const { return error_; }

   private:
    static bool SendAll(int fd, const std::string& data) {
        size_t offset = 0;
        while (offset < data.size()) {
            const ssize_t sent =
                send(fd, data.data() + offset, data.size() - offset, 0);
            if (sent <= 0) return false;
            offset += static_cast<size_t>(sent);
        }
        return true;
    }

    static std::string ReadRequest(int fd) {
        std::string request;
        std::array<char, 4096> buffer{};
        while (request.find("\r\n\r\n") == std::string::npos) {
            const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
            if (received <= 0) break;
            request.append(buffer.data(), static_cast<size_t>(received));
        }
        return request;
    }

    void Serve() {
        for (const auto& response : responses_) {
            pollfd descriptor{listen_fd_, POLLIN, 0};
            if (poll(&descriptor, 1, 5000) <= 0) {
                error_ = "timed out waiting for OSS request";
                return;
            }
            const int client = accept(listen_fd_, nullptr, nullptr);
            if (client < 0) {
                error_ = "accept failed";
                return;
            }
            requests_.push_back(ReadRequest(client));
            const std::string status_text =
                response.status == 204   ? "No Content"
                : response.status == 206 ? "Partial Content"
                                         : "OK";
            const std::string response_data =
                "HTTP/1.1 " + std::to_string(response.status) + " " +
                status_text +
                "\r\nContent-Type: application/xml\r\n"
                "Content-Length: " +
                std::to_string(response.body.size()) +
                "\r\nConnection: close\r\n\r\n" + response.body;
            if (!SendAll(client, response_data)) error_ = "send failed";
            close(client);
            if (!error_.empty()) return;
        }
        close(listen_fd_);
        listen_fd_ = -1;
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::thread thread_;
    std::vector<Response> responses_;
    std::vector<std::string> requests_;
    std::string error_;
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
