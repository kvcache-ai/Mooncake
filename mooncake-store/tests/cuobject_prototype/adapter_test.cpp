#include "adapter.h"

#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <optional>

#include "../oss_fault_test_helpers.h"

namespace mooncake::cuobject_prototype {
namespace {
using Server = test::OssFaultHttpServer;

// Simulates only the RDMA data operation, never used for performance numbers.
// The real HTTP request/response goes through the existing loopback fixture.
class FakeRdma : public RdmaClient {
   public:
    bool Register(void* ptr, size_t size) override {
        registrations++;
        buffers[ptr] = size;
        return true;
    }
    void Unregister(void* ptr) override { buffers.erase(ptr); }
    std::string Token(void* ptr, size_t size, bool upload) override {
        if (!buffers.contains(ptr) || buffers[ptr] < size) return {};
        last_ptr = ptr;
        acquired++;
        if (upload) {
            uploaded.assign(static_cast<char*>(ptr), size);
        } else {
            if (download.size() != size) return {};
            std::memcpy(ptr, download.data(), size);
        }
        return "test-rdma-token";
    }
    void Release(const std::string&) override { released++; }
    void Abort(const std::string&) override { aborted++; }
    std::map<void*, size_t> buffers;
    std::string download = "hello";
    std::string uploaded;
    void* last_ptr = nullptr;
    size_t registrations = 0, acquired = 0, released = 0, aborted = 0;
};

class Environment {
   public:
    explicit Environment(uint16_t port) {
        for (const auto* name :
             {"MOONCAKE_S3_ENDPOINT", "AWS_ENDPOINT_URL", "MOONCAKE_S3_BUCKET",
              "MOONCAKE_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION",
              "MOONCAKE_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID",
              "MOONCAKE_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY",
              "MOONCAKE_S3_SESSION_TOKEN", "AWS_SESSION_TOKEN",
              "MOONCAKE_S3_ANONYMOUS", "MOONCAKE_S3_PATH_STYLE", "NO_PROXY",
              "no_proxy"}) {
            const char* value = std::getenv(name);
            previous[name] =
                value ? std::optional<std::string>(value) : std::nullopt;
            unsetenv(name);
        }
        setenv("MOONCAKE_S3_ENDPOINT",
               ("http://127.0.0.1:" + std::to_string(port)).c_str(), 1);
        setenv("MOONCAKE_S3_BUCKET", "test-bucket", 1);
        setenv("MOONCAKE_S3_ANONYMOUS", "true", 1);
        setenv("MOONCAKE_S3_PATH_STYLE", "true", 1);
        setenv("NO_PROXY", "127.0.0.1", 1);
        setenv("no_proxy", "127.0.0.1", 1);
    }
    ~Environment() {
        for (const auto& [name, value] : previous) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
    }
    std::map<std::string, std::optional<std::string>> previous;
};

std::string GetHeaders(const std::string& count = "5") {
    return "x-amz-rdma-reply: 206\r\nx-amz-rdma-bytes-transferred: " + count +
           "\r\n";
}

TEST(CuObjectPrototype, GetReusesOriginalBufferWithoutHttpPayload) {
    Server server({{"data", Server::Reply(206, "", GetHeaders())}});
    Environment environment(server.port());
    auto client = std::make_shared<FakeRdma>();
    Adapter adapter("prototype", client);
    ASSERT_TRUE(adapter.Init());
    char buffer[5]{};
    ASSERT_TRUE(client->Register(buffer, sizeof(buffer)));
    for (int i = 0; i < 8; ++i) {
        std::memset(buffer, 0, sizeof(buffer));
        auto result = adapter.Get("data", buffer, sizeof(buffer));
        ASSERT_TRUE(result);
        EXPECT_EQ(*result, sizeof(buffer));
        EXPECT_EQ(std::string(buffer, sizeof(buffer)), "hello");
        EXPECT_EQ(client->last_ptr, buffer);
    }
    EXPECT_EQ(client->registrations, 1);
    EXPECT_EQ(client->released, 8);
    EXPECT_EQ(client->aborted, 0);
    server.Stop();
    ASSERT_EQ(server.requests().size(), 8);
    for (const auto& request : server.requests()) {
        EXPECT_NE(request.find("x-amz-rdma-token: test-rdma-token"),
                  std::string::npos);
        EXPECT_NE(request.find("Range: bytes=0-4"), std::string::npos);
        EXPECT_EQ(request.substr(request.find("\r\n\r\n") + 4), "");
    }
}

TEST(CuObjectPrototype, AcceptsFullGetWithMatchingAcknowledgmentAndCount) {
    Server server(
        {{"data", Server::Reply(200, "",
                                "x-amz-rdma-reply: 200\r\n"
                                "x-amz-rdma-bytes-transferred: 5\r\n")}});
    Environment environment(server.port());
    auto client = std::make_shared<FakeRdma>();
    Adapter adapter("prototype", client);
    ASSERT_TRUE(adapter.Init());
    char buffer[5]{};
    client->Register(buffer, sizeof(buffer));
    auto results =
        adapter.GetBatch({ObjectGetRequest{"data", buffer, sizeof(buffer)}});
    ASSERT_EQ(results.size(), 1);
    ASSERT_TRUE(results[0]);
    EXPECT_EQ(*results[0], sizeof(buffer));
    EXPECT_EQ(std::string(buffer, sizeof(buffer)), "hello");
    EXPECT_EQ(client->released, 1);
    EXPECT_EQ(client->aborted, 0);
}

TEST(CuObjectPrototype, AcceptsMinioSuccessAcknowledgmentForRangedGet) {
    Server server({{"data", Server::Reply(206, "",
                                          "x-amz-rdma-reply: 200\r\n"
                                          "x-amz-rdma-bytes: 5\r\n")}});
    Environment environment(server.port());
    auto client = std::make_shared<FakeRdma>();
    Adapter adapter("prototype", client);
    ASSERT_TRUE(adapter.Init());
    char buffer[5]{};
    ASSERT_TRUE(client->Register(buffer, sizeof(buffer)));
    auto result = adapter.Get("data", buffer, sizeof(buffer));
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, sizeof(buffer));
    EXPECT_EQ(std::string(buffer, sizeof(buffer)), "hello");
    EXPECT_EQ(client->released, 1);
    EXPECT_EQ(client->aborted, 0);
}

TEST(CuObjectPrototype, PutUsesRegisteredSourceAndSendsZeroHttpBody) {
    Server server(
        {{"data", Server::Reply(200, "", "x-amz-rdma-reply: 200\r\n")}});
    Environment environment(server.port());
    auto client = std::make_shared<FakeRdma>();
    Adapter adapter("prototype", client);
    ASSERT_TRUE(adapter.Init());
    char buffer[] = "hello";
    ASSERT_TRUE(client->Register(buffer, 5));
    iovec iov{buffer, 5};
    auto result = adapter.PutBatch({ObjectPutRequest{"data", &iov, 1}});
    ASSERT_EQ(result.size(), 1);
    EXPECT_TRUE(result[0]);
    EXPECT_EQ(client->last_ptr, buffer);
    EXPECT_EQ(client->uploaded, "hello");
    EXPECT_EQ(client->released, 1);
    server.Stop();
    ASSERT_EQ(server.requests().size(), 1);
    const auto request = server.requests()[0];
    EXPECT_NE(request.find("Content-Length: 0"), std::string::npos);
    EXPECT_EQ(request.substr(request.find("\r\n\r\n") + 4), "");
}

TEST(CuObjectPrototype, RejectsUnregisteredBuffersAndMultiSlicePuts) {
    Server server({});
    Environment environment(server.port());
    auto client = std::make_shared<FakeRdma>();
    Adapter adapter("prototype", client);
    ASSERT_TRUE(adapter.Init());
    char buffer[5]{};
    EXPECT_FALSE(adapter.Get("data", buffer, sizeof(buffer)));
    iovec slices[2]{{buffer, 2}, {buffer + 2, 3}};
    auto result = adapter.PutV("data", slices, 2);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
    EXPECT_EQ(client->acquired, 0);
}

TEST(CuObjectPrototype, RejectsMissingAcknowledgmentAndInvalidByteCounts) {
    const std::vector<std::string> headers = {
        "",
        GetHeaders("4"),
        GetHeaders("5garbage"),
        GetHeaders("-1"),
        GetHeaders("184467440737095516160"),
        GetHeaders() + "x-amz-rdma-bytes: 4\r\n",
        "x-amz-rdma-reply: 200\r\nx-amz-rdma-bytes: 4\r\n",
        GetHeaders() + "x-amz-rdma-reply: 206\r\n"};
    for (const auto& fields : headers) {
        Server server({{"data", Server::Reply(206, "", fields)}});
        Environment environment(server.port());
        auto client = std::make_shared<FakeRdma>();
        Adapter adapter("prototype", client);
        ASSERT_TRUE(adapter.Init());
        char buffer[5]{};
        client->Register(buffer, 5);
        EXPECT_FALSE(adapter.Get("data", buffer, 5)) << fields;
        EXPECT_EQ(client->released, 0);
        EXPECT_EQ(client->aborted, 1);
    }
}

TEST(CuObjectPrototype, RejectsOrdinaryS3EmptyPutAcknowledgment) {
    Server server({{"data", Server::Reply(200, "", "ETag: \"empty\"\r\n")}});
    Environment environment(server.port());
    auto client = std::make_shared<FakeRdma>();
    Adapter adapter("prototype", client);
    ASSERT_TRUE(adapter.Init());
    char buffer[] = "hello";
    client->Register(buffer, 5);
    EXPECT_FALSE(adapter.Put("data", {buffer, 5}));
    EXPECT_EQ(client->aborted, 1);
    EXPECT_EQ(client->released, 0);
}

TEST(CuObjectPrototype, AbortsOnHttpBodyErrorOrConnectionReset) {
    for (const auto& response :
         {Server::Reply(206, "unexpected body", GetHeaders()),
          Server::Reply(500, "error"), Server::Reset()}) {
        Server server({{"data", response}});
        Environment environment(server.port());
        auto client = std::make_shared<FakeRdma>();
        Adapter adapter("prototype", client);
        ASSERT_TRUE(adapter.Init());
        char buffer[5]{};
        client->Register(buffer, 5);
        EXPECT_FALSE(adapter.Get("data", buffer, 5));
        EXPECT_EQ(client->aborted, 1);
        EXPECT_EQ(client->released, 0);
    }
}

}  // namespace
}  // namespace mooncake::cuobject_prototype
