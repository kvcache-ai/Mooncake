#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "storage/distributed/s3_adapter.h"

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
        Remember(name);
        setenv(name.c_str(), value.c_str(), 1);
    }

    void Unset(const std::string& name) {
        Remember(name);
        unsetenv(name.c_str());
    }

   private:
    void Remember(const std::string& name) {
        if (!original_values_.contains(name)) {
            const char* original = std::getenv(name.c_str());
            original_values_[name] =
                original ? std::optional<std::string>(original) : std::nullopt;
        }
    }

    std::map<std::string, std::optional<std::string>> original_values_;
};

// Clears every variable the S3 config reads so ambient AWS_* settings on a
// developer machine cannot leak into a test.
void ClearS3Environment(ScopedEnvironment& env) {
    for (const char* name :
         {"MOONCAKE_S3_ENDPOINT", "AWS_ENDPOINT_URL", "MOONCAKE_S3_BUCKET",
          "MOONCAKE_S3_REGION", "AWS_REGION", "MOONCAKE_S3_ACCESS_KEY_ID",
          "AWS_ACCESS_KEY_ID", "MOONCAKE_S3_SECRET_ACCESS_KEY",
          "AWS_SECRET_ACCESS_KEY", "MOONCAKE_S3_SESSION_TOKEN",
          "AWS_SESSION_TOKEN", "AWS_DEFAULT_REGION", "MOONCAKE_S3_PATH_STYLE",
          "MOONCAKE_S3_ANONYMOUS", "MOONCAKE_S3_MAX_CONNECTIONS"}) {
        env.Unset(name);
    }
}

class TestableS3Adapter : public S3ObjectStorageAdapter {
   public:
    using S3ObjectStorageAdapter::BuildSignedHeadersAt;
    using S3ObjectStorageAdapter::BuildUrl;
    using S3ObjectStorageAdapter::S3ObjectStorageAdapter;
};

constexpr char kAccessKey[] = "AKIDEXAMPLE";
constexpr char kSecretKey[] = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
constexpr char kTimestamp[] = "20261004T120000Z";

void ConfigureCredentials(ScopedEnvironment& env) {
    env.Set("MOONCAKE_S3_ACCESS_KEY_ID", kAccessKey);
    env.Set("MOONCAKE_S3_SECRET_ACCESS_KEY", kSecretKey);
}

std::string HeaderValue(const std::vector<std::string>& headers,
                        const std::string& name) {
    for (const auto& header : headers) {
        if (header.size() > name.size() + 1 &&
            strncasecmp(header.c_str(), name.c_str(), name.size()) == 0 &&
            header[name.size()] == ':') {
            return header.substr(name.size() + 2);
        }
    }
    return {};
}

// Expected Authorization values below were computed independently with
// botocore's S3SigV4Auth (botocore 1.40.28) for the same request, signed
// headers, credentials and timestamp, with the payload hash pinned to
// UNSIGNED-PAYLOAD (botocore otherwise hashes the body over plain http).

TEST(S3ObjectStorageAdapterTest, SignsPathStylePutLikeBotocore) {
    ScopedEnvironment env;
    ClearS3Environment(env);
    ConfigureCredentials(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "http://127.0.0.1:8333");
    env.Set("MOONCAKE_S3_BUCKET", "kv-bucket");
    env.Set("MOONCAKE_S3_PATH_STYLE", "true");
    TestableS3Adapter adapter("/mooncake");
    ASSERT_TRUE(adapter.Init());

    // Logical key "a/b c" maps to physical key "mooncake/a%2Fb%20c".
    const std::string physical_key = "mooncake/a%2Fb%20c";
    EXPECT_EQ(adapter.BuildUrl(physical_key, {}),
              "http://127.0.0.1:8333/kv-bucket/mooncake/a%252Fb%2520c");
    const auto headers =
        adapter.BuildSignedHeadersAt("PUT", physical_key, {}, "", kTimestamp);
    EXPECT_EQ(HeaderValue(headers, "Host"), "127.0.0.1:8333");
    EXPECT_EQ(HeaderValue(headers, "x-amz-date"), kTimestamp);
    EXPECT_EQ(HeaderValue(headers, "x-amz-content-sha256"), "UNSIGNED-PAYLOAD");
    EXPECT_EQ(
        HeaderValue(headers, "Authorization"),
        "AWS4-HMAC-SHA256 "
        "Credential=AKIDEXAMPLE/20261004/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
        "Signature="
        "2647eaf330ae5eaa05f5579325ee243b4a0cee98cafde0d940fd3c6f65560fac");
    for (const auto& header : headers) {
        EXPECT_EQ(header.find("x-oss-"), std::string::npos) << header;
    }
}

TEST(S3ObjectStorageAdapterTest, SignsEmptyQueryValueWithEqualsSign) {
    ScopedEnvironment env;
    ClearS3Environment(env);
    ConfigureCredentials(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "http://127.0.0.1:8333");
    env.Set("MOONCAKE_S3_BUCKET", "kv-bucket");
    env.Set("MOONCAKE_S3_PATH_STYLE", "true");
    TestableS3Adapter adapter("");
    ASSERT_TRUE(adapter.Init());

    const std::map<std::string, std::string> query{{"encoding-type", "url"},
                                                   {"list-type", "2"},
                                                   {"max-keys", "1000"},
                                                   {"prefix", ""}};
    EXPECT_EQ(adapter.BuildUrl("", query),
              "http://127.0.0.1:8333/kv-bucket/"
              "?encoding-type=url&list-type=2&max-keys=1000&prefix=");
    const auto headers =
        adapter.BuildSignedHeadersAt("GET", "", query, "", kTimestamp);
    EXPECT_EQ(
        HeaderValue(headers, "Authorization"),
        "AWS4-HMAC-SHA256 "
        "Credential=AKIDEXAMPLE/20261004/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
        "Signature="
        "9865884cab1e39c4552e33b0fcd6f3cf6450619d664dc9327b40624dd3b11409");
}

TEST(S3ObjectStorageAdapterTest, SignsVirtualHostedRequestWithSessionToken) {
    ScopedEnvironment env;
    ClearS3Environment(env);
    env.Set("AWS_ACCESS_KEY_ID", kAccessKey);
    env.Set("AWS_SECRET_ACCESS_KEY", kSecretKey);
    env.Set("AWS_ENDPOINT_URL", "https://s3.us-west-2.amazonaws.com");
    env.Set("MOONCAKE_S3_BUCKET", "kv-bucket");
    env.Set("AWS_REGION", "us-west-2");
    env.Set("AWS_SESSION_TOKEN", "SESSIONTOKEN/abc+=");
    TestableS3Adapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    EXPECT_EQ(adapter.BuildUrl("p/key one", {}),
              "https://kv-bucket.s3.us-west-2.amazonaws.com/p/key%20one");
    const auto headers = adapter.BuildSignedHeadersAt("GET", "p/key one", {},
                                                      "bytes=0-9", kTimestamp);
    EXPECT_EQ(HeaderValue(headers, "Host"),
              "kv-bucket.s3.us-west-2.amazonaws.com");
    EXPECT_EQ(HeaderValue(headers, "x-amz-security-token"),
              "SESSIONTOKEN/abc+=");
    EXPECT_EQ(
        HeaderValue(headers, "Authorization"),
        "AWS4-HMAC-SHA256 "
        "Credential=AKIDEXAMPLE/20261004/us-west-2/s3/aws4_request, "
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date;"
        "x-amz-security-token, "
        "Signature="
        "bf221ce4cddae3cb8b5e1d86a22908177b5e5ac78713a0d0c9a2b441ffe12382");
}

TEST(S3ObjectStorageAdapterTest, OmitsDefaultPortFromSignedHost) {
    ScopedEnvironment env;
    ClearS3Environment(env);
    ConfigureCredentials(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "https://S3.Example.com:443");
    env.Set("MOONCAKE_S3_BUCKET", "kv-bucket");
    env.Set("MOONCAKE_S3_REGION", "eu-central-1");
    env.Set("MOONCAKE_S3_PATH_STYLE", "true");
    TestableS3Adapter adapter("");
    ASSERT_TRUE(adapter.Init());

    const auto headers =
        adapter.BuildSignedHeadersAt("HEAD", "k", {}, "", kTimestamp);
    EXPECT_EQ(HeaderValue(headers, "Host"), "s3.example.com");
    EXPECT_EQ(
        HeaderValue(headers, "Authorization"),
        "AWS4-HMAC-SHA256 "
        "Credential=AKIDEXAMPLE/20261004/eu-central-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
        "Signature="
        "6dc495469ec643cac87c6b99dd1302e6421a12a4efefb0f7d32eccf4ffe2464c");
}

TEST(S3ObjectStorageAdapterTest, AnonymousRequestsCarryNoAuthorization) {
    ScopedEnvironment env;
    ClearS3Environment(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "http://127.0.0.1:8333");
    env.Set("MOONCAKE_S3_BUCKET", "kv-bucket");
    env.Set("MOONCAKE_S3_ANONYMOUS", "true");
    TestableS3Adapter adapter("");
    ASSERT_TRUE(adapter.Init());
    const auto headers =
        adapter.BuildSignedHeadersAt("GET", "k", {}, "", kTimestamp);
    EXPECT_TRUE(HeaderValue(headers, "Authorization").empty());
    EXPECT_FALSE(HeaderValue(headers, "Host").empty());
}

// Serves one scripted response per connection and records each request.
constexpr char kCloseWithoutResponse[] = "<close>";

std::string RawResponse(const std::string& status_line,
                        const std::string& extra_headers,
                        const std::string& body) {
    return "HTTP/1.1 " + status_line + "\r\n" + extra_headers +
           "Content-Length: " + std::to_string(body.size()) +
           "\r\nConnection: close\r\n\r\n" + body;
}

std::string SlowDown() {
    return RawResponse("503 Slow Down", "Content-Type: application/xml\r\n",
                       "<Error><Code>SlowDown</Code><Message>Please reduce "
                       "your request rate.</Message></Error>");
}

class OneShotHttpServer {
   public:
    explicit OneShotHttpServer(std::vector<std::string> bodies)
        : bodies_(std::move(bodies)) {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) throw std::runtime_error("socket failed");
        int reuse = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t size = sizeof(address);
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), size) !=
                0 ||
            listen(listen_fd_, 4) != 0 ||
            getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                        &size) != 0) {
            close(listen_fd_);
            throw std::runtime_error("listen failed");
        }
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { Serve(); });
    }

    ~OneShotHttpServer() {
        if (thread_.joinable()) thread_.join();
        close(listen_fd_);
    }

    uint16_t port() const { return port_; }
    const std::vector<std::string>& requests() const { return requests_; }

   private:
    void Serve() {
        for (const auto& body : bodies_) {
            pollfd descriptor{listen_fd_, POLLIN, 0};
            if (poll(&descriptor, 1, 5000) <= 0) return;
            const int client = accept(listen_fd_, nullptr, nullptr);
            if (client < 0) return;
            std::string request;
            std::array<char, 4096> buffer{};
            while (request.find("\r\n\r\n") == std::string::npos) {
                pollfd in{client, POLLIN, 0};
                if (poll(&in, 1, 5000) <= 0) break;
                const ssize_t received =
                    recv(client, buffer.data(), buffer.size(), 0);
                if (received <= 0) break;
                request.append(buffer.data(), static_cast<size_t>(received));
            }
            requests_.push_back(request);
            // kCloseWithoutResponse drops the connection; a body that starts
            // with a status line is sent as the complete response; anything
            // else is sent as a 200 XML body.
            if (body != kCloseWithoutResponse) {
                const std::string response =
                    body.rfind("HTTP/1.1 ", 0) == 0
                        ? body
                        : "HTTP/1.1 200 OK\r\nContent-Type: "
                          "application/xml\r\nContent-Length: " +
                              std::to_string(body.size()) +
                              "\r\nConnection: close\r\n\r\n" + body;
                send(client, response.data(), response.size(), MSG_NOSIGNAL);
            }
            close(client);
        }
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::thread thread_;
    std::vector<std::string> bodies_;
    std::vector<std::string> requests_;
};

std::string ListPage(const std::string& key, bool truncated,
                     const std::string& token) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>"
           "<Contents><Key>" +
           key + "</Key><Size>3</Size></Contents><IsTruncated>" +
           (truncated ? "true" : "false") + "</IsTruncated>" +
           (token.empty() ? ""
                          : "<NextContinuationToken>" + token +
                                "</NextContinuationToken>") +
           "</ListBucketResult>";
}

// S3 returns NextContinuationToken unencoded even with encoding-type=url, so
// the adapter must send it back byte for byte. OSS encodes it; decoding an S3
// token would corrupt any token that contains '%'.
TEST(S3ObjectStorageAdapterTest, ListKeysSendsContinuationTokenUnchanged) {
    const std::string token = "1/abc%2Fdef+==";
    OneShotHttpServer server(
        {ListPage("p/first", true, token), ListPage("p/second", false, "")});
    ScopedEnvironment env;
    ClearS3Environment(env);
    ConfigureCredentials(env);
    env.Set("MOONCAKE_S3_ENDPOINT",
            "http://127.0.0.1:" + std::to_string(server.port()));
    env.Set("MOONCAKE_S3_BUCKET", "kv-bucket");
    env.Set("MOONCAKE_S3_PATH_STYLE", "true");
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    auto keys = adapter.ListKeys();
    ASSERT_TRUE(keys.has_value());
    ASSERT_EQ(keys->size(), 2u);
    EXPECT_EQ((*keys)[0].logical_key, "first");
    EXPECT_EQ((*keys)[1].logical_key, "second");

    ASSERT_EQ(server.requests().size(), 2u);
    const std::string& second = server.requests()[1];
    // "1/abc%2Fdef+==" URI-encoded once.
    EXPECT_NE(second.find("continuation-token=1%2Fabc%252Fdef%2B%3D%3D"),
              std::string::npos)
        << second;
    for (const auto& request : server.requests()) {
        // libcurl sends the Host header the adapter signed, exactly once.
        const std::string host =
            "\r\nHost: 127.0.0.1:" + std::to_string(server.port()) + "\r\n";
        EXPECT_NE(request.find(host), std::string::npos) << request;
        EXPECT_EQ(request.find("\r\nHost:", request.find(host) + 1),
                  std::string::npos)
            << request;
        EXPECT_NE(request.find("prefix=p%2F"), std::string::npos) << request;
    }
}

void ConfigureLocalEndpoint(ScopedEnvironment& env, uint16_t port) {
    ClearS3Environment(env);
    ConfigureCredentials(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "http://127.0.0.1:" + std::to_string(port));
    env.Set("MOONCAKE_S3_BUCKET", "kv-bucket");
    env.Set("MOONCAKE_S3_PATH_STYLE", "true");
}

// A throttled or failing service answers 503/5xx; the request is signed and
// sent again rather than failing the caller.
TEST(S3ObjectStorageAdapterTest, RetriesTransientStatusThenSucceeds) {
    OneShotHttpServer server({SlowDown(), ListPage("p/only", false, "")});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    auto keys = adapter.ListKeys();
    ASSERT_TRUE(keys.has_value());
    ASSERT_EQ(keys->size(), 1u);
    EXPECT_EQ((*keys)[0].logical_key, "only");
    EXPECT_EQ(server.requests().size(), 2u);
}

TEST(S3ObjectStorageAdapterTest, DoesNotRetryClientErrors) {
    OneShotHttpServer server({RawResponse(
        "403 Forbidden", "Content-Type: application/xml\r\n",
        "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>")});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    EXPECT_FALSE(adapter.ListKeys().has_value());
    EXPECT_EQ(server.requests().size(), 1u);
}

TEST(S3ObjectStorageAdapterTest, GivesUpAfterThreeAttempts) {
    OneShotHttpServer server({SlowDown(), SlowDown(), SlowDown()});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    EXPECT_FALSE(adapter.ListKeys().has_value());
    EXPECT_EQ(server.requests().size(), 3u);
}

TEST(S3ObjectStorageAdapterTest, BatchGetRetriesTransientFailure) {
    OneShotHttpServer server(
        {SlowDown(), RawResponse("206 Partial Content",
                                 "Content-Range: bytes 0-2/3\r\n", "abc")});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    std::string buffer(3, '\0');
    auto results = adapter.GetBatch({{"k", buffer.data(), buffer.size()}});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_EQ(buffer, "abc");
    EXPECT_EQ(server.requests().size(), 2u);
}

// A connection dropped before any response (for example a reset after a
// stalled connect) is retried, and the upload is sent again from the start.
TEST(S3ObjectStorageAdapterTest, BatchPutRetriesDroppedConnection) {
    OneShotHttpServer server(
        {kCloseWithoutResponse, RawResponse("200 OK", "", "")});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    std::string payload = "payload";
    iovec iov{payload.data(), payload.size()};
    auto results = adapter.PutBatch({{"k", &iov, 1}});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_EQ(server.requests().size(), 2u);
}

// The request target on the wire must be the path that was signed. libcurl
// collapses "." and ".." segments unless told not to, which would make the
// service see a different path than the canonical URI in the signature.
TEST(S3ObjectStorageAdapterTest, SendsDotSegmentsInPrefixAndKeyUnchanged) {
    OneShotHttpServer server({RawResponse("200 OK", "", ""),
                              RawResponse("200 OK", "", ""),
                              RawResponse("200 OK", "", "")});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("/mooncake/../tenant");
    ASSERT_TRUE(adapter.Init());

    const std::string payload = "v";
    ASSERT_TRUE(adapter.Put("ordinary-key", payload));
    ASSERT_TRUE(adapter.Put(".", payload));
    ASSERT_TRUE(adapter.Put("..", payload));

    ASSERT_EQ(server.requests().size(), 3u);
    const char* const targets[] = {
        "PUT /kv-bucket/mooncake/../tenant/ordinary-key HTTP/1.1\r\n",
        "PUT /kv-bucket/mooncake/../tenant/. HTTP/1.1\r\n",
        "PUT /kv-bucket/mooncake/../tenant/.. HTTP/1.1\r\n"};
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(server.requests()[i].rfind(targets[i], 0), 0u)
            << server.requests()[i].substr(0, 80);
    }
}

// ListObjectsV2 can answer HTTP 200 with an incomplete document. A partial
// listing must be an error, never a successful shorter listing.
TEST(S3ObjectStorageAdapterTest, RejectsMalformedListResponses) {
    const std::string first =
        "<Contents><Key>p/first</Key><Size>3</Size></Contents>";
    const std::vector<std::string> malformed = {
        // Second Contents element never closed.
        "<ListBucketResult>" + first +
            "<Contents><Key>p/second</Key><Size>3</Size>"
            "<IsTruncated>false</IsTruncated></ListBucketResult>",
        // Document cut off before the root element closes.
        "<ListBucketResult>" + first + "<IsTruncated>false</IsTruncated>",
        // Cut off before any key, root never closed.
        "<ListBucketResult><Name>kv-bucket</Name>",
        // Complete elements but no IsTruncated.
        "<ListBucketResult>" + first + "</ListBucketResult>",
        // A Contents element without a Size.
        "<ListBucketResult><Contents><Key>p/first</Key></Contents>"
        "<IsTruncated>false</IsTruncated></ListBucketResult>",
        // A Size that is not a number.
        "<ListBucketResult><Contents><Key>p/first</Key><Size>3x</Size>"
        "</Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
        // An IsTruncated that is neither true nor false.
        "<ListBucketResult>" + first +
            "<IsTruncated>maybe</IsTruncated></ListBucketResult>",
        // Text after the root element.
        "<ListBucketResult>" + first +
            "<IsTruncated>false</IsTruncated></ListBucketResult>junk",
    };
    for (const auto& body : malformed) {
        SCOPED_TRACE(body);
        OneShotHttpServer server({body});
        ScopedEnvironment env;
        ConfigureLocalEndpoint(env, server.port());
        S3ObjectStorageAdapter adapter("p");
        ASSERT_TRUE(adapter.Init());

        auto keys = adapter.ListKeys();
        EXPECT_FALSE(keys.has_value());
        EXPECT_EQ(server.requests().size(), 1u);
    }
}

// Whitespace inside tags, the S3 namespace and entity-encoded keys are valid
// XML; every listed object must come back.
TEST(S3ObjectStorageAdapterTest, ListKeysAcceptsWhitespaceInTags) {
    OneShotHttpServer server(
        {"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult "
         "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
         "<Contents ><Key >p/first</Key ><Size >3</Size ></Contents >"
         "<Contents><Key>p/a&amp;b</Key><Size>4</Size></Contents>"
         "<IsTruncated>false</IsTruncated></ListBucketResult>"});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    auto keys = adapter.ListKeys();
    ASSERT_TRUE(keys.has_value());
    ASSERT_EQ(keys->size(), 2u);
    EXPECT_EQ((*keys)[0].logical_key, "first");
    EXPECT_EQ((*keys)[0].size, 3u);
    EXPECT_EQ((*keys)[1].logical_key, "a&b");
    EXPECT_EQ((*keys)[1].size, 4u);
}

// An empty but complete listing is still a valid, successful result.
TEST(S3ObjectStorageAdapterTest, AcceptsEmptyCompleteListResponse) {
    OneShotHttpServer server(
        {"<ListBucketResult><KeyCount>0</KeyCount>"
         "<IsTruncated>false</IsTruncated>"
         "</ListBucketResult>"});
    ScopedEnvironment env;
    ConfigureLocalEndpoint(env, server.port());
    S3ObjectStorageAdapter adapter("p");
    ASSERT_TRUE(adapter.Init());

    auto keys = adapter.ListKeys();
    ASSERT_TRUE(keys.has_value());
    EXPECT_TRUE(keys->empty());
}

}  // namespace
}  // namespace mooncake
