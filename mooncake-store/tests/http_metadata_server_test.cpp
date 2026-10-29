#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <vector>
#include <string>
#include <thread>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_http/coro_http_client.hpp>

#include "http_metadata_server.h"
#ifdef USE_HTTP
#include "transfer_metadata_plugin.h"
#endif

namespace mooncake::testing {

// Restore the process environment even when an ASSERT aborts a test. Tests that
// change these variables must not run concurrently in the same process.
class ScopedEnvironment {
   public:
    explicit ScopedEnvironment(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) old_value_ = value;
        unsetenv(name);
    }
    ~ScopedEnvironment() {
        if (old_value_)
            setenv(name_.c_str(), old_value_->c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

   private:
    std::string name_;
    std::optional<std::string> old_value_;
};

class HttpMetadataServerTest : public ::testing::Test {
   protected:
    ScopedEnvironment token_{"MC_METADATA_HTTP_TOKEN"};
    ScopedEnvironment insecure_{"MC_METADATA_HTTP_ALLOW_INSECURE"};
    ScopedEnvironment keys_{"MC_METADATA_MAX_KEYS"};
    ScopedEnvironment bytes_{"MC_METADATA_MAX_TOTAL_BYTES"};
    ScopedEnvironment body_{"MC_METADATA_MAX_BODY_BYTES"};

    struct HttpResponse {
        int status;
        std::string body;
    };

    HttpResponse Get(int port, const std::string& path,
                     const std::string& authorization = "") {
        coro_http::coro_http_client client;
        if (!authorization.empty())
            client.add_header("Authorization", authorization);
        auto response = async_simple::coro::syncAwait(client.async_get(
            "http://127.0.0.1:" + std::to_string(port) + path));
        return {response.status, std::string(response.resp_body)};
    }

    HttpResponse Put(int port, const std::string& path, const std::string& body,
                     const std::string& authorization = "") {
        coro_http::coro_http_client client;
        if (!authorization.empty())
            client.add_header("Authorization", authorization);
        auto response = async_simple::coro::syncAwait(
            client.async_put("http://127.0.0.1:" + std::to_string(port) + path,
                             body, coro_http::req_content_type::json));
        return {response.status, std::string(response.resp_body)};
    }

    HttpResponse Delete(int port, const std::string& path,
                        const std::string& authorization = "") {
        coro_http::coro_http_client client;
        if (!authorization.empty())
            client.add_header("Authorization", authorization);
        auto response = async_simple::coro::syncAwait(client.async_delete(
            "http://127.0.0.1:" + std::to_string(port) + path, "",
            coro_http::req_content_type::json));
        return {response.status, std::string(response.resp_body)};
    }

    // Send headers without a body to prove rejection happens before body reads.
    // Bound the response wait so a regression fails instead of hanging the
    // suite.
    int RawRequest(int port, const std::string& request) {
        asio::io_context io;
        asio::ip::tcp::socket socket(io);
        socket.connect(
            {asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port)});
        asio::write(socket, asio::buffer(request));
        socket.non_blocking(true);
        std::string response;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline) {
            char buffer[1024];
            std::error_code ec;
            const auto size = socket.read_some(asio::buffer(buffer), ec);
            if (!ec) {
                response.append(buffer, size);
                if (response.find("\r\n") != std::string::npos) {
                    return std::stoi(
                        response.substr(response.find(' ') + 1, 3));
                }
            } else if (ec != asio::error::would_block &&
                       ec != asio::error::try_again) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return 0;
    }

    void WaitUntilReady(int port) {
        for (int i = 0; i < 50; ++i) {
            if (Get(port, "/health").status == 200) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "HTTP metadata server did not become ready";
    }
};

TEST_F(HttpMetadataServerTest, AllowsIdempotentRpcMetaRepublish) {
    setenv("MC_METADATA_MAX_KEYS", "1", 1);
    const auto entry_bytes =
        std::string("mooncake/rpc_meta/10.0.0.1:12384").size() +
        std::string(R"({"ip_or_host_name":"10.0.0.1","rpc_port":15228})")
            .size();
    setenv("MC_METADATA_MAX_TOTAL_BYTES", std::to_string(entry_bytes).c_str(),
           1);
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    const int port = server.port();
    WaitUntilReady(port);

    const std::string path =
        "/metadata?key=mooncake%2Frpc_meta%2F10.0.0.1%3A12384";
    const std::string body =
        R"({"ip_or_host_name":"10.0.0.1","rpc_port":15228})";

    EXPECT_EQ(Put(port, path, body).status, 200);

    auto second = Put(port, path, body);
    EXPECT_EQ(second.status, 200);
    EXPECT_EQ(second.body, "metadata unchanged");

    auto stored = Get(port, path);
    EXPECT_EQ(stored.status, 200);
    EXPECT_EQ(stored.body, body);

    server.stop();
}

TEST_F(HttpMetadataServerTest, RejectsChangedRpcMetaRepublish) {
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    const int port = server.port();
    WaitUntilReady(port);

    const std::string path =
        "/metadata?key=mooncake%2Frpc_meta%2F10.0.0.1%3A12384";
    const std::string original =
        R"({"ip_or_host_name":"10.0.0.1","rpc_port":15228})";
    const std::string changed =
        R"({"ip_or_host_name":"10.0.0.1","rpc_port":16000})";

    EXPECT_EQ(Put(port, path, original).status, 200);

    auto second = Put(port, path, changed);
    EXPECT_EQ(second.status, 400);
    EXPECT_EQ(second.body, "Duplicate rpc_meta key not allowed");

    auto stored = Get(port, path);
    EXPECT_EQ(stored.status, 200);
    EXPECT_EQ(stored.body, original);

    server.stop();
}

TEST_F(HttpMetadataServerTest, StartReportsBindFailure) {
    HttpMetadataServer first(0, "127.0.0.1");
    ASSERT_TRUE(first.start());
    const int port = first.port();
    WaitUntilReady(port);

    // A second server cannot bind the already-taken port; start() must report
    // the failure instead of claiming a healthy server that never came up.
    HttpMetadataServer second(static_cast<uint16_t>(port), "127.0.0.1");
    EXPECT_FALSE(second.start());
    EXPECT_FALSE(second.is_running());

    first.stop();
}

TEST_F(HttpMetadataServerTest, EnforcesBindTokenMatrix) {
    for (const auto* host :
         {"127.0.0.1", "localhost", "::1", "0.0.0.0", "::"}) {
        const bool loopback =
            std::string(host) != "0.0.0.0" && std::string(host) != "::";
        for (const auto* token : {"", "matrix-test-token"}) {
            for (const auto* opt_in : {"", "0", "false", "1", "TrUe", "YES"}) {
                SCOPED_TRACE(std::string(host) + ": token=" +
                             (std::string(token).empty() ? "unset" : "set") +
                             ", opt_in=" + opt_in);
                setenv("MC_METADATA_HTTP_TOKEN", token, 1);
                setenv("MC_METADATA_HTTP_ALLOW_INSECURE", opt_in, 1);
                HttpMetadataServer server(0, host);
                const bool allowed = loopback || !std::string(token).empty() ||
                                     std::string(opt_in) == "1" ||
                                     std::string(opt_in) == "TrUe" ||
                                     std::string(opt_in) == "YES";
                EXPECT_EQ(server.start(), allowed);
                EXPECT_EQ(server.is_running(), allowed);
                EXPECT_EQ(server.poll(),
                          allowed ? KVPoll::Success : KVPoll::Failed);
            }
        }
    }
}

TEST_F(HttpMetadataServerTest, DefaultBindRefusesUnauthenticatedStartup) {
    HttpMetadataServer server(0);
    EXPECT_FALSE(server.start());
    EXPECT_FALSE(server.start());
    EXPECT_FALSE(server.is_running());
}

TEST_F(HttpMetadataServerTest, AuthenticatesEveryMetadataOperation) {
    setenv("MC_METADATA_HTTP_TOKEN", "metadata-test-secret", 1);
    // Insecure mode must never disable a configured token, even on loopback.
    setenv("MC_METADATA_HTTP_ALLOW_INSECURE", "1", 1);
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    const int port = server.port();
    WaitUntilReady(
        port);  // Health probes do not expose metadata or need a token.
    const std::string path = "/metadata?key=protected";
    const std::string auth = "Bearer metadata-test-secret";
    ASSERT_EQ(Put(port, path, "original", auth).status, 200);
    for (const auto* invalid :
         {"", "Bearer", "Bearer ", "Basic metadata-test-secret",
          "Bearer Metadata-test-secret", "Bearer wrong",
          "Bearer metadata-test-secreu", "Bearer  metadata-test-secret"}) {
        EXPECT_EQ(Get(port, path, invalid).status, 401);
        EXPECT_EQ(Put(port, path, "changed", invalid).status, 401);
        EXPECT_EQ(Delete(port, path, invalid).status, 401);
    }
    EXPECT_EQ(Get(port, "/metadata").status, 401);
    EXPECT_EQ(Put(port, "/metadata", "").status, 401);
    EXPECT_EQ(Delete(port, "/metadata").status, 401);
    EXPECT_EQ(Get(port, path, "bEaReR metadata-test-secret").body, "original");
    EXPECT_EQ(Put(port, path, "changed", auth).status, 200);
    EXPECT_EQ(Get(port, path, auth).body, "changed");
    EXPECT_EQ(Delete(port, path, auth).status, 200);
    EXPECT_EQ(Get(port, path, auth).status, 404);
}

TEST_F(HttpMetadataServerTest, InsecureOptInAllowsLegacyRoundTrip) {
    setenv("MC_METADATA_HTTP_ALLOW_INSECURE", "1", 1);
    HttpMetadataServer server(0);
    ASSERT_TRUE(server.start());
    const int port = server.port();
    EXPECT_EQ(Put(port, "/metadata?key=legacy", "value").status, 200);
    EXPECT_EQ(Get(port, "/metadata?key=legacy").body, "value");
    EXPECT_EQ(Delete(port, "/metadata?key=legacy").status, 200);
}

TEST_F(HttpMetadataServerTest, RejectsInvalidQuotasAtConstruction) {
    for (const auto* name :
         {"MC_METADATA_MAX_KEYS", "MC_METADATA_MAX_TOTAL_BYTES",
          "MC_METADATA_MAX_BODY_BYTES"}) {
        for (const auto* value :
             {"0", "-1", "abc", "1x", "1.5", " 1", "9223372036854775808",
              "18446744073709551616"}) {
            SCOPED_TRACE(std::string(name) + "=" + value);
            setenv(name, value, 1);
            EXPECT_THROW(HttpMetadataServer(0, "127.0.0.1"),
                         std::invalid_argument);
        }
        setenv(name, "", 1);
        EXPECT_NO_THROW(HttpMetadataServer(0, "127.0.0.1"));
        unsetenv(name);
    }
}

TEST_F(HttpMetadataServerTest, KeyQuotaRecoversAfterDeleteAndInternalCleanup) {
    setenv("MC_METADATA_MAX_KEYS", "2", 1);
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    const int port = server.port();
    ASSERT_EQ(Put(port, "/metadata?key=a", "1").status, 200);
    ASSERT_EQ(Put(port, "/metadata?key=b", "2").status, 200);
    EXPECT_EQ(Put(port, "/metadata?key=c", "3").status, 503);
    EXPECT_EQ(Put(port, "/metadata?key=a", "replacement").status, 200);
    EXPECT_EQ(Delete(port, "/metadata?key=missing").status, 404);
    EXPECT_EQ(Put(port, "/metadata?key=c", "3").status, 503);
    EXPECT_EQ(Delete(port, "/metadata?key=a").status, 200);
    EXPECT_EQ(Put(port, "/metadata?key=c", "3").status, 200);
    EXPECT_TRUE(server.removeKey("b"));
    EXPECT_FALSE(server.removeKey("b"));
    EXPECT_EQ(Put(port, "/metadata?key=d", "4").status, 200);
    EXPECT_EQ(server.removeKeys({"missing", "c", "c", "d"}), 2);
    EXPECT_EQ(Put(port, "/metadata?key=e", "5").status, 200);
    EXPECT_EQ(Put(port, "/metadata?key=f", "6").status, 200);
    EXPECT_EQ(Put(port, "/metadata?key=g", "7").status, 503);
}

TEST_F(HttpMetadataServerTest, ByteQuotaAccountsForDecodedKeysAndOverwrites) {
    setenv("MC_METADATA_MAX_TOTAL_BYTES", "10", 1);
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    const int port = server.port();
    // UTF-8 key is 3 bytes, not 1 character or 9 percent-encoded bytes.
    const std::string path = "/metadata?key=%E9%94%AE";
    ASSERT_EQ(Put(port, path, "1234567").status, 200);
    EXPECT_EQ(Put(port, path, "12345678").status, 503);
    EXPECT_EQ(Get(port, path).body, "1234567");
    EXPECT_EQ(Put(port, "/metadata?key=other", "").status, 503);
    EXPECT_EQ(Put(port, path, "12").status, 200);  // Shrink: 5 bytes left.
    EXPECT_EQ(Put(port, "/metadata?key=other", "").status, 200);
    EXPECT_EQ(Put(port, path, "123").status, 503);
    EXPECT_TRUE(server.removeKey("键"));
    EXPECT_EQ(Put(port, "/metadata?key=a", "1234").status, 200);
    EXPECT_EQ(server.removeKeys({"a", "other", "a", "absent"}), 2);
    EXPECT_EQ(Put(port, path, "1234567").status, 200);
    EXPECT_EQ(Delete(port, path).status, 200);
    EXPECT_EQ(Put(port, "/metadata?key=01234567890", "").status, 503);
    EXPECT_EQ(Put(port, path, "1234567").status, 200);
}

TEST_F(HttpMetadataServerTest, RejectsOversizedBodiesBeforeReadingThem) {
    setenv("MC_METADATA_MAX_BODY_BYTES", "4", 1);
    setenv("MC_METADATA_MAX_TOTAL_BYTES", "10", 1);
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    const int port = server.port();
    const std::string path = "/metadata?key=a";
    ASSERT_EQ(Put(port, path, "1234").status, 200);
    // The pinned YLT parser returns 400 before allocating/reading the body.
    EXPECT_EQ(Put(port, path, "12345").status, 400);
    EXPECT_EQ(Put(port, "/metadata?key=rejected", "12345").status, 400);
    EXPECT_EQ(Get(port, "/metadata?key=rejected").status, 404);
    EXPECT_EQ(
        RawRequest(port,
                   "PUT /metadata?key=a HTTP/1.1\r\nHost: localhost\r\n"
                   "Content-Length: 1000000000\r\nConnection: close\r\n\r\n"),
        400);
    EXPECT_EQ(Get(port, path).body, "1234");
    EXPECT_EQ(Put(port, "/metadata?key=b", "1234").status, 200);
}

TEST_F(HttpMetadataServerTest, RejectsStreamedBodiesWithoutChangingStore) {
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    const int port = server.port();
    ASSERT_EQ(Put(port, "/metadata?key=a", "original").status, 200);
    for (const auto* headers :
         {"Transfer-Encoding: chunked\r\n",
          "Content-Type: multipart/form-data; boundary=test\r\n"}) {
        EXPECT_EQ(
            RawRequest(port,
                       "PUT /metadata?key=a HTTP/1.1\r\nHost: localhost\r\n" +
                           std::string(headers) + "Connection: close\r\n\r\n"),
            400);
        EXPECT_EQ(Get(port, "/metadata?key=a").body, "original");
    }
}

TEST_F(HttpMetadataServerTest, ConcurrentPutsCannotExceedEitherQuota) {
    for (const bool limit_keys : {true, false}) {
        SCOPED_TRACE(limit_keys ? "key quota" : "byte quota");
        setenv("MC_METADATA_MAX_KEYS", limit_keys ? "4" : "100", 1);
        setenv("MC_METADATA_MAX_TOTAL_BYTES", limit_keys ? "1000" : "20", 1);
        HttpMetadataServer server(0, "127.0.0.1");
        ASSERT_TRUE(server.start());
        const int port = server.port();
        std::atomic<int> ready{0};
        std::atomic<bool> go{false};
        std::vector<int> statuses(16);
        std::vector<std::thread> writers;
        std::vector<std::string> keys;
        for (int i = 0; i < 16; ++i)
            keys.push_back("k" + std::to_string(10 + i));
        for (int i = 0; i < 16; ++i) {
            writers.emplace_back([&, i] {
                ++ready;
                while (!go.load()) std::this_thread::yield();
                statuses[i] =
                    Put(port, "/metadata?key=" + keys[i], "ab").status;
            });
        }
        while (ready.load() != 16) std::this_thread::yield();
        go = true;
        for (auto& writer : writers) writer.join();
        int accepted = 0;
        size_t stored_bytes = 0;
        for (int i = 0; i < 16; ++i) {
            EXPECT_TRUE(statuses[i] == 200 || statuses[i] == 503);
            const auto stored = Get(port, "/metadata?key=" + keys[i]);
            EXPECT_EQ(stored.status, statuses[i] == 200 ? 200 : 404);
            if (stored.status == 200) {
                ++accepted;
                stored_bytes += keys[i].size() + stored.body.size();
            }
        }
        EXPECT_EQ(accepted, 4);
        EXPECT_EQ(stored_bytes, 20);
        EXPECT_EQ(server.removeKeys(keys), 4);
        for (int i = 0; i < 4; ++i)
            EXPECT_EQ(Put(port, "/metadata?key=" + keys[i], "ab").status, 200);
        EXPECT_EQ(Put(port, "/metadata?key=k99", "ab").status, 503);
    }
}

TEST_F(HttpMetadataServerTest, RealHttpStoragePluginAuthenticatesRoundTrip) {
#ifdef USE_HTTP
    // HTTPStoragePlugin snapshots its token once per process. This is the only
    // test using that plugin and always uses this same token, including
    // repeats.
    setenv("MC_METADATA_HTTP_TOKEN", "plugin-test-token", 1);
    HttpMetadataServer server(0, "127.0.0.1");
    ASSERT_TRUE(server.start());
    auto plugin = MetadataStoragePlugin::Create(
        "http://127.0.0.1:" + std::to_string(server.port()) + "/metadata");
    ASSERT_NE(plugin, nullptr);
    const std::string key = "mooncake/ram/host:1234/键 +%";
    Json::Value value;
    value["tcp_data_port"] = 12345;
    value["name"] = "test-host";
    ASSERT_TRUE(plugin->set(key, value));
    Json::Value loaded;
    ASSERT_TRUE(plugin->get(key, loaded));
    EXPECT_EQ(loaded, value);
    EXPECT_EQ(plugin->getWithStatus(key, loaded), GetResult::kFound);
    ASSERT_TRUE(plugin->remove(key));
    EXPECT_EQ(plugin->getWithStatus(key, loaded), GetResult::kNotFound);
    ASSERT_TRUE(plugin->set(key, value));
    EXPECT_TRUE(
        server.removeKey(key));  // Internal cleanup uses the decoded key.
    EXPECT_EQ(plugin->getWithStatus(key, loaded), GetResult::kNotFound);
#else
    GTEST_SKIP() << "Build with USE_HTTP=ON to test the real HTTPStoragePlugin";
#endif
}

}  // namespace mooncake::testing
