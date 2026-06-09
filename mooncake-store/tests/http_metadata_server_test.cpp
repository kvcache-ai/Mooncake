#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_http/coro_http_client.hpp>

#include "http_metadata_server.h"
#include "utils.h"

namespace mooncake::testing {

class HttpMetadataServerTest : public ::testing::Test {
   protected:
    struct HttpResponse {
        int status;
        std::string body;
    };

    HttpResponse Get(int port, const std::string& path) {
        coro_http::coro_http_client client;
        auto response = async_simple::coro::syncAwait(client.async_get(
            "http://127.0.0.1:" + std::to_string(port) + path));
        return {response.status, std::string(response.resp_body)};
    }

    HttpResponse Put(int port, const std::string& path,
                     const std::string& body) {
        coro_http::coro_http_client client;
        auto response = async_simple::coro::syncAwait(
            client.async_put("http://127.0.0.1:" + std::to_string(port) + path,
                             body, coro_http::req_content_type::json));
        return {response.status, std::string(response.resp_body)};
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
    int port = getFreeTcpPort();
    HttpMetadataServer server(static_cast<uint16_t>(port), "127.0.0.1");
    ASSERT_TRUE(server.start());
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
    int port = getFreeTcpPort();
    HttpMetadataServer server(static_cast<uint16_t>(port), "127.0.0.1");
    ASSERT_TRUE(server.start());
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

}  // namespace mooncake::testing
