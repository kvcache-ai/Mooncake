/**
 * @file p2p_client_http_endpoints_test.cpp
 * @brief Integration tests for the P2P client HTTP data-ops endpoints
 *        (GET /get, PUT /put, DELETE /remove_local, DELETE /remove_all_local).
 *
 * Starts an in-process P2P master, brings up a single P2PClientService with
 * the HTTP server enabled on a free port, and drives the endpoints with
 * coro_http_client.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>


#include <csignal>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>

#include "client_config_builder.h"
#include "p2p_client_service.h"
#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

class P2PClientHttpEndpointsTest : public ::testing::Test {
   protected:
    static std::shared_ptr<P2PClientService> CreateP2PClient(
        const std::string& host_name, uint32_t rpc_port, uint16_t http_port) {
        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port,
            /*rpc_thread_num=*/2, /*lock_shard_count=*/1024,
            /*route_cache_max_memory_bytes=*/300 * 1024 * 1024,
            /*route_cache_ttl_ms=*/5 * 60 * 1000,
            /*local_transfer_mode=*/"te",
            /*local_memcpy_async_worker_num=*/32, http_port,
            /*enable_http_server=*/true);

        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);

        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK)
            << "Init failed: " << static_cast<int>(err);
        return client;
    }

    static void SetUpTestSuite() {
        google::InitGoogleLogging("P2PClientHttpEndpointsTest");
        FLAGS_logtostderr = 1;

        ASSERT_TRUE(master_.Start()) << "Failed to start P2P master";
        master_address_ = master_.master_address();
        LOG(INFO) << "P2P master started at " << master_address_;

        // Take a free HTTP port to avoid colliding with the default 9003.
        const uint16_t http_port = static_cast<uint16_t>(getFreeTcpPort());
        const uint32_t rpc_port = static_cast<uint32_t>(getFreeTcpPort());
        client_ = CreateP2PClient("localhost:18901", rpc_port, http_port);
        ASSERT_NE(client_, nullptr);
        ASSERT_TRUE(client_->IsHttpServerEnabled());
        http_base_url_ =
            "http://127.0.0.1:" + std::to_string(client_->GetHttpPort());
        LOG(INFO) << "P2P client HTTP server at " << http_base_url_;
    }

    static void TearDownTestSuite() {
        client_.reset();
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    // Build a URL like "http://127.0.0.1:PORT/get?key=k".
    static std::string Url(const std::string& path,
                           const std::string& query = "") {
        return query.empty() ? http_base_url_ + path
                             : http_base_url_ + path + "?" + query;
    }

    static coro_http::resp_data HttpGet(const std::string& url) {
        coro_http::coro_http_client client;
        return client.get(url);
    }

    static coro_http::resp_data HttpPost(const std::string& url,
                                         std::string body = "") {
        coro_http::coro_http_client client;
        return client.post(url, std::move(body),
                           coro_http::req_content_type::octet_stream);
    }

    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client_;
    static std::string http_base_url_;
};

InProcP2PMaster P2PClientHttpEndpointsTest::master_;
std::string P2PClientHttpEndpointsTest::master_address_;
std::shared_ptr<P2PClientService> P2PClientHttpEndpointsTest::client_ = nullptr;
std::string P2PClientHttpEndpointsTest::http_base_url_;

// ============================================================================
// /put + /get
// ============================================================================

TEST_F(P2PClientHttpEndpointsTest, HttpPutThenGet) {
    const std::string key = "http_put_then_get";
    const std::string body = "hello-from-http";

    auto put_resp = HttpPost(Url("/put", "key=" + key), body);
    ASSERT_EQ(put_resp.status, 200) << "PUT failed: status=" << put_resp.status
                                    << " body=" << put_resp.resp_body;

    auto get_resp = HttpGet(Url("/get", "key=" + key));
    ASSERT_EQ(get_resp.status, 200) << "GET failed: status=" << get_resp.status
                                    << " body=" << get_resp.resp_body;
    EXPECT_EQ(get_resp.resp_body, body);
}

TEST_F(P2PClientHttpEndpointsTest, HttpGetMissingKeyParam) {
    auto resp = HttpGet(Url("/get"));
    EXPECT_NE(resp.status, 200)
        << "Server unexpectedly accepted /get without ?key=";
}

TEST_F(P2PClientHttpEndpointsTest, HttpGetUnknownKey) {
    auto resp = HttpGet(Url("/get", "key=http_does_not_exist_xyz"));
    EXPECT_EQ(resp.status, 404)
        << "Unexpected status=" << resp.status << " body=" << resp.resp_body;
}

// ============================================================================
// /remove_local
// ============================================================================

TEST_F(P2PClientHttpEndpointsTest, HttpRemoveLocalCycle) {
    const std::string key = "http_remove_local_cycle";
    const std::string body = "to_be_removed_via_http";

    ASSERT_EQ(HttpPost(Url("/put", "key=" + key), body).status, 200);

    auto del_resp = HttpPost(Url("/remove_local", "key=" + key));
    ASSERT_EQ(del_resp.status, 200)
        << "DELETE failed: status=" << del_resp.status
        << " body=" << del_resp.resp_body;
    EXPECT_EQ(del_resp.resp_body, "OK");

    auto get_resp = HttpGet(Url("/get", "key=" + key));
    EXPECT_EQ(get_resp.status, 404);
}

TEST_F(P2PClientHttpEndpointsTest, HttpRemoveLocalMissingKeyParam) {
    auto resp = HttpPost(Url("/remove_local"));
    EXPECT_NE(resp.status, 200)
        << "Server unexpectedly accepted /remove_local without ?key=";
}

// ============================================================================
// /remove_all_local: PUT N → DELETE returns N → every GET returns 404.
// ============================================================================

TEST_F(P2PClientHttpEndpointsTest, HttpRemoveAllLocal) {
    // Baseline so the count assertion below is exact.
    auto baseline = HttpPost(Url("/remove_all_local"));
    ASSERT_EQ(baseline.status, 200)
        << "Baseline /remove_all_local failed: " << baseline.resp_body;

    const int kNumKeys = 4;
    std::vector<std::string> keys;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "http_remove_all_local_" + std::to_string(i);
        std::string body = "payload_" + std::to_string(i);
        ASSERT_EQ(HttpPost(Url("/put", "key=" + key), body).status, 200)
            << "PUT failed for " << key;
        keys.push_back(std::move(key));
    }

    auto del_resp = HttpPost(Url("/remove_all_local"));
    ASSERT_EQ(del_resp.status, 200)
        << "DELETE /remove_all_local failed: status=" << del_resp.status
        << " body=" << del_resp.resp_body;
    EXPECT_EQ(del_resp.resp_body, std::to_string(kNumKeys));

    for (const auto& key : keys) {
        auto get_resp = HttpGet(Url("/get", "key=" + key));
        EXPECT_EQ(get_resp.status, 404)
            << "Get unexpectedly returned " << get_resp.status
            << " for key=" << key;
    }
}

}  // namespace testing
}  // namespace mooncake
