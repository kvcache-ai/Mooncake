/**
 * @file p2p_master_http_endpoints_test.cpp
 * @brief Integration tests for the P2P master HTTP endpoints
 *        (GET /get_key_count, GET /get_all_keys).
 *
 * Brings up an in-process WrappedP2PMasterService with the embedded HTTP
 * server bound to a free port (no RPC server), registers one client with a
 * P2P segment, adds replicas for three keys, and drives the endpoints with
 * coro_http_client.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>

#include "master_config.h"
#include "p2p_rpc_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

class P2PMasterHttpEndpointsTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("P2PMasterHttpEndpointsTest");
        FLAGS_logtostderr = 1;

        const uint16_t http_port = static_cast<uint16_t>(getFreeTcpPort());

        WrappedMasterServiceConfig wms_cfg;
        wms_cfg.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
        wms_cfg.default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
        wms_cfg.allow_evict_soft_pinned_objects = true;
        wms_cfg.enable_metric_reporting = false;
        wms_cfg.eviction_ratio = DEFAULT_EVICTION_RATIO;
        wms_cfg.eviction_high_watermark_ratio =
            DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
        wms_cfg.view_version = 0;
        wms_cfg.enable_ha = false;
        wms_cfg.cluster_id = DEFAULT_CLUSTER_ID;
        wms_cfg.root_fs_dir = DEFAULT_ROOT_FS_DIR;
        wms_cfg.memory_allocator = BufferAllocatorType::OFFSET;
        wms_cfg.max_client_per_key = 0;  // no limit for P2P
        wms_cfg.client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
        wms_cfg.client_crashed_ttl_sec = DEFAULT_CLIENT_CRASHED_TTL_SEC;
        wms_cfg.http_port = http_port;

        wrapped_ = std::make_unique<WrappedP2PMasterService>(wms_cfg);
        // Give the HTTP server a moment to come up (mirrors InProcP2PMaster).
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        ASSERT_EQ(wrapped_->GetHttpPort(), http_port);

        http_base_url_ = "http://127.0.0.1:" + std::to_string(http_port);
        LOG(INFO) << "P2P master HTTP server at " << http_base_url_;

        // Register one client with a single P2P segment.
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "p2p_master_http_segment";
        segment.size = 16 * 1024 * 1024;
        segment.extra = P2PSegmentExtraData{
            .priority = 0,
            .tags = {},
            .memory_type = MemoryType::DRAM,
        };
        segment_id_ = segment.id;

        RegisterClientRequest reg_req;
        reg_req.client_id = generate_uuid();
        reg_req.ip_address = "127.0.0.1";
        reg_req.rpc_port = 50051;
        reg_req.segments = {segment};
        reg_req.deployment_mode = DeploymentMode::P2P;
        client_id_ = reg_req.client_id;

        auto reg_res = wrapped_->RegisterClient(reg_req);
        ASSERT_TRUE(reg_res.has_value())
            << "RegisterClient failed: " << reg_res.error();

        // Add replicas for three distinct keys.
        for (int i = 0; i < kNumKeys; ++i) {
            AddReplicaRequest req;
            req.key = "master_http_key_" + std::to_string(i);
            req.size = 1024;
            req.client_id = client_id_;
            req.segment_id = segment_id_;
            auto res = wrapped_->AddReplica(req);
            ASSERT_TRUE(res.has_value())
                << "AddReplica failed for " << req.key << ": " << res.error();
        }
    }

    static void TearDownTestSuite() {
        wrapped_.reset();
        google::ShutdownGoogleLogging();
    }

    static coro_http::resp_data HttpGet(const std::string& url) {
        coro_http::coro_http_client client;
        return client.get(url);
    }

    static std::vector<std::string> SplitLines(const std::string& body) {
        std::vector<std::string> lines;
        size_t start = 0;
        while (start < body.size()) {
            size_t pos = body.find('\n', start);
            if (pos == std::string::npos) {
                lines.push_back(body.substr(start));
                break;
            }
            lines.push_back(body.substr(start, pos - start));
            start = pos + 1;
        }
        return lines;
    }

    static constexpr int kNumKeys = 3;
    static std::unique_ptr<WrappedP2PMasterService> wrapped_;
    static std::string http_base_url_;
    static UUID client_id_;
    static UUID segment_id_;
};

std::unique_ptr<WrappedP2PMasterService>
    P2PMasterHttpEndpointsTest::wrapped_;
std::string P2PMasterHttpEndpointsTest::http_base_url_;
UUID P2PMasterHttpEndpointsTest::client_id_;
UUID P2PMasterHttpEndpointsTest::segment_id_;

TEST_F(P2PMasterHttpEndpointsTest, GetKeyCountEndpoint) {
    auto resp = HttpGet(http_base_url_ + "/get_key_count");
    ASSERT_EQ(resp.status, 200)
        << "status=" << resp.status << " body=" << resp.resp_body;
    EXPECT_EQ(resp.resp_body, std::to_string(kNumKeys));
}

TEST_F(P2PMasterHttpEndpointsTest, GetAllKeysEndpoint) {
    auto resp = HttpGet(http_base_url_ + "/get_all_keys");
    ASSERT_EQ(resp.status, 200)
        << "status=" << resp.status << " body=" << resp.resp_body;

    auto lines = SplitLines(resp.resp_body);
    ASSERT_EQ(lines.size(), static_cast<size_t>(kNumKeys));
    for (int i = 0; i < kNumKeys; ++i) {
        const std::string expected = "master_http_key_" + std::to_string(i);
        EXPECT_TRUE(std::find(lines.begin(), lines.end(), expected) !=
                    lines.end())
            << "missing key " << expected << " in /get_all_keys response";
    }
}

}  // namespace testing
}  // namespace mooncake
