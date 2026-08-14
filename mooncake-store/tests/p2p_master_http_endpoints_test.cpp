/**
 * @file p2p_master_http_endpoints_test.cpp
 * @brief Integration tests for the P2P master HTTP endpoints
 *        (GET /health, GET /get_key_count, GET /get_all_keys,
 *         GET /batch_query_keys, GET /metrics).
 *
 * Brings up an in-process WrappedP2PMasterService with the embedded HTTP
 * server bound to a free port (no RPC server)
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_reader.h>

#include "master_config.h"
#include "p2p_rpc_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

// Mirrors of the JSON shapes emitted by GET /batch_query_keys (see
// WrappedMasterService::init_http_server in rpc_service.cpp).
struct BatchQueryBufferDescriptor {
    uint64_t size_ = 0;
    uint64_t buffer_address_ = 0;
    std::string protocol_;
    std::string transport_endpoint_;
    YLT_REFL(BatchQueryBufferDescriptor, size_, buffer_address_, protocol_,
             transport_endpoint_);
};

struct BatchQueryKeyResult {
    bool ok = false;
    std::string error;
    std::vector<BatchQueryBufferDescriptor> values;
    YLT_REFL(BatchQueryKeyResult, ok, error, values);
};

struct BatchQueryResponse {
    bool success = false;
    std::string error;
    std::unordered_map<std::string, BatchQueryKeyResult> data;
    YLT_REFL(BatchQueryResponse, success, error, data);
};

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
        wrapped_->init();
        // Give the HTTP server a moment to come up (mirrors InProcP2PMaster).
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        ASSERT_EQ(wrapped_->GetHttpPort(), http_port);

        http_base_url_ = "http://127.0.0.1:" + std::to_string(http_port);
        LOG(INFO) << "P2P master HTTP server at " << http_base_url_;

        // Register one client owning two P2P segments so tests can also
        // exercise keys with replicas on multiple segments.
        Segment segment_a;
        segment_a.id = generate_uuid();
        segment_a.name = "p2p_master_http_segment_a";
        segment_a.size = kSegmentSize;
        segment_a.extra = P2PSegmentExtraData{
            .priority = 0,
            .tags = {},
            .memory_type = MemoryType::DRAM,
        };
        Segment segment_b = segment_a;
        segment_b.id = generate_uuid();
        segment_b.name = "p2p_master_http_segment_b";
        segment_id_a_ = segment_a.id;
        segment_id_b_ = segment_b.id;

        RegisterClientRequest reg_req;
        reg_req.client_id = generate_uuid();
        reg_req.ip_address = "127.0.0.1";
        reg_req.rpc_port = 50051;
        reg_req.segments = {segment_a, segment_b};
        reg_req.deployment_mode = DeploymentMode::P2P;
        client_id_ = reg_req.client_id;

        auto reg_res = wrapped_->RegisterClient(reg_req);
        ASSERT_TRUE(reg_res.has_value())
            << "RegisterClient failed: " << reg_res.error();

        // Tests populate keys themselves; start from an empty keyspace.
        ASSERT_EQ(wrapped_->GetMasterService().GetKeyCount(), 0u);
    }

    static void TearDownTestSuite() {
        wrapped_.reset();
        google::ShutdownGoogleLogging();
    }

    // Owns the response body: resp_data::resp_body is a string_view into
    // the client's internal buffer and would dangle once the client used
    // below is destroyed.
    struct HttpResponse {
        int status;
        std::string resp_body;
    };

    static HttpResponse HttpGet(const std::string& url) {
        coro_http::coro_http_client client;
        auto resp = client.get(url);
        return {resp.status, std::string(resp.resp_body)};
    }

    static std::vector<std::string> SplitLines(std::string_view body) {
        std::vector<std::string> lines;
        size_t start = 0;
        while (start < body.size()) {
            size_t pos = body.find('\n', start);
            if (pos == std::string_view::npos) {
                lines.emplace_back(body.substr(start));
                break;
            }
            lines.emplace_back(body.substr(start, pos - start));
            start = pos + 1;
        }
        return lines;
    }

    // ---- HTTP accessors -------------------------------------------------

    static int64_t HttpGetKeyCount() {
        auto resp = HttpGet(http_base_url_ + "/get_key_count");
        EXPECT_EQ(resp.status, 200) << "body=" << resp.resp_body;
        try {
            size_t consumed = 0;
            const int64_t value = std::stoll(resp.resp_body, &consumed);
            EXPECT_EQ(consumed, resp.resp_body.size())
                << "trailing garbage in /get_key_count body: "
                << resp.resp_body;
            return value;
        } catch (const std::exception&) {
            ADD_FAILURE() << "non-numeric /get_key_count body: "
                          << resp.resp_body;
            return -1;
        }
    }

    static std::vector<std::string> HttpGetAllKeys() {
        auto resp = HttpGet(http_base_url_ + "/get_all_keys");
        EXPECT_EQ(resp.status, 200) << "body=" << resp.resp_body;
        return SplitLines(resp.resp_body);
    }

    // ---- Data population helpers (drive the master via the service API) -

    static void AddKey(const std::string& key, size_t size,
                       const UUID& segment_id) {
        // AddReplicaRequest::key is a string_view; `key` outlives the
        // synchronous AddReplica call.
        AddReplicaRequest req;
        req.key = key;
        req.size = size;
        req.client_id = client_id_;
        req.segment_id = segment_id;
        auto res = wrapped_->AddReplica(req);
        ASSERT_TRUE(res.has_value())
            << "AddReplica failed for " << key << ": " << res.error();
    }

    static void RemoveKey(const std::string& key, const UUID& segment_id) {
        RemoveReplicaRequest req;
        req.key = key;
        req.client_id = client_id_;
        req.segment_id = segment_id;
        auto res = wrapped_->RemoveReplica(req);
        ASSERT_TRUE(res.has_value())
            << "RemoveReplica failed for " << key << ": " << res.error();
    }

    // ---- Prometheus text-format helpers ----------------------------------

    // Returns std::nullopt when the metric line is absent. ylt omits
    // metrics whose value is 0 and which never changed, so callers should
    // treat "absent" as 0.
    static std::optional<int64_t> ParseMetricValue(const std::string& text,
                                                   const std::string& name) {
        std::istringstream iss(text);
        std::string line;
        while (std::getline(iss, line)) {
            if (line.rfind(name + " ", 0) == 0) {
                return std::stoll(line.substr(name.size() + 1));
            }
        }
        return std::nullopt;
    }

    static int64_t GetMetricViaHttp(const std::string& name) {
        auto resp = HttpGet(http_base_url_ + "/metrics");
        EXPECT_EQ(resp.status, 200) << "body=" << resp.resp_body;
        return ParseMetricValue(resp.resp_body, name).value_or(0);
    }

    // ---- JSON helpers -----------------------------------------------------

    template <typename T>
    static T ParseJson(const std::string& body, const char* what) {
        T parsed{};
        try {
            struct_json::from_json(parsed, body);
        } catch (const std::exception& e) {
            ADD_FAILURE() << "failed to parse " << what << " JSON: " << e.what()
                          << ", body=" << body;
        }
        return parsed;
    }

    static constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    static std::unique_ptr<WrappedP2PMasterService> wrapped_;
    static std::string http_base_url_;
    static UUID client_id_;
    static UUID segment_id_a_;
    static UUID segment_id_b_;
};

std::unique_ptr<WrappedP2PMasterService> P2PMasterHttpEndpointsTest::wrapped_;
std::string P2PMasterHttpEndpointsTest::http_base_url_;
UUID P2PMasterHttpEndpointsTest::client_id_;
UUID P2PMasterHttpEndpointsTest::segment_id_a_;
UUID P2PMasterHttpEndpointsTest::segment_id_b_;

TEST_F(P2PMasterHttpEndpointsTest, HealthEndpointReturnsOk) {
    auto resp = HttpGet(http_base_url_ + "/health");
    ASSERT_EQ(resp.status, 200) << "body=" << resp.resp_body;
    EXPECT_EQ(resp.resp_body, "OK");
}

TEST_F(P2PMasterHttpEndpointsTest,
       GetKeyCountTracksReplicaAdditionsAndRemovals) {
    const int64_t baseline = HttpGetKeyCount();

    const std::vector<std::string> keys = {
        "count_test_key_0", "count_test_key_1", "count_test_key_2",
        "count_test_key_3", "count_test_key_4",
    };

    // The counter must follow every single addition.
    for (size_t i = 0; i < keys.size(); ++i) {
        AddKey(keys[i], /*size=*/1024 + i, segment_id_a_);
        EXPECT_EQ(HttpGetKeyCount(), baseline + static_cast<int64_t>(i) + 1)
            << "key count not updated after adding " << keys[i];
    }

    // Cross-check the HTTP value against the in-process service state.
    EXPECT_EQ(HttpGetKeyCount(),
              static_cast<int64_t>(wrapped_->GetMasterService().GetKeyCount()));

    // Removing replicas of two keys must drop the count accordingly.
    RemoveKey(keys[0], segment_id_a_);
    RemoveKey(keys[1], segment_id_a_);
    EXPECT_EQ(HttpGetKeyCount(), baseline + 3);

    // Clean up the remaining keys; the count must return to the baseline.
    for (size_t i = 2; i < keys.size(); ++i) {
        RemoveKey(keys[i], segment_id_a_);
    }
    EXPECT_EQ(HttpGetKeyCount(), baseline);
}

TEST_F(P2PMasterHttpEndpointsTest, GetAllKeysReturnsExactlyThePopulatedKeySet) {
    const auto baseline_lines = HttpGetAllKeys();
    const std::set<std::string> baseline(baseline_lines.begin(),
                                         baseline_lines.end());
    ASSERT_EQ(baseline.size(), baseline_lines.size())
        << "baseline key listing contains duplicates";

    const std::vector<std::string> keys = {
        "allkeys_test_key_0",
        "allkeys_test_key_1",
        "allkeys_test_key_2",
        "allkeys_test_key_3",
    };
    for (size_t i = 0; i < keys.size(); ++i) {
        AddKey(keys[i], /*size=*/512 * (i + 1), segment_id_a_);
    }

    const auto lines = HttpGetAllKeys();
    const std::set<std::string> got(lines.begin(), lines.end());
    ASSERT_EQ(lines.size(), got.size())
        << "duplicate keys in /get_all_keys response";

    // Exact set equality: every populated key is listed, nothing else is.
    std::set<std::string> expected = baseline;
    expected.insert(keys.begin(), keys.end());
    EXPECT_EQ(got, expected);

    // Removing one key must make it disappear from the listing.
    RemoveKey(keys[0], segment_id_a_);
    const auto lines_after_remove = HttpGetAllKeys();
    const std::set<std::string> got_after_remove(lines_after_remove.begin(),
                                                 lines_after_remove.end());
    expected.erase(keys[0]);
    EXPECT_EQ(got_after_remove, expected);

    // Clean up; the listing must return to the baseline set.
    for (size_t i = 1; i < keys.size(); ++i) {
        RemoveKey(keys[i], segment_id_a_);
    }
    const auto final_lines = HttpGetAllKeys();
    const std::set<std::string> final_set(final_lines.begin(),
                                          final_lines.end());
    EXPECT_EQ(final_set, baseline);
}

TEST_F(P2PMasterHttpEndpointsTest,
       BatchQueryKeysDistinguishesPopulatedAndMissingKeys) {
    const std::string key0 = "batchquery_test_key_0";
    const std::string key1 = "batchquery_test_key_1";
    const std::string missing_key = "batchquery_test_missing_key";

    AddKey(key0, /*size=*/1024, segment_id_a_);
    AddKey(key1, /*size=*/2048, segment_id_b_);

    auto resp = HttpGet(http_base_url_ + "/batch_query_keys?keys=" + key0 +
                        "," + key1 + "," + missing_key);
    ASSERT_EQ(resp.status, 200) << "body=" << resp.resp_body;

    auto parsed =
        ParseJson<BatchQueryResponse>(resp.resp_body, "/batch_query_keys");
    ASSERT_TRUE(parsed.success) << "body=" << resp.resp_body;
    ASSERT_EQ(parsed.data.size(), 3u);

    // Populated keys are reported as ok.
    ASSERT_EQ(parsed.data.count(key0), 1u);
    EXPECT_TRUE(parsed.data[key0].ok);
    EXPECT_TRUE(parsed.data[key0].error.empty());
    ASSERT_EQ(parsed.data.count(key1), 1u);
    EXPECT_TRUE(parsed.data[key1].ok);
    // P2P replicas are proxy replicas (client/segment references), not
    // memory descriptors, so the endpoint currently emits no buffer values
    // for them.
    EXPECT_TRUE(parsed.data[key0].values.empty());
    EXPECT_TRUE(parsed.data[key1].values.empty());

    // A key that was never populated is reported with the lookup error.
    ASSERT_EQ(parsed.data.count(missing_key), 1u);
    EXPECT_FALSE(parsed.data[missing_key].ok);
    EXPECT_EQ(parsed.data[missing_key].error,
              toString(ErrorCode::OBJECT_NOT_FOUND));

    RemoveKey(key0, segment_id_a_);
    RemoveKey(key1, segment_id_b_);

    // Once removed, the same keys must now be reported as missing.
    auto resp_after_remove =
        HttpGet(http_base_url_ + "/batch_query_keys?keys=" + key0 + "," + key1);
    ASSERT_EQ(resp_after_remove.status, 200)
        << "body=" << resp_after_remove.resp_body;
    auto parsed_after_remove = ParseJson<BatchQueryResponse>(
        resp_after_remove.resp_body, "/batch_query_keys after remove");
    ASSERT_TRUE(parsed_after_remove.success);
    ASSERT_EQ(parsed_after_remove.data.size(), 2u);
    for (const auto& key : {key0, key1}) {
        ASSERT_EQ(parsed_after_remove.data.count(key), 1u);
        EXPECT_FALSE(parsed_after_remove.data[key].ok)
            << "removed key " << key << " still reported as present";
    }
}

TEST_F(P2PMasterHttpEndpointsTest, BatchQueryKeysRejectsEmptyKeyList) {
    // No query parameter at all.
    auto resp = HttpGet(http_base_url_ + "/batch_query_keys");
    ASSERT_EQ(resp.status, 400) << "body=" << resp.resp_body;
    auto parsed =
        ParseJson<BatchQueryResponse>(resp.resp_body, "/batch_query_keys");
    EXPECT_FALSE(parsed.success);
    EXPECT_NE(parsed.error.find("No keys provided"), std::string::npos)
        << "body=" << resp.resp_body;
    EXPECT_TRUE(parsed.data.empty());

    // Empty `keys` query parameter.
    auto resp_empty = HttpGet(http_base_url_ + "/batch_query_keys?keys=");
    ASSERT_EQ(resp_empty.status, 400) << "body=" << resp_empty.resp_body;
}

TEST_F(P2PMasterHttpEndpointsTest,
       MetricsEndpointReflectsPopulatedDataAndOperations) {
    const std::string kKeyCountMetric = "master_key_count";
    const std::string kAddReplicaMetric = "master_add_replica_requests_total";
    const std::string kRemoveReplicaMetric =
        "master_remove_replica_requests_total";

    const int64_t key_count_baseline = GetMetricViaHttp(kKeyCountMetric);
    const int64_t add_baseline = GetMetricViaHttp(kAddReplicaMetric);
    const int64_t remove_baseline = GetMetricViaHttp(kRemoveReplicaMetric);

    const std::vector<std::string> keys = {
        "metrics_test_key_0",
        "metrics_test_key_1",
        "metrics_test_key_2",
    };
    for (const auto& key : keys) {
        AddKey(key, /*size=*/4096, segment_id_a_);
    }

    // The key-count gauge and the add-replica counter must follow the
    // populated data.
    EXPECT_EQ(GetMetricViaHttp(kKeyCountMetric), key_count_baseline + 3);
    EXPECT_EQ(GetMetricViaHttp(kAddReplicaMetric), add_baseline + 3);

    for (const auto& key : keys) {
        RemoveKey(key, segment_id_a_);
    }

    // The gauge must drop back to the baseline and the remove-replica
    // counter must have advanced by the number of removals.
    EXPECT_EQ(GetMetricViaHttp(kKeyCountMetric), key_count_baseline);
    EXPECT_EQ(GetMetricViaHttp(kRemoveReplicaMetric), remove_baseline + 3);

    // Cluster data-plane series are always emitted (zero values included).
    auto resp = HttpGet(http_base_url_ + "/metrics");
    ASSERT_EQ(resp.status, 200) << "body=" << resp.resp_body;
    EXPECT_NE(resp.resp_body.find("master_cluster_total_get_requests"),
              std::string::npos);
}

}  // namespace testing
}  // namespace mooncake
