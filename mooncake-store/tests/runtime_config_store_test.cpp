#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <csignal>
#include <cstdint>
#include <memory>
#include <string>

#include <ylt/coro_http/coro_http_client.hpp>

#include "client_config_builder.h"
#include "p2p_client_service.h"
#include "runtime_config_store.h"
#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

class RuntimeConfigTest : public ::testing::Test {
   protected:
    static std::shared_ptr<P2PClientService> CreateP2PClient(
        const std::string& host_name, uint32_t rpc_port, uint16_t http_port) {
        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            0, nullptr, "", rpc_port, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000,
            "te", 32, http_port, true);

        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);

        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK);
        return client;
    }

    static void SetUpTestSuite() {
        google::InitGoogleLogging("RuntimeConfigTest");
        FLAGS_logtostderr = 1;

        ASSERT_TRUE(master_.Start());
        master_address_ = master_.master_address();

        const uint16_t http_port = static_cast<uint16_t>(getFreeTcpPort());
        const uint32_t rpc_port = static_cast<uint32_t>(getFreeTcpPort());
        client_ = CreateP2PClient("localhost:18951", rpc_port, http_port);
        ASSERT_NE(client_, nullptr);
        ASSERT_TRUE(client_->IsHttpServerEnabled());
        http_base_url_ =
            "http://127.0.0.1:" + std::to_string(client_->GetHttpPort());

        centralized_store_ = std::make_unique<RuntimeConfigStore>(
            DeploymentMode::CENTRALIZATION);
    }

    static void TearDownTestSuite() {
        centralized_store_.reset();
        client_.reset();
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    RuntimeConfigStore& p2p_store() { return client_->getRuntimeConfigStore(); }

    RuntimeConfigStore& centralized_store() { return *centralized_store_; }

    static std::string Url(const std::string& path,
                           const std::string& query = "") {
        return query.empty() ? http_base_url_ + path
                             : http_base_url_ + path + "?" + query;
    }

    struct HttpResponse {
        int status;
        std::string body;
    };

    static HttpResponse HttpGet(const std::string& url) {
        coro_http::coro_http_client c;
        auto resp = c.get(url);
        return {resp.status, std::string(resp.resp_body)};
    }

    static HttpResponse HttpPost(const std::string& url,
                                 std::string body = "") {
        coro_http::coro_http_client c;
        auto resp =
            c.post(url, std::move(body), coro_http::req_content_type::json);
        return {resp.status, std::string(resp.resp_body)};
    }

    static Json::Value ParseJson(std::string_view str) {
        Json::Value val;
        Json::CharReaderBuilder builder;
        auto reader =
            std::unique_ptr<Json::CharReader>(builder.newCharReader());
        std::string errors;
        reader->parse(str.data(), str.data() + str.size(), &val, &errors);
        return val;
    }

    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client_;
    static std::string http_base_url_;
    static std::unique_ptr<RuntimeConfigStore> centralized_store_;
};

InProcP2PMaster RuntimeConfigTest::master_;
std::string RuntimeConfigTest::master_address_;
std::shared_ptr<P2PClientService> RuntimeConfigTest::client_ = nullptr;
std::string RuntimeConfigTest::http_base_url_;
std::unique_ptr<RuntimeConfigStore> RuntimeConfigTest::centralized_store_ =
    nullptr;

// ============================================================================
// Store: construction defaults
// ============================================================================

TEST_F(RuntimeConfigTest, CentralizedModeReturnsReplicateConfig) {
    auto wc = centralized_store().getDefaultWriteConfig();
    ASSERT_TRUE(std::holds_alternative<ReplicateConfig>(wc));
    auto& cfg = std::get<ReplicateConfig>(wc);
    EXPECT_EQ(cfg.replica_num, 1u);
    EXPECT_FALSE(cfg.with_soft_pin);
}

TEST_F(RuntimeConfigTest, P2PModeReturnsWriteRouteRequestConfig) {
    auto wc = p2p_store().getDefaultWriteConfig();
    ASSERT_TRUE(std::holds_alternative<WriteRouteRequestConfig>(wc));
    auto& cfg = std::get<WriteRouteRequestConfig>(wc);
    EXPECT_TRUE(cfg.allow_local);
    EXPECT_TRUE(cfg.prefer_local);
    EXPECT_EQ(cfg.max_candidates, 2u);
    auto rc = p2p_store().getDefaultReadConfig();
    EXPECT_EQ(rc.max_candidates, 0u);
    EXPECT_FALSE(rc.p2p_config.has_value());
}

// ============================================================================
// Store: centralized write patch
// ============================================================================

TEST_F(RuntimeConfigTest, UpdateCentralizedWriteConfigPatch) {
    Json::Value patch;
    patch["replica_num"] = 3;
    patch["with_soft_pin"] = true;
    centralized_store().updateWriteConfig(patch);

    auto wc = centralized_store().getDefaultWriteConfig();
    auto& cfg = std::get<ReplicateConfig>(wc);
    EXPECT_EQ(cfg.replica_num, 3u);
    EXPECT_TRUE(cfg.with_soft_pin);
    EXPECT_FALSE(cfg.prefer_alloc_in_same_node);
}

TEST_F(RuntimeConfigTest, UpdateCentralizedPreferredSegments) {
    Json::Value patch;
    Json::Value segs(Json::arrayValue);
    segs.append("seg_a");
    segs.append("seg_b");
    patch["preferred_segments"] = segs;
    patch["prefer_alloc_in_same_node"] = true;
    centralized_store().updateWriteConfig(patch);

    auto wc2 = centralized_store().getDefaultWriteConfig();
    auto& cfg2 = std::get<ReplicateConfig>(wc2);
    ASSERT_EQ(cfg2.preferred_segments.size(), 2u);
    EXPECT_EQ(cfg2.preferred_segments[0], "seg_a");
    EXPECT_TRUE(cfg2.prefer_alloc_in_same_node);
}

// ============================================================================
// Store: P2P write patch
// ============================================================================

TEST_F(RuntimeConfigTest, UpdateP2PWriteConfigPatch) {
    Json::Value patch;
    patch["prefer_local"] = false;
    patch["max_candidates"] = 5;
    patch["priority_limit"] = 10;
    p2p_store().updateWriteConfig(patch);

    auto wc = p2p_store().getDefaultWriteConfig();
    auto& cfg = std::get<WriteRouteRequestConfig>(wc);
    EXPECT_FALSE(cfg.prefer_local);
    EXPECT_EQ(cfg.max_candidates, 5u);
    EXPECT_EQ(cfg.priority_limit, 10);
    EXPECT_TRUE(cfg.allow_local);
}

TEST_F(RuntimeConfigTest, UpdateP2PWriteConfigStrategy) {
    Json::Value patch;
    patch["strategy"] = static_cast<int>(ObjectIterateStrategy::RANDOM);
    p2p_store().updateWriteConfig(patch);

    auto wc2 = p2p_store().getDefaultWriteConfig();
    auto& cfg2 = std::get<WriteRouteRequestConfig>(wc2);
    EXPECT_EQ(cfg2.strategy, ObjectIterateStrategy::RANDOM);
}

TEST_F(RuntimeConfigTest, UpdateP2PWriteConfigTagFilters) {
    Json::Value patch;
    Json::Value tags(Json::arrayValue);
    tags.append("gpu");
    tags.append("high_priority");
    patch["tag_filters"] = tags;
    p2p_store().updateWriteConfig(patch);

    auto wc3 = p2p_store().getDefaultWriteConfig();
    auto& cfg3 = std::get<WriteRouteRequestConfig>(wc3);
    ASSERT_EQ(cfg3.tag_filters.size(), 2u);
    EXPECT_EQ(cfg3.tag_filters[0], "gpu");
}

// ============================================================================
// Store: read config patch
// ============================================================================

TEST_F(RuntimeConfigTest, UpdateReadConfigPatch) {
    Json::Value patch;
    patch["max_candidates"] = 10;
    p2p_store().updateReadConfig(patch);
    EXPECT_EQ(p2p_store().getDefaultReadConfig().max_candidates, 10u);
}

TEST_F(RuntimeConfigTest, UpdateReadConfigP2PExtra) {
    Json::Value patch;
    Json::Value p2p;
    Json::Value tags(Json::arrayValue);
    tags.append("ssd");
    p2p["tag_filters"] = tags;
    p2p["priority_limit"] = 5;
    patch["p2p_config"] = p2p;
    p2p_store().updateReadConfig(patch);

    auto rc = p2p_store().getDefaultReadConfig();
    ASSERT_TRUE(rc.p2p_config.has_value());
    EXPECT_EQ(rc.p2p_config->priority_limit, 5);
    ASSERT_EQ(rc.p2p_config->tag_filters.size(), 1u);
    EXPECT_EQ(rc.p2p_config->tag_filters[0], "ssd");
}

// ============================================================================
// Store: patch semantics
// ============================================================================

TEST_F(RuntimeConfigTest, PatchPreservesUnmentionedFields) {
    Json::Value p1;
    p1["prefer_local"] = false;
    p2p_store().updateWriteConfig(p1);
    Json::Value p2;
    p2["max_candidates"] = 7;
    p2p_store().updateWriteConfig(p2);

    auto wc = p2p_store().getDefaultWriteConfig();
    auto& cfg = std::get<WriteRouteRequestConfig>(wc);
    EXPECT_FALSE(cfg.prefer_local);
    EXPECT_EQ(cfg.max_candidates, 7u);
}

// ============================================================================
// Store: loadFromJson
// ============================================================================

TEST_F(RuntimeConfigTest, LoadFromJsonBothSections) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    Json::Value root;
    root["write"]["prefer_local"] = false;
    root["write"]["max_candidates"] = 3;
    root["read"]["max_candidates"] = 8;
    store.loadFromJson(root);

    auto wc = store.getDefaultWriteConfig();
    auto& wcfg = std::get<WriteRouteRequestConfig>(wc);
    EXPECT_FALSE(wcfg.prefer_local);
    EXPECT_EQ(wcfg.max_candidates, 3u);
    EXPECT_EQ(store.getDefaultReadConfig().max_candidates, 8u);
}

TEST_F(RuntimeConfigTest, LoadFromJsonNullIsNoOp) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    store.loadFromJson(Json::Value());
    auto wc = store.getDefaultWriteConfig();
    auto& cfg = std::get<WriteRouteRequestConfig>(wc);
    EXPECT_TRUE(cfg.prefer_local);
    EXPECT_EQ(cfg.max_candidates, 2u);
}

// ============================================================================
// Store: exportConfig + round trip
// ============================================================================

TEST_F(RuntimeConfigTest, ExportRoundTrip) {
    RuntimeConfigStore s1(DeploymentMode::P2P);
    Json::Value input;
    input["write"]["max_candidates"] = 4;
    input["write"]["prefer_local"] = false;
    input["read"]["max_candidates"] = 6;
    s1.loadFromJson(input);

    RuntimeConfigStore s2(DeploymentMode::P2P);
    s2.loadFromJson(s1.exportConfig());

    auto wc1 = s1.getDefaultWriteConfig();
    auto wc2 = s2.getDefaultWriteConfig();
    auto& w1 = std::get<WriteRouteRequestConfig>(wc1);
    auto& w2 = std::get<WriteRouteRequestConfig>(wc2);
    EXPECT_EQ(w1.max_candidates, w2.max_candidates);
    EXPECT_EQ(w1.prefer_local, w2.prefer_local);
    EXPECT_EQ(s1.getDefaultReadConfig().max_candidates,
              s2.getDefaultReadConfig().max_candidates);
}

// ============================================================================
// HTTP: GET endpoints
// ============================================================================

TEST_F(RuntimeConfigTest, HttpGetAllConfig) {
    auto resp = HttpGet(Url("/config"));
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(resp.body);
    EXPECT_TRUE(json.isMember("write"));
    EXPECT_TRUE(json.isMember("read"));
}

TEST_F(RuntimeConfigTest, HttpGetWriteConfig) {
    auto resp = HttpGet(Url("/config/write"));
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(resp.body);
    EXPECT_TRUE(json.isMember("prefer_local"));
    EXPECT_TRUE(json.isMember("max_candidates"));
}

TEST_F(RuntimeConfigTest, HttpGetReadConfig) {
    auto resp = HttpGet(Url("/config/read"));
    ASSERT_EQ(resp.status, 200);
    EXPECT_TRUE(ParseJson(resp.body).isMember("max_candidates"));
}

// ============================================================================
// HTTP: POST update endpoints
// ============================================================================

TEST_F(RuntimeConfigTest, HttpUpdateWriteConfig) {
    auto resp = HttpPost(Url("/config/update_write"),
                         R"({"prefer_local": false, "max_candidates": 5})");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(resp.body);
    EXPECT_FALSE(json["prefer_local"].asBool());
    EXPECT_EQ(json["max_candidates"].asUInt64(), 5u);

    auto get_json = ParseJson(HttpGet(Url("/config/write")).body);
    EXPECT_EQ(get_json["max_candidates"].asUInt64(), 5u);
}

TEST_F(RuntimeConfigTest, HttpUpdateWriteConfigInvalidJson) {
    EXPECT_EQ(HttpPost(Url("/config/update_write"), "not json").status, 400);
}

TEST_F(RuntimeConfigTest, HttpUpdateReadConfig) {
    auto resp =
        HttpPost(Url("/config/update_read"), R"({"max_candidates": 8})");
    ASSERT_EQ(resp.status, 200);
    EXPECT_EQ(ParseJson(resp.body)["max_candidates"].asUInt64(), 8u);
}

TEST_F(RuntimeConfigTest, HttpUpdateFull) {
    auto resp = HttpPost(
        Url("/config/update"),
        R"({"write":{"max_candidates":3},"read":{"max_candidates":7}})");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(resp.body);
    EXPECT_EQ(json["write"]["max_candidates"].asUInt64(), 3u);
    EXPECT_EQ(json["read"]["max_candidates"].asUInt64(), 7u);
}

// ============================================================================
// HTTP: single field get/set
// ============================================================================

TEST_F(RuntimeConfigTest, HttpGetSingleField) {
    HttpPost(Url("/config/update_write"), R"({"priority_limit": 42})");
    auto resp = HttpGet(Url("/config/get", "section=write&key=priority_limit"));
    ASSERT_EQ(resp.status, 200);
    EXPECT_EQ(ParseJson(resp.body).asInt(), 42);
}

TEST_F(RuntimeConfigTest, HttpGetSingleFieldMissingParams) {
    EXPECT_EQ(HttpGet(Url("/config/get", "section=write")).status, 400);
    EXPECT_EQ(HttpGet(Url("/config/get")).status, 400);
}

TEST_F(RuntimeConfigTest, HttpGetSingleFieldNotFound) {
    EXPECT_EQ(
        HttpGet(Url("/config/get", "section=write&key=nonexistent")).status,
        404);
}

TEST_F(RuntimeConfigTest, HttpSetSingleField) {
    auto resp =
        HttpPost(Url("/config/set", "section=write&key=early_return"), "false");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(HttpGet(Url("/config/write")).body);
    EXPECT_FALSE(json["early_return"].asBool());
}

TEST_F(RuntimeConfigTest, HttpSetSingleFieldReadSection) {
    auto resp =
        HttpPost(Url("/config/set", "section=read&key=max_candidates"), "99");
    ASSERT_EQ(resp.status, 200);
    auto json2 = ParseJson(HttpGet(Url("/config/read")).body);
    EXPECT_EQ(json2["max_candidates"].asUInt64(), 99u);
}

TEST_F(RuntimeConfigTest, HttpSetSingleFieldMissingParams) {
    EXPECT_EQ(HttpPost(Url("/config/set", "section=write"), "false").status,
              400);
}

TEST_F(RuntimeConfigTest, HttpSetSingleFieldUnknownSection) {
    EXPECT_EQ(
        HttpPost(Url("/config/set", "section=unknown&key=foo"), "1").status,
        400);
}

// ============================================================================
// Review fix: /config/set returns 404 for unknown key
// ============================================================================

TEST_F(RuntimeConfigTest, HttpSetSingleFieldUnknownKey) {
    auto resp =
        HttpPost(Url("/config/set", "section=write&key=nonexistent_key"), "42");
    EXPECT_EQ(resp.status, 404);
}

TEST_F(RuntimeConfigTest, HttpSetSingleFieldUnknownKeyReadSection) {
    auto resp =
        HttpPost(Url("/config/set", "section=read&key=nonexistent_key"), "42");
    EXPECT_EQ(resp.status, 404);
}

// ============================================================================
// Review fix: applyPatch ignores wrong JSON types (no crash)
// ============================================================================

TEST_F(RuntimeConfigTest, ApplyPatchIgnoresWrongTypes) {
    RuntimeConfigStore store(DeploymentMode::P2P);

    Json::Value bad;
    bad["max_candidates"] = "not_a_number";
    bad["prefer_local"] = 123;
    bad["strategy"] = Json::Value(Json::arrayValue);
    bad["tag_filters"] = false;
    bad["priority_limit"] = Json::Value(Json::objectValue);
    store.updateWriteConfig(bad);

    auto wc = store.getDefaultWriteConfig();
    auto& cfg = std::get<WriteRouteRequestConfig>(wc);
    EXPECT_EQ(cfg.max_candidates, 2u);
    EXPECT_TRUE(cfg.prefer_local);
    EXPECT_EQ(cfg.strategy, ObjectIterateStrategy::CAPACITY_PRIORITY);
    EXPECT_TRUE(cfg.tag_filters.empty());
    EXPECT_EQ(cfg.priority_limit, 0);
}

TEST_F(RuntimeConfigTest, ApplyPatchIgnoresWrongTypesCentralized) {
    RuntimeConfigStore store(DeploymentMode::CENTRALIZATION);

    Json::Value bad;
    bad["replica_num"] = Json::Value(Json::objectValue);
    bad["with_soft_pin"] = "yes";
    bad["preferred_segments"] = 42;
    bad["preferred_segment"] = true;
    bad["prefer_alloc_in_same_node"] = Json::Value(Json::arrayValue);
    store.updateWriteConfig(bad);

    auto wc = store.getDefaultWriteConfig();
    auto& cfg = std::get<ReplicateConfig>(wc);
    EXPECT_EQ(cfg.replica_num, 1u);
    EXPECT_FALSE(cfg.with_soft_pin);
    EXPECT_TRUE(cfg.preferred_segments.empty());
    EXPECT_EQ(cfg.preferred_segment, "");
    EXPECT_FALSE(cfg.prefer_alloc_in_same_node);
}

TEST_F(RuntimeConfigTest, HttpUpdateWriteConfigWrongTypesNoEffect) {
    HttpPost(Url("/config/update_write"),
             R"({"max_candidates": 10, "prefer_local": true})");

    auto resp = HttpPost(Url("/config/update_write"),
                         R"({"max_candidates": "bad", "prefer_local": 999})");
    ASSERT_EQ(resp.status, 200);

    auto json = ParseJson(HttpGet(Url("/config/write")).body);
    EXPECT_EQ(json["max_candidates"].asUInt64(), 10u);
    EXPECT_TRUE(json["prefer_local"].asBool());
}

// ============================================================================
// Review fix: loadFromJson is atomic (write+read in single lock)
// ============================================================================

TEST_F(RuntimeConfigTest, LoadFromJsonAtomicUpdate) {
    RuntimeConfigStore store(DeploymentMode::P2P);

    Json::Value root;
    root["write"]["max_candidates"] = 11;
    root["read"]["max_candidates"] = 22;
    store.loadFromJson(root);

    auto exported = store.exportConfig();
    EXPECT_EQ(exported["write"]["max_candidates"].asUInt64(), 11u);
    EXPECT_EQ(exported["read"]["max_candidates"].asUInt64(), 22u);
}

// ============================================================================
// Store: loadFromJson with non-object input
// ============================================================================

TEST_F(RuntimeConfigTest, LoadFromJsonArrayIsNoOp) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    Json::Value arr(Json::arrayValue);
    arr.append(1);
    store.loadFromJson(arr);
    auto wc = store.getDefaultWriteConfig();
    EXPECT_TRUE(std::holds_alternative<WriteRouteRequestConfig>(wc));
}

TEST_F(RuntimeConfigTest, LoadFromJsonStringIsNoOp) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    store.loadFromJson(Json::Value("hello"));
    auto wc = store.getDefaultWriteConfig();
    EXPECT_TRUE(std::holds_alternative<WriteRouteRequestConfig>(wc));
}

TEST_F(RuntimeConfigTest, LoadFromJsonNumberIsNoOp) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    store.loadFromJson(Json::Value(42));
    auto wc = store.getDefaultWriteConfig();
    EXPECT_TRUE(std::holds_alternative<WriteRouteRequestConfig>(wc));
}

// ============================================================================
// Store: applyPatch with non-object input
// ============================================================================

TEST_F(RuntimeConfigTest, UpdateWriteConfigNonObjectIsNoOp) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    store.updateWriteConfig(Json::Value("not an object"));
    store.updateWriteConfig(Json::Value(Json::arrayValue));
    store.updateWriteConfig(Json::Value(123));
    auto wc = store.getDefaultWriteConfig();
    auto& cfg = std::get<WriteRouteRequestConfig>(wc);
    EXPECT_EQ(cfg.max_candidates, 2u);
}

TEST_F(RuntimeConfigTest, UpdateReadConfigNonObjectIsNoOp) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    store.updateReadConfig(Json::Value(false));
    store.updateReadConfig(Json::Value(Json::arrayValue));
    auto rc = store.getDefaultReadConfig();
    EXPECT_EQ(rc.max_candidates, 0u);
}

// ============================================================================
// HTTP: empty body returns 400
// ============================================================================

TEST_F(RuntimeConfigTest, HttpUpdateWriteEmptyBody) {
    EXPECT_EQ(HttpPost(Url("/config/update_write"), "").status, 400);
}

TEST_F(RuntimeConfigTest, HttpUpdateReadEmptyBody) {
    EXPECT_EQ(HttpPost(Url("/config/update_read"), "").status, 400);
}

TEST_F(RuntimeConfigTest, HttpUpdateFullEmptyBody) {
    EXPECT_EQ(HttpPost(Url("/config/update"), "").status, 400);
}

// ============================================================================
// HTTP: non-object body returns 400
// ============================================================================

TEST_F(RuntimeConfigTest, HttpUpdateWriteArrayBody) {
    EXPECT_EQ(HttpPost(Url("/config/update_write"), "[1,2,3]").status, 400);
}

TEST_F(RuntimeConfigTest, HttpUpdateReadStringBody) {
    EXPECT_EQ(HttpPost(Url("/config/update_read"), R"("hello")").status, 400);
}

TEST_F(RuntimeConfigTest, HttpUpdateFullNumberBody) {
    EXPECT_EQ(HttpPost(Url("/config/update"), "42").status, 400);
}

// ============================================================================
// HTTP: /config/set accepts primitive body
// ============================================================================

TEST_F(RuntimeConfigTest, HttpSetPrimitiveBool) {
    HttpPost(Url("/config/update_write"), R"({"allow_local": true})");
    auto resp =
        HttpPost(Url("/config/set", "section=write&key=allow_local"), "false");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(HttpGet(Url("/config/write")).body);
    EXPECT_FALSE(json["allow_local"].asBool());
}

TEST_F(RuntimeConfigTest, HttpSetPrimitiveInt) {
    auto resp =
        HttpPost(Url("/config/set", "section=write&key=max_candidates"), "7");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(HttpGet(Url("/config/write")).body);
    EXPECT_EQ(json["max_candidates"].asUInt64(), 7u);
}

}  // namespace testing
}  // namespace mooncake
