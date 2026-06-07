#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

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

    static coro_http::resp_data HttpGet(const std::string& url) {
        coro_http::coro_http_client c;
        return c.get(url);
    }

    static coro_http::resp_data HttpPost(const std::string& url,
                                         std::string body = "") {
        coro_http::coro_http_client c;
        return c.post(url, std::move(body), coro_http::req_content_type::json);
    }

    static Json::Value ParseJson(const std::string& str) {
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
    EXPECT_EQ(cfg.max_candidates, 0u);
}

TEST_F(RuntimeConfigTest, DefaultReadConfig) {
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

    auto& cfg =
        std::get<ReplicateConfig>(centralized_store().getDefaultWriteConfig());
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

    auto& cfg =
        std::get<ReplicateConfig>(centralized_store().getDefaultWriteConfig());
    ASSERT_EQ(cfg.preferred_segments.size(), 2u);
    EXPECT_EQ(cfg.preferred_segments[0], "seg_a");
    EXPECT_TRUE(cfg.prefer_alloc_in_same_node);
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

    auto& cfg =
        std::get<WriteRouteRequestConfig>(p2p_store().getDefaultWriteConfig());
    EXPECT_FALSE(cfg.prefer_local);
    EXPECT_EQ(cfg.max_candidates, 5u);
    EXPECT_EQ(cfg.priority_limit, 10);
    EXPECT_TRUE(cfg.allow_local);
}

TEST_F(RuntimeConfigTest, UpdateP2PWriteConfigStrategy) {
    Json::Value patch;
    patch["strategy"] = static_cast<int>(ObjectIterateStrategy::RANDOM);
    p2p_store().updateWriteConfig(patch);

    auto& cfg =
        std::get<WriteRouteRequestConfig>(p2p_store().getDefaultWriteConfig());
    EXPECT_EQ(cfg.strategy, ObjectIterateStrategy::RANDOM);
}

TEST_F(RuntimeConfigTest, UpdateP2PWriteConfigTagFilters) {
    Json::Value patch;
    Json::Value tags(Json::arrayValue);
    tags.append("gpu");
    tags.append("high_priority");
    patch["tag_filters"] = tags;
    p2p_store().updateWriteConfig(patch);

    auto& cfg =
        std::get<WriteRouteRequestConfig>(p2p_store().getDefaultWriteConfig());
    ASSERT_EQ(cfg.tag_filters.size(), 2u);
    EXPECT_EQ(cfg.tag_filters[0], "gpu");
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

    auto& cfg =
        std::get<WriteRouteRequestConfig>(p2p_store().getDefaultWriteConfig());
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

    auto& wcfg =
        std::get<WriteRouteRequestConfig>(store.getDefaultWriteConfig());
    EXPECT_FALSE(wcfg.prefer_local);
    EXPECT_EQ(wcfg.max_candidates, 3u);
    EXPECT_EQ(store.getDefaultReadConfig().max_candidates, 8u);
}

TEST_F(RuntimeConfigTest, LoadFromJsonNullIsNoOp) {
    RuntimeConfigStore store(DeploymentMode::P2P);
    store.loadFromJson(Json::Value());
    auto& cfg =
        std::get<WriteRouteRequestConfig>(store.getDefaultWriteConfig());
    EXPECT_TRUE(cfg.prefer_local);
    EXPECT_EQ(cfg.max_candidates, 0u);
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

    auto& w1 = std::get<WriteRouteRequestConfig>(s1.getDefaultWriteConfig());
    auto& w2 = std::get<WriteRouteRequestConfig>(s2.getDefaultWriteConfig());
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
    auto json = ParseJson(resp.resp_body);
    EXPECT_TRUE(json.isMember("write"));
    EXPECT_TRUE(json.isMember("read"));
}

TEST_F(RuntimeConfigTest, HttpGetWriteConfig) {
    auto resp = HttpGet(Url("/config/write"));
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(resp.resp_body);
    EXPECT_TRUE(json.isMember("prefer_local"));
    EXPECT_TRUE(json.isMember("max_candidates"));
}

TEST_F(RuntimeConfigTest, HttpGetReadConfig) {
    auto resp = HttpGet(Url("/config/read"));
    ASSERT_EQ(resp.status, 200);
    EXPECT_TRUE(ParseJson(resp.resp_body).isMember("max_candidates"));
}

// ============================================================================
// HTTP: POST update endpoints
// ============================================================================

TEST_F(RuntimeConfigTest, HttpUpdateWriteConfig) {
    auto resp = HttpPost(Url("/config/update_write"),
                         R"({"prefer_local": false, "max_candidates": 5})");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(resp.resp_body);
    EXPECT_FALSE(json["prefer_local"].asBool());
    EXPECT_EQ(json["max_candidates"].asUInt64(), 5u);

    auto get_json = ParseJson(HttpGet(Url("/config/write")).resp_body);
    EXPECT_EQ(get_json["max_candidates"].asUInt64(), 5u);
}

TEST_F(RuntimeConfigTest, HttpUpdateWriteConfigInvalidJson) {
    EXPECT_EQ(HttpPost(Url("/config/update_write"), "not json").status, 400);
}

TEST_F(RuntimeConfigTest, HttpUpdateReadConfig) {
    auto resp =
        HttpPost(Url("/config/update_read"), R"({"max_candidates": 8})");
    ASSERT_EQ(resp.status, 200);
    EXPECT_EQ(ParseJson(resp.resp_body)["max_candidates"].asUInt64(), 8u);
}

TEST_F(RuntimeConfigTest, HttpUpdateFull) {
    auto resp = HttpPost(
        Url("/config/update"),
        R"({"write":{"max_candidates":3},"read":{"max_candidates":7}})");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(resp.resp_body);
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
    EXPECT_EQ(ParseJson(resp.resp_body).asInt(), 42);
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
    auto json = ParseJson(HttpGet(Url("/config/write")).resp_body);
    EXPECT_FALSE(json["early_return"].asBool());
}

TEST_F(RuntimeConfigTest, HttpSetSingleFieldReadSection) {
    auto resp =
        HttpPost(Url("/config/set", "section=read&key=max_candidates"), "99");
    ASSERT_EQ(resp.status, 200);
    auto json = ParseJson(HttpGet(Url("/config/read")).resp_body);
    EXPECT_EQ(json["max_candidates"].asUInt64(), 99u);
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

}  // namespace testing
}  // namespace mooncake
