// Focused EventManager, HTTP contract, registration lifecycle, event adapter,
// and static configuration tests.

#include <gtest/gtest.h>
#include <asio.hpp>
#include <json/json.h>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_http/coro_http_server.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdio>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/kvevent/config.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"
#include "event_manager_test_peer.h"

namespace conductor {
namespace kvevent {

uint16_t EventManagerTestPeer::HttpPort(EventManager& manager) {
    return manager.http_server_ ? manager.http_server_->port() : 0;
}

}  // namespace kvevent
}  // namespace conductor

namespace {

using conductor::common::HashProfileConfig;
using conductor::common::ServiceConfig;
using conductor::kvevent::EventManager;
using conductor::kvevent::EventManagerTestPeer;
using conductor::kvevent::KVEventHandler;
using conductor::kvevent::KVEventHandlerTestPeer;
using conductor::kvevent::MakeServiceKey;
using conductor::prefixindex::ContextKey;
using conductor::prefixindex::CreateHashStrategy;
using conductor::prefixindex::EngineOwner;
using conductor::prefixindex::EngineRegistration;
using conductor::prefixindex::GpuMutation;
using conductor::prefixindex::HashBlock;
using conductor::prefixindex::HashProfile;
using conductor::prefixindex::PrefixCacheTable;
using conductor::prefixindex::ProjectedPrefix;
using conductor::prefixindex::SharedMutation;
using conductor::prefixindex::SharedObjectOwner;
using conductor::prefixindex::StorageTier;
using conductor::zmq::BlockRemovedEvent;
using conductor::zmq::BlockStoredEvent;

constexpr std::string_view kRootDigest =
    "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e";
constexpr std::string_view kOtherRootDigest =
    "0000000000000000000000000000000000000000000000000000000000000000";

HashProfile TestProfile(std::string root_digest = std::string(kRootDigest)) {
    return {.strategy = "vllm_v1",
            .algorithm = "sha256_cbor",
            .root_digest = std::move(root_digest),
            .index_projection = "low64_be"};
}

HashProfileConfig TestProfileConfig(
    std::string root_digest = std::string(kRootDigest)) {
    return {.strategy = "vllm_v1",
            .algorithm = "sha256_cbor",
            .root_digest = std::move(root_digest),
            .index_projection = "low64_be"};
}

ContextKey ContextFor(const ServiceConfig& service) {
    return {.tenant_id = service.tenant_id,
            .model_name = service.model_name,
            .lora_name = service.lora_name,
            .block_size = service.block_size};
}

EngineRegistration RegistrationFor(const ServiceConfig& service) {
    return {
        .context = ContextFor(service),
        .profile = {.strategy = service.hash_profile.strategy,
                    .algorithm = service.hash_profile.algorithm,
                    .root_digest = service.hash_profile.root_digest,
                    .index_projection = service.hash_profile.index_projection},
        .instance_id = service.instance_id,
        .dp_rank = service.dp_rank,
        .effective_block_size = service.block_size,
        .cache_group = service.cache_group};
}

ServiceConfig VllmService(const std::string& instance_id = "instance-1",
                          const std::string& tenant_id = "default",
                          int dp_rank = 0, int64_t block_size = 16) {
    ServiceConfig service;
    service.endpoint = "tcp://127.0.0.1:59999";
    service.replay_endpoint = "tcp://127.0.0.1:59998";
    service.type = conductor::common::kServiceTypeVLLM;
    service.model_name = "test-model";
    service.instance_id = instance_id;
    service.tenant_id = tenant_id;
    service.dp_rank = dp_rank;
    service.block_size = block_size;
    service.hash_profile = TestProfileConfig();
    return service;
}

ServiceConfig MooncakeService(const ServiceConfig& engine) {
    ServiceConfig service = engine;
    service.endpoint = "tcp://127.0.0.1:60999";
    service.replay_endpoint = "tcp://127.0.0.1:60998";
    service.type = conductor::common::kServiceTypeMooncake;
    service.instance_id = "shared-pool";
    service.dp_rank = 0;
    return service;
}

std::vector<int32_t> Sequence(int32_t first, size_t count) {
    std::vector<int32_t> values;
    values.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        values.push_back(first + static_cast<int32_t>(index));
    }
    return values;
}

std::vector<ProjectedPrefix> ProjectedFor(
    const ContextKey& context, const HashProfile& profile,
    const std::vector<int32_t>& tokens,
    std::optional<std::string> cache_salt = std::nullopt) {
    std::string error;
    auto strategy = CreateHashStrategy(profile, &error);
    EXPECT_NE(strategy, nullptr) << error;
    if (!strategy) {
        return {};
    }
    std::vector<HashBlock> blocks;
    error = strategy->Compute(context, tokens, std::move(cache_salt), &blocks);
    EXPECT_TRUE(error.empty()) << error;
    std::vector<ProjectedPrefix> prefixes;
    prefixes.reserve(blocks.size());
    for (const auto& block : blocks) {
        prefixes.push_back(block.projected);
    }
    return prefixes;
}

struct HttpResponse {
    int status = 0;
    std::string body;
    std::map<std::string, std::string> headers;
};

std::string LowerAscii(std::string_view value) {
    std::string out(value);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char value) {
                       return static_cast<char>(std::tolower(value));
                   });
    return out;
}

template <typename Result>
HttpResponse ToHttpResponse(const Result& result) {
    HttpResponse response;
    response.status = result.status;
    response.body = std::string(result.resp_body);
    for (const auto& header : result.resp_headers) {
        response.headers[LowerAscii(header.name)] = std::string(header.value);
    }
    return response;
}

HttpResponse HttpPostJson(uint16_t port, const std::string& path,
                          const std::string& body) {
    coro_http::coro_http_client client;
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + path;
    return ToHttpResponse(
        client.post(url, body, coro_http::req_content_type::json));
}

HttpResponse HttpGet(uint16_t port, const std::string& path) {
    coro_http::coro_http_client client;
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + path;
    return ToHttpResponse(client.get(url));
}

bool ParseJsonDocument(const std::string& document, Json::Value* value,
                       std::string* errors) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    return reader->parse(document.data(), document.data() + document.size(),
                         value, errors);
}

std::string JsonDocument(const Json::Value& value) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, value);
}

Json::Value TokenArray(const std::vector<int32_t>& tokens) {
    Json::Value values(Json::arrayValue);
    for (const int32_t token : tokens) {
        values.append(Json::Value::Int64(token));
    }
    return values;
}

Json::Value QueryJson(const ContextKey& context,
                      const std::vector<int32_t>& tokens) {
    Json::Value query(Json::objectValue);
    query["model"] = context.model_name;
    query["block_size"] = Json::Value::Int64(context.block_size);
    query["token_ids"] = TokenArray(tokens);
    if (context.tenant_id != "default") {
        query["tenant_id"] = context.tenant_id;
    }
    if (!context.lora_name.empty()) {
        query["lora_name"] = context.lora_name;
    }
    return query;
}

Json::Value ServiceJson(const ServiceConfig& service) {
    Json::Value value(Json::objectValue);
    value["endpoint"] = service.endpoint;
    value["replay_endpoint"] = service.replay_endpoint;
    value["type"] = service.type;
    value["modelname"] = service.model_name;
    value["lora_name"] = service.lora_name;
    value["tenant_id"] = service.tenant_id;
    value["instance_id"] = service.instance_id;
    value["block_size"] = Json::Value::Int64(service.block_size);
    value["dp_rank"] = service.dp_rank;
    if (service.cache_group.has_value()) {
        value["cache_group"] = Json::Value::Int64(*service.cache_group);
    }
    Json::Value profile(Json::objectValue);
    profile["strategy"] = service.hash_profile.strategy;
    profile["algorithm"] = service.hash_profile.algorithm;
    profile["root_digest"] = service.hash_profile.root_digest;
    profile["index_projection"] = service.hash_profile.index_projection;
    value["hash_profile"] = profile;
    return value;
}

Json::Value ParseJsonResponse(const HttpResponse& response) {
    Json::Value body;
    std::string errors;
    EXPECT_TRUE(ParseJsonDocument(response.body, &body, &errors)) << errors;
    return body;
}

TEST(MakeServiceKey, IncludesRank) {
    EXPECT_EQ(MakeServiceKey("instance-1", "tenant-1", 2),
              "instance-1|tenant-1|2");
}

TEST(EventManager, InitialState) {
    EventManager manager({}, 13333);
    EXPECT_NE(manager.GetIndexer(), nullptr);
    EXPECT_EQ(EventManagerTestPeer::ServicesLen(manager), 0u);
    EXPECT_FALSE(manager.IsStopped());
}

TEST(SubscribeToService, ExactDuplicateIsIdempotent) {
    EventManager manager({}, 0);
    const auto service = VllmService();
    const auto first = EventManagerTestPeer::Subscribe(manager, service);
    ASSERT_TRUE(first.first) << first.second;
    const auto duplicate = EventManagerTestPeer::Subscribe(manager, service);
    EXPECT_FALSE(duplicate.first);
    EXPECT_TRUE(duplicate.second.empty());
    EXPECT_EQ(EventManagerTestPeer::SubscriberCount(manager), 1u);
    EXPECT_EQ(manager.GetIndexer()->Query(ContextFor(service), {}).size(), 1u);
}

TEST(SubscribeToService, ConflictingDuplicateIsRejectedWithoutIndexChange) {
    EventManager manager({}, 0);
    const auto service = VllmService();
    ASSERT_TRUE(EventManagerTestPeer::Subscribe(manager, service).first);

    auto conflicting = service;
    conflicting.model_name = "other-model";
    const auto result = EventManagerTestPeer::Subscribe(manager, conflicting);
    EXPECT_FALSE(result.first);
    EXPECT_NE(result.second.find("conflicting registration"),
              std::string::npos);
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().context_count, 1);
    EXPECT_EQ(manager.GetIndexer()->Query(ContextFor(service), {}).size(), 1u);
    EXPECT_TRUE(
        manager.GetIndexer()->Query(ContextFor(conflicting), {}).empty());
}

TEST(SubscribeToService, InvalidRegistrationCreatesNoState) {
    EventManager manager({}, 0);
    auto service = VllmService();
    service.endpoint.clear();
    service.cache_group = 1;

    const auto result = EventManagerTestPeer::Subscribe(manager, service);
    EXPECT_FALSE(result.first);
    EXPECT_FALSE(result.second.empty());
    EXPECT_EQ(EventManagerTestPeer::SubscriberCount(manager), 0u);
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().context_count, 0);
}

TEST(SubscribeToService, ManagerStoppedCreatesNoState) {
    EventManager manager({}, 0);
    manager.Stop();
    const auto service = VllmService();
    const auto result = EventManagerTestPeer::Subscribe(manager, service);
    EXPECT_FALSE(result.first);
    EXPECT_EQ(result.second, "manager stopped");
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().context_count, 0);
}

TEST(RegistrationLifecycle, StaticServicesRegisterEveryRank) {
    auto rank_zero = VllmService("instance-1", "default", 0);
    auto rank_one = rank_zero;
    rank_one.dp_rank = 1;
    EventManager manager({rank_zero, rank_one}, 0);
    manager.Start();

    const auto results = manager.GetIndexer()->Query(ContextFor(rank_zero), {});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results.at("instance-1").dp,
              (std::map<int64_t, int64_t>{{0, 0}, {1, 0}}));
}

TEST(RegistrationLifecycle, PartialUnregisterPreservesRemainingRank) {
    EventManager manager({}, 0);
    auto rank_zero = VllmService("instance-1", "default", 0);
    auto rank_one = rank_zero;
    rank_one.dp_rank = 1;
    ASSERT_TRUE(EventManagerTestPeer::Register(manager, rank_zero).first);
    ASSERT_TRUE(EventManagerTestPeer::Register(manager, rank_one).first);

    const auto removed_zero =
        EventManagerTestPeer::Unsubscribe(manager, "instance-1", "default", 0);
    ASSERT_TRUE(removed_zero.first) << removed_zero.second;
    ASSERT_TRUE(removed_zero.second.empty());
    const auto after_zero =
        manager.GetIndexer()->Query(ContextFor(rank_zero), {});
    ASSERT_EQ(after_zero.size(), 1u);
    EXPECT_EQ(after_zero.at("instance-1").dp,
              (std::map<int64_t, int64_t>{{1, 0}}));
    EXPECT_EQ(EventManagerTestPeer::ServicesLen(manager), 1u);

    const auto removed_one =
        EventManagerTestPeer::Unsubscribe(manager, "instance-1", "default", 1);
    ASSERT_TRUE(removed_one.first) << removed_one.second;
    EXPECT_TRUE(manager.GetIndexer()->Query(ContextFor(rank_zero), {}).empty());
    EXPECT_EQ(EventManagerTestPeer::ServicesLen(manager), 0u);
}

TEST(EventManager, StopIsIdempotent) {
    EventManager manager({}, 0);
    manager.Stop();
    manager.Stop();
    EXPECT_TRUE(manager.IsStopped());
}

TEST(EventManager, StopWaitsForHttpServerShutdown) {
    auto manager = std::make_unique<EventManager>(
        std::vector<conductor::common::ServiceConfig>{}, 0);
    ASSERT_TRUE(manager->StartHTTPServer());
    const uint16_t port = EventManagerTestPeer::HttpPort(*manager);
    ASSERT_NE(port, 0);

    asio::io_context io_context;
    const asio::ip::tcp::endpoint endpoint(asio::ip::make_address("127.0.0.1"),
                                           port);
    asio::ip::tcp::socket client(io_context);
    asio::error_code error;
    client.connect(endpoint, error);
    ASSERT_FALSE(error) << error.message();
    client.close();

    manager->Stop();
    manager.reset();

    asio::ip::tcp::socket after_stop(io_context);
    error.clear();
    after_stop.connect(endpoint, error);
    EXPECT_TRUE(error);
}

class QueryHttpTest : public ::testing::Test {
   protected:
    void SetUp() override {
        manager_ = std::make_unique<EventManager>(
            std::vector<conductor::common::ServiceConfig>{}, 0);
        ASSERT_TRUE(manager_->StartHTTPServer());
        port_ = EventManagerTestPeer::HttpPort(*manager_);
        ASSERT_NE(port_, 0);

        instance_one_ = VllmService("1", "default", 0, 16);
        instance_two_ = VllmService("2", "default", 1, 16);
        ASSERT_TRUE(manager_->GetIndexer()
                        ->Register(RegistrationFor(instance_one_))
                        .error.empty());
        ASSERT_TRUE(manager_->GetIndexer()
                        ->Register(RegistrationFor(instance_two_))
                        .error.empty());

        tokens_ = Sequence(1, 48);
        const auto prefixes =
            ProjectedFor(ContextFor(instance_one_), TestProfile(), tokens_);
        ASSERT_EQ(prefixes.size(), 3u);
        ASSERT_TRUE(manager_->GetIndexer()
                        ->StoreGpu({.context = ContextFor(instance_one_),
                                    .prefixes = {prefixes[0], prefixes[1]},
                                    .owner = {.source_stream = "engine-1",
                                              .instance_id = "1",
                                              .dp_rank = 0},
                                    .effective_block_size = 16})
                        .empty());
        ASSERT_TRUE(manager_->GetIndexer()
                        ->StoreShared({.context = ContextFor(instance_one_),
                                       .prefixes = prefixes,
                                       .tier = StorageTier::kCpu,
                                       .owner = {.source_stream = "pool",
                                                 .backend_id = "cpu-backend",
                                                 .object_id = "cpu-object"},
                                       .effective_block_size = 16})
                        .empty());
        ASSERT_TRUE(manager_->GetIndexer()
                        ->StoreShared({.context = ContextFor(instance_one_),
                                       .prefixes = prefixes,
                                       .tier = StorageTier::kDisk,
                                       .owner = {.source_stream = "pool",
                                                 .backend_id = "disk-backend",
                                                 .object_id = "disk-object"},
                                       .effective_block_size = 16})
                        .empty());
    }

    void TearDown() override {
        if (manager_) {
            manager_->Stop();
        }
    }

    HttpResponse Post(const Json::Value& body,
                      const std::string& path = "/query") const {
        return HttpPostJson(port_, path, JsonDocument(body));
    }

    Json::Value ValidQuery() const {
        return QueryJson(ContextFor(instance_one_), tokens_);
    }

    void ExpectJsonStatus(const HttpResponse& response, int status,
                          Json::Value* body) const {
        EXPECT_EQ(response.status, status);
        const auto content_type = response.headers.find("content-type");
        ASSERT_NE(content_type, response.headers.end());
        EXPECT_EQ(content_type->second, "application/json");
        std::string errors;
        ASSERT_TRUE(ParseJsonDocument(response.body, body, &errors)) << errors;
        ASSERT_TRUE(body->isObject());
    }

    std::unique_ptr<EventManager> manager_;
    uint16_t port_ = 0;
    ServiceConfig instance_one_;
    ServiceConfig instance_two_;
    std::vector<int32_t> tokens_;
};

TEST_F(QueryHttpTest, ReturnsExactSharedCacheResponse) {
    Json::Value body;
    ASSERT_NO_FATAL_FAILURE(ExpectJsonStatus(Post(ValidQuery()), 200, &body));

    Json::Value expected;
    std::string errors;
    ASSERT_TRUE(ParseJsonDocument(
        R"({"instances":{"1":{"longest_matched":48,"gpu":32,"dp":{"0":32},"cpu":48,"disk":48},"2":{"longest_matched":48,"gpu":0,"dp":{"1":0},"cpu":48,"disk":48}}})",
        &expected, &errors))
        << errors;
    EXPECT_EQ(body, expected);
    EXPECT_EQ(body.getMemberNames(), (std::vector<std::string>{"instances"}));
}

TEST_F(QueryHttpTest, FiltersCompatibleInstanceAndDropsUnknownFilter) {
    Json::Value query = ValidQuery();
    query["instance_id"] = "1";
    const Json::Value selected = ParseJsonResponse(Post(query));
    ASSERT_EQ(selected["instances"].getMemberNames(),
              (std::vector<std::string>{"1"}));

    query["instance_id"] = "missing";
    const Json::Value missing = ParseJsonResponse(Post(query));
    EXPECT_TRUE(missing["instances"].isObject());
    EXPECT_TRUE(missing["instances"].empty());
}

TEST_F(QueryHttpTest, MissingContextReturnsEmptyWithoutCreatingState) {
    Json::Value query = ValidQuery();
    query["model"] = "missing-model";
    const auto before = manager_->GetIndexer()->GetGlobalView().context_count;
    const Json::Value response = ParseJsonResponse(Post(query));
    EXPECT_TRUE(response["instances"].empty());
    EXPECT_EQ(manager_->GetIndexer()->GetGlobalView().context_count, before);

    query = ValidQuery();
    query["tenant_id"] = "other-tenant";
    EXPECT_TRUE(ParseJsonResponse(Post(query))["instances"].empty());
    query = ValidQuery();
    query["lora_name"] = "other-lora";
    EXPECT_TRUE(ParseJsonResponse(Post(query))["instances"].empty());
    query = ValidQuery();
    query["block_size"] = 32;
    EXPECT_TRUE(ParseJsonResponse(Post(query))["instances"].empty());
    EXPECT_EQ(manager_->GetIndexer()->GetGlobalView().context_count, before);
}

TEST_F(QueryHttpTest, SaltIsRequestOnlyAndNullEmptyOmittedMeanNoSalt) {
    auto salted_service = VllmService("salted", "default", 0, 4);
    salted_service.model_name = "salt-model";
    ASSERT_TRUE(manager_->GetIndexer()
                    ->Register(RegistrationFor(salted_service))
                    .error.empty());
    const std::vector<int32_t> tokens = Sequence(100, 8);
    const auto salted_prefixes = ProjectedFor(ContextFor(salted_service),
                                              TestProfile(), tokens, "pepper");
    ASSERT_TRUE(manager_->GetIndexer()
                    ->StoreGpu({.context = ContextFor(salted_service),
                                .prefixes = salted_prefixes,
                                .owner = {.source_stream = "salt-engine",
                                          .instance_id = "salted",
                                          .dp_rank = 0},
                                .effective_block_size = 4})
                    .empty());

    Json::Value query = QueryJson(ContextFor(salted_service), tokens);
    query["cache_salt"] = "pepper";
    EXPECT_EQ(
        ParseJsonResponse(Post(query))["instances"]["salted"]["gpu"].asInt64(),
        8);

    query.removeMember("cache_salt");
    EXPECT_EQ(
        ParseJsonResponse(Post(query))["instances"]["salted"]["gpu"].asInt64(),
        0);
    query["cache_salt"] = "";
    EXPECT_EQ(
        ParseJsonResponse(Post(query))["instances"]["salted"]["gpu"].asInt64(),
        0);
    query["cache_salt"] = Json::Value(Json::nullValue);
    EXPECT_EQ(
        ParseJsonResponse(Post(query))["instances"]["salted"]["gpu"].asInt64(),
        0);
}

TEST_F(QueryHttpTest, RejectsMalformedInputsBeforeLookup) {
    struct Case {
        const char* name;
        std::string body;
        const char* reason;
    };

    Json::Value missing_model = ValidQuery();
    missing_model.removeMember("model");
    Json::Value empty_model = ValidQuery();
    empty_model["model"] = "";
    Json::Value wrong_model = ValidQuery();
    wrong_model["model"] = 7;
    Json::Value missing_block = ValidQuery();
    missing_block.removeMember("block_size");
    Json::Value zero_block = ValidQuery();
    zero_block["block_size"] = 0;
    Json::Value fractional_block = ValidQuery();
    fractional_block["block_size"] = 16.5;
    Json::Value missing_tokens = ValidQuery();
    missing_tokens.removeMember("token_ids");
    Json::Value string_token = ValidQuery();
    string_token["token_ids"] = Json::Value(Json::arrayValue);
    string_token["token_ids"].append("1");
    Json::Value large_token = ValidQuery();
    large_token["token_ids"] = Json::Value(Json::arrayValue);
    large_token["token_ids"].append(Json::Value::Int64(2147483648LL));
    Json::Value small_token = ValidQuery();
    small_token["token_ids"] = Json::Value(Json::arrayValue);
    small_token["token_ids"].append(Json::Value::Int64(-2147483649LL));
    Json::Value bad_salt = ValidQuery();
    bad_salt["cache_salt"] = 42;
    Json::Value bad_tenant = ValidQuery();
    bad_tenant["tenant_id"] = Json::Value(Json::nullValue);
    Json::Value bad_lora = ValidQuery();
    bad_lora["lora_name"] = false;
    Json::Value bad_instance = ValidQuery();
    bad_instance["instance_id"] = 3;
    Json::Value override_profile = ValidQuery();
    override_profile["root_digest"] = std::string(kRootDigest);

    const std::vector<Case> cases = {
        {"malformed JSON", "{not-json", "invalid_json"},
        {"missing model", JsonDocument(missing_model), "missing"},
        {"empty model", JsonDocument(empty_model), "invalid_value"},
        {"wrong model", JsonDocument(wrong_model), "invalid_type"},
        {"missing block", JsonDocument(missing_block), "missing"},
        {"zero block", JsonDocument(zero_block), "out_of_range"},
        {"fractional block", JsonDocument(fractional_block), "invalid_type"},
        {"missing tokens", JsonDocument(missing_tokens), "missing"},
        {"string token", JsonDocument(string_token), "invalid_type"},
        {"large token", JsonDocument(large_token), "out_of_range"},
        {"small token", JsonDocument(small_token), "out_of_range"},
        {"bad salt", JsonDocument(bad_salt), "invalid_type"},
        {"bad tenant", JsonDocument(bad_tenant), "invalid_type"},
        {"bad lora", JsonDocument(bad_lora), "invalid_type"},
        {"bad instance", JsonDocument(bad_instance), "invalid_type"},
        {"profile override", JsonDocument(override_profile), "unknown_field"},
    };

    const auto before = manager_->GetIndexer()->GetGlobalView();
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        const HttpResponse response =
            HttpPostJson(port_, "/query", test_case.body);
        EXPECT_EQ(response.status, 400);
        const Json::Value error = ParseJsonResponse(response);
        EXPECT_EQ(error["reason"].asString(), test_case.reason);
        const auto after = manager_->GetIndexer()->GetGlobalView();
        EXPECT_EQ(after.context_count, before.context_count);
        ASSERT_EQ(after.contexts.size(), before.contexts.size());
        EXPECT_EQ(after.contexts[0].prefix_count,
                  before.contexts[0].prefix_count);
        EXPECT_EQ(after.contexts[0].instance_ranks,
                  before.contexts[0].instance_ranks);
    }
}

TEST_F(QueryHttpTest, AcceptsEmptyTokensAndSignedInt32Boundaries) {
    Json::Value query = ValidQuery();
    query["token_ids"] = Json::Value(Json::arrayValue);
    Json::Value empty = ParseJsonResponse(Post(query));
    EXPECT_EQ(empty["instances"]["1"]["dp"]["0"].asInt64(), 0);
    EXPECT_EQ(empty["instances"]["2"]["dp"]["1"].asInt64(), 0);

    query["token_ids"].append(
        Json::Value::Int64(std::numeric_limits<int32_t>::min()));
    query["token_ids"].append(
        Json::Value::Int64(std::numeric_limits<int32_t>::max()));
    EXPECT_EQ(Post(query).status, 200);
}

TEST_F(QueryHttpTest, GetIsMethodNotAllowedAndV2IsAbsent) {
    EXPECT_EQ(HttpGet(port_, "/query").status, 405);
    EXPECT_EQ(Post(ValidQuery(), "/v2/query").status, 404);
}

class RegistrationHttpTest : public ::testing::Test {
   protected:
    void SetUp() override {
        manager_ = std::make_unique<EventManager>(
            std::vector<conductor::common::ServiceConfig>{}, 0);
        ASSERT_TRUE(manager_->StartHTTPServer());
        port_ = EventManagerTestPeer::HttpPort(*manager_);
        ASSERT_NE(port_, 0);
    }

    void TearDown() override {
        if (manager_) {
            manager_->Stop();
        }
    }

    HttpResponse Register(const ServiceConfig& service) const {
        return HttpPostJson(port_, "/register",
                            JsonDocument(ServiceJson(service)));
    }

    HttpResponse Unregister(const std::string& instance_id, int dp_rank) const {
        Json::Value body(Json::objectValue);
        body["instance_id"] = instance_id;
        body["tenant_id"] = "default";
        body["dp_rank"] = dp_rank;
        return HttpPostJson(port_, "/unregister", JsonDocument(body));
    }

    Json::Value QueryEmpty(const ServiceConfig& service) const {
        return ParseJsonResponse(HttpPostJson(
            port_, "/query", JsonDocument(QueryJson(ContextFor(service), {}))));
    }

    std::unique_ptr<EventManager> manager_;
    uint16_t port_ = 0;
};

TEST_F(RegistrationHttpTest, PartialUnregisterKeepsInstanceUntilLastRank) {
    auto rank_zero = VllmService("instance-1", "default", 0);
    auto rank_one = rank_zero;
    rank_one.dp_rank = 1;
    ASSERT_EQ(Register(rank_zero).status, 200);
    ASSERT_EQ(Register(rank_one).status, 200);

    Json::Value registered = QueryEmpty(rank_zero);
    EXPECT_TRUE(registered["instances"]["instance-1"]["dp"].isMember("0"));
    EXPECT_TRUE(registered["instances"]["instance-1"]["dp"].isMember("1"));

    ASSERT_EQ(Unregister("instance-1", 0).status, 200);
    Json::Value partial = QueryEmpty(rank_zero);
    EXPECT_FALSE(partial["instances"]["instance-1"]["dp"].isMember("0"));
    EXPECT_TRUE(partial["instances"]["instance-1"]["dp"].isMember("1"));

    ASSERT_EQ(Unregister("instance-1", 1).status, 200);
    EXPECT_TRUE(QueryEmpty(rank_zero)["instances"].empty());
}

TEST_F(RegistrationHttpTest, RejectsProfileConflictAndMalformedGroup) {
    auto service = VllmService();
    ASSERT_EQ(Register(service).status, 200);

    auto conflict = service;
    conflict.hash_profile.root_digest = std::string(kOtherRootDigest);
    EXPECT_EQ(Register(conflict).status, 400);
    const auto view = manager_->GetIndexer()->GetGlobalView();
    ASSERT_EQ(view.contexts.size(), 1u);
    EXPECT_EQ(view.contexts[0].profile, TestProfile());

    Json::Value invalid = ServiceJson(VllmService("bad-group"));
    invalid["cache_group"] = 1;
    EXPECT_EQ(HttpPostJson(port_, "/register", JsonDocument(invalid)).status,
              400);
    EXPECT_EQ(manager_->GetIndexer()->GetGlobalView().context_count, 1);

    invalid = ServiceJson(VllmService("mixed-group"));
    invalid["cache_group"] = Json::Value(Json::arrayValue);
    invalid["cache_group"].append(0);
    invalid["cache_group"].append(1);
    EXPECT_EQ(HttpPostJson(port_, "/register", JsonDocument(invalid)).status,
              400);
    EXPECT_EQ(manager_->GetIndexer()->GetGlobalView().context_count, 1);

    invalid = ServiceJson(VllmService("bad-profile"));
    invalid["hash_profile"]["root_digest"] = "NOT-A-DIGEST";
    EXPECT_EQ(HttpPostJson(port_, "/register", JsonDocument(invalid)).status,
              400);
    EXPECT_EQ(manager_->GetIndexer()->GetGlobalView().context_count, 1);
}

TEST_F(RegistrationHttpTest, MooncakeSubscriptionIsNotQueryInstance) {
    auto engine = VllmService("engine");
    ASSERT_EQ(Register(engine).status, 200);
    ASSERT_EQ(Register(MooncakeService(engine)).status, 200);

    const Json::Value response = QueryEmpty(engine);
    ASSERT_EQ(response["instances"].getMemberNames(),
              (std::vector<std::string>{"engine"}));
}

TEST_F(RegistrationHttpTest, RejectsConflictingMooncakeProfileBeforeStart) {
    auto engine = VllmService("engine");
    ASSERT_EQ(Register(engine).status, 200);

    auto pool = MooncakeService(engine);
    pool.hash_profile.root_digest = std::string(kOtherRootDigest);
    EXPECT_EQ(Register(pool).status, 400);
    EXPECT_EQ(EventManagerTestPeer::SubscriberCount(*manager_), 1u);

    const auto view = manager_->GetIndexer()->GetGlobalView();
    ASSERT_EQ(view.contexts.size(), 1u);
    EXPECT_EQ(view.contexts[0].profile, TestProfile());
}

TEST(KVEventHandlerTest, RejectsBlockAndRankMismatchWithoutMutation) {
    EventManager manager({}, 0);
    const auto service = VllmService("engine", "default", 2, 4);
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(service)).error.empty());
    KVEventHandler handler(&manager, service);

    BlockStoredEvent event;
    event.block_size = 8;
    event.block_hashes = {100};
    event.medium = "GPU";
    EXPECT_NE(KVEventHandlerTestPeer::BlockStored(handler, event, 2), "");
    event.block_size = 4;
    EXPECT_NE(KVEventHandlerTestPeer::BlockStored(handler, event, 1), "");
    ASSERT_EQ(manager.GetIndexer()->GetGlobalView().contexts.size(), 1u);
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts[0].prefix_count,
              0u);
}

TEST(KVEventHandlerTest, VllmUsesProjectedGpuHashesAndExactOwnerRemoval) {
    EventManager manager({}, 0);
    const auto service = VllmService("engine", "default", 0, 4);
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(service)).error.empty());
    KVEventHandler handler(&manager, service);
    const auto tokens = Sequence(1, 4);
    const auto prefixes =
        ProjectedFor(ContextFor(service), TestProfile(), tokens);
    ASSERT_EQ(prefixes.size(), 1u);

    BlockStoredEvent stored;
    stored.block_size = 4;
    stored.block_hashes = {prefixes[0].value};
    stored.medium = "gPu";
    ASSERT_TRUE(
        KVEventHandlerTestPeer::BlockStored(handler, stored, 0).empty());
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(service), tokens)
                  .at("engine")
                  .gpu,
              4);

    BlockRemovedEvent removed;
    removed.block_hashes = stored.block_hashes;
    removed.medium = "GPU";
    ASSERT_TRUE(
        handler.HandleEvent(conductor::zmq::KVEvent(removed), 0).empty());
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(service), tokens)
                  .at("engine")
                  .gpu,
              0);

    stored.medium = "cpu";
    EXPECT_TRUE(
        KVEventHandlerTestPeer::BlockStored(handler, stored, 0).empty());
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts[0].prefix_count,
              0u);
}

TEST(KVEventHandlerTest, MooncakeSharedMutationRequiresExactProfile) {
    EventManager manager({}, 0);
    const auto engine = VllmService("engine", "default", 0, 4);
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(engine)).error.empty());
    const auto tokens = Sequence(1, 4);
    const auto prefixes =
        ProjectedFor(ContextFor(engine), TestProfile(), tokens);
    ASSERT_EQ(prefixes.size(), 1u);

    auto pool = MooncakeService(engine);
    KVEventHandler pool_handler(&manager, pool);
    BlockStoredEvent stored;
    stored.block_size = 4;
    stored.block_hashes = {prefixes[0].value};
    stored.medium = "CPU";
    ASSERT_TRUE(
        KVEventHandlerTestPeer::BlockStored(pool_handler, stored, -1).empty());
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(engine), tokens)
                  .at("engine")
                  .cpu,
              4);

    auto conflicting_pool = pool;
    conflicting_pool.hash_profile.root_digest = std::string(kOtherRootDigest);
    KVEventHandler conflicting_handler(&manager, conflicting_pool);
    stored.medium = "DISK";
    EXPECT_NE(
        KVEventHandlerTestPeer::BlockStored(conflicting_handler, stored, -1),
        "");
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(engine), tokens)
                  .at("engine")
                  .disk,
              0);
}

class ConfigEnvGuard {
   public:
    ConfigEnvGuard() { unsetenv("CONDUCTOR_CONFIG_PATH"); }
    ~ConfigEnvGuard() { unsetenv("CONDUCTOR_CONFIG_PATH"); }
    void SetPath(const std::string& path) {
        setenv("CONDUCTOR_CONFIG_PATH", path.c_str(), 1);
    }
};

TEST(ParseConfig, MissingFileReturnsEmptyList) {
    ConfigEnvGuard guard;
    guard.SetPath("/nonexistent/conductor_config.json");
    int port = 13333;
    const auto services = conductor::kvevent::ParseConfig(&port);
    EXPECT_TRUE(services.empty());
    EXPECT_EQ(port, 13333);
}

TEST(ParseConfig, LoadsProfilesAndSkipsInvalidEntries) {
    ConfigEnvGuard guard;
    const std::string path = ::testing::TempDir() + "conductor_cfg_test.json";
    {
        std::ofstream out(path);
        out << R"({
          "http_server_port": 14444,
          "kvevent_instance": {
            "inst-vllm": {"endpoint": "tcp://127.0.0.1:5557",
                           "replay_endpoint": "tcp://127.0.0.1:5558",
                           "type": "vLLM", "modelname": "m1",
                           "block_size": 16, "dp_rank": 2,
                           "tenant_id": "", "cache_group": 0,
                           "hash_profile": {
                             "strategy": "vllm_v1",
                             "algorithm": "sha256_cbor",
                             "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
                             "index_projection": "low64_be"}},
            "inst-moon": {"endpoint": "tcp://127.0.0.1:6557",
                           "type": "Mooncake", "modelname": "m1",
                           "block_size": 16, "dp_rank": 0,
                           "hash_profile": {
                             "strategy": "vllm_v1",
                             "algorithm": "sha256_cbor",
                             "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
                             "index_projection": "low64_be"}},
            "bad-group": {"endpoint": "tcp://127.0.0.1:7557",
                           "type": "vLLM", "modelname": "m1",
                           "block_size": 16, "dp_rank": 0,
                           "cache_group": 1},
            "bad-tenant": {"endpoint": "tcp://127.0.0.1:7558",
                           "type": "vLLM", "modelname": "m1",
                           "block_size": 16, "dp_rank": 0,
                           "tenant_id": 7},
            "bad-lora": {"endpoint": "tcp://127.0.0.1:7559",
                           "type": "vLLM", "modelname": "m1",
                           "block_size": 16, "dp_rank": 0,
                           "lora_name": null},
            "bad-type": {"endpoint": "tcp://127.0.0.1:8557",
                          "type": "SGLang", "modelname": "m1"}
          }
        })";
    }
    guard.SetPath(path);

    int port = 13333;
    auto services = conductor::kvevent::ParseConfig(&port);
    EXPECT_EQ(port, 14444);
    ASSERT_EQ(services.size(), 2u);
    std::sort(services.begin(), services.end(),
              [](const auto& left, const auto& right) {
                  return left.instance_id < right.instance_id;
              });
    EXPECT_EQ(services[0].instance_id, "inst-moon");
    EXPECT_EQ(services[1].instance_id, "inst-vllm");
    EXPECT_EQ(services[1].tenant_id, "default");
    EXPECT_EQ(services[1].dp_rank, 2);
    EXPECT_EQ(services[1].cache_group, std::optional<int64_t>(0));
    EXPECT_EQ(services[1].hash_profile, TestProfileConfig());
    std::remove(path.c_str());
}

TEST(ParseConfig, ExplicitInstanceIdSupportsMultipleStaticRanks) {
    ConfigEnvGuard guard;
    const std::string path =
        ::testing::TempDir() + "conductor_cfg_multi_rank.json";
    {
        std::ofstream out(path);
        out << R"({
          "kvevent_instance": {
            "stream-rank-0": {
              "endpoint": "tcp://127.0.0.1:5557", "type": "vLLM",
              "modelname": "m1", "instance_id": "engine-a",
              "block_size": 16, "dp_rank": 0,
              "hash_profile": {
                "strategy": "vllm_v1", "algorithm": "sha256_cbor",
                "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
                "index_projection": "low64_be"}},
            "stream-rank-1": {
              "endpoint": "tcp://127.0.0.1:5558", "type": "vLLM",
              "modelname": "m1", "instance_id": "engine-a",
              "block_size": 16, "dp_rank": 1,
              "hash_profile": {
                "strategy": "vllm_v1", "algorithm": "sha256_cbor",
                "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
                "index_projection": "low64_be"}}
          }
        })";
    }
    guard.SetPath(path);

    int port = 13333;
    auto services = conductor::kvevent::ParseConfig(&port);
    ASSERT_EQ(services.size(), 2u);
    std::sort(services.begin(), services.end(),
              [](const auto& left, const auto& right) {
                  return left.dp_rank < right.dp_rank;
              });
    EXPECT_EQ(services[0].instance_id, "engine-a");
    EXPECT_EQ(services[1].instance_id, "engine-a");
    EXPECT_EQ(services[0].dp_rank, 0);
    EXPECT_EQ(services[1].dp_rank, 1);
    EXPECT_EQ(services[0].tenant_id, "default");
    EXPECT_EQ(services[1].tenant_id, "default");
    std::remove(path.c_str());
}

TEST(ParseConfig, MissingPortFieldZeroesPort) {
    ConfigEnvGuard guard;
    const std::string path = ::testing::TempDir() + "conductor_cfg_np.json";
    {
        std::ofstream out(path);
        out << R"({"kvevent_instance": {}})";
    }
    guard.SetPath(path);
    int port = 13333;
    conductor::kvevent::ParseConfig(&port);
    EXPECT_EQ(port, 0);
    std::remove(path.c_str());
}

TEST(ParseConfigDeathTest, MalformedJsonExits) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ConfigEnvGuard guard;
    const std::string path = ::testing::TempDir() + "conductor_cfg_bad.json";
    {
        std::ofstream out(path);
        out << "{not json";
    }
    guard.SetPath(path);
    int port = 0;
    EXPECT_EXIT(conductor::kvevent::ParseConfig(&port),
                ::testing::ExitedWithCode(1), "");
    std::remove(path.c_str());
}

}  // namespace
