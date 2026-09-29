// Dual-protocol equivalence e2e: one EventManager serving the same
// ConductorService over HTTP+msgpack and coro_rpc. Both channels must agree
// field by field (serialization formats differ, so comparison is semantic,
// not bytewise).

#include <gtest/gtest.h>
#include <json/json.h>
#include <msgpack.hpp>
#include <ylt/coro_http/coro_http_client.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "conductor/client/conductor_client.h"
#include "conductor/common/types.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"
#include "event_manager_test_peer.h"

namespace mooncake::conductor::kvevent {
namespace {

using mooncake::conductor::prefixindex::ContextKey;
using mooncake::conductor::prefixindex::HashBlock;
using mooncake::conductor::prefixindex::HashProfile;
using mooncake::conductor::prefixindex::ProjectedPrefix;
using mooncake::conductor::prefixindex::StorageTier;

// The e2e test pins a fixed RPC port: 0 means "disabled", and a random port
// would not exercise the production-style fixed-port scenario. On conflict,
// StartRPCServer/Setup fail explicitly instead of silently skipping.
constexpr int kRpcPort = 19334;
constexpr int kConcurrencyThreadsPerChannel = 8;
constexpr int kConcurrencyQueriesPerThread = 100;

ContextKey E2EContext() {
    return {.tenant_id = "default",
            .model_name = "e2e-model",
            .lora_name = "",
            .block_size = 16};
}

HashProfile E2EProfile() {
    const common::HashProfileConfig source{
        .strategy = "vllm_v1",
        .algorithm = "sha256_cbor",
        .python_hash_seed = "0",
        .index_projection = "low64_be",
    };
    HashProfile profile;
    EXPECT_EQ(prefixindex::ResolveHashProfile(source, &profile), "");
    return profile;
}

std::vector<int32_t> Sequence(int32_t first, size_t count) {
    std::vector<int32_t> values;
    values.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        values.push_back(first + static_cast<int32_t>(index));
    }
    return values;
}

std::vector<ProjectedPrefix> ProjectedFor(const ContextKey& context,
                                          const HashProfile& profile,
                                          const std::vector<int32_t>& tokens) {
    std::string error;
    auto strategy = prefixindex::CreateHashStrategy(profile, &error);
    EXPECT_NE(strategy, nullptr) << error;
    if (!strategy) {
        return {};
    }
    std::vector<HashBlock> blocks;
    error = strategy->Compute(context, tokens, std::nullopt, &blocks);
    EXPECT_TRUE(error.empty()) << error;
    std::vector<ProjectedPrefix> prefixes;
    prefixes.reserve(blocks.size());
    for (const auto& block : blocks) {
        prefixes.push_back(block.projected);
    }
    return prefixes;
}

// Seed directly through GetIndexer() (the same path QueryHttpTest uses):
// register one engine whose first two blocks land on npu, with all three
// blocks also on cpu_share and disk via the shared pool.
void SeedEngine(EventManager& manager, const ContextKey& context,
                const HashProfile& profile,
                const std::vector<int32_t>& tokens) {
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register({.context = context,
                                .profile = profile,
                                .instance_id = "e2e-engine",
                                .dp_rank = 0,
                                .effective_block_size = context.block_size})
                    .error.empty());
    const auto prefixes = ProjectedFor(context, profile, tokens);
    ASSERT_EQ(prefixes.size(), 3u);
    ASSERT_TRUE(manager.GetIndexer()
                    ->StoreEngine({.context = context,
                                   .prefixes = {prefixes[0], prefixes[1]},
                                   .owner = {.source_stream = "e2e-stream",
                                             .instance_id = "e2e-engine",
                                             .dp_rank = 0},
                                   .effective_block_size = context.block_size})
                    .empty());
    ASSERT_TRUE(manager.GetIndexer()
                    ->StoreShared({.context = context,
                                   .prefixes = prefixes,
                                   .tier = StorageTier::kCpuShare,
                                   .owner = {.source_stream = "e2e-pool",
                                             .backend_id = "cpu-backend",
                                             .object_id = "cpu-object"},
                                   .effective_block_size = context.block_size})
                    .empty());
    ASSERT_TRUE(manager.GetIndexer()
                    ->StoreShared({.context = context,
                                   .prefixes = prefixes,
                                   .tier = StorageTier::kDisk,
                                   .owner = {.source_stream = "e2e-pool",
                                             .backend_id = "disk-backend",
                                             .object_id = "disk-object"},
                                   .effective_block_size = context.block_size})
                    .empty());
}

// The msgpack request body matches ParseQueryMsgpackRequest exactly:
// model (non-empty string), block_size (positive integer), token_ids
// (integer array). When tenant_id is omitted, the parsing layer normalizes
// it to "default", matching the explicit context.tenant_id on the RPC side.
std::string QueryMsgpackBody(const QueryRequest& request,
                             bool include_tenant = false) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(include_tenant ? 4 : 3);
    if (include_tenant) {
        packer.pack("tenant_id");
        packer.pack(request.context.tenant_id);
    }
    packer.pack("model");
    packer.pack(request.context.model_name);
    packer.pack("block_size");
    packer.pack(request.context.block_size);
    packer.pack("token_ids");
    packer.pack_array(static_cast<uint32_t>(request.token_ids.size()));
    for (const int32_t token : request.token_ids) {
        packer.pack(token);
    }
    return std::string(buffer.data(), buffer.size());
}

struct HttpResponse {
    int status = 0;
    std::string body;
};

// POST signature matches HttpPostMsgpack in event_manager_test.cpp;
// resp_body is a string_view into the client's internal buffer, so copy it
// immediately.
HttpResponse HttpPost(coro_http::coro_http_client& client, uint16_t port,
                      const std::string& path, const std::string& body) {
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + path;
    auto result = client.post(url, body, coro_http::req_content_type::none,
                              {{"Content-Type", "application/msgpack"}});
    return {result.status, std::string(result.resp_body)};
}

HttpResponse HttpGet(coro_http::coro_http_client& client, uint16_t port,
                     const std::string& path) {
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + path;
    auto result = client.get(url);
    return {result.status, std::string(result.resp_body)};
}

Json::Value MsgpackToJson(const msgpack::object& object) {
    switch (object.type) {
        case msgpack::type::NIL:
            return Json::Value(Json::nullValue);
        case msgpack::type::BOOLEAN:
            return Json::Value(object.via.boolean);
        case msgpack::type::POSITIVE_INTEGER:
            return Json::Value(Json::Int64(object.via.u64));
        case msgpack::type::NEGATIVE_INTEGER:
            return Json::Value(Json::Int64(object.via.i64));
        case msgpack::type::FLOAT32:
        case msgpack::type::FLOAT64:
            return Json::Value(object.via.f64);
        case msgpack::type::STR:
            return Json::Value(
                std::string(object.via.str.ptr, object.via.str.size));
        case msgpack::type::BIN:
            return Json::Value(
                std::string(object.via.bin.ptr, object.via.bin.size));
        case msgpack::type::ARRAY: {
            Json::Value array(Json::arrayValue);
            for (uint32_t i = 0; i < object.via.array.size; ++i) {
                array.append(MsgpackToJson(object.via.array.ptr[i]));
            }
            return array;
        }
        case msgpack::type::MAP: {
            Json::Value map(Json::objectValue);
            for (uint32_t i = 0; i < object.via.map.size; ++i) {
                const auto& kv = object.via.map.ptr[i];
                std::string key;
                if (kv.key.type == msgpack::type::STR) {
                    key.assign(kv.key.via.str.ptr, kv.key.via.str.size);
                } else {
                    key = "<non-string-key>";
                }
                map[key] = MsgpackToJson(kv.val);
            }
            return map;
        }
        default:
            return Json::Value(Json::nullValue);
    }
}

Json::Value DecodeMsgpackBody(const std::string& body) {
    try {
        const auto handle = msgpack::unpack(body.data(), body.size());
        return MsgpackToJson(handle.get());
    } catch (const std::exception& e) {
        ADD_FAILURE() << "failed to decode msgpack response: " << e.what();
        return Json::Value(Json::nullValue);
    }
}

// Convert the RPC channel's QueryResult into the same JSON shape as the HTTP
// /query response ({"instances": {id: {longest_matched, dp, rank_matches,
// npu, ...}}}) so the two channels can be compared field by field with
// EXPECT_EQ.
Json::Value QueryResultToJson(const QueryResult& result) {
    Json::Value instances(Json::objectValue);
    for (const auto& [instance_id, hit] : result.instances) {
        Json::Value value(Json::objectValue);
        value["longest_matched"] = Json::Int64(hit.longest_match_tokens);
        Json::Value dp(Json::objectValue);
        for (const auto& [rank, tokens] : hit.dp) {
            dp[std::to_string(rank)] = Json::Int64(tokens);
        }
        value["dp"] = dp;
        Json::Value rank_matches(Json::objectValue);
        for (const auto& [rank, match] : hit.rank_matches) {
            Json::Value entry(Json::objectValue);
            entry["npu"] = Json::Int64(match.npu);
            entry["cpu_local"] = Json::Int64(match.cpu_local);
            entry["cpu_share"] = Json::Int64(match.cpu_share);
            entry["disk"] = Json::Int64(match.disk);
            rank_matches[std::to_string(rank)] = entry;
        }
        value["rank_matches"] = rank_matches;
        value["npu"] = Json::Int64(hit.npu);
        value["cpu_local"] = Json::Int64(hit.cpu_local);
        value["cpu_share"] = Json::Int64(hit.cpu_share);
        value["disk"] = Json::Int64(hit.disk);
        instances[instance_id] = value;
    }
    Json::Value root(Json::objectValue);
    root["instances"] = instances;
    return root;
}

// Registration config: the context matches E2EContext() (only identical
// contexts land in the same GlobalView entry); hash_profile sets only the
// recipe fields, with root_digest derived by the server (each channel
// resolves it independently, with identical results). The endpoint points at
// a free port: ZMQ connects lazily, so no real publisher is needed. tenant_id
// is set explicitly rather than relying on the normalization fallback.
common::ServiceConfig E2EService(const std::string& instance_id,
                                 const std::string& endpoint) {
    common::ServiceConfig svc;
    svc.endpoint = endpoint;
    svc.publisher_kind = common::PublisherKind::kVllm;
    svc.model_name = E2EContext().model_name;
    svc.lora_name = E2EContext().lora_name;
    svc.tenant_id = E2EContext().tenant_id;
    svc.instance_id = instance_id;
    svc.block_size = E2EContext().block_size;
    svc.dp_rank = 0;
    svc.hash_profile.strategy = "vllm_v1";
    svc.hash_profile.algorithm = "sha256_cbor";
    svc.hash_profile.python_hash_seed = "0";
    svc.hash_profile.index_projection = "low64_be";
    return svc;
}

// The HTTP /register msgpack field names follow the ParseServiceConfigRequest
// wire contract (modelname/type), which differs from the RPC API's struct
// field names.
std::string RegisterMsgpackBody(const common::ServiceConfig& svc) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(8);
    packer.pack("endpoint");
    packer.pack(svc.endpoint);
    packer.pack("type");
    packer.pack(std::string(common::PublisherKindName(svc.publisher_kind)));
    packer.pack("modelname");
    packer.pack(svc.model_name);
    packer.pack("instance_id");
    packer.pack(svc.instance_id);
    packer.pack("block_size");
    packer.pack(svc.block_size);
    packer.pack("dp_rank");
    packer.pack(svc.dp_rank);
    packer.pack("tenant_id");
    packer.pack(svc.tenant_id);
    packer.pack("hash_profile");
    packer.pack_map(4);
    packer.pack("strategy");
    packer.pack(svc.hash_profile.strategy);
    packer.pack("algorithm");
    packer.pack(svc.hash_profile.algorithm);
    packer.pack("python_hash_seed");
    packer.pack(svc.hash_profile.python_hash_seed);
    packer.pack("index_projection");
    packer.pack(svc.hash_profile.index_projection);
    return std::string(buffer.data(), buffer.size());
}

std::string UnregisterMsgpackBody(const std::string& instance_id,
                                  const std::string& tenant_id, int dp_rank) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(3);
    packer.pack("instance_id");
    packer.pack(instance_id);
    packer.pack("tenant_id");
    packer.pack(tenant_id);
    packer.pack("dp_rank");
    packer.pack(dp_rank);
    return std::string(buffer.data(), buffer.size());
}

// Matches PackHashProfile's 5-key wire shape.
Json::Value HashProfileToJson(const common::ResolvedHashProfile& profile) {
    Json::Value value(Json::objectValue);
    value["strategy"] = profile.strategy;
    value["algorithm"] = profile.algorithm;
    value["python_hash_seed"] = profile.python_hash_seed;
    value["root_digest"] = profile.root_digest;
    value["index_projection"] = profile.index_projection;
    return value;
}

// Matches PackServiceConfig's wire shape (all integers are Json::Int64,
// matching MsgpackToJson's mapping of POSITIVE_INTEGER).
Json::Value ServiceConfigToJson(const common::ServiceConfig& svc) {
    Json::Value value(Json::objectValue);
    value["Endpoint"] = svc.endpoint;
    value["ReplayEndpoint"] = svc.replay_endpoint;
    value["Type"] = std::string(common::PublisherKindName(svc.publisher_kind));
    value["ModelName"] = svc.model_name;
    value["LoraName"] = svc.lora_name;
    value["TenantID"] = svc.tenant_id;
    value["InstanceID"] = svc.instance_id;
    value["BlockSize"] = Json::Int64(svc.block_size);
    value["DPRank"] = Json::Int64(svc.dp_rank);
    if (svc.cache_group.has_value()) {
        value["CacheGroup"] = Json::Int64(*svc.cache_group);
    } else {
        value["CacheGroup"] = Json::Value(Json::nullValue);
    }
    value["HashProfile"] = HashProfileToJson(svc.hash_profile);
    return value;
}

// Convert the RPC ListServices result into the same JSON shape as the HTTP
// /services response ({"count": N, "services": [...]}) so the two channels
// can be compared field by field with EXPECT_EQ.
Json::Value ServicesToJson(const std::vector<common::ServiceConfig>& services) {
    Json::Value root(Json::objectValue);
    root["count"] = Json::Int64(services.size());
    Json::Value array(Json::arrayValue);
    for (const auto& svc : services) {
        array.append(ServiceConfigToJson(svc));
    }
    root["services"] = array;
    return root;
}

// Convert the RPC GetGlobalView result into the same JSON shape as the HTTP
// /global_view response
// ({"context_count", "contexts": [{..., "hash_profile", "instances"}]}).
Json::Value GlobalViewToJson(const prefixindex::GlobalView& view) {
    Json::Value root(Json::objectValue);
    root["context_count"] = Json::Int64(view.context_count);
    Json::Value contexts(Json::arrayValue);
    for (const auto& context_view : view.contexts) {
        Json::Value c(Json::objectValue);
        c["model_name"] = context_view.context.model_name;
        c["lora_name"] = context_view.context.lora_name;
        c["block_size"] = Json::Int64(context_view.context.block_size);
        c["tenant_id"] = context_view.context.tenant_id;
        c["prefix_count"] = Json::Int64(context_view.prefix_count);
        c["hash_profile"] = HashProfileToJson(context_view.profile);
        Json::Value instances(Json::objectValue);
        for (const auto& [instance_id, ranks] : context_view.instance_ranks) {
            Json::Value rank_array(Json::arrayValue);
            for (const int64_t rank : ranks) {
                rank_array.append(Json::Int64(rank));
            }
            instances[instance_id] = rank_array;
        }
        c["instances"] = instances;
        contexts.append(std::move(c));
    }
    root["contexts"] = contexts;
    return root;
}

// The RPC server binds inside async_start, so give the immediate connect a
// short retry window; a real conflict such as an occupied port fails an
// explicit ASSERT once the window expires.
void SetupWithRetry(ConductorClient& client, const std::string& addr) {
    for (int attempt = 0; attempt < 40; ++attempt) {
        if (client.Setup({addr}).has_value()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(client.Setup({addr}).has_value())
        << "cannot connect to RPC server at " << addr;
}

TEST(ClientServerE2ETest, QueryEquivalenceAcrossProtocols) {
    EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ASSERT_NE(http_port, 0);

    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));

    // Send the same request over both channels: the context is
    // field-for-field identical (the HTTP side omits tenant_id, which
    // normalizes to "default").
    QueryRequest request;
    request.context = E2EContext();
    request.token_ids = Sequence(1, 48);
    const std::string http_body = QueryMsgpackBody(request);

    coro_http::coro_http_client http;

    // 1) Empty index: both sides should return {"instances": {}}.
    auto rpc_empty = client.Query(request);
    ASSERT_TRUE(rpc_empty.has_value());
    EXPECT_TRUE(rpc_empty->instances.empty());
    const HttpResponse empty_resp =
        HttpPost(http, http_port, "/query", http_body);
    ASSERT_EQ(empty_resp.status, 200);
    EXPECT_EQ(DecodeMsgpackBody(empty_resp.body),
              QueryResultToJson(*rpc_empty));

    // 2) After seeding: the non-empty results are field-for-field equivalent.
    ASSERT_NO_FATAL_FAILURE(
        SeedEngine(manager, request.context, E2EProfile(), request.token_ids));

    auto rpc_result = client.Query(request);
    ASSERT_TRUE(rpc_result.has_value());
    ASSERT_EQ(rpc_result->instances.size(), 1u);
    const auto& hit = rpc_result->instances.at("e2e-engine");
    EXPECT_EQ(hit.longest_match_tokens, 48);
    EXPECT_EQ(hit.npu, 32);
    EXPECT_EQ(hit.cpu_share, 48);
    EXPECT_EQ(hit.disk, 48);

    const HttpResponse http_resp =
        HttpPost(http, http_port, "/query", http_body);
    ASSERT_EQ(http_resp.status, 200);
    const Json::Value http_json = DecodeMsgpackBody(http_resp.body);
    ASSERT_TRUE(http_json.isMember("instances"));
    EXPECT_EQ(http_json, QueryResultToJson(*rpc_result));

    client.Close();
    manager.Stop();
}

TEST(ClientServerE2ETest, EmptyTenantUsesDefaultAcrossProtocols) {
    EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));
    coro_http::coro_http_client http;

    QueryRequest request;
    request.context = E2EContext();
    request.token_ids = Sequence(1, 48);
    ASSERT_NO_FATAL_FAILURE(
        SeedEngine(manager, request.context, E2EProfile(), request.token_ids));
    for (const std::string tenant : {"", "default", "other-tenant"}) {
        SCOPED_TRACE(tenant);
        request.context.tenant_id = tenant;
        const auto rpc = client.Query(request);
        ASSERT_TRUE(rpc.has_value());
        const auto response = HttpPost(http, http_port, "/query",
                                       QueryMsgpackBody(request, true));
        ASSERT_EQ(response.status, 200);
        EXPECT_EQ(DecodeMsgpackBody(response.body), QueryResultToJson(*rpc));
        if (tenant == "other-tenant") {
            EXPECT_TRUE(rpc->instances.empty());
        } else {
            ASSERT_EQ(rpc->instances.size(), 1u);
            EXPECT_EQ(rpc->instances.at("e2e-engine").longest_match_tokens, 48);
        }
    }

    auto svc = E2EService("empty-tenant", "tcp://127.0.0.1:29421");
    svc.tenant_id.clear();
    ASSERT_TRUE(client.Register(svc).has_value());
    const auto removed = client.Unregister(svc.instance_id, "", 0);
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed->removed_key, "empty-tenant|default|0");
    EXPECT_EQ(
        DecodeMsgpackBody(HttpGet(http, http_port, "/services").body)["count"]
            .asInt64(),
        0);

    ASSERT_TRUE(client.Register(svc).has_value());
    EXPECT_EQ(HttpPost(http, http_port, "/unregister",
                       UnregisterMsgpackBody(svc.instance_id, "", 0))
                  .status,
              200);
    const auto services = client.ListServices();
    ASSERT_TRUE(services.has_value());
    EXPECT_TRUE(services->empty());
    client.Close();
    manager.Stop();
}

TEST(ClientServerE2ETest, InvalidQueriesRejectedAcrossProtocols) {
    EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));
    coro_http::coro_http_client http;

    QueryRequest request;
    request.context = E2EContext();
    request.token_ids = {1, 2, 3};
    for (const auto& context : std::vector<ContextKey>{
             {.model_name = "", .block_size = 16},
             {.model_name = "e2e-model", .block_size = 0},
             {.model_name = "e2e-model", .block_size = -1}}) {
        SCOPED_TRACE(context.block_size);
        request.context = context;
        const auto rpc = client.Query(request);
        ASSERT_FALSE(rpc.has_value());
        EXPECT_EQ(rpc.error(), ErrorCode::INVALID_PARAMS);
        const auto response =
            HttpPost(http, http_port, "/query", QueryMsgpackBody(request));
        EXPECT_EQ(response.status, 400);
    }

    // HTTP permits an empty token list; validation must not reject it.
    request.context = E2EContext();
    request.token_ids.clear();
    const auto rpc = client.Query(request);
    ASSERT_TRUE(rpc.has_value());
    const auto response =
        HttpPost(http, http_port, "/query", QueryMsgpackBody(request));
    EXPECT_EQ(response.status, 200);
    EXPECT_EQ(DecodeMsgpackBody(response.body), QueryResultToJson(*rpc));
    client.Close();
    manager.Stop();
}

TEST(ClientServerE2ETest, ClientBeforeSetupReturnsUnavailable) {
    ConductorClient client;
    auto result = client.ListServices();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::CONDUCTOR_UNAVAILABLE);
}

TEST(ClientServerE2ETest, ClientAgainstDeadServerReturnsRpcFail) {
    ConductorClient client;
    // Connect to a port with no listener: connect fails -> RPC_FAIL.
    auto setup = client.Setup({"127.0.0.1:1", /*connect_timeout_ms=*/200,
                               /*request_timeout_ms=*/200});
    ASSERT_FALSE(setup.has_value());
    EXPECT_EQ(setup.error(), ErrorCode::RPC_FAIL);
}

TEST(ClientServerE2ETest, ConcurrentQueriesAcrossProtocols) {
    EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ASSERT_NE(http_port, 0);

    QueryRequest request;
    request.context = E2EContext();
    request.token_ids = Sequence(1, 48);
    ASSERT_NO_FATAL_FAILURE(
        SeedEngine(manager, request.context, E2EProfile(), request.token_ids));
    const std::string http_body = QueryMsgpackBody(request);

    // ConductorClient serializes calls on an internal mutex, so sharing it
    // across threads is the intended usage.
    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));

    std::atomic<int> rpc_ok{0};
    std::atomic<int> http_ok{0};
    std::vector<std::thread> threads;
    threads.reserve(2 * kConcurrencyThreadsPerChannel);

    for (int t = 0; t < kConcurrencyThreadsPerChannel; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kConcurrencyQueriesPerThread; ++i) {
                auto result = client.Query(request);
                if (result.has_value() && result->instances.size() == 1u) {
                    rpc_ok.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
        threads.emplace_back([&, http_port] {
            coro_http::coro_http_client http;
            for (int i = 0; i < kConcurrencyQueriesPerThread; ++i) {
                const HttpResponse resp =
                    HttpPost(http, http_port, "/query", http_body);
                if (resp.status != 200) {
                    continue;
                }
                try {
                    const auto handle =
                        msgpack::unpack(resp.body.data(), resp.body.size());
                    const msgpack::object root = handle.get();
                    if (root.type == msgpack::type::MAP &&
                        root.via.map.size == 1 &&
                        root.via.map.ptr[0].val.type == msgpack::type::MAP &&
                        root.via.map.ptr[0].val.via.map.size == 1) {
                        http_ok.fetch_add(1, std::memory_order_relaxed);
                    }
                } catch (const std::exception&) {
                    // Decode failures show up as a shortfall in the final
                    // http_ok total assertion.
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(rpc_ok.load(),
              kConcurrencyThreadsPerChannel * kConcurrencyQueriesPerThread);
    EXPECT_EQ(http_ok.load(),
              kConcurrencyThreadsPerChannel * kConcurrencyQueriesPerThread);

    client.Close();
    manager.Stop();
}

TEST(ClientServerE2ETest, RegisterUnregisterEquivalenceAcrossProtocols) {
    EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ASSERT_NE(http_port, 0);

    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));
    coro_http::coro_http_client http;

    // Register the same logical config over both channels (with different
    // instance_id/endpoint).
    const common::ServiceConfig rpc_svc =
        E2EService("instance-rpc", "tcp://127.0.0.1:29401");
    auto rpc_register = client.Register(rpc_svc);
    ASSERT_TRUE(rpc_register.has_value());
    EXPECT_EQ(rpc_register->instance_id, "instance-rpc");
    EXPECT_TRUE(rpc_register->is_new);

    const common::ServiceConfig http_svc =
        E2EService("instance-http", "tcp://127.0.0.1:29402");
    const HttpResponse register_resp =
        HttpPost(http, http_port, "/register", RegisterMsgpackBody(http_svc));
    ASSERT_EQ(register_resp.status, 200);
    const Json::Value register_json = DecodeMsgpackBody(register_resp.body);
    EXPECT_EQ(register_json["status"].asString(), "registered successfully");
    EXPECT_EQ(register_json["instance_id"].asString(), "instance-http");

    // Cross-channel visibility: RPC ListServices sees both instances.
    auto rpc_services = client.ListServices();
    ASSERT_TRUE(rpc_services.has_value());
    EXPECT_EQ(rpc_services->size(), 2u);

    // Unregister the RPC-registered instance over HTTP, then confirm over RPC
    // that only the other one remains.
    const HttpResponse unregister_resp =
        HttpPost(http, http_port, "/unregister",
                 UnregisterMsgpackBody("instance-rpc", "default", 0));
    ASSERT_EQ(unregister_resp.status, 200);
    rpc_services = client.ListServices();
    ASSERT_TRUE(rpc_services.has_value());
    ASSERT_EQ(rpc_services->size(), 1u);
    EXPECT_EQ((*rpc_services)[0].instance_id, "instance-http");

    // Reverse: unregister the HTTP-registered instance over RPC, then confirm
    // via HTTP /services that none remain.
    auto rpc_unregister = client.Unregister("instance-http", "default", 0);
    ASSERT_TRUE(rpc_unregister.has_value());
    const HttpResponse services_resp = HttpGet(http, http_port, "/services");
    ASSERT_EQ(services_resp.status, 200);
    const Json::Value services_json = DecodeMsgpackBody(services_resp.body);
    EXPECT_EQ(services_json["count"].asInt64(), 0);
    EXPECT_TRUE(services_json["services"].empty());

    client.Close();
    manager.Stop();
}

TEST(ClientServerE2ETest, ServicesGlobalViewEquivalenceAcrossProtocols) {
    EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ASSERT_NE(http_port, 0);

    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));
    coro_http::coro_http_client http;

    // Seed and register one service so that both /services and /global_view
    // are non-empty; both land in the same context (E2EContext), so the
    // GlobalView should hold a single entry with two instances.
    const std::vector<int32_t> tokens = Sequence(1, 48);
    ASSERT_NO_FATAL_FAILURE(
        SeedEngine(manager, E2EContext(), E2EProfile(), tokens));
    const common::ServiceConfig svc =
        E2EService("instance-rpc", "tcp://127.0.0.1:29411");
    ASSERT_TRUE(client.Register(svc).has_value());

    auto rpc_services = client.ListServices();
    ASSERT_TRUE(rpc_services.has_value());
    ASSERT_EQ(rpc_services->size(), 1u);
    const HttpResponse services_resp = HttpGet(http, http_port, "/services");
    ASSERT_EQ(services_resp.status, 200);
    EXPECT_EQ(DecodeMsgpackBody(services_resp.body),
              ServicesToJson(*rpc_services));

    auto rpc_view = client.GetGlobalView();
    ASSERT_TRUE(rpc_view.has_value());
    ASSERT_EQ(rpc_view->context_count, 1);
    ASSERT_EQ(rpc_view->contexts.size(), 1u);
    EXPECT_EQ(rpc_view->contexts[0].prefix_count, 3u);
    EXPECT_EQ(rpc_view->contexts[0].instance_ranks.size(), 2u);
    const HttpResponse view_resp = HttpGet(http, http_port, "/global_view");
    ASSERT_EQ(view_resp.status, 200);
    EXPECT_EQ(DecodeMsgpackBody(view_resp.body), GlobalViewToJson(*rpc_view));

    client.Close();
    manager.Stop();
}

TEST(ClientServerE2ETest, UnregisterMissingServiceAcrossProtocols) {
    EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ASSERT_NE(http_port, 0);

    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));

    // RPC: a server-side business error crosses the wire in the expected
    // error state, with the code value unremapped.
    auto rpc_result = client.Unregister("no-such-instance", "default", 0);
    ASSERT_FALSE(rpc_result.has_value());
    EXPECT_EQ(rpc_result.error(), ErrorCode::SERVICE_NOT_FOUND);

    // HTTP: the same business error maps to 404 plus a machine-readable
    // {"error": ...} body.
    coro_http::coro_http_client http;
    const HttpResponse resp =
        HttpPost(http, http_port, "/unregister",
                 UnregisterMsgpackBody("no-such-instance", "default", 0));
    ASSERT_EQ(resp.status, 404);
    const Json::Value body = DecodeMsgpackBody(resp.body);
    ASSERT_TRUE(body.isMember("error"));
    EXPECT_TRUE(body["error"].isString());

    client.Close();
    manager.Stop();
}

}  // namespace
}  // namespace mooncake::conductor::kvevent
