// Dual-protocol equivalence e2e: one EventManager serving the same
// ConductorService over HTTP+msgpack and coro_rpc. Both channels must agree
// field by field (serialization formats differ, so comparison is semantic,
// not bytewise).

#include <gtest/gtest.h>
#include <json/json.h>
#include <msgpack.hpp>
#include <ylt/coro_http/coro_http_client.hpp>

#include <atomic>
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "conductor/client/conductor_client.h"
#include "conductor/common/types.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"
#include "conductor/zmq/transport.h"
#include "event_manager_test_peer.h"
#include "test_fixtures.h"

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
// register one engine whose first two blocks land on device, with all three
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
// device, ...}}}) so the two channels can be compared field by field with
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
            entry["device"] = Json::Int64(match.device);
            entry["cpu_local"] = Json::Int64(match.cpu_local);
            entry["cpu_share"] = Json::Int64(match.cpu_share);
            entry["disk"] = Json::Int64(match.disk);
            rank_matches[std::to_string(rank)] = entry;
        }
        value["rank_matches"] = rank_matches;
        value["device"] = Json::Int64(hit.device);
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
    EXPECT_EQ(hit.device, 32);
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

// A query consumer with a deliberately small, test-only routing policy:
// compare cumulative device/local/shared/disk prefixes, keeping inventory order
// for ties. No production Router policy or model service is required here.
struct MockBackend {
    std::string instance_id;
    int64_t rank;
    std::vector<std::vector<int32_t>> requests;

    void Handle(const QueryRequest& request) {
        requests.push_back(request.token_ids);
    }
};

enum class RouteReason { kCacheHit, kMiss, kQueryFailed };

struct RouteDecision {
    size_t backend = 0;
    RouteReason reason = RouteReason::kMiss;
};

class MockRouter {
   public:
    using Query =
        std::function<std::optional<Json::Value>(const QueryRequest&)>;

    MockRouter(Query query, std::array<MockBackend, 2>& backends)
        : query_(std::move(query)), backends_(backends) {}

    RouteDecision Route(const QueryRequest& request) {
        RouteDecision decision;
        const auto hits = query_(request);
        if (!hits) {
            decision.reason = RouteReason::kQueryFailed;
        } else {
            std::array<int64_t, 4> best{};
            for (size_t index = 0; index < backends_.size(); ++index) {
                const auto& backend = backends_[index];
                const auto& match = (*hits)[backend.instance_id]["rank_matches"]
                                           [std::to_string(backend.rank)];
                if (match.isNull()) continue;
                const std::array<int64_t, 4> score{
                    match["device"].asInt64(), match["cpu_local"].asInt64(),
                    match["cpu_share"].asInt64(), match["disk"].asInt64()};
                if (score > best) {
                    best = score;
                    decision = {index, RouteReason::kCacheHit};
                }
            }
        }
        backends_[decision.backend].Handle(request);
        return decision;
    }

   private:
    Query query_;
    std::array<MockBackend, 2>& backends_;
};

class RouterEventPublisher {
   public:
    RouterEventPublisher() : socket_(context_, ZMQ_XPUB) {
        socket_.set(ZMQ_RCVTIMEO, 5000);
        socket_.set(ZMQ_SNDTIMEO, 5000);
        socket_.bind("tcp://127.0.0.1:*");
    }

    std::string Endpoint() { return socket_.Endpoint(); }

    bool WaitForSubscriber() {
        zmq::detail::Message message;
        const auto size = socket_.recv(message);
        return size && *size > 0 &&
               static_cast<const unsigned char*>(message.data())[0] == 1;
    }

    void Publish(const std::string& payload) {
        uint64_t sequence = ++sequence_;
        unsigned char bytes[8];
        for (int index = 7; index >= 0; --index) {
            bytes[index] = static_cast<unsigned char>(sequence & 0xff);
            sequence >>= 8;
        }
        const std::array<std::string_view, 3> frames{
            "", zmq::detail::Buffer(bytes, sizeof(bytes)), payload};
        ASSERT_TRUE(zmq::detail::SendMultipart(socket_, frames));
    }

    // Preserve upstream event fields while setting the registered DP rank.
    void PublishFixture(const std::string& hex, int64_t rank) {
        std::string payload;
        for (size_t i = 0; i < hex.size(); i += 2) {
            payload.push_back(
                static_cast<char>(std::stoul(hex.substr(i, 2), nullptr, 16)));
        }
        const auto batch = msgpack::unpack(payload.data(), payload.size());
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> packer(&buffer);
        packer.pack_array(3);
        // msgpack-c 6 re-packs an integral double as an integer, which the
        // decoder rejects as a timestamp, so write the float64 encoding.
        PackFloat64(buffer, batch.get().via.array.ptr[0].as<double>());
        packer.pack(batch.get().via.array.ptr[1]);
        packer.pack(rank);
        Publish(std::string(buffer.data(), buffer.size()));
    }

    static void PackFloat64(msgpack::sbuffer& buffer, double value) {
        const auto bits = std::bit_cast<uint64_t>(value);
        char encoded[9] = {static_cast<char>(0xcb)};
        for (int index = 0; index < 8; ++index) {
            encoded[1 + index] = static_cast<char>(bits >> (56 - 8 * index));
        }
        buffer.write(encoded, sizeof(encoded));
    }

   private:
    zmq::detail::Context context_;
    zmq::detail::Socket socket_;
    uint64_t sequence_ = 0;
};

void PublishVllmCpu(RouterEventPublisher& publisher, uint64_t hash,
                    bool removed = false) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(3);
    packer.pack(1.25);
    packer.pack_array(1);
    packer.pack_map(removed ? 3 : 8);
    packer.pack("type");
    packer.pack(removed ? "BlockRemoved" : "BlockStored");
    packer.pack("block_hashes");
    packer.pack(std::vector<uint64_t>{hash});
    packer.pack("medium");
    packer.pack("CPU");
    if (!removed) {
        packer.pack("parent_block_hash");
        packer.pack_nil();
        packer.pack("token_ids");
        packer.pack_nil();
        packer.pack("block_size");
        packer.pack(16);
        packer.pack("lora_id");
        packer.pack_nil();
        packer.pack("lora_name");
        packer.pack_nil();
    }
    packer.pack(0);
    publisher.Publish(std::string(buffer.data(), buffer.size()));
}

void PublishMooncakeCpu(RouterEventPublisher& publisher, uint64_t hash,
                        bool removed = false) {
    constexpr int64_t timestamp = 1700000000123;
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(3);
    packer.pack(timestamp);
    packer.pack_array(1);
    packer.pack_map(removed ? 16 : 18);
    packer.pack("event_type");
    packer.pack(removed ? "removed" : "stored");
    packer.pack("event_id");
    packer.pack(1);
    packer.pack("timestamp");
    packer.pack(timestamp);
    packer.pack("model_name");
    packer.pack(E2EContext().model_name);
    packer.pack("block_size");
    packer.pack(E2EContext().block_size);
    packer.pack("additional_salt");
    packer.pack_nil();
    packer.pack("lora_name");
    packer.pack_nil();
    packer.pack("tenant_id");
    packer.pack(E2EContext().tenant_id);
    packer.pack("backend_id");
    packer.pack("e2e-backend");
    packer.pack("medium");
    packer.pack("CPU");
    packer.pack("dp_rank");
    packer.pack(0);
    packer.pack("group_id");
    packer.pack_nil();
    packer.pack("seq_hashes");
    packer.pack(std::vector<uint64_t>{hash});
    packer.pack("base_block_idx");
    packer.pack_nil();
    packer.pack("object_key");
    packer.pack("e2e-object");
    char low64[17];
    std::snprintf(low64, sizeof(low64), "%016llx",
                  static_cast<unsigned long long>(hash));
    packer.pack("connector_block_hash");
    packer.pack(std::string(48, '0') + low64);
    if (!removed) {
        packer.pack("parent_hash");
        packer.pack_nil();
        packer.pack("token_ids");
        packer.pack_nil();
    }
    packer.pack(0);
    publisher.Publish(std::string(buffer.data(), buffer.size()));
}

enum class QueryProtocol { kHttp, kRpc };

class RouterQueryTest : public ::testing::TestWithParam<QueryProtocol> {
   protected:
    void SetUp() override {
        http_.set_conn_timeout(std::chrono::seconds(1));
        http_.set_req_timeout(std::chrono::seconds(3));
        ASSERT_TRUE(manager_.StartHTTPServer());
        ASSERT_TRUE(manager_.StartRPCServer());
        manager_.Start();
        http_port_ = EventManagerTestPeer::HttpPort(manager_);
        ASSERT_NO_FATAL_FAILURE(
            SetupWithRetry(client_, "127.0.0.1:" + std::to_string(kRpcPort)));
        data_ = test::LoadJsonFixture(
            "sglang_map_event_vectors.json")["integration"];
        request_.context = {.tenant_id = "default",
                            .model_name = "router-model",
                            .block_size = 4};
        for (const auto& token : data_["token_ids"]) {
            request_.token_ids.push_back(token.asInt());
        }
        for (size_t i = 0; i < backends_.size(); ++i) {
            auto service =
                Service(backends_[i].instance_id, publishers_[i].Endpoint(),
                        common::PublisherKind::kSglang, backends_[i].rank);
            ASSERT_NO_FATAL_FAILURE(Register(service));
            ASSERT_TRUE(publishers_[i].WaitForSubscriber());
        }
        ASSERT_NO_FATAL_FAILURE(
            Register(Service("shared-pool", pool_.Endpoint(),
                             common::PublisherKind::kMooncake, 0)));
        ASSERT_TRUE(pool_.WaitForSubscriber());
    }

    void TearDown() override {
        client_.Close();
        manager_.Stop();
    }

    common::ServiceConfig Service(const std::string& id,
                                  const std::string& endpoint,
                                  common::PublisherKind kind, int64_t rank) {
        auto service = E2EService(id, endpoint);
        service.model_name = request_.context.model_name;
        service.block_size = 4;
        service.publisher_kind = kind;
        service.dp_rank = rank;
        service.hash_profile.strategy = "sglang";
        service.hash_profile.algorithm = "sha256_raw";
        service.hash_profile.index_projection = "first64_be";
        return service;
    }

    void Register(const common::ServiceConfig& service) {
        if (GetParam() == QueryProtocol::kRpc) {
            ASSERT_TRUE(client_.Register(service).has_value());
        } else {
            ASSERT_EQ(HttpPost(http_, http_port_, "/register",
                               RegisterMsgpackBody(service))
                          .status,
                      200);
        }
    }

    std::optional<Json::Value> Query(const QueryRequest& request) {
        if (GetParam() == QueryProtocol::kRpc) {
            const auto result = client_.Query(request);
            if (!result) return std::nullopt;
            return QueryResultToJson(*result)["instances"];
        }
        const auto response =
            HttpPost(http_, http_port_, "/query", QueryMsgpackBody(request));
        if (response.status != 200) return std::nullopt;
        return DecodeMsgpackBody(response.body)["instances"];
    }

    Json::Value Expected(std::array<int64_t, 4> a,
                         std::optional<std::array<int64_t, 4>> b) {
        QueryResult result;
        for (size_t index = 0; index < backends_.size(); ++index) {
            if (index == 1 && !b) break;
            const auto values = index == 0 ? a : *b;
            prefixindex::CacheHitResult hit;
            hit.longest_match_tokens = values[3];
            hit.device = values[0];
            hit.cpu_local = values[1];
            hit.cpu_share = values[2];
            hit.disk = values[3];
            hit.dp = {{backends_[index].rank, values[0]}};
            hit.rank_matches = {{backends_[index].rank,
                                 {values[0], values[1], values[2], values[3]}}};
            result.instances.emplace(backends_[index].instance_id, hit);
        }
        return QueryResultToJson(result)["instances"];
    }

    ::testing::AssertionResult WaitFor(const Json::Value& expected) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        std::optional<Json::Value> actual;
        do {
            actual = Query(request_);
            if (actual && *actual == expected)
                return ::testing::AssertionSuccess();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (std::chrono::steady_clock::now() < deadline);
        return ::testing::AssertionFailure()
               << "expected " << expected << ", received "
               << (actual ? actual->toStyledString() : "query failed");
    }

    void ExpectRoute(size_t target, RouteReason reason) {
        const std::array<size_t, 2> before{backends_[0].requests.size(),
                                           backends_[1].requests.size()};
        const auto decision = router_.Route(request_);
        EXPECT_EQ(decision.backend, target);
        EXPECT_EQ(decision.reason, reason);
        for (size_t i = 0; i < backends_.size(); ++i) {
            EXPECT_EQ(backends_[i].requests.size(), before[i] + (i == target));
        }
        ASSERT_FALSE(backends_[target].requests.empty());
        EXPECT_EQ(backends_[target].requests.back(), request_.token_ids);
    }

    void PublishEngine(size_t index, const char* fixture) {
        publishers_[index].PublishFixture(data_[fixture].asString(),
                                          backends_[index].rank);
    }

    void PublishPool(bool clear = false) {
        constexpr int64_t timestamp = 1700000000123;
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> packer(&buffer);
        const auto field = [&](const char* key, const auto& value) {
            packer.pack(key);
            packer.pack(value);
        };
        packer.pack_array(3);
        packer.pack(timestamp);
        packer.pack_array(clear ? 1 : 7);
        uint64_t id = 0;
        for (const auto& [medium, count] :
             std::array<std::pair<const char*, int>, 2>{
                 {{"CPU", 3}, {"DISK", 4}}}) {
            for (int i = 0; i < (clear ? 1 : count); ++i) {
                packer.pack_map(clear ? 11 : 17);
                field("event_type", clear ? "cleared" : "stored");
                field("event_id", ++id);
                field("timestamp", timestamp);
                field("model_name", request_.context.model_name);
                field("block_size", 4);
                field("additional_salt", msgpack::type::nil_t{});
                field("lora_name", msgpack::type::nil_t{});
                field("tenant_id", "default");
                field("backend_id", "router-pool");
                if (clear) {
                    field("medium", msgpack::type::nil_t{});
                } else {
                    field("medium", medium);
                }
                field("dp_rank", 0);
                if (clear) break;
                field("group_id", msgpack::type::nil_t{});
                field("seq_hashes", std::vector<uint64_t>{});
                field("base_block_idx", msgpack::type::nil_t{});
                field("object_key",
                      "router_" + data_["upstream_hex_digests"][i].asString() +
                          "_k");
                field("parent_hash", msgpack::type::nil_t{});
                field("token_ids", msgpack::type::nil_t{});
            }
            if (clear) break;
        }
        packer.pack(0);
        pool_.Publish(std::string(buffer.data(), buffer.size()));
    }

    void Seed() {
        publishers_[0].PublishFixture(
            data_["tier_publishes"][0]["payload_hex"].asString(), 0);
        for (const auto& tier : data_["tier_publishes"]) {
            publishers_[1].PublishFixture(tier["payload_hex"].asString(), 12);
        }
        PublishPool();
        ASSERT_TRUE(WaitFor(Expected({4, 4, 12, 16}, {{4, 8, 12, 16}})));
    }

    std::array<RouterEventPublisher, 2> publishers_;
    RouterEventPublisher pool_;
    EventManager manager_{{}, 0, kRpcPort};
    ConductorClient client_;
    coro_http::coro_http_client http_;
    uint16_t http_port_ = 0;
    Json::Value data_;
    QueryRequest request_;
    std::array<MockBackend, 2> backends_{
        {{"engine-a", 0, {}}, {"engine-b", 12, {}}}};
    MockRouter router_{[this](const auto& request) { return Query(request); },
                       backends_};
};

TEST_P(RouterQueryTest, RoutesUsingFourTierHitsAndRank) {
    ASSERT_NO_FATAL_FAILURE(Seed());
    // Device prefixes tie; only engine-b can reuse the longer local Host
    // prefix.
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(1, RouteReason::kCacheHit));
}

TEST_P(RouterQueryTest, ReroutesAfterRemoveClearAndUnregister) {
    ASSERT_NO_FATAL_FAILURE(Seed());
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(1, RouteReason::kCacheHit));
    PublishEngine(1, "remove_first_block_hex");
    ASSERT_TRUE(WaitFor(Expected({4, 4, 12, 16}, {{0, 8, 12, 16}})));
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(0, RouteReason::kCacheHit));
    PublishEngine(0, "clear_all_hex");
    ASSERT_TRUE(WaitFor(Expected({0, 0, 12, 16}, {{0, 8, 12, 16}})));
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(1, RouteReason::kCacheHit));
    if (GetParam() == QueryProtocol::kRpc) {
        ASSERT_TRUE(client_.Unregister("engine-b", "default", 12).has_value());
    } else {
        ASSERT_EQ(HttpPost(http_, http_port_, "/unregister",
                           UnregisterMsgpackBody("engine-b", "default", 12))
                      .status,
                  200);
    }
    ASSERT_TRUE(WaitFor(Expected({0, 0, 12, 16}, std::nullopt)));
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(0, RouteReason::kCacheHit));
    PublishPool(true);
    ASSERT_TRUE(WaitFor(Expected({0, 0, 0, 0}, std::nullopt)));
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(0, RouteReason::kMiss));
}

TEST_P(RouterQueryTest, FallsBackOnMissAndQueryFailure) {
    ASSERT_TRUE(WaitFor(Expected({0, 0, 0, 0}, {{0, 0, 0, 0}})));
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(0, RouteReason::kMiss));
    ASSERT_NO_FATAL_FAILURE(Seed());
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(1, RouteReason::kCacheHit));
    manager_.Stop();
    EXPECT_FALSE(Query(request_).has_value());
    ASSERT_NO_FATAL_FAILURE(ExpectRoute(0, RouteReason::kQueryFailed));
}

TEST(ClientServerE2ETest, VllmCpuLocalYieldsToMooncakeAcrossPublicQueries) {
    RouterEventPublisher a;
    RouterEventPublisher b;
    RouterEventPublisher pool;
    EventManager manager({}, 0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const uint16_t http_port = EventManagerTestPeer::HttpPort(manager);
    ConductorClient client;
    ASSERT_NO_FATAL_FAILURE(
        SetupWithRetry(client, "127.0.0.1:" + std::to_string(kRpcPort)));
    auto engine_a = E2EService("engine-a", a.Endpoint());
    auto engine_b = E2EService("engine-b", b.Endpoint());
    auto shared_pool = E2EService("shared-pool", pool.Endpoint());
    shared_pool.publisher_kind = common::PublisherKind::kMooncake;
    ASSERT_TRUE(client.Register(engine_a).has_value());
    ASSERT_TRUE(a.WaitForSubscriber());
    ASSERT_TRUE(client.Register(engine_b).has_value());
    ASSERT_TRUE(b.WaitForSubscriber());
    ASSERT_TRUE(client.Register(shared_pool).has_value());
    ASSERT_TRUE(pool.WaitForSubscriber());

    QueryRequest request{.context = E2EContext(), .token_ids = Sequence(1, 16)};
    const auto hashes =
        ProjectedFor(request.context, E2EProfile(), request.token_ids);
    ASSERT_EQ(hashes.size(), 1u);
    coro_http::coro_http_client http;
    auto query =
        [&](const QueryRequest& requested) -> std::optional<Json::Value> {
        const auto rpc = client.Query(requested);
        if (!rpc) return std::nullopt;
        return QueryResultToJson(*rpc)["instances"];
    };
    auto wait_for = [&](int64_t local_b, int64_t shared_a,
                        int64_t shared_b) -> ::testing::AssertionResult {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        Json::Value last_rpc;
        Json::Value last_http;
        do {
            const auto result = query(request);
            if (result) last_rpc = *result;
            const auto response =
                HttpPost(http, http_port, "/query", QueryMsgpackBody(request));
            if (response.status == 200) {
                last_http = DecodeMsgpackBody(response.body)["instances"];
            }
            if (result && last_rpc == last_http &&
                last_rpc["engine-b"]["cpu_local"].asInt64() == local_b &&
                last_rpc["engine-a"]["cpu_share"].asInt64() == shared_a &&
                last_rpc["engine-b"]["cpu_share"].asInt64() == shared_b) {
                return ::testing::AssertionSuccess();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (std::chrono::steady_clock::now() < deadline);
        return ::testing::AssertionFailure()
               << "query state did not converge: rpc=" << last_rpc
               << " http=" << last_http;
    };
    std::array<MockBackend, 2> backends{
        {{"engine-a", 0, {}}, {"engine-b", 0, {}}}};
    MockRouter router(query, backends);

    PublishVllmCpu(b, hashes[0].value);
    ASSERT_TRUE(wait_for(16, 0, 16));
    EXPECT_EQ(router.Route(request).backend, 1u);
    PublishMooncakeCpu(pool, hashes[0].value);
    ASSERT_TRUE(wait_for(0, 16, 16));
    EXPECT_EQ(router.Route(request).backend, 0u);
    PublishMooncakeCpu(pool, hashes[0].value, true);
    ASSERT_TRUE(wait_for(0, 0, 0));
    EXPECT_EQ(router.Route(request).reason, RouteReason::kMiss);
    PublishVllmCpu(b, hashes[0].value);
    ASSERT_TRUE(wait_for(16, 0, 16));
    EXPECT_EQ(router.Route(request).backend, 1u);

    client.Close();
    manager.Stop();
}

INSTANTIATE_TEST_SUITE_P(
    PublicInterfaces, RouterQueryTest,
    ::testing::Values(QueryProtocol::kHttp, QueryProtocol::kRpc),
    [](const ::testing::TestParamInfo<QueryProtocol>& info) {
        return info.param == QueryProtocol::kHttp ? "Http" : "Rpc";
    });

}  // namespace
}  // namespace mooncake::conductor::kvevent
