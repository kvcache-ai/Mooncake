// Tests for EventManager and KVEventHandler: service-key construction,
// initial state, subscribe/unsubscribe behaviour, the guarded services
// append, event handling, and config file loading.

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
#include "event_manager_test_peer.h"
#include "prefix_indexer_test_peer.h"

namespace conductor {
namespace kvevent {

uint16_t EventManagerTestPeer::HttpPort(EventManager& mgr) {
    return mgr.http_server_ ? mgr.http_server_->port() : 0;
}

}  // namespace kvevent
}  // namespace conductor

namespace {

using conductor::common::ServiceConfig;
using conductor::kvevent::EventManager;
using conductor::kvevent::EventManagerTestPeer;
using conductor::kvevent::KVEventHandler;
using conductor::kvevent::KVEventHandlerTestPeer;
using conductor::kvevent::MakeServiceKey;
using conductor::prefixindex::ModelContext;
using conductor::prefixindex::PrefixCacheTableTestPeer;
using conductor::zmq::BlockStoredEvent;

ServiceConfig VllmService(const std::string& instance_id = "instance-1",
                          const std::string& tenant_id = "tenant-1",
                          int dp_rank = 0) {
    ServiceConfig svc;
    svc.endpoint = "tcp://127.0.0.1:5557";
    svc.type = conductor::common::kServiceTypeVLLM;
    svc.model_name = "test-model";
    svc.instance_id = instance_id;
    svc.tenant_id = tenant_id;
    svc.dp_rank = dp_rank;
    svc.block_size = 16;
    return svc;
}

struct HttpResponse {
    int status = 0;
    std::string body;
    std::map<std::string, std::string> headers;
};

std::string LowerAscii(std::string_view value) {
    std::string out(value);
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return out;
}

HttpResponse HttpPostJson(uint16_t port, const std::string& path,
                          const std::string& body) {
    coro_http::coro_http_client client;
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + path;
    auto result = client.post(url, body, coro_http::req_content_type::json);

    HttpResponse response;
    response.status = result.status;
    response.body = std::string(result.resp_body);
    for (const auto& header : result.resp_headers) {
        response.headers[LowerAscii(header.name)] = std::string(header.value);
    }
    return response;
}

bool ParseJsonDocument(const std::string& document, Json::Value* value,
                       std::string* errors) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    return reader->parse(document.data(), document.data() + document.size(),
                         value, errors);
}

// --- makeServiceKey ------------------------------------------------------

TEST(MakeServiceKey, Format) {
    EXPECT_EQ(MakeServiceKey("instance-1", "tenant-1", 0),
              "instance-1|tenant-1|0");
}

// --- constructor state ---------------------------------------------------

TEST(EventManager, InitialState) {
    EventManager mgr({}, 13333);
    EXPECT_NE(mgr.GetIndexer(), nullptr);
    EXPECT_EQ(EventManagerTestPeer::ServicesLen(mgr), 0u);
    EXPECT_FALSE(mgr.IsStopped());
}

// --- subscribeToService --------------------------------------------------

TEST(SubscribeToService, DuplicateReturnsFalse) {
    EventManager mgr({}, 0);
    const auto svc = VllmService();

    // Pre-populate subscribers to simulate an existing subscription so
    // SubscribeToService hits the early-return path.
    EventManagerTestPeer::FakeSubscriber(
        mgr, MakeServiceKey(svc.instance_id, svc.tenant_id, svc.dp_rank));

    const auto [is_new, err] = EventManagerTestPeer::Subscribe(mgr, svc);
    EXPECT_EQ(err, "");
    EXPECT_FALSE(is_new);
}

TEST(SubscribeToService, MissingEndpointErrors) {
    EventManager mgr({}, 0);
    auto svc = VllmService();
    svc.endpoint = "";
    svc.block_size = 0;

    const auto [is_new, err] = EventManagerTestPeer::Subscribe(mgr, svc);
    EXPECT_FALSE(err.empty());
    EXPECT_FALSE(is_new);
}

TEST(SubscribeToService, ManagerStoppedErrorsWithoutCreatingState) {
    EventManager mgr({}, 0);
    mgr.Stop();

    const auto svc = VllmService();
    const auto [is_new, err] = EventManagerTestPeer::Subscribe(mgr, svc);

    EXPECT_FALSE(is_new);
    EXPECT_EQ(err, "manager stopped");
    EXPECT_EQ(EventManagerTestPeer::SubscriberCount(mgr), 0u);
    EXPECT_EQ(EventManagerTestPeer::ActiveConfigCount(mgr), 0u);
    EXPECT_EQ(EventManagerTestPeer::ServicesLen(mgr), 0u);
    EXPECT_FALSE(EventManagerTestPeer::TenantHasInstance(mgr, svc.tenant_id,
                                                         svc.instance_id));
}

// ZMQ connect is async, so a subscription succeeds even against a
// nonexistent endpoint; assert the service-key discrimination directly
// rather than relying on the connect path.
TEST(SubscribeToService, DifferentDPRankIsNewKey) {
    EXPECT_NE(MakeServiceKey("instance-1", "tenant-1", 0),
              MakeServiceKey("instance-1", "tenant-1", 1));
}

TEST(SubscribeToService, DifferentTenantIsNewKey) {
    EXPECT_NE(MakeServiceKey("instance-1", "tenant-a", 0),
              MakeServiceKey("instance-1", "tenant-b", 0));
}

// --- guarded services append (no duplicate on re-register) ---------------

TEST(SubscribeToService, NoServicesDuplicateOnReRegister) {
    EventManager mgr({}, 0);
    const auto svc = VllmService();
    EventManagerTestPeer::FakeSubscriber(
        mgr, MakeServiceKey(svc.instance_id, svc.tenant_id, svc.dp_rank));

    for (int round = 0; round < 2; ++round) {
        const auto [is_new, err] = EventManagerTestPeer::Subscribe(mgr, svc);
        ASSERT_EQ(err, "");
        if (is_new) {
            EventManagerTestPeer::AppendService(mgr, svc);
        }
        EXPECT_EQ(EventManagerTestPeer::ServicesLen(mgr), 0u)
            << "round=" << round;
    }
}

// --- Stop idempotence (spec: 重复停止幂等) --------------------------------

TEST(EventManager, StopIsIdempotent) {
    EventManager mgr({}, 0);
    mgr.Stop();
    mgr.Stop();  // second call must return immediately
    EXPECT_TRUE(mgr.IsStopped());
}

TEST(EventManager, StopWaitsForHttpServerShutdown) {
    auto mgr = std::make_unique<EventManager>(
        std::vector<conductor::common::ServiceConfig>{}, 0);
    ASSERT_TRUE(mgr->StartHTTPServer());
    const uint16_t port = EventManagerTestPeer::HttpPort(*mgr);
    ASSERT_NE(port, 0);

    asio::io_context io_context;
    const asio::ip::tcp::endpoint endpoint(asio::ip::make_address("127.0.0.1"),
                                           port);
    asio::ip::tcp::socket client(io_context);
    asio::error_code ec;
    client.connect(endpoint, ec);
    ASSERT_FALSE(ec) << ec.message();
    const std::string request =
        "GET /services HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: "
        "close\r\n\r\n";
    asio::write(client, asio::buffer(request), ec);
    ASSERT_FALSE(ec) << ec.message();
    std::array<char, 1024> response_buffer{};
    const size_t response_size =
        client.read_some(asio::buffer(response_buffer), ec);
    ASSERT_GT(response_size, 0u);
    const std::string response(response_buffer.data(), response_size);
    EXPECT_NE(response.find(" 200 "), std::string::npos);
    client.close();

    mgr->Stop();
    mgr.reset();

    asio::ip::tcp::socket after_stop(io_context);
    ec.clear();
    after_stop.connect(endpoint, ec);
    EXPECT_TRUE(ec);
}

class QueryHttpTest : public ::testing::Test {
   protected:
    static constexpr int64_t kLastAccessSentinel = -424242;

    void SetUp() override {
        manager_ = std::make_unique<EventManager>(
            std::vector<conductor::common::ServiceConfig>{}, 0);
        ASSERT_TRUE(manager_->StartHTTPServer());
        port_ = EventManagerTestPeer::HttpPort(*manager_);
        ASSERT_NE(port_, 0);

        conductor::common::StoredEvent event;
        event.block_hashes = {100};
        event.block_size = 2;
        event.model_name = "test-model";
        event.instance_id = "instance-1";
        event.token_ids = {std::numeric_limits<int32_t>::min(),
                           std::numeric_limits<int32_t>::max()};
        event.medium = "cpu";
        ASSERT_EQ(manager_->GetIndexer()->ProcessStoreEvent(event, 0), "");

        query_context_.model_name = event.model_name;
        query_context_.block_size = event.block_size;
        query_context_.tenant_id = "default";
        query_context_.instance_id = event.instance_id;
    }

    void TearDown() override {
        if (manager_) {
            manager_->Stop();
        }
    }

    std::string QueryBody(const std::string& token_ids_json) const {
        return R"({"model":"test-model","instance_id":"instance-1","block_size":2,"token_ids":)" +
               token_ids_json + "}";
    }

    HttpResponse Post(const std::string& body) const {
        return HttpPostJson(port_, "/query", body);
    }

    void ExpectJsonResponse(const HttpResponse& response, int status,
                            Json::Value* body) const {
        EXPECT_EQ(response.status, status);
        const auto content_type = response.headers.find("content-type");
        ASSERT_NE(content_type, response.headers.end());
        EXPECT_EQ(content_type->second, "application/json");
        ASSERT_FALSE(response.body.empty());

        std::string errors;
        ASSERT_TRUE(ParseJsonDocument(response.body, body, &errors)) << errors;
        ASSERT_TRUE(body->isObject());
    }

    void ExpectJsonError(const HttpResponse& response, const char* reason,
                         bool expect_field,
                         std::optional<size_t> index = std::nullopt) const {
        Json::Value body;
        ASSERT_NO_FATAL_FAILURE(ExpectJsonResponse(response, 400, &body));
        ASSERT_TRUE(body.isMember("error"));
        ASSERT_TRUE(body["error"].isString());
        EXPECT_FALSE(body["error"].asString().empty());
        ASSERT_TRUE(body.isMember("reason"));
        EXPECT_EQ(body["reason"].asString(), reason);
        if (expect_field) {
            ASSERT_TRUE(body.isMember("field"));
            EXPECT_EQ(body["field"].asString(), "token_ids");
        } else {
            EXPECT_FALSE(body.isMember("field"));
        }
        if (index.has_value()) {
            ASSERT_TRUE(body.isMember("index"));
            ASSERT_TRUE(body["index"].isIntegral());
            EXPECT_EQ(body["index"].asUInt64(), *index);
        } else {
            EXPECT_FALSE(body.isMember("index"));
        }
    }

    std::unique_ptr<EventManager> manager_;
    uint16_t port_ = 0;
    ModelContext query_context_;
};

TEST_F(QueryHttpTest, RejectsInvalidTokensAtomicallyAndRemainsLive) {
    struct Case {
        const char* name;
        std::string body;
        const char* reason;
        bool expect_field;
        std::optional<size_t> index;
    };

    const std::vector<Case> cases = {
        {"malformed json",
         R"({"model":"test-model","instance_id":"instance-1","block_size":2,"token_ids":[-2147483648,2147483647],"bad":})",
         "invalid_json", false, std::nullopt},
        {"trailing garbage",
         R"({"model":"test-model","instance_id":"instance-1","block_size":2,"token_ids":[-2147483648,2147483647]} trailing)",
         "invalid_json", false, std::nullopt},
        {"comment syntax",
         R"({"model":"test-model","instance_id":"instance-1","block_size":2,/* invalid JSON */"token_ids":[-2147483648,2147483647]})",
         "invalid_json", false, std::nullopt},
        {"trailing comma",
         R"({"model":"test-model","instance_id":"instance-1","block_size":2,"token_ids":[-2147483648,2147483647],})",
         "invalid_json", false, std::nullopt},
        {"top-level array", "[]", "invalid_json", false, std::nullopt},
        {"missing token_ids",
         R"({"model":"test-model","instance_id":"instance-1","block_size":2})",
         "missing", true, std::nullopt},
        {"token_ids not array", QueryBody("1"), "not_array", true,
         std::nullopt},
        {"string element", QueryBody(R"(["1"])"), "invalid_type", true, 0},
        {"boolean element", QueryBody("[true]"), "invalid_type", true, 0},
        {"null element", QueryBody("[null]"), "invalid_type", true, 0},
        {"object element", QueryBody("[{}]"), "invalid_type", true, 0},
        {"array element", QueryBody("[[]]"), "invalid_type", true, 0},
        {"decimal real", QueryBody("[1.0]"), "invalid_type", true, 0},
        {"exponent real", QueryBody("[1e0]"), "invalid_type", true, 0},
        {"below int32", QueryBody("[-2147483649]"), "out_of_range", true, 0},
        {"above int32", QueryBody("[2147483648]"), "out_of_range", true, 0},
        {"mixed invalid type", QueryBody("[1,2,false]"), "invalid_type", true,
         2},
        {"mixed out of range", QueryBody("[1,-2,2147483648]"), "out_of_range",
         true, 2},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        PrefixCacheTableTestPeer::SetLastAccess(
            *manager_->GetIndexer(), query_context_, kLastAccessSentinel);

        const auto invalid_response = Post(test_case.body);
        ASSERT_NO_FATAL_FAILURE(
            ExpectJsonError(invalid_response, test_case.reason,
                            test_case.expect_field, test_case.index));
        EXPECT_EQ(PrefixCacheTableTestPeer::GetLastAccess(
                      *manager_->GetIndexer(), query_context_),
                  kLastAccessSentinel);

        Json::Value valid_body;
        const auto valid_response = Post(QueryBody("[]"));
        ASSERT_NO_FATAL_FAILURE(
            ExpectJsonResponse(valid_response, 200, &valid_body));
        EXPECT_NE(PrefixCacheTableTestPeer::GetLastAccess(
                      *manager_->GetIndexer(), query_context_),
                  kLastAccessSentinel);
    }
}

TEST_F(QueryHttpTest, AcceptsEmptyArrayAndFullInt32Range) {
    Json::Value empty_body;
    ASSERT_NO_FATAL_FAILURE(
        ExpectJsonResponse(Post(QueryBody("[]")), 200, &empty_body));
    ASSERT_TRUE(empty_body.isMember("default"));
    ASSERT_TRUE(empty_body["default"].isMember("instance-1"));
    EXPECT_EQ(empty_body["default"]["instance-1"]["longest_matched"].asInt64(),
              0);

    Json::Value boundary_body;
    ASSERT_NO_FATAL_FAILURE(ExpectJsonResponse(
        Post(QueryBody("[-2147483648,2147483647]")), 200, &boundary_body));
    const Json::Value& result = boundary_body["default"]["instance-1"];
    EXPECT_EQ(result["longest_matched"].asInt64(), 2);
    EXPECT_EQ(result["CPU"].asInt64(), 2);
}

// --- KVEventHandler -------------------------------------------------------

TEST(KVEventHandlerTest, BlockSizeMismatchReturnsEmpty) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 64;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 128;  // mismatch: 128 != 64
    event.block_hashes = {100};
    event.token_ids = {1, 2, 3, 4};

    EXPECT_EQ(KVEventHandlerTestPeer::BlockStored(handler, event, 0), "");
    // Discarded: nothing reached the indexer.
    EXPECT_TRUE(mgr.GetIndexer()->GetGlobalView().model_contexts.empty());
}

TEST(KVEventHandlerTest, BlockSizeMatchProcessesEvent) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 4;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 4;  // matches
    event.block_hashes = {100, 200};
    event.token_ids = {1, 2, 3, 4, 5, 6, 7, 8};
    event.parent_block_hash = 0;

    EXPECT_EQ(KVEventHandlerTestPeer::BlockStored(handler, event, 0), "");
    EXPECT_EQ(mgr.GetIndexer()->GetGlobalView().model_contexts.size(), 1u);
}

TEST(KVEventHandlerTest, EmptyBlockHashesWithMismatch) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 64;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 128;  // mismatch
    EXPECT_EQ(KVEventHandlerTestPeer::BlockStored(handler, event, 0), "");
}

TEST(KVEventHandlerTest, HandleEventDispatchesBlockStored) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 64;
    svc.model_name = "test-model";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent event;
    event.block_size = 128;  // mismatch -> discarded, returns ""
    event.block_hashes = {100};
    event.token_ids = {1, 2, 3, 4};

    EXPECT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(event), 0), "");
}

TEST(KVEventHandlerTest, ManagerStoppedReturnsError) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 4;
    KVEventHandler handler(&mgr, svc);
    mgr.Stop();

    BlockStoredEvent event;
    event.block_size = 4;
    event.block_hashes = {100};
    event.token_ids = {1, 2, 3, 4};

    EXPECT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(event), 0),
              "manager stopped");
}

// Spec scenario (bug-for-bug): the handler builds RemovedEvent WITHOUT
// the Medium field even when the zmq event carries one. Verified through
// observable behavior: store GPU + cpu replicas, remove with medium set,
// query still reports GPU (mediumSet not pruned — and it never could be,
// since the handler dropped the information).
TEST(KVEventHandlerTest, RemovedEventDropsMediumBugForBug) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 4;
    svc.model_name = "test-model";
    svc.instance_id = "instance-1";
    KVEventHandler handler(&mgr, svc);

    BlockStoredEvent stored;
    stored.block_size = 4;
    stored.block_hashes = {100};
    stored.token_ids = {1, 2, 3, 4};
    stored.medium = "GPU";
    ASSERT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(stored), 0), "");
    stored.medium = "cpu";
    ASSERT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(stored), 1), "");

    conductor::zmq::BlockRemovedEvent removed;
    removed.block_hashes = {100};
    removed.medium = "GPU";  // carried by the wire event...
    ASSERT_EQ(handler.HandleEvent(conductor::zmq::KVEvent(removed), 0), "");

    conductor::prefixindex::ModelContext ctx;
    ctx.model_name = "test-model";
    ctx.block_size = 4;
    ctx.tenant_id = "default";
    ctx.instance_id = "instance-1";
    const auto result = mgr.GetIndexer()->CacheHitCompute(ctx, {1, 2, 3, 4});
    // ...but dropped by the handler: GPU still reported (dirty read).
    EXPECT_EQ(result.gpu, 4);
    EXPECT_EQ(result.cpu, 4);
}

// --- ParseConfig (config file loading) -----------------------------------

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
    EXPECT_EQ(port, 13333);  // untouched when the file is missing
}

TEST(ParseConfig, LoadsServicesAndSkipsUnknownTypes) {
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
                           "tenant_id": "t1", "additionalsalt": "s1"},
            "inst-moon": {"endpoint": "tcp://127.0.0.1:6557",
                           "type": "Mooncake", "modelname": "m2",
                           "block_size": 32},
            "inst-bad":  {"endpoint": "tcp://127.0.0.1:7557",
                           "type": "SGLang", "modelname": "m3"}
          }
        })";
    }
    guard.SetPath(path);

    int port = 13333;
    auto services = conductor::kvevent::ParseConfig(&port);
    EXPECT_EQ(port, 14444);
    ASSERT_EQ(services.size(), 2u);  // SGLang entry skipped

    std::sort(services.begin(), services.end(),
              [](const auto& a, const auto& b) {
                  return a.instance_id < b.instance_id;
              });
    EXPECT_EQ(services[0].instance_id, "inst-moon");
    EXPECT_EQ(services[0].type, conductor::common::kServiceTypeMooncake);
    EXPECT_EQ(services[1].instance_id, "inst-vllm");
    EXPECT_EQ(services[1].type, conductor::common::kServiceTypeVLLM);
    EXPECT_EQ(services[1].tenant_id, "t1");
    EXPECT_EQ(services[1].block_size, 16);
    EXPECT_EQ(services[1].dp_rank, 2);
    EXPECT_EQ(services[1].additional_salt, "s1");
    std::remove(path.c_str());
}

// BUG (preserved): a parsed file with no http_server_port field sets the
// port to 0 unconditionally, overwriting any prior value.
TEST(ParseConfig, MissingPortFieldZeroesPortBugForBug) {
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

// JSON parse failure exits — verified with a death test.
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
