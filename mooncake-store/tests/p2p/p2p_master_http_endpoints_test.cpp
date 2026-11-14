/**
 * @file p2p_master_http_endpoints_test.cpp
 * @brief Integration tests for the P2P master HTTP endpoints
 *        (GET /health, GET /server_info, GET /get_key_count, GET /get_all_keys,
 *         GET /batch_query_routes, GET /metrics, GET /ha/status, GET
 * /ha/doctor).
 *
 * Brings up an in-process P2PMasterRpcService with the embedded HTTP
 * server bound to a kernel-assigned port (no RPC server)
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <future>
#include <initializer_list>
#include <limits>
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

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"
#include "p2p/ha/p2p_status_http_server.h"
#include "p2p/master/p2p_master.h"
#include "p2p/master/p2p_rpc_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

class P2PMasterHttpEndpointsTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("P2PMasterHttpEndpointsTest");
        FLAGS_logtostderr = 1;

        P2PMasterConfig wms_cfg;
        wms_cfg.metrics.enable_reporting = false;
        wms_cfg.routes.max_clients_per_key = 0;  // no limit for P2P
        wms_cfg.metrics.http_port = 0;

        wrapped_ = std::make_unique<P2PMasterRpcService>(wms_cfg);
        wrapped_->init();
        const uint16_t http_port = wrapped_->GetHttpPort();
        ASSERT_NE(http_port, 0);

        http_base_url_ = "http://127.0.0.1:" + std::to_string(http_port);
        LOG(INFO) << "P2P master HTTP server at " << http_base_url_;
        ASSERT_EQ(HttpGet(http_base_url_ + "/health").status, 200);

        const auto server_info = HttpGet(http_base_url_ + "/server_info");
        ASSERT_EQ(server_info.status, 200);
        EXPECT_NE(server_info.resp_body.find("store_version="),
                  std::string::npos);
        EXPECT_NE(server_info.resp_body.find("architecture=p2p"),
                  std::string::npos);

        // Register one client owning two P2P segments so tests can also
        // exercise keys with replicas on multiple segments.
        P2PSegment segment_a;
        // JSON must preserve both zero and full-width unsigned UUID halves.
        segment_a.id = {0, std::numeric_limits<uint64_t>::max()};
        segment_a.name = "p2p_master_http_segment_a";
        segment_a.size = kSegmentSize;
        segment_a.priority = 0;
        segment_a.memory_type = MemoryType::DRAM;
        P2PSegment segment_b = segment_a;
        segment_b.id = {std::numeric_limits<uint64_t>::max(), 0};
        segment_b.name = "p2p_master_http_segment_b";
        segment_id_a_ = segment_a.id;
        segment_id_b_ = segment_b.id;

        P2PRegisterClientRequest reg_req;
        reg_req.client_id = {std::numeric_limits<uint64_t>::max(),
                             std::numeric_limits<uint64_t>::max()};
        reg_req.ip_address = "127.0.0.1";
        reg_req.rpc_port = 50051;
        reg_req.segments = {segment_a, segment_b};
        client_id_ = reg_req.client_id;

        auto reg_res = wrapped_->RegisterClient(reg_req);
        ASSERT_TRUE(reg_res.has_value())
            << "RegisterClient failed: " << reg_res.error();
        ASSERT_TRUE(
            wrapped_
                ->Heartbeat({.client_id = client_id_,
                             .service_state = P2PClientServiceState::ONLINE})
                .has_value());

        // Tests populate keys themselves; start from an empty keyspace.
        ASSERT_EQ(wrapped_->GetMasterService().GetKeyCount(), 0u);
    }

    static void TearDownTestSuite() {
        wrapped_.reset();
        google::ShutdownGoogleLogging();
    }

    void SetUp() override {
        // Timed doctor tests must not let the shared fixture client expire.
        P2PHeartbeatRequest request;
        request.client_id = client_id_;
        request.service_state = P2PClientServiceState::ONLINE;
        ASSERT_TRUE(wrapped_->Heartbeat(request).has_value());
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

    static HttpResponse HttpPost(const std::string& url) {
        coro_http::coro_http_client client;
        auto resp =
            client.post(url, "", coro_http::req_content_type::octet_stream);
        return {resp.status, std::string(resp.resp_body)};
    }

    static int64_t UnixTimeMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
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
        P2PPublishRouteRequest req;
        req.key = key;
        req.object_size = size;
        req.client_id = client_id_;
        req.segment_id = segment_id;
        auto res = wrapped_->PublishRoute(req);
        ASSERT_TRUE(res.has_value())
            << "PublishRoute failed for " << key << ": " << res.error();
    }

    static void RemoveKey(const std::string& key, const UUID& segment_id) {
        P2PWithdrawRouteRequest req;
        req.key = key;
        req.client_id = client_id_;
        req.segment_id = segment_id;
        auto res = wrapped_->WithdrawRoute(req);
        ASSERT_TRUE(res.has_value())
            << "WithdrawRoute failed for " << key << ": " << res.error();
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

    static Json::Value ParseJson(const std::string& body, const char* what) {
        Json::Value parsed;
        Json::CharReaderBuilder builder;
        std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        std::string error;
        if (!reader->parse(body.data(), body.data() + body.size(), &parsed,
                           &error)) {
            ADD_FAILURE() << "failed to parse " << what << " JSON: " << error
                          << ", body=" << body;
        }
        return parsed;
    }

    static void ExpectUuid(const Json::Value& value, const UUID& expected) {
        ASSERT_TRUE(value.isArray());
        ASSERT_EQ(value.size(), 2u);
        ASSERT_TRUE(value[0].isUInt64());
        ASSERT_TRUE(value[1].isUInt64());
        EXPECT_EQ(value[0].asUInt64(), expected.first);
        EXPECT_EQ(value[1].asUInt64(), expected.second);
    }

    static Json::Value ErrorCodes(std::initializer_list<ErrorCode> codes) {
        Json::Value value(Json::arrayValue);
        for (const auto code : codes) {
            value.append(static_cast<int>(code));
        }
        return value;
    }

    static constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    static std::unique_ptr<P2PMasterRpcService> wrapped_;
    static std::string http_base_url_;
    static UUID client_id_;
    static UUID segment_id_a_;
    static UUID segment_id_b_;
};

namespace {
struct HAStatusResetGuard {
    ~HAStatusResetGuard() { HAStatusManager::instance().Reset(); }
};
}  // namespace

std::unique_ptr<P2PMasterRpcService> P2PMasterHttpEndpointsTest::wrapped_;
std::string P2PMasterHttpEndpointsTest::http_base_url_;
UUID P2PMasterHttpEndpointsTest::client_id_;
UUID P2PMasterHttpEndpointsTest::segment_id_a_;
UUID P2PMasterHttpEndpointsTest::segment_id_b_;

TEST_F(P2PMasterHttpEndpointsTest, HealthEndpointReturnsOk) {
    auto resp = HttpGet(http_base_url_ + "/health");
    ASSERT_EQ(resp.status, 200) << "body=" << resp.resp_body;
    EXPECT_EQ(resp.resp_body, "OK");
}

#ifdef STORE_USE_REDIS
TEST_F(P2PMasterHttpEndpointsTest,
       RunWithHAPublishesLocalIdentityAndDetectsViewMismatch) {
    HAStatusResetGuard reset_guard;
    P2PMasterConfig config;
    config.ha.enabled = true;
    config.ha.election_backend = ElectionBackend::REDIS;
    config.oplog.enabled = true;
    config.cluster_id = "p2p_master_instance_id_startup_test";
    config.rpc.address = "127.0.0.1";
    config.rpc.port = 0;
    config.rpc.thread_num = 1;
    config.metrics.http_port = 0;
    config.redis.endpoint = "127.0.0.1:1";

    // Use the supervisor-facing Run path; the closed endpoint makes Redis
    // connect fail after RunWithHA has initialized status.
    P2PMaster master(config);
    EXPECT_EQ(master.Run(), -1);

    auto& status = HAStatusManager::instance();
    const auto instance_id = status.Snapshot().instance_id;
    EXPECT_FALSE(instance_id.empty());

    constexpr std::string_view kLocalEndpoint = "127.0.0.1:50051";
    status.Configure(/*ha_enabled=*/true, config.cluster_id,
                     std::string(kLocalEndpoint));
    status.SetRoleState("primary", "RUNNING");
    status.SetMasterView(std::string(kLocalEndpoint), /*epoch=*/7,
                         /*timestamp_ms=*/123456789, /*ttl_sec=*/4,
                         /*instance_id=*/"another-instance");
    const auto report = status.GetDoctorReport();
    const auto issue = std::find_if(
        report.issues.begin(), report.issues.end(),
        [](const auto& issue) { return issue.code == "MASTER_VIEW_MISMATCH"; });
    ASSERT_NE(issue, report.issues.end());
    EXPECT_NE(issue->message.find(instance_id), std::string::npos);
    EXPECT_NE(issue->message.find("another-instance"), std::string::npos);
}
#endif  // STORE_USE_REDIS

TEST_F(P2PMasterHttpEndpointsTest, StepdownEndpointInvokesCallback) {
    HAStatusResetGuard reset_guard;
    std::atomic<uint32_t> cooldown_sec{0};
    std::atomic<uint32_t> drain_timeout_ms{0};
    P2PMasterConfig config;
    config.ha.enabled = true;
    config.metrics.enable_reporting = false;
    config.metrics.http_port = 0;
    P2PMasterRpcService service(config);
    HAStatusManager::instance().SetRoleState("primary", "RUNNING");
    std::promise<P2PMasterRpcService::StepdownRequest> accepted_request;
    auto accepted = accepted_request.get_future();
    service.SetStepdownCallback(
        [&](const P2PMasterRpcService::StepdownRequest& request) {
            cooldown_sec.store(request.cooldown_sec);
            drain_timeout_ms.store(request.drain_timeout_ms);
            accepted_request.set_value(request);
        });
    service.init();

    const auto before_ms = UnixTimeMs();
    const auto steady_before = std::chrono::steady_clock::now();
    const auto response =
        HttpPost("http://127.0.0.1:" + std::to_string(service.GetHttpPort()) +
                 "/ha/stepdown");
    const auto steady_after = std::chrono::steady_clock::now();
    const auto after_ms = UnixTimeMs();
    ASSERT_EQ(response.status, 200) << "body=" << response.resp_body;
    const auto body = ParseJson(response.resp_body, "stepdown");
    EXPECT_TRUE(body["success"].asBool());
    EXPECT_TRUE(body["already_requested"].isBool());
    EXPECT_FALSE(body["already_requested"].asBool());
    EXPECT_EQ(body["cooldown_sec"].asUInt(), 30u);
    EXPECT_EQ(cooldown_sec.load(), 30u);
    EXPECT_EQ(body["drain_timeout_ms"].asUInt(), 3000u);
    EXPECT_EQ(drain_timeout_ms.load(), 3000u);
    ASSERT_TRUE(body["election_paused_until_ms"].isInt64());
    const auto deadline_ms = body["election_paused_until_ms"].asInt64();
    EXPECT_GE(deadline_ms, before_ms + 30000);
    EXPECT_LE(deadline_ms, after_ms + 30000);
    ASSERT_EQ(accepted.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    const auto request = accepted.get();
    EXPECT_EQ(request.election_paused_until_ms, deadline_ms);
    EXPECT_GE(request.election_paused_until,
              steady_before + std::chrono::seconds(30));
    EXPECT_LE(request.election_paused_until,
              steady_after + std::chrono::seconds(30));
}

TEST_F(P2PMasterHttpEndpointsTest, StepdownEndpointValidatesCooldown) {
    HAStatusResetGuard reset_guard;
    std::atomic<uint32_t> callback_count{0};
    std::atomic<uint32_t> cooldown_sec{0};
    P2PMasterConfig config;
    config.ha.enabled = true;
    config.metrics.enable_reporting = false;
    config.metrics.http_port = 0;
    P2PMasterRpcService service(config);
    HAStatusManager::instance().SetRoleState("primary", "RUNNING");
    service.SetStepdownCallback(
        [&](const P2PMasterRpcService::StepdownRequest& request) {
            cooldown_sec.store(request.cooldown_sec);
            callback_count.fetch_add(1);
        });
    service.init();
    const std::string url =
        "http://127.0.0.1:" + std::to_string(service.GetHttpPort()) +
        "/ha/stepdown?cooldown_sec=";
    for (const auto* value : {"-1", "abc", "1x", "1.5", "3601", "4294967296"}) {
        const auto response = HttpPost(url + value);
        EXPECT_EQ(response.status, 400) << value << ": " << response.resp_body;
        EXPECT_EQ(callback_count.load(), 0u);
    }
    // Rejected parameters must not latch the one-shot stepdown flag.
    const auto accepted = HttpPost(url + "7");
    ASSERT_EQ(accepted.status, 200) << accepted.resp_body;
    EXPECT_EQ(
        ParseJson(accepted.resp_body, "stepdown")["cooldown_sec"].asUInt(), 7u);
    EXPECT_EQ(cooldown_sec.load(), 7u);
    EXPECT_EQ(callback_count.load(), 1u);
    const auto deadline_ms =
        ParseJson(accepted.resp_body, "stepdown")["election_paused_until_ms"]
            .asInt64();
    // A later retry must not refresh the deadline, even with the same cooldown.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto retry = HttpPost(url + "7");
    ASSERT_EQ(retry.status, 200) << retry.resp_body;
    EXPECT_EQ(ParseJson(retry.resp_body, "stepdown")["election_paused_until_ms"]
                  .asInt64(),
              deadline_ms);
    // A duplicate request must not overwrite the accepted cooldown.
    const auto duplicate = HttpPost(url + "0");
    ASSERT_EQ(duplicate.status, 200) << duplicate.resp_body;
    const auto duplicate_body = ParseJson(duplicate.resp_body, "stepdown");
    EXPECT_TRUE(duplicate_body["success"].asBool());
    EXPECT_TRUE(duplicate_body["already_requested"].asBool());
    EXPECT_EQ(duplicate_body["cooldown_sec"].asUInt(), 7u);
    EXPECT_EQ(duplicate_body["drain_timeout_ms"].asUInt(), 3000u);
    EXPECT_EQ(duplicate_body["election_paused_until_ms"].asInt64(),
              deadline_ms);
    EXPECT_EQ(cooldown_sec.load(), 7u);
    EXPECT_EQ(callback_count.load(), 1u);
}

TEST_F(P2PMasterHttpEndpointsTest, StepdownEndpointValidatesDrainTimeout) {
    HAStatusResetGuard reset_guard;
    std::atomic<uint32_t> callback_count{0};
    std::atomic<uint32_t> cooldown_sec{0};
    std::atomic<uint32_t> drain_timeout_ms{0};
    P2PMasterConfig config;
    config.ha.enabled = true;
    config.metrics.enable_reporting = false;
    config.metrics.http_port = 0;
    P2PMasterRpcService service(config);
    HAStatusManager::instance().SetRoleState("primary", "RUNNING");
    service.SetStepdownCallback(
        [&](const P2PMasterRpcService::StepdownRequest& request) {
            cooldown_sec.store(request.cooldown_sec);
            drain_timeout_ms.store(request.drain_timeout_ms);
            callback_count.fetch_add(1);
        });
    service.init();
    const std::string url =
        "http://127.0.0.1:" + std::to_string(service.GetHttpPort()) +
        "/ha/stepdown?cooldown_sec=7&drain_timeout_ms=";
    for (const auto* value :
         {"-1", "abc", "1x", "1.5", "30001", "4294967296"}) {
        const auto response = HttpPost(url + value);
        EXPECT_EQ(response.status, 400) << value << ": " << response.resp_body;
        EXPECT_NE(
            ParseJson(response.resp_body, "stepdown")["error"].asString().find(
                "drain_timeout_ms"),
            std::string::npos);
        EXPECT_EQ(callback_count.load(), 0u);
    }
    // Invalid requests must not prevent a later valid request.
    const auto accepted = HttpPost(url + "1234");
    ASSERT_EQ(accepted.status, 200) << accepted.resp_body;
    const auto body = ParseJson(accepted.resp_body, "stepdown");
    EXPECT_EQ(body["cooldown_sec"].asUInt(), 7u);
    EXPECT_EQ(body["drain_timeout_ms"].asUInt(), 1234u);
    EXPECT_EQ(cooldown_sec.load(), 7u);
    EXPECT_EQ(drain_timeout_ms.load(), 1234u);
    EXPECT_EQ(callback_count.load(), 1u);
    const auto duplicate = HttpPost(url + "0");
    ASSERT_EQ(duplicate.status, 200) << duplicate.resp_body;
    const auto duplicate_body = ParseJson(duplicate.resp_body, "stepdown");
    EXPECT_TRUE(duplicate_body["success"].asBool());
    EXPECT_TRUE(duplicate_body["already_requested"].asBool());
    EXPECT_EQ(duplicate_body["cooldown_sec"].asUInt(), 7u);
    EXPECT_EQ(duplicate_body["drain_timeout_ms"].asUInt(), 1234u);
    EXPECT_EQ(drain_timeout_ms.load(), 1234u);
    EXPECT_EQ(callback_count.load(), 1u);
}

TEST_F(P2PMasterHttpEndpointsTest,
       ConcurrentStepdownRequestsKeepFirstParameters) {
    HAStatusResetGuard reset_guard;
    constexpr auto kTimeout = std::chrono::seconds(3);
    constexpr size_t kRequests = 4;
    std::atomic<uint32_t> callback_count{0};
    std::atomic<uint32_t> accepted_cooldown{0};
    std::atomic<uint32_t> accepted_drain{0};
    std::atomic<int64_t> accepted_deadline_ms{0};
    std::promise<void> callback_entered;
    auto entered = callback_entered.get_future();
    std::promise<void> release_callback;
    auto released = release_callback.get_future().share();
    std::promise<void> start_requests;
    auto start = start_requests.get_future().share();
    P2PMasterConfig config;
    config.ha.enabled = true;
    config.metrics.enable_reporting = false;
    config.metrics.http_port = 0;
    P2PMasterRpcService service(config);
    HAStatusManager::instance().SetRoleState("primary", "RUNNING");
    service.SetStepdownCallback(
        [&](const P2PMasterRpcService::StepdownRequest& request) {
            if (callback_count.fetch_add(1) == 0) {
                accepted_cooldown.store(request.cooldown_sec);
                accepted_drain.store(request.drain_timeout_ms);
                accepted_deadline_ms.store(request.election_paused_until_ms);
                callback_entered.set_value();
            }
            released.wait();
        });
    service.init();
    const std::string base_url =
        "http://127.0.0.1:" + std::to_string(service.GetHttpPort());
    const std::string url = base_url + "/ha/stepdown";
    // Establish all four connections before blocking any HTTP worker: one
    // worker also runs the accept loop. Each request reuses its connection.
    std::vector<std::unique_ptr<coro_http::coro_http_client>> clients;
    for (size_t i = 0; i < kRequests; ++i) {
        clients.emplace_back(std::make_unique<coro_http::coro_http_client>());
        ASSERT_EQ(clients.back()->get(base_url + "/health").status, 200);
    }
    std::vector<std::future<HttpResponse>> requests;
    {
        // Unblock the callback before any request future is destroyed, even
        // if an assertion below fails.
        struct ReleaseGatesOnExit {
            std::promise<void>& start;
            std::promise<void>& callback;
            bool started{false};
            void StartRequests() {
                start.set_value();
                started = true;
            }
            ~ReleaseGatesOnExit() {
                if (!started) {
                    start.set_value();
                }
                callback.set_value();
            }
        } release_guard{start_requests, release_callback};
        for (size_t i = 0; i < kRequests; ++i) {
            requests.emplace_back(std::async(std::launch::async, [&, i] {
                start.wait();
                auto response = clients[i]->post(
                    url + "?cooldown_sec=" + std::to_string(7 + i) +
                        "&drain_timeout_ms=" + std::to_string(1234 + i * 1000),
                    "", coro_http::req_content_type::octet_stream);
                return HttpResponse{response.status,
                                    std::string(response.resp_body)};
            }));
        }
        release_guard.StartRequests();
        const auto entered_status = entered.wait_for(kTimeout);
        EXPECT_EQ(entered_status, std::future_status::ready);
        if (entered_status == std::future_status::ready) {
            const size_t first = accepted_cooldown.load() - 7;
            EXPECT_LT(first, kRequests);
            for (size_t i = 0; i < requests.size(); ++i) {
                if (i != first) {
                    // Retries must complete while the first callback is still
                    // waiting for active RPC handlers to stop.
                    EXPECT_EQ(requests[i].wait_for(kTimeout),
                              std::future_status::ready);
                }
            }
            EXPECT_EQ(callback_count.load(), 1u);
        }
    }
    size_t first_responses = 0;
    for (auto& request : requests) {
        const auto response = request.get();
        EXPECT_EQ(response.status, 200) << response.resp_body;
        const auto body = ParseJson(response.resp_body, "stepdown");
        EXPECT_TRUE(body["success"].asBool());
        EXPECT_TRUE(body["already_requested"].isBool());
        if (!body["already_requested"].asBool()) {
            ++first_responses;
        }
        EXPECT_EQ(body["cooldown_sec"].asUInt(), accepted_cooldown.load());
        EXPECT_EQ(body["drain_timeout_ms"].asUInt(), accepted_drain.load());
        EXPECT_TRUE(body["election_paused_until_ms"].isInt64());
        EXPECT_GT(accepted_deadline_ms.load(), 0);
        EXPECT_EQ(body["election_paused_until_ms"].asInt64(),
                  accepted_deadline_ms.load());
    }
    EXPECT_EQ(first_responses, 1u);
    EXPECT_EQ(callback_count.load(), 1u);
    // A cached request does not bypass parameter or role validation.
    EXPECT_EQ(HttpPost(url + "?drain_timeout_ms=30001").status, 400);
    HAStatusManager::instance().SetRoleState("standby", "STEPPED_DOWN");
    EXPECT_EQ(HttpPost(url).status, 503);
    EXPECT_EQ(callback_count.load(), 1u);
}

TEST_F(P2PMasterHttpEndpointsTest, StepdownEndpointAcceptsParameterBounds) {
    HAStatusResetGuard reset_guard;
    for (const uint32_t seconds : {0u, 3600u}) {
        std::atomic<uint32_t> cooldown_sec{1};
        std::atomic<uint32_t> drain_timeout_ms{1};
        const uint32_t timeout_ms = seconds == 0 ? 0 : 30000;
        P2PMasterConfig config;
        config.ha.enabled = true;
        config.metrics.enable_reporting = false;
        config.metrics.http_port = 0;
        P2PMasterRpcService service(config);
        HAStatusManager::instance().SetRoleState("primary", "RUNNING");
        service.SetStepdownCallback(
            [&](const P2PMasterRpcService::StepdownRequest& request) {
                cooldown_sec.store(request.cooldown_sec);
                drain_timeout_ms.store(request.drain_timeout_ms);
            });
        service.init();
        const auto before_ms = UnixTimeMs();
        const auto response = HttpPost(
            "http://127.0.0.1:" + std::to_string(service.GetHttpPort()) +
            "/ha/stepdown?cooldown_sec=" + std::to_string(seconds) +
            "&drain_timeout_ms=" + std::to_string(timeout_ms));
        const auto after_ms = UnixTimeMs();
        ASSERT_EQ(response.status, 200) << response.resp_body;
        EXPECT_EQ(cooldown_sec.load(), seconds);
        EXPECT_EQ(
            ParseJson(response.resp_body, "stepdown")["cooldown_sec"].asUInt(),
            seconds);
        EXPECT_EQ(drain_timeout_ms.load(), timeout_ms);
        EXPECT_EQ(ParseJson(response.resp_body, "stepdown")["drain_timeout_ms"]
                      .asUInt(),
                  timeout_ms);
        const auto body = ParseJson(response.resp_body, "stepdown");
        ASSERT_TRUE(body["election_paused_until_ms"].isInt64());
        const auto deadline_ms = body["election_paused_until_ms"].asInt64();
        EXPECT_GE(deadline_ms,
                  before_ms + static_cast<int64_t>(seconds) * 1000);
        EXPECT_LE(deadline_ms, after_ms + static_cast<int64_t>(seconds) * 1000);
    }
}

TEST_F(P2PMasterHttpEndpointsTest, StepdownEndpointRequiresRunningPrimary) {
    HAStatusResetGuard reset_guard;
    std::atomic<uint32_t> callback_count{0};
    P2PMasterConfig config;
    config.ha.enabled = true;
    config.metrics.enable_reporting = false;
    config.metrics.http_port = 0;
    P2PMasterRpcService service(config);
    service.SetStepdownCallback(
        [&](const P2PMasterRpcService::StepdownRequest&) {
            callback_count.fetch_add(1);
        });
    service.init();
    const std::string url =
        "http://127.0.0.1:" + std::to_string(service.GetHttpPort()) +
        "/ha/stepdown";
    auto& status = HAStatusManager::instance();
    struct RejectedState {
        bool ha_enabled;
        const char* role;
        const char* state;
    };
    const RejectedState rejected_states[] = {
        {false, "primary", "RUNNING"},
        {true, "standby", "RUNNING"},
        {true, "standby", "WATCHING"},
        {true, "promoting", "PROMOTING"},
        // The active HTTP server is already listening while metadata recovery
        // runs with primary/STARTING, before business RPCs start.
        {true, "primary", "STARTING"},
        {true, "primary", "STOPPED"},
        {true, "standby", "STEPPED_DOWN"},
    };
    for (const auto& rejected : rejected_states) {
        SCOPED_TRACE(::testing::Message() << "ha=" << rejected.ha_enabled
                                          << ", role=" << rejected.role
                                          << ", state=" << rejected.state);
        status.Configure(rejected.ha_enabled, config.cluster_id, "127.0.0.1:0");
        status.SetRoleState(rejected.role, rejected.state);
        const auto response = HttpPost(url);
        EXPECT_EQ(response.status, 503) << response.resp_body;
        const auto body = ParseJson(response.resp_body, "stepdown");
        EXPECT_FALSE(body["success"].asBool());
        EXPECT_NE(body["error"].asString().find("primary/RUNNING"),
                  std::string::npos);
        EXPECT_EQ(callback_count.load(), 0u);
    }
    // Rejected requests must not latch the stepdown request: becoming ready
    // should allow a subsequent request to reach the callback exactly once.
    status.SetRoleState("primary", "RUNNING");
    const auto response = HttpPost(url);
    ASSERT_EQ(response.status, 200) << response.resp_body;
    EXPECT_TRUE(ParseJson(response.resp_body, "stepdown")["success"].asBool());
    EXPECT_EQ(callback_count.load(), 1u);
}

TEST_F(P2PMasterHttpEndpointsTest, HAStatusEndpointReturnsCurrentState) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Configure(/*ha_enabled=*/true, "test_cluster", "127.0.0.1:50051");
    status.SetInstanceId("test-instance");
    status.SetRoleState("primary", "RUNNING");
    status.SetMasterView("127.0.0.1:50051", 7,
                         /*timestamp_ms=*/123456789,
                         /*ttl_sec=*/4,
                         /*instance_id=*/"candidate-1");
    status.SetOpLogProgress(/*applied_sequence_id=*/12,
                            /*latest_sequence_id=*/15,
                            /*trimmed_sequence_id=*/3,
                            /*gc_cursor_sequence_id=*/2);
    status.SetOpLogWriterRunning(true);
    status.SetOpLogWriteProgress(/*committed_sequence_id=*/15, /*pending=*/4,
                                 /*capacity=*/100);
    status.SetOpLogPollSuccess();
    status.SetSnapshotReady(true);
    status.RecordRegistryHeartbeat(ErrorCode::OK, "");

    auto response = HttpGet(http_base_url_ + "/ha/status");
    ASSERT_EQ(response.status, 200) << "body=" << response.resp_body;
    const auto body = ParseJson(response.resp_body, "/ha/status");
    EXPECT_EQ(body["ha_enabled"].asBool(), true);
    EXPECT_EQ(body["cluster_id"].asString(), "test_cluster");
    EXPECT_EQ(body["local_endpoint"].asString(), "127.0.0.1:50051");
    EXPECT_EQ(body["instance_id"].asString(), "test-instance");
    EXPECT_EQ(body["role"].asString(), "primary");
    EXPECT_EQ(body["state"].asString(), "RUNNING");
    EXPECT_EQ(body["leader_endpoint"].asString(), "127.0.0.1:50051");
    EXPECT_EQ(body["epoch"].asUInt64(), 7u);
    EXPECT_EQ(body["master_view_address"].asString(), "127.0.0.1:50051");
    EXPECT_EQ(body["master_view_epoch"].asUInt64(), 7u);
    EXPECT_EQ(body["master_view_timestamp_ms"].asInt64(), 123456789);
    EXPECT_EQ(body["master_view_ttl_sec"].asInt64(), 4);
    // A freshly set/refreshed master_view has positive remaining lease; the
    // old fixed-timestamp computation drifted negative under renewals.
    EXPECT_GT(body["master_view_expires_in_ms"].asInt64(), 0);
    EXPECT_EQ(body["master_view_instance_id"].asString(), "candidate-1");
    EXPECT_EQ(body["applied_sequence_id"].asUInt64(), 12u);
    EXPECT_EQ(body["oplog_applied_sequence_id"].asUInt64(), 12u);
    EXPECT_EQ(body["latest_sequence_id"].asUInt64(), 15u);
    EXPECT_EQ(body["oplog_latest_sequence_id"].asUInt64(), 15u);
    EXPECT_TRUE(body["oplog_latest_read_ok"].asBool());
    EXPECT_EQ(body["trimmed_sequence_id"].asUInt64(), 3u);
    EXPECT_EQ(body["oplog_trimmed_sequence_id"].asUInt64(), 3u);
    EXPECT_EQ(body["gc_cursor_sequence_id"].asUInt64(), 2u);
    EXPECT_EQ(body["oplog_gc_cursor_sequence_id"].asUInt64(), 2u);
    EXPECT_EQ(body["oplog_lag"].asUInt64(), 3u);
    EXPECT_EQ(body["oplog_apply_lag"].asUInt64(), 3u);
    EXPECT_EQ(body["oplog_pending_writes"].asUInt64(), 4u);
    EXPECT_EQ(body["oplog_writer_pending_writes"].asUInt64(), 4u);
    EXPECT_EQ(body["oplog_writer_queue_capacity"].asUInt64(), 100u);
    EXPECT_TRUE(body["oplog_writer_running"].asBool());
    EXPECT_EQ(body["oplog_writer_last_committed_sequence_id"].asUInt64(), 15u);
    EXPECT_GT(body["oplog_last_successful_poll_timestamp_ms"].asInt64(), 0);
    EXPECT_GE(body["oplog_poll_success_age_ms"].asInt64(), 0);
    EXPECT_EQ(body["snapshot_ready"].asBool(), true);
    EXPECT_EQ(body["registry_heartbeat_ok"].asBool(), true);
    EXPECT_EQ(body["registry_heartbeat_last_success_timestamp_ms"].asInt64(),
              body["registry_heartbeat_last_success_ms"].asInt64());
    EXPECT_EQ(body["registry_heartbeat_last_failure_timestamp_ms"].asInt64(),
              body["registry_heartbeat_last_failure_ms"].asInt64());
    EXPECT_EQ(body["status_updated_at_ms"].asInt64(),
              body["updated_at_ms"].asInt64());
    EXPECT_GT(body["updated_at_ms"].asInt64(), 0);

    auto metrics_response = HttpGet(http_base_url_ + "/metrics");
    ASSERT_EQ(metrics_response.status, 200)
        << "body=" << metrics_response.resp_body;
    EXPECT_EQ(ParseMetricValue(metrics_response.resp_body, "ha_master_role")
                  .value_or(0),
              5);
    EXPECT_EQ(ParseMetricValue(metrics_response.resp_body, "ha_snapshot_ready")
                  .value_or(0),
              1);
    EXPECT_EQ(
        ParseMetricValue(metrics_response.resp_body, "ha_registry_heartbeat_ok")
            .value_or(0),
        1);
    EXPECT_GT(ParseMetricValue(metrics_response.resp_body,
                               "ha_status_updated_timestamp_ms")
                  .value_or(0),
              0);
}

TEST_F(P2PMasterHttpEndpointsTest,
       HADoctorReportsCurrentIssuesAndIndependentRecovery) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "127.0.0.1:50051");
    status.SetRoleState("standby", "WATCHING");

    const auto doctor = [&] {
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        EXPECT_EQ(response.status, 200) << "body=" << response.resp_body;
        return ParseJson(response.resp_body, "/ha/doctor");
    };

    status.SetDoctorIssue("OPLOG_APPLY_FAILED", "seq=123: invalid payload");
    status.RecordRegistryHeartbeat(ErrorCode::INTERNAL_ERROR, "Redis timeout");
    auto report = doctor();
    EXPECT_EQ(report["health"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 2u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "OPLOG_APPLY_FAILED");
    EXPECT_EQ(report["issues"][0]["message"].asString(),
              "seq=123: invalid payload");
    EXPECT_EQ(report["issues"][0]["category"].asString(), "oplog");
    EXPECT_EQ(report["issues"][0]["component"].asString(), "oplog_applier");
    EXPECT_EQ(report["issues"][0]["severity"].asString(), "error");
    EXPECT_EQ(report["issues"][1]["code"].asString(),
              "REGISTRY_HEARTBEAT_FAILED");
    EXPECT_EQ(report["issues"][1]["category"].asString(), "registry");
    EXPECT_EQ(report["issues"][1]["component"].asString(),
              "registry_heartbeat");
    EXPECT_EQ(report["issues"][1]["severity"].asString(), "error");

    // Repeated failures replace the current message without accumulating.
    status.SetDoctorIssue("OPLOG_APPLY_FAILED", "seq=124: invalid payload");
    status.RecordRegistryHeartbeat(ErrorCode::OK, "");
    report = doctor();
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["message"].asString(),
              "seq=124: invalid payload");
    status.SetDoctorIssue("OPLOG_APPLY_FAILED");
    report = doctor();
    EXPECT_EQ(report["health"].asString(), "ok");
    EXPECT_TRUE(report["issues"].empty());
}

TEST_F(P2PMasterHttpEndpointsTest,
       HADoctorDoesNotTreatGapsOrHistoryAsFailures) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "127.0.0.1:50051");
    status.SetLastError("An earlier snapshot attempt failed");
    status.SetOpLogProgress(200, 1000, 50);
    for (const auto* state : {"SYNCING", "WATCHING", "PROMOTING"}) {
        status.SetRoleState("standby", state);
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        ASSERT_EQ(response.status, 200) << "body=" << response.resp_body;
        auto report = ParseJson(response.resp_body, "/ha/doctor");
        EXPECT_TRUE(report["issues"].empty());
        EXPECT_EQ(report["health"].asString(),
                  std::string(state) == "WATCHING" ? "ok" : "unknown");
    }
}

TEST_F(P2PMasterHttpEndpointsTest, HADoctorReturnsElectionInstability) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "local");
    status.SetRoleState("standby", "WATCHING");
    for (uint64_t epoch = 1; epoch <= 4; ++epoch) {
        status.SetMasterView("leader", epoch);
    }
    auto response = HttpGet(http_base_url_ + "/ha/doctor");
    ASSERT_EQ(response.status, 200) << response.resp_body;
    const auto report = ParseJson(response.resp_body, "/ha/doctor");
    EXPECT_EQ(report["health"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "ELECTION_UNSTABLE");
}

TEST_F(P2PMasterHttpEndpointsTest, HADoctorHandlesLifecycleAndFatalFailure) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    const auto doctor = [&] {
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        EXPECT_EQ(response.status, 200) << "body=" << response.resp_body;
        return ParseJson(response.resp_body, "/ha/doctor");
    };

    status.Reset();
    EXPECT_EQ(doctor()["health"].asString(), "unknown");
    status.Configure(false, "test_cluster", "127.0.0.1:50051");
    EXPECT_EQ(doctor()["health"].asString(), "unknown");
    status.Configure(true, "test_cluster", "127.0.0.1:50051");
    status.SetRoleState("promoting", "FAILED");
    status.SetLastError("Failed to promote: INTERNAL_ERROR");
    auto report = doctor();
    EXPECT_EQ(report["health"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "HA_FAILED");
    EXPECT_EQ(report["issues"][0]["message"].asString(),
              "Failed to promote: INTERNAL_ERROR");
    status.SetDoctorIssue("ELECTION_FAILED", "Redis unavailable");
    status.ClearDoctorIssues();
    status.ClearLastError();
    status.SetRoleState("starting", "INITIALIZING");
    report = doctor();
    EXPECT_EQ(report["health"].asString(), "unknown");
    EXPECT_TRUE(report["issues"].empty());
    status.SetLeader("127.0.0.1:50051", 1);
    status.SetRoleState("primary", "RUNNING");
    EXPECT_EQ(doctor()["health"].asString(), "ok");
}

TEST_F(P2PMasterHttpEndpointsTest,
       HADoctorKeepsIncompleteCatchupUntilRoleEnds) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "test-primary");
    status.SetRoleState("primary", "STARTING");
    status.SetLeader("test-primary", 1);
    status.SetMasterView("test-primary", 1);
    status.SetDoctorIssue("PROMOTION_CATCHUP_INCOMPLETE",
                          "oplog_applied_sequence_id=100, "
                          "oplog_latest_sequence_id=200");
    const auto doctor = [&] {
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        EXPECT_EQ(response.status, 200) << "body=" << response.resp_body;
        return ParseJson(response.resp_body, "/ha/doctor");
    };

    status.SetRoleState("primary", "RUNNING");
    status.SetOpLogProgress(201, 201);
    status.RecordRegistryHeartbeat(ErrorCode::OK, "");
    auto report = doctor();
    EXPECT_EQ(report["health"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(),
              "PROMOTION_CATCHUP_INCOMPLETE");
    EXPECT_EQ(report["issues"][0]["message"].asString(),
              "oplog_applied_sequence_id=100, oplog_latest_sequence_id=200");

    status.SetDoctorIssue("OPLOG_READ_FAILED", "standby read failed");
    status.SetRoleState("standby", "WATCHING");
    report = doctor();
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "OPLOG_READ_FAILED");
    status.ClearDoctorIssues();
    EXPECT_EQ(doctor()["health"].asString(), "ok");
}

TEST_F(P2PMasterHttpEndpointsTest, HADoctorTimesOnlyBusyLatestStalls) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "test-primary", 1);
    status.SetLeader("test-primary", 1);
    status.SetRoleState("primary", "RUNNING");
    // This is the assigned primary watermark, not the writer's committed
    // latest. The stall diagnosis must use the latter.
    status.SetOpLogProgress(999, 999);
    const auto doctor = [&] {
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        EXPECT_EQ(response.status, 200) << response.resp_body;
        return ParseJson(response.resp_body, "/ha/doctor");
    };

    status.SetOpLogWriterRunning(true);
    status.SetOpLogWriteProgress(10, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    // New pending work begins a fresh timer. Idle time is excluded.
    status.SetOpLogWriteProgress(10, 1);
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    // Additional work cannot reset the timer while confirmed latest stalls.
    status.SetOpLogWriteProgress(10, 2);
    auto report = doctor();
    EXPECT_EQ(report["health"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "OPLOG_LATEST_STALLED");
    EXPECT_NE(report["issues"][0]["message"].asString().find(
                  "latest=10, pending=2, stalled_ms="),
              std::string::npos);

    status.Configure(true, "test_cluster", "test-primary", 0);
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    status.Configure(true, "test_cluster", "test-primary", 1);
    EXPECT_EQ(doctor()["health"].asString(), "degraded");

    // Confirmed progress restarts the timer while work remains busy.
    status.SetOpLogWriteProgress(11, 1);
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["health"].asString(), "degraded");

    // Draining work disarms the timer.
    status.SetOpLogWriteProgress(12, 0);
    EXPECT_EQ(doctor()["health"].asString(), "ok");

    // A stopped writer clears cache and rejects late callbacks from the
    // previous worker.
    status.SetOpLogWriteProgress(12, 1);
    status.SetOpLogWriterRunning(false);
    status.SetOpLogWriteProgress(12, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["health"].asString(), "ok");
}

TEST_F(P2PMasterHttpEndpointsTest, HADoctorDetectsCatchupStallAndRecovery) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "test-standby", 10, 1);
    status.SetRoleState("standby", "WATCHING");
    const auto doctor = [&] {
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        EXPECT_EQ(response.status, 200) << response.resp_body;
        return ParseJson(response.resp_body, "/ha/doctor");
    };

    status.SetOpLogProgress(100, 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    status.SetOpLogProgress(100, 200);
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    // More backlog cannot restart the timer.
    status.SetOpLogProgress(100, 300);
    auto report = doctor();
    EXPECT_EQ(report["health"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "OPLOG_CATCHUP_STALLED");
    EXPECT_NE(report["issues"][0]["message"].asString().find(
                  "oplog_applied_sequence_id=100, "
                  "oplog_latest_sequence_id=300, stalled_ms="),
              std::string::npos);
    EXPECT_NE(report["health"].asString(), "ok");

    // A failed latest read retains the last known latest.
    status.SetOpLogProgress(100, std::nullopt);
    EXPECT_EQ(status.Snapshot().latest_sequence_id, 300u);
    EXPECT_EQ(doctor()["health"].asString(), "degraded");
    status.SetDoctorIssue("OPLOG_READ_FAILED", "Redis read failed");
    report = doctor();
    EXPECT_EQ(report["issues"].size(), 2u);

    status.Configure(true, "test_cluster", "test-standby", 10, 0);
    report = doctor();
    EXPECT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(),
              std::string("OPLOG_READ_FAILED"));
    status.Configure(true, "test_cluster", "test-standby", 10, 1);
    status.SetDoctorIssue("OPLOG_READ_FAILED");

    // Applied progress restarts the timer without requiring a new latest.
    status.SetOpLogProgress(101, std::nullopt);
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    status.SetOpLogProgress(101, std::nullopt);
    EXPECT_EQ(doctor()["health"].asString(), "degraded");
    status.SetOpLogProgress(300, 300);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["health"].asString(), "ok");
}

TEST_F(P2PMasterHttpEndpointsTest,
       HADoctorCatchupStallFollowsStandbyLifecycle) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "test-standby", 10, 1);
    const auto doctor = [&] {
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        EXPECT_EQ(response.status, 200) << response.resp_body;
        return ParseJson(response.resp_body, "/ha/doctor");
    };

    status.SetRoleState("standby", "SYNCING");
    status.SetOpLogProgress(10, 20);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(doctor()["issues"].empty());
    status.SetRoleState("standby", "WATCHING");

    // Unknown latest cannot initialize the timer.
    status.SetOpLogProgress(10, std::nullopt);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    status.SetOpLogProgress(10, 20);
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    // No newer sample means the stall is not confirmed yet.
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    status.SetOpLogProgress(10, 20);
    EXPECT_EQ(doctor()["health"].asString(), "degraded");

    status.SetLeader("test-standby", 1);
    status.SetRoleState("primary", "RUNNING");
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    status.SetRoleState("standby", "WATCHING");
    status.SetOpLogProgress(10, 20);

    // A new HA round discards the old timer and last-known latest.
    status.ClearDoctorIssues();
    status.SetOpLogProgress(10, std::nullopt);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    status.SetOpLogProgress(10, std::nullopt);
    EXPECT_EQ(doctor()["health"].asString(), "ok");
    status.SetOpLogProgress(10, 20);

    status.Configure(false, "test_cluster", "test-standby", 10, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(doctor()["issues"].empty());
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
       BatchQueryRoutesReturnsDescriptorsAndErrors) {
    const std::string key0 = "batchquery_test_key_0";
    const std::string key1 = "batchquery_test_key_1";
    const std::string missing_key = "batchquery_test_missing_key";

    AddKey(key0, /*size=*/1024, segment_id_a_);
    AddKey(key1, /*size=*/2048, segment_id_b_);

    auto resp = HttpGet(http_base_url_ + "/batch_query_routes?keys=" + key0 +
                        "," + key1 + "," + missing_key);
    ASSERT_EQ(resp.status, 200) << "body=" << resp.resp_body;

    const auto body = ParseJson(resp.resp_body, "batch routes");
    ASSERT_TRUE(body["responses"].isArray());
    ASSERT_EQ(body["responses"].size(), 3u);
    EXPECT_EQ(body["error_codes"], ErrorCodes({ErrorCode::OK, ErrorCode::OK,
                                               ErrorCode::OBJECT_NOT_FOUND}));
    ASSERT_EQ(body["responses"][0].size(), 1u);
    ASSERT_EQ(body["responses"][1].size(), 1u);
    EXPECT_EQ(body["responses"][2], Json::Value(Json::arrayValue));
    const auto& route = body["responses"][0][0];
    ExpectUuid(route["client_id"], client_id_);
    ExpectUuid(route["segment_id"], segment_id_a_);
    EXPECT_EQ(route["ip_address"].asString(), "127.0.0.1");
    EXPECT_EQ(route["rpc_port"].asUInt(), 50051u);
    EXPECT_EQ(route["object_size"].asUInt64(), 1024u);
    ExpectUuid(body["responses"][1][0]["segment_id"], segment_id_b_);
    EXPECT_EQ(body["responses"][1][0]["object_size"].asUInt64(), 2048u);

    RemoveKey(key0, segment_id_a_);
    RemoveKey(key1, segment_id_b_);

    // Once removed, the same keys must now be reported as missing.
    auto resp_after_remove = HttpGet(
        http_base_url_ + "/batch_query_routes?keys=" + key0 + "," + key1);
    ASSERT_EQ(resp_after_remove.status, 200)
        << "body=" << resp_after_remove.resp_body;
    const auto removed =
        ParseJson(resp_after_remove.resp_body, "removed routes");
    ASSERT_EQ(removed["responses"].size(), 2u);
    EXPECT_EQ(removed["responses"][0], Json::Value(Json::arrayValue));
    EXPECT_EQ(removed["responses"][1], Json::Value(Json::arrayValue));
    EXPECT_EQ(
        removed["error_codes"],
        ErrorCodes({ErrorCode::OBJECT_NOT_FOUND, ErrorCode::OBJECT_NOT_FOUND}));
}

TEST_F(P2PMasterHttpEndpointsTest,
       BatchQueryRoutesPreservesDuplicatePositions) {
    const std::string key = "batchquery_duplicate";
    AddKey(key, 1024, segment_id_a_);
    const auto response =
        HttpGet(http_base_url_ + "/batch_query_routes?keys=" + key + "," + key);
    ASSERT_EQ(response.status, 200);
    const auto body = ParseJson(response.resp_body, "duplicate routes");
    ASSERT_EQ(body["responses"].size(), 2u);
    EXPECT_EQ(body["error_codes"], ErrorCodes({ErrorCode::OK, ErrorCode::OK}));
    for (const auto& routes : body["responses"]) {
        ASSERT_EQ(routes.size(), 1u);
        ExpectUuid(routes[0]["segment_id"], segment_id_a_);
    }
    RemoveKey(key, segment_id_a_);
}

TEST_F(P2PMasterHttpEndpointsTest, CentralizedBatchQueryEndpointIsAbsent) {
    const auto response =
        HttpGet(http_base_url_ + "/batch_query_keys?keys=key");
    EXPECT_EQ(response.status, 404);
}

TEST_F(P2PMasterHttpEndpointsTest, BatchQueryRoutesRejectsEmptyKeyList) {
    // No query parameter at all.
    auto resp = HttpGet(http_base_url_ + "/batch_query_routes");
    ASSERT_EQ(resp.status, 400) << "body=" << resp.resp_body;
    EXPECT_NE(resp.resp_body.find("No keys provided"), std::string::npos);

    // Empty `keys` query parameter.
    auto resp_empty = HttpGet(http_base_url_ + "/batch_query_routes?keys=");
    ASSERT_EQ(resp_empty.status, 400) << "body=" << resp_empty.resp_body;
}

TEST_F(P2PMasterHttpEndpointsTest, MetricsEndpointsExposeRedisHAHealthMetrics) {
    auto& ha_metrics = HAMetricManager::instance();
    ha_metrics.set_election_last_successful_renew_timestamp_ms(70);
    ha_metrics.set_oplog_last_successful_write_timestamp_ms(60);
    ha_metrics.set_oplog_consecutive_write_failures(1);
    ha_metrics.set_oplog_snapshot_ready_standby_count(2);
    ha_metrics.inc_election_renew_failures();

    auto metrics_response = HttpGet(http_base_url_ + "/metrics");
    ASSERT_EQ(metrics_response.status, 200)
        << "body=" << metrics_response.resp_body;
    for (const char* name : {
             "ha_election_last_successful_renew_timestamp_ms",
             "ha_oplog_last_successful_write_timestamp_ms",
             "ha_oplog_consecutive_write_failures",
             "ha_oplog_snapshot_ready_standby_count",
             "ha_election_renew_failures_total",
         }) {
        EXPECT_NE(metrics_response.resp_body.find(name), std::string::npos)
            << name;
    }

    auto summary_response = HttpGet(http_base_url_ + "/metrics/summary");
    ASSERT_EQ(summary_response.status, 200)
        << "body=" << summary_response.resp_body;
    EXPECT_NE(summary_response.resp_body.find("HA Metrics Summary:"),
              std::string::npos);
    EXPECT_NE(summary_response.resp_body.find("consecutive_write_fail=1"),
              std::string::npos);
    EXPECT_NE(summary_response.resp_body.find("snapshot_ready_standby=2"),
              std::string::npos);

    ha_metrics.set_election_last_successful_renew_timestamp_ms(0);
    ha_metrics.set_oplog_last_successful_write_timestamp_ms(0);
    ha_metrics.set_oplog_consecutive_write_failures(0);
    ha_metrics.set_oplog_snapshot_ready_standby_count(0);
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

// Reproduces the real standby lifecycle: the supervisor-level status server
// runs while the active (primary-only) HTTP surface is absent.
TEST_F(P2PMasterHttpEndpointsTest, StatusServerServesStandbyDiagnostics) {
    P2PStatusHttpServer status_server(/*port=*/0, "127.0.0.1");
    status_server.Start();
    const uint16_t status_port = status_server.port();
    ASSERT_NE(status_port, 0);
    const std::string status_base_url =
        "http://127.0.0.1:" + std::to_string(status_port);

    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Configure(/*ha_enabled=*/true, "test_cluster", "");
    status.SetRoleState("standby", "WATCHING");

    auto health = HttpGet(status_base_url + "/health");
    ASSERT_EQ(health.status, 200) << "body=" << health.resp_body;
    EXPECT_EQ(health.resp_body, "OK");

    auto ha_status = HttpGet(status_base_url + "/ha/status");
    ASSERT_EQ(ha_status.status, 200) << "body=" << ha_status.resp_body;
    const auto status_body = ParseJson(ha_status.resp_body, "/ha/status");
    EXPECT_EQ(status_body["role"].asString(), "standby");
    EXPECT_EQ(status_body["state"].asString(), "WATCHING");

    auto doctor = HttpGet(status_base_url + "/ha/doctor");
    ASSERT_EQ(doctor.status, 200) << "body=" << doctor.resp_body;
    const auto doctor_body = ParseJson(doctor.resp_body, "/ha/doctor");
    EXPECT_EQ(doctor_body["status"]["role"].asString(), "standby");

    auto metrics = HttpGet(status_base_url + "/metrics");
    ASSERT_EQ(metrics.status, 200);
    auto summary = HttpGet(status_base_url + "/metrics/summary");
    ASSERT_EQ(summary.status, 200);

    // Binding to one loopback address must leave the same port available
    // on a different address. A wildcard listener would fail this probe.
    const auto expect_scoped_binding = [status_port] {
        const int fd = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(fd, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(0x7f000002);
        address.sin_port = htons(status_port);
        const int result =
            bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
        EXPECT_EQ(close(fd), 0);
        EXPECT_EQ(result, 0);
    };
    ASSERT_NO_FATAL_FAILURE(expect_scoped_binding());

    status_server.Stop();
    P2PMasterConfig active_config;
    active_config.metrics.enable_reporting = false;
    active_config.metrics.http_port = status_port;
    active_config.metrics.host = "127.0.0.1";
    P2PMasterRpcService active(active_config);
    active.init();
    ASSERT_EQ(HttpGet(status_base_url + "/health").status, 200);
    ASSERT_NO_FATAL_FAILURE(expect_scoped_binding());
}

}  // namespace testing
}  // namespace mooncake
