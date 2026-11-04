/**
 * @file p2p_master_http_endpoints_test.cpp
 * @brief Integration tests for the P2P master HTTP endpoints
 *        (GET /health, GET /get_key_count, GET /get_all_keys,
 *         GET /batch_query_routes, GET /metrics, GET /ha/status, GET
 * /ha/doctor).
 *
 * Brings up an in-process P2PMasterRpcService with the embedded HTTP
 * server bound to a kernel-assigned port (no RPC server)
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
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

#include "p2p/ha/ha_status.h"
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

TEST_F(P2PMasterHttpEndpointsTest, StepdownEndpointInvokesCallback) {
    std::atomic<bool> stepdown_called{false};
    wrapped_->SetStepdownCallback(
        [&stepdown_called] { stepdown_called.store(true); });

    const auto response = HttpPost(http_base_url_ + "/ha/stepdown");
    ASSERT_EQ(response.status, 200) << "body=" << response.resp_body;
    const auto body = ParseJson(response.resp_body, "stepdown");
    EXPECT_TRUE(body["success"].asBool());
    EXPECT_TRUE(stepdown_called.load());
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
    status.SetOpLogWriteProgress(/*committed_sequence_id=*/15,
                                 /*pending=*/4);
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
    EXPECT_TRUE(body["oplog_writer_running"].asBool());
    EXPECT_EQ(body["oplog_writer_last_committed_sequence_id"].asUInt64(), 15u);
    EXPECT_EQ(body["snapshot_ready"].asBool(), true);
    EXPECT_EQ(body["registry_heartbeat_ok"].asBool(), true);
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
    EXPECT_EQ(report["status"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 2u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "OPLOG_APPLY_FAILED");
    EXPECT_EQ(report["issues"][0]["message"].asString(),
              "seq=123: invalid payload");

    // Repeated failures replace the current message without accumulating.
    status.SetDoctorIssue("OPLOG_APPLY_FAILED", "seq=124: invalid payload");
    status.RecordRegistryHeartbeat(ErrorCode::OK, "");
    report = doctor();
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["message"].asString(),
              "seq=124: invalid payload");
    status.SetDoctorIssue("OPLOG_APPLY_FAILED");
    report = doctor();
    EXPECT_EQ(report["status"].asString(), "ok");
    EXPECT_TRUE(report["issues"].empty());
}

TEST_F(P2PMasterHttpEndpointsTest,
       HADoctorDoesNotTreatGapsOrHistoryAsFailures) {
    HAStatusResetGuard reset_guard;
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, "test_cluster", "127.0.0.1:50051");
    status.SetLastError("An earlier snapshot attempt failed");
    status.SetOpLogProgress(10, 1000, 50);
    for (const auto* state : {"SYNCING", "WATCHING", "PROMOTING"}) {
        status.SetRoleState("standby", state);
        auto response = HttpGet(http_base_url_ + "/ha/doctor");
        ASSERT_EQ(response.status, 200) << "body=" << response.resp_body;
        auto report = ParseJson(response.resp_body, "/ha/doctor");
        EXPECT_TRUE(report["issues"].empty());
        EXPECT_EQ(report["status"].asString(),
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
    EXPECT_EQ(report["status"].asString(), "degraded");
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
    EXPECT_EQ(doctor()["status"].asString(), "unknown");
    status.Configure(false, "test_cluster", "127.0.0.1:50051");
    EXPECT_EQ(doctor()["status"].asString(), "unknown");
    status.Configure(true, "test_cluster", "127.0.0.1:50051");
    status.SetRoleState("promoting", "FAILED");
    status.SetLastError("Failed to promote: INTERNAL_ERROR");
    auto report = doctor();
    EXPECT_EQ(report["status"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "HA_FAILED");
    EXPECT_EQ(report["issues"][0]["message"].asString(),
              "Failed to promote: INTERNAL_ERROR");
    status.SetDoctorIssue("ELECTION_FAILED", "Redis unavailable");
    status.ClearDoctorIssues();
    status.ClearLastError();
    status.SetRoleState("starting", "INITIALIZING");
    report = doctor();
    EXPECT_EQ(report["status"].asString(), "unknown");
    EXPECT_TRUE(report["issues"].empty());
    status.SetLeader("127.0.0.1:50051", 1);
    status.SetRoleState("primary", "RUNNING");
    EXPECT_EQ(doctor()["status"].asString(), "ok");
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
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    // New pending work begins a fresh timer. Idle time is excluded.
    status.SetOpLogWriteProgress(10, 1);
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    // Additional work cannot reset the timer while confirmed latest stalls.
    status.SetOpLogWriteProgress(10, 2);
    auto report = doctor();
    EXPECT_EQ(report["status"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "OPLOG_LATEST_STALLED");
    EXPECT_NE(report["issues"][0]["message"].asString().find(
                  "latest=10, pending=2, stalled_ms="),
              std::string::npos);

    status.Configure(true, "test_cluster", "test-primary", 0);
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    status.Configure(true, "test_cluster", "test-primary", 1);
    EXPECT_EQ(doctor()["status"].asString(), "degraded");

    // Confirmed progress restarts the timer while work remains busy.
    status.SetOpLogWriteProgress(11, 1);
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["status"].asString(), "degraded");

    // Draining work disarms the timer.
    status.SetOpLogWriteProgress(12, 0);
    EXPECT_EQ(doctor()["status"].asString(), "ok");

    // A stopped writer clears cache and rejects late callbacks from the
    // previous worker.
    status.SetOpLogWriteProgress(12, 1);
    status.SetOpLogWriterRunning(false);
    status.SetOpLogWriteProgress(12, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["status"].asString(), "ok");
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
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    status.SetOpLogProgress(100, 200);
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    // More backlog cannot restart the timer.
    status.SetOpLogProgress(100, 300);
    auto report = doctor();
    EXPECT_EQ(report["status"].asString(), "degraded");
    ASSERT_EQ(report["issues"].size(), 1u);
    EXPECT_EQ(report["issues"][0]["code"].asString(), "OPLOG_CATCHUP_STALLED");
    EXPECT_NE(report["issues"][0]["message"].asString().find(
                  "applied=100, latest=300, stalled_ms="),
              std::string::npos);
    EXPECT_NE(report["status"].asString(), "ok");

    // A failed latest read retains the last known latest.
    status.SetOpLogProgress(100, std::nullopt);
    EXPECT_EQ(status.Snapshot().latest_sequence_id, 300u);
    EXPECT_EQ(doctor()["status"].asString(), "degraded");
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
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    status.SetOpLogProgress(101, std::nullopt);
    EXPECT_EQ(doctor()["status"].asString(), "degraded");
    status.SetOpLogProgress(300, 300);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(doctor()["status"].asString(), "ok");
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
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    status.SetOpLogProgress(10, 20);
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    // No newer sample means the stall is not confirmed yet.
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    status.SetOpLogProgress(10, 20);
    EXPECT_EQ(doctor()["status"].asString(), "degraded");

    status.SetLeader("test-standby", 1);
    status.SetRoleState("primary", "RUNNING");
    EXPECT_EQ(doctor()["status"].asString(), "ok");
    status.SetRoleState("standby", "WATCHING");
    status.SetOpLogProgress(10, 20);

    // A new HA round discards the old timer and last-known latest.
    status.ClearDoctorIssues();
    status.SetOpLogProgress(10, std::nullopt);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    status.SetOpLogProgress(10, std::nullopt);
    EXPECT_EQ(doctor()["status"].asString(), "ok");
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
