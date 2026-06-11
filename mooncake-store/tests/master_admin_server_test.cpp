#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <memory>
#include <string>
#include <thread>

#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include "ha/ha_types.h"
#include "master_config.h"
#include "rpc_service.h"
#include "types.h"
#include "utils.h"

#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {
namespace test {

namespace {

struct HttpCreateDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCreateDrainJobResponse, success, job_id, status, error_code,
         error_message);

struct HttpQueryDrainJobResponse {
    bool success{false};
    std::string job_id;
    int32_t type{0};
    std::string type_name;
    int32_t status{0};
    std::string status_name;
    std::string message;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpQueryDrainJobResponse, success, job_id, type, type_name, status,
         status_name, message, error_code, error_message);

struct HttpCancelDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCancelDrainJobResponse, success, job_id, status, error_code,
         error_message);

struct HttpSegmentStatusResponse {
    bool success{false};
    std::string segment;
    int32_t status{0};
    std::string status_name;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpSegmentStatusResponse, success, segment, status, status_name,
         error_code, error_message);

struct HttpErrorResponse {
    bool success{false};
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpErrorResponse, success, error_code, error_message);

struct HttpSegmentsDetailResponse {
    uint64_t total_segments{0};
};
YLT_REFL(HttpSegmentsDetailResponse, total_segments);

struct HttpBatchQueryResponse {
    bool success{false};
    std::string error;
};
YLT_REFL(HttpBatchQueryResponse, success, error);

}  // namespace

class MasterAdminServerTest : public ::testing::Test {
   protected:
    struct HttpResponse {
        int http_status;
        std::string body;
    };

    void SetUp() override {
        google::InitGoogleLogging("MasterAdminServerTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static std::string BaseUrl(int port) {
        return "http://127.0.0.1:" + std::to_string(port);
    }

    HttpResponse HttpGet(int port, const std::string& path) {
        coro_http::coro_http_client client;
        auto result = client.get(BaseUrl(port) + path);
        return {result.status, std::string(result.resp_body)};
    }

    HttpResponse HttpPostJson(int port, const std::string& path,
                              const std::string& body) {
        coro_http::coro_http_client client;
        auto result = client.post(BaseUrl(port) + path, body,
                                  coro_http::req_content_type::json);
        return {result.status, std::string(result.resp_body)};
    }

    // Create a MasterAdminServer with a mounted segment and a put key,
    // return the port.
    // If populate_data is true, mount a segment and put a key.
    struct ServerWithData {
        int port;
        std::shared_ptr<WrappedMasterService> service;
        Segment segment;
        std::string key;
    };

    ServerWithData CreateServerWithData() {
        WrappedMasterServiceConfig svc_config;
        svc_config.default_kv_lease_ttl = 5000;
        svc_config.enable_metric_reporting = false;
        auto service = std::make_shared<WrappedMasterService>(svc_config);

        Segment seg;
        seg.id = generate_uuid();
        seg.name = "admin_test_segment";
        seg.base = 0x300000000;
        seg.size = 8 * 1024 * 1024;
        UUID client_id = generate_uuid();
        (void)service->MountSegment(seg, client_id);

        ReplicateConfig cfg;
        cfg.replica_num = 1;
        auto ps = service->PutStart(client_id, "admin_test_key", 1024, cfg);
        if (ps.has_value()) {
            (void)service->PutEnd(client_id, "admin_test_key",
                                  ReplicaType::MEMORY);
        }

        int port = getFreeTcpPort();
        auto admin = std::make_unique<MasterAdminServer>(
            static_cast<uint16_t>(port), false);
        admin->Start();
        admin->SetRuntimeState(ha::MasterRuntimeState::kServing);
        admin->SetServiceDelegate(service);
        admin->SetServiceAvailable(true);

        detached_admins_.push_back(std::move(admin));
        return {port, service, seg, "admin_test_key"};
    }

    void TearDownAdminServer(MasterAdminServer* server) { server->Stop(); }

    // Keep admin servers alive for the duration of each test.
    std::vector<std::unique_ptr<MasterAdminServer>> detached_admins_;
};

// =========================================================================
// Tests for endpoints that are always available (no service required)
// =========================================================================

TEST_F(MasterAdminServerTest, MetricsEndpointReturns200) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);

    auto resp = HttpGet(port, "/metrics");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("master_"), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, MetricsSummaryEndpointReturns200) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);

    auto resp = HttpGet(port, "/metrics/summary");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("role="), std::string::npos);
    EXPECT_NE(resp.body.find("state="), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, HealthEndpointReturns200InStandby) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kStandby);

    auto resp = HttpGet(port, "/health");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"status\":\"ok\""), std::string::npos);
    EXPECT_NE(resp.body.find("\"role\":\"standby\""), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, HealthEndpointReturns200InServing) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);

    auto resp = HttpGet(port, "/health");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"role\":\"leader\""), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, HealthEndpointIncludesLeaderInfoWhenSet) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kStandby);
    admin.SetObservedLeader(ha::MasterView{
        .leader_address = "10.0.0.1:19000",
        .view_version = 42,
    });

    auto resp = HttpGet(port, "/health");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"leader_address\":\"10.0.0.1:19000\""),
              std::string::npos);
    EXPECT_NE(resp.body.find("\"view_version\":42"), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, RoleEndpointReturnsCorrectRoleForServing) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);

    auto resp = HttpGet(port, "/role");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "leader");

    admin.Stop();
}

TEST_F(MasterAdminServerTest, RoleEndpointReturnsCorrectRoleForStandby) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kStandby);

    auto resp = HttpGet(port, "/role");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "standby");

    admin.Stop();
}

TEST_F(MasterAdminServerTest, RoleEndpointReturnsCorrectRoleForCandidate) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kCandidate);

    auto resp = HttpGet(port, "/role");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "standby");

    admin.Stop();
}

TEST_F(MasterAdminServerTest, RoleEndpointReturnsCorrectRoleForRecovering) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kRecovering);

    auto resp = HttpGet(port, "/role");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "standby");

    admin.Stop();
}

TEST_F(MasterAdminServerTest, HaStatusEndpointReturnsCorrectState) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());

    admin.SetRuntimeState(ha::MasterRuntimeState::kStandby);
    auto resp = HttpGet(port, "/ha_status");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "standby");

    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    resp = HttpGet(port, "/ha_status");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "serving");

    admin.SetRuntimeState(ha::MasterRuntimeState::kRecovering);
    resp = HttpGet(port, "/ha_status");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "recovering");

    admin.SetRuntimeState(ha::MasterRuntimeState::kCatchingUp);
    resp = HttpGet(port, "/ha_status");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "catching_up");

    admin.Stop();
}

TEST_F(MasterAdminServerTest, LeaderEndpointWithoutLeader) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());

    auto resp = HttpGet(port, "/leader");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"present\":false"), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, LeaderEndpointWithLeader) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetObservedLeader(ha::MasterView{
        .leader_address = "192.168.1.1:19000",
        .view_version = 5,
    });

    auto resp = HttpGet(port, "/leader");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"present\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("192.168.1.1:19000"), std::string::npos);
    EXPECT_NE(resp.body.find("\"view_version\":5"), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, LeaderEndpointAfterClearingLeader) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetObservedLeader(ha::MasterView{
        .leader_address = "192.168.1.1:19000",
        .view_version = 5,
    });

    auto resp = HttpGet(port, "/leader");
    EXPECT_NE(resp.body.find("\"present\":true"), std::string::npos);

    admin.SetObservedLeader(std::nullopt);
    resp = HttpGet(port, "/leader");
    EXPECT_NE(resp.body.find("\"present\":false"), std::string::npos);

    admin.Stop();
}

// =========================================================================
// Tests for endpoints that are always available but with various runtime
// states (service not set)
// =========================================================================

TEST_F(MasterAdminServerTest, AllAlwaysAvailableEndpointsInStartingState) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kStarting);

    auto metrics = HttpGet(port, "/metrics");
    EXPECT_EQ(metrics.http_status, 200);

    auto summary = HttpGet(port, "/metrics/summary");
    EXPECT_EQ(summary.http_status, 200);

    auto health = HttpGet(port, "/health");
    EXPECT_EQ(health.http_status, 200);
    EXPECT_NE(health.body.find("\"ha_state\":\"starting\""),
              std::string::npos);

    auto role = HttpGet(port, "/role");
    EXPECT_EQ(role.http_status, 200);
    EXPECT_EQ(role.body, "standby");

    auto ha_status = HttpGet(port, "/ha_status");
    EXPECT_EQ(ha_status.http_status, 200);
    EXPECT_EQ(ha_status.body, "starting");

    auto leader = HttpGet(port, "/leader");
    EXPECT_EQ(leader.http_status, 200);
    EXPECT_NE(leader.body.find("\"present\":false"), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, AlwaysAvailableEndpointsInLeaderWarmup) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kLeaderWarmup);

    auto role = HttpGet(port, "/role");
    EXPECT_EQ(role.body, "leader");

    auto health = HttpGet(port, "/health");
    EXPECT_NE(health.body.find("\"role\":\"leader\""), std::string::npos);
    EXPECT_NE(health.body.find("\"ha_state\":\"leader_warmup\""),
              std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, RoleWithAllStates) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());

    // MasterRuntimeRoleToString groups kStarting/kStandby/kCandidate/
    // kRecovering/kCatchingUp as "standby"; only leader states are "leader".
    struct {
        ha::MasterRuntimeState state;
        std::string expected_role;
    } cases[] = {
        {ha::MasterRuntimeState::kStarting, "standby"},
        {ha::MasterRuntimeState::kStandby, "standby"},
        {ha::MasterRuntimeState::kCandidate, "standby"},
        {ha::MasterRuntimeState::kRecovering, "standby"},
        {ha::MasterRuntimeState::kCatchingUp, "standby"},
        {ha::MasterRuntimeState::kLeaderWarmup, "leader"},
        {ha::MasterRuntimeState::kServing, "leader"},
    };

    for (const auto& tc : cases) {
        admin.SetRuntimeState(tc.state);
        auto resp = HttpGet(port, "/role");
        EXPECT_EQ(resp.body, tc.expected_role)
            << "Unexpected role for state "
            << ha::MasterRuntimeStateToString(tc.state);
    }

    admin.Stop();
}

// =========================================================================
// Tests for endpoints that require an active service
// =========================================================================

TEST_F(MasterAdminServerTest, ServiceEndpointsReturn503WhenServiceUnavailable) {
    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kStandby);

    const std::string unavailable_msg = "service plane is not active";

    auto keys = HttpGet(port, "/get_all_keys");
    EXPECT_EQ(keys.http_status, 503);
    EXPECT_NE(keys.body.find(unavailable_msg), std::string::npos);

    auto segments = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(segments.http_status, 503);
    EXPECT_NE(segments.body.find(unavailable_msg), std::string::npos);

    auto detail = HttpGet(port, "/get_segments_detail");
    EXPECT_EQ(detail.http_status, 503);
    EXPECT_NE(detail.body.find(unavailable_msg), std::string::npos);

    auto query_seg = HttpGet(port, "/query_segment?segment=foo");
    EXPECT_EQ(query_seg.http_status, 503);
    EXPECT_NE(query_seg.body.find(unavailable_msg), std::string::npos);

    auto query_key = HttpGet(port, "/query_key?key=foo");
    EXPECT_EQ(query_key.http_status, 503);
    EXPECT_NE(query_key.body.find(unavailable_msg), std::string::npos);

    auto batch = HttpGet(port, "/batch_query_keys?keys=foo");
    EXPECT_EQ(batch.http_status, 503);
    EXPECT_NE(batch.body.find(unavailable_msg), std::string::npos);

    auto seg_status = HttpGet(port, "/api/v1/segments/status?segment=foo");
    EXPECT_EQ(seg_status.http_status, 503);
    EXPECT_NE(seg_status.body.find(unavailable_msg), std::string::npos);

    auto drain_create = HttpPostJson(port, "/api/v1/drain_jobs", "{}");
    EXPECT_EQ(drain_create.http_status, 503);

    // drain_jobs/query and drain_jobs/cancel validate job_id before
    // checking service availability, so we must use a valid UUID.
    std::string valid_uuid = UuidToString(generate_uuid());
    auto drain_query =
        HttpGet(port, "/api/v1/drain_jobs/query?job_id=" + valid_uuid);
    EXPECT_EQ(drain_query.http_status, 503);

    auto drain_cancel =
        HttpPostJson(port,
                     "/api/v1/drain_jobs/cancel?job_id=" + valid_uuid, "");
    EXPECT_EQ(drain_cancel.http_status, 503);

    admin.Stop();
}

// -----------------------------------------------------------------------
// GET /get_all_keys
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, GetAllKeysReturnsEmptyWhenNoKeys) {
    auto sd = CreateServerWithData();
    // Remove the key so there are none.
    (void)sd.service->Remove(sd.key, "default");

    auto resp = HttpGet(sd.port, "/get_all_keys");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_TRUE(resp.body.empty() || resp.body == "\n");
}

TEST_F(MasterAdminServerTest, GetAllKeysReturnsKeys) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/get_all_keys");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find(sd.key), std::string::npos);
}

// -----------------------------------------------------------------------
// GET /query_key
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, QueryKeyReturnsDataForExistingKey) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/query_key?key=" + sd.key);
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"buffer_address_\""), std::string::npos);
}

TEST_F(MasterAdminServerTest, QueryKeyReturns404ForNonexistentKey) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/query_key?key=nonexistent_key_xyz");
    EXPECT_EQ(resp.http_status, 404);
}

TEST_F(MasterAdminServerTest, QueryKeyWithoutKeyParamReturns404) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/query_key");
    EXPECT_EQ(resp.http_status, 404);
}

// -----------------------------------------------------------------------
// GET /query_segment
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, QuerySegmentReturnsDataForExistingSegment) {
    auto sd = CreateServerWithData();

    auto resp =
        HttpGet(sd.port, "/query_segment?segment=" + sd.segment.name);
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find(sd.segment.name), std::string::npos);
    EXPECT_NE(resp.body.find("Used(bytes)"), std::string::npos);
    EXPECT_NE(resp.body.find("Capacity(bytes)"), std::string::npos);
}

TEST_F(MasterAdminServerTest, QuerySegmentReturns500ForNonexistentSegment) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/query_segment?segment=nonexistent_seg");
    EXPECT_EQ(resp.http_status, 500);
}

// -----------------------------------------------------------------------
// GET /get_all_segments
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, GetAllSegmentsReturnsSegments) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/get_all_segments");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find(sd.segment.name), std::string::npos);
}

// -----------------------------------------------------------------------
// GET /get_segments_detail
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, GetSegmentsDetailReturnsDetailedInfo) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/get_segments_detail");
    EXPECT_EQ(resp.http_status, 200);

    HttpSegmentsDetailResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_GT(parsed.total_segments, 0u);
    EXPECT_NE(resp.body.find(sd.segment.name), std::string::npos);
    EXPECT_NE(resp.body.find("\"allocator_used_bytes\""), std::string::npos);
    EXPECT_NE(resp.body.find("\"allocator_capacity_bytes\""),
              std::string::npos);
}

// -----------------------------------------------------------------------
// POST /api/v1/drain_jobs
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, CreateDrainJobSucceedsWithValidRequest) {
    auto sd = CreateServerWithData();

    std::string body = R"({"segments":[")" + sd.segment.name + R"("]})";
    auto resp = HttpPostJson(sd.port, "/api/v1/drain_jobs", body);
    EXPECT_EQ(resp.http_status, 200);

    HttpCreateDrainJobResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_TRUE(parsed.success);
    EXPECT_FALSE(parsed.job_id.empty());
    EXPECT_EQ(parsed.status, "CREATED");
}

TEST_F(MasterAdminServerTest, CreateDrainJobFailsWithInvalidJson) {
    auto sd = CreateServerWithData();

    auto resp = HttpPostJson(sd.port, "/api/v1/drain_jobs", "not json");
    EXPECT_EQ(resp.http_status, 400);

    HttpErrorResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_FALSE(parsed.success);
}

TEST_F(MasterAdminServerTest, CreateDrainJobFailsWithEmptyBody) {
    auto sd = CreateServerWithData();

    auto resp = HttpPostJson(sd.port, "/api/v1/drain_jobs", "{}");
    EXPECT_NE(resp.http_status, 200);
}

// -----------------------------------------------------------------------
// GET /api/v1/drain_jobs/query
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, QueryDrainJobReturnsCreatedJob) {
    auto sd = CreateServerWithData();

    std::string body = R"({"segments":[")" + sd.segment.name + R"("]})";
    auto create_resp = HttpPostJson(sd.port, "/api/v1/drain_jobs", body);
    EXPECT_EQ(create_resp.http_status, 200);

    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_FALSE(create_parsed.job_id.empty());

    auto query_resp = HttpGet(sd.port,
                              "/api/v1/drain_jobs/query?job_id=" +
                                  create_parsed.job_id);
    EXPECT_EQ(query_resp.http_status, 200);

    HttpQueryDrainJobResponse query_parsed;
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
    EXPECT_EQ(query_parsed.job_id, create_parsed.job_id);
}

TEST_F(MasterAdminServerTest, QueryDrainJobFailsWithInvalidJobId) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/api/v1/drain_jobs/query?job_id=not-a-uuid");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerTest, QueryDrainJobFailsWithMissingJobId) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/api/v1/drain_jobs/query");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerTest, QueryDrainJobFailsForNonexistentJob) {
    auto sd = CreateServerWithData();

    auto resp =
        HttpGet(sd.port, "/api/v1/drain_jobs/query?job_id=" +
                             UuidToString(generate_uuid()));
    EXPECT_EQ(resp.http_status, 404);
}

// -----------------------------------------------------------------------
// POST /api/v1/drain_jobs/cancel
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, CancelDrainJobSucceeds) {
    auto sd = CreateServerWithData();

    std::string body = R"({"segments":[")" + sd.segment.name + R"("]})";
    auto create_resp = HttpPostJson(sd.port, "/api/v1/drain_jobs", body);
    EXPECT_EQ(create_resp.http_status, 200);

    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_FALSE(create_parsed.job_id.empty());

    auto cancel_resp =
        HttpPostJson(sd.port,
                     "/api/v1/drain_jobs/cancel?job_id=" + create_parsed.job_id,
                     "");
    EXPECT_EQ(cancel_resp.http_status, 200);

    HttpCancelDrainJobResponse cancel_parsed;
    struct_json::from_json(cancel_parsed, cancel_resp.body);
    EXPECT_TRUE(cancel_parsed.success);
    EXPECT_EQ(cancel_parsed.job_id, create_parsed.job_id);
    EXPECT_EQ(cancel_parsed.status, "CANCELED");
}

TEST_F(MasterAdminServerTest, CancelDrainJobFailsWithInvalidJobId) {
    auto sd = CreateServerWithData();

    auto resp =
        HttpPostJson(sd.port,
                     "/api/v1/drain_jobs/cancel?job_id=not-a-uuid", "");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerTest, CancelDrainJobFailsWithMissingJobId) {
    auto sd = CreateServerWithData();

    auto resp = HttpPostJson(sd.port, "/api/v1/drain_jobs/cancel", "");
    EXPECT_EQ(resp.http_status, 400);
}

// -----------------------------------------------------------------------
// GET /api/v1/segments/status
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, SegmentStatusReturnsDataForExistingSegment) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port,
                        "/api/v1/segments/status?segment=" + sd.segment.name);
    EXPECT_EQ(resp.http_status, 200);

    HttpSegmentStatusResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_TRUE(parsed.success);
    EXPECT_EQ(parsed.segment, sd.segment.name);
    EXPECT_FALSE(parsed.status_name.empty());
}

TEST_F(MasterAdminServerTest, SegmentStatusFailsWithMissingSegment) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/api/v1/segments/status");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerTest, SegmentStatusReturnsErrorForNonexistentSegment) {
    auto sd = CreateServerWithData();

    auto resp =
        HttpGet(sd.port, "/api/v1/segments/status?segment=no_such_segment");
    EXPECT_NE(resp.http_status, 200);
}

// -----------------------------------------------------------------------
// GET /batch_query_keys
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerTest, BatchQueryKeysWithNoKeysParamReturns400) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/batch_query_keys");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerTest, BatchQueryKeysWithEmptyKeysParamReturns400) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/batch_query_keys?keys=");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerTest, BatchQueryKeysReturnsDataForExistingKey) {
    auto sd = CreateServerWithData();

    auto resp =
        HttpGet(sd.port, "/batch_query_keys?keys=" + sd.key);
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"data\":{"), std::string::npos);
    EXPECT_NE(resp.body.find(sd.key), std::string::npos);
}

TEST_F(MasterAdminServerTest, BatchQueryKeysReturnsErrorForNonexistentKey) {
    auto sd = CreateServerWithData();

    auto resp = HttpGet(sd.port, "/batch_query_keys?keys=nonexistent_key");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"ok\":false"), std::string::npos);
}

TEST_F(MasterAdminServerTest, BatchQueryKeysMultipleKeys) {
    auto sd = CreateServerWithData();

    // Add a second key
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto client_id = generate_uuid();
    auto ps = sd.service->PutStart(client_id, "second_key", 512, cfg);
    if (ps.has_value()) {
        (void)sd.service->PutEnd(client_id, "second_key", ReplicaType::MEMORY);
    }

    auto resp = HttpGet(sd.port, "/batch_query_keys?keys=admin_test_key,second_key");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("admin_test_key"), std::string::npos);
    EXPECT_NE(resp.body.find("second_key"), std::string::npos);
}

TEST_F(MasterAdminServerTest, BatchQueryKeysWithExistingAndNonexistentKeys) {
    auto sd = CreateServerWithData();

    auto resp =
        HttpGet(sd.port, "/batch_query_keys?keys=admin_test_key,nonexistent");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"ok\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"ok\":false"), std::string::npos);
}

// =========================================================================
// Tests for runtime state transitions and endpoint behavior
// =========================================================================

TEST_F(MasterAdminServerTest, ServiceUnavailableAfterDelegateCleared) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto resp = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(resp.http_status, 200);

    admin.SetServiceDelegate(nullptr);

    resp = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(resp.http_status, 503);
    EXPECT_NE(resp.body.find("service plane is not active"),
              std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, ServiceUnavailableAfterSetServiceAvailableFalse) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto resp = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(resp.http_status, 200);

    admin.SetServiceAvailable(false);

    resp = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(resp.http_status, 503);

    admin.SetServiceAvailable(true);
    resp = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(resp.http_status, 200);

    admin.Stop();
}

// =========================================================================
// End-to-end drain job flow test
// =========================================================================

TEST_F(MasterAdminServerTest, DrainJobFullLifecycle) {
    auto sd = CreateServerWithData();

    // 1. Create drain job
    std::string body = R"({"segments":[")" + sd.segment.name + R"("]})";
    auto create_resp = HttpPostJson(sd.port, "/api/v1/drain_jobs", body);
    ASSERT_EQ(create_resp.http_status, 200);
    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_TRUE(create_parsed.success);
    std::string job_id = create_parsed.job_id;

    // 2. Query the job
    auto query_resp =
        HttpGet(sd.port, "/api/v1/drain_jobs/query?job_id=" + job_id);
    EXPECT_EQ(query_resp.http_status, 200);
    HttpQueryDrainJobResponse query_parsed;
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
    EXPECT_EQ(query_parsed.job_id, job_id);

    // 3. Cancel the job
    auto cancel_resp =
        HttpPostJson(sd.port, "/api/v1/drain_jobs/cancel?job_id=" + job_id, "");
    EXPECT_EQ(cancel_resp.http_status, 200);
    HttpCancelDrainJobResponse cancel_parsed;
    struct_json::from_json(cancel_parsed, cancel_resp.body);
    EXPECT_TRUE(cancel_parsed.success);
    EXPECT_EQ(cancel_parsed.job_id, job_id);
    EXPECT_EQ(cancel_parsed.status, "CANCELED");

    // 4. Query after cancel - should succeed with updated status
    query_resp = HttpGet(sd.port, "/api/v1/drain_jobs/query?job_id=" + job_id);
    EXPECT_EQ(query_resp.http_status, 200);
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
}

// =========================================================================
// Multiple mounts and keys
// =========================================================================

TEST_F(MasterAdminServerTest, MultipleSegmentsAndKeys) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    UUID client_id = generate_uuid();

    // Mount two segments
    Segment seg1;
    seg1.id = generate_uuid();
    seg1.name = "seg_alpha";
    seg1.base = 0x400000000;
    seg1.size = 8 * 1024 * 1024;
    ASSERT_TRUE(service->MountSegment(seg1, client_id).has_value());

    Segment seg2;
    seg2.id = generate_uuid();
    seg2.name = "seg_beta";
    seg2.base = 0x500000000;
    seg2.size = 4 * 1024 * 1024;
    ASSERT_TRUE(service->MountSegment(seg2, client_id).has_value());

    // Put two keys
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto ps1 = service->PutStart(client_id, "key_one", 1024, cfg);
    if (ps1.has_value()) {
        (void)service->PutEnd(client_id, "key_one", ReplicaType::MEMORY);
    }
    auto ps2 = service->PutStart(client_id, "key_two", 2048, cfg);
    if (ps2.has_value()) {
        (void)service->PutEnd(client_id, "key_two", ReplicaType::MEMORY);
    }

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    // get_all_segments lists both
    auto seg_resp = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(seg_resp.http_status, 200);
    EXPECT_NE(seg_resp.body.find("seg_alpha"), std::string::npos);
    EXPECT_NE(seg_resp.body.find("seg_beta"), std::string::npos);

    // get_all_keys lists both
    auto keys_resp = HttpGet(port, "/get_all_keys");
    EXPECT_EQ(keys_resp.http_status, 200);
    EXPECT_NE(keys_resp.body.find("key_one"), std::string::npos);
    EXPECT_NE(keys_resp.body.find("key_two"), std::string::npos);

    // query_segment for each
    auto q1 = HttpGet(port, "/query_segment?segment=seg_alpha");
    EXPECT_EQ(q1.http_status, 200);
    EXPECT_NE(q1.body.find("seg_alpha"), std::string::npos);

    auto q2 = HttpGet(port, "/query_segment?segment=seg_beta");
    EXPECT_EQ(q2.http_status, 200);
    EXPECT_NE(q2.body.find("seg_beta"), std::string::npos);

    // segment status for each
    auto s1 = HttpGet(port, "/api/v1/segments/status?segment=seg_alpha");
    EXPECT_EQ(s1.http_status, 200);
    HttpSegmentStatusResponse parsed1;
    struct_json::from_json(parsed1, s1.body);
    EXPECT_TRUE(parsed1.success);
    EXPECT_EQ(parsed1.segment, "seg_alpha");

    auto s2 = HttpGet(port, "/api/v1/segments/status?segment=seg_beta");
    EXPECT_EQ(s2.http_status, 200);
    HttpSegmentStatusResponse parsed2;
    struct_json::from_json(parsed2, s2.body);
    EXPECT_TRUE(parsed2.success);
    EXPECT_EQ(parsed2.segment, "seg_beta");

    // get_segments_detail shows both
    auto detail_resp = HttpGet(port, "/get_segments_detail");
    EXPECT_EQ(detail_resp.http_status, 200);
    HttpSegmentsDetailResponse detail_parsed;
    struct_json::from_json(detail_parsed, detail_resp.body);
    EXPECT_EQ(detail_parsed.total_segments, 2u);

    // batch_query_keys for both
    auto batch_resp = HttpGet(port, "/batch_query_keys?keys=key_one,key_two");
    EXPECT_EQ(batch_resp.http_status, 200);
    EXPECT_NE(batch_resp.body.find("key_one"), std::string::npos);
    EXPECT_NE(batch_resp.body.find("key_two"), std::string::npos);

    admin.Stop();
}

}  // namespace test
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
