#include <glog/logging.h>
#include <gtest/gtest.h>

#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/struct_json/json_reader.h>

#include "master_metric_manager.h"
#include "master_service.h"
#include "replica.h"
#include "rpc_service.h"
#include "types.h"
#include "utils.h"

namespace mooncake::test {

class TenantQuotaHttpTest : public ::testing::Test {
   protected:
    struct HttpResponse {
        int http_status;
        std::string body;
    };

    void SetUp() override {
        google::InitGoogleLogging("TenantQuotaHttpTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    HttpResponse HttpGet(int port, const std::string& path) {
        coro_http::coro_http_client client;
        auto result =
            client.get("http://127.0.0.1:" + std::to_string(port) + path);
        return {result.status, std::string(result.resp_body)};
    }

    HttpResponse HttpPut(int port, const std::string& path,
                         const std::string& body) {
        coro_http::coro_http_client client;
        auto result = async_simple::coro::syncAwait(
            client.async_put("http://127.0.0.1:" + std::to_string(port) + path,
                             body, coro_http::req_content_type::json));
        return {result.status, std::string(result.resp_body)};
    }

    HttpResponse HttpDelete(int port, const std::string& path) {
        coro_http::coro_http_client client;
        auto result = async_simple::coro::syncAwait(client.async_delete(
            "http://127.0.0.1:" + std::to_string(port) + path, "",
            coro_http::req_content_type::json));
        return {result.status, std::string(result.resp_body)};
    }

    static constexpr size_t kSegmentBase = 0x300000000;
    static constexpr size_t kSegmentSize = 64 * 1024 * 1024;  // 64 MiB

    struct AdminServerContext {
        std::shared_ptr<WrappedMasterService> service;
        std::unique_ptr<MasterAdminServer> admin_server;
        int http_port;
        UUID client_id;  // valid only when a segment is mounted
    };

    AdminServerContext StartAdminServer() {
        WrappedMasterServiceConfig service_config;
        service_config.default_kv_lease_ttl = 100;
        service_config.enable_metric_reporting = false;
        service_config.enable_tenant_quota = true;
        auto service = std::make_shared<WrappedMasterService>(service_config);

        const int http_port = getFreeTcpPort();
        auto admin_server = std::make_unique<MasterAdminServer>(
            static_cast<uint16_t>(http_port),
            /*enable_metric_reporting=*/false);
        EXPECT_TRUE(admin_server->Start());
        admin_server->SetRuntimeState(ha::MasterRuntimeState::kServing);
        admin_server->SetServiceDelegate(service);
        admin_server->SetServiceAvailable(true);

        return {service, std::move(admin_server), http_port, {}};
    }

    // Start admin server with a mounted segment so that PutStart/PutEnd
    // can allocate real storage.
    AdminServerContext StartAdminServerWithSegment() {
        WrappedMasterServiceConfig service_config;
        service_config.default_kv_lease_ttl = 0;
        service_config.enable_metric_reporting = false;
        service_config.enable_tenant_quota = true;
        auto service = std::make_shared<WrappedMasterService>(service_config);

        // Mount a segment so PutStart can allocate.
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "test_segment";
        segment.base = kSegmentBase;
        segment.size = kSegmentSize;
        segment.te_endpoint = segment.name;
        UUID client_id = generate_uuid();
        EXPECT_TRUE(service->MountSegment(segment, client_id).has_value());

        const int http_port = getFreeTcpPort();
        auto admin_server = std::make_unique<MasterAdminServer>(
            static_cast<uint16_t>(http_port),
            /*enable_metric_reporting=*/false);
        EXPECT_TRUE(admin_server->Start());
        admin_server->SetRuntimeState(ha::MasterRuntimeState::kServing);
        admin_server->SetServiceDelegate(service);
        admin_server->SetServiceAvailable(true);

        return {service, std::move(admin_server), http_port, client_id};
    }
};

// --- GET /tenants/quota (list all) ---

TEST_F(TenantQuotaHttpTest, ListAllEmptyReturnsSuccess) {
    auto ctx = StartAdminServer();
    auto resp = HttpGet(ctx.http_port, "/tenants/quota");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"tenants\""), std::string::npos);
    ctx.admin_server->Stop();
}

// --- PUT + GET /tenants/quota/query?tenant_id=... ---

TEST_F(TenantQuotaHttpTest, PutAndGetTenantQuota) {
    auto ctx = StartAdminServer();

    // PUT a quota for tenant_a
    auto put_resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
                R"({"max_bytes": 1073741824})");
    EXPECT_EQ(put_resp.http_status, 200);
    EXPECT_NE(put_resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(put_resp.body.find("\"tenant_id\":\"tenant_a\""),
              std::string::npos);
    EXPECT_NE(put_resp.body.find("\"max_bytes\":1073741824"),
              std::string::npos);

    // GET the same tenant
    auto get_resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a");
    EXPECT_EQ(get_resp.http_status, 200);
    EXPECT_NE(get_resp.body.find("\"tenant_id\":\"tenant_a\""),
              std::string::npos);
    EXPECT_NE(get_resp.body.find("\"max_bytes\":1073741824"),
              std::string::npos);

    // Verify it appears in list
    auto list_resp = HttpGet(ctx.http_port, "/tenants/quota");
    EXPECT_EQ(list_resp.http_status, 200);
    EXPECT_NE(list_resp.body.find("tenant_a"), std::string::npos);

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutIsIdempotent) {
    auto ctx = StartAdminServer();

    // First PUT
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
            R"({"max_bytes": 1000})");

    // Update with new value
    auto put_resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
                R"({"max_bytes": 2000})");
    EXPECT_EQ(put_resp.http_status, 200);
    EXPECT_NE(put_resp.body.find("\"max_bytes\":2000"), std::string::npos);

    // Verify updated
    auto get_resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a");
    EXPECT_NE(get_resp.body.find("\"max_bytes\":2000"), std::string::npos);

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutInvalidJsonReturnsBadRequest) {
    auto ctx = StartAdminServer();

    auto resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
                "not valid json");
    EXPECT_EQ(resp.http_status, 400);
    // Invalid JSON triggers the from_json exception path.
    EXPECT_NE(resp.body.find("Invalid JSON body"), std::string::npos)
        << "body=" << resp.body;

    ctx.admin_server->Stop();
}

// --- GET /tenants/quota/query?tenant_id=... (not found) ---

TEST_F(TenantQuotaHttpTest, GetNonexistentTenantReturns404) {
    auto ctx = StartAdminServer();

    auto resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=nonexistent");
    EXPECT_EQ(resp.http_status, 404);
    EXPECT_NE(resp.body.find("Tenant not found"), std::string::npos);

    ctx.admin_server->Stop();
}

// --- DELETE /tenants/quota/query?tenant_id=... ---

TEST_F(TenantQuotaHttpTest, DeleteExistingTenantQuota) {
    auto ctx = StartAdminServer();

    // Create
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
            R"({"max_bytes": 1000})");

    // Delete
    auto del_resp =
        HttpDelete(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a");
    EXPECT_EQ(del_resp.http_status, 200);
    EXPECT_NE(del_resp.body.find("\"success\":true"), std::string::npos);

    // Verify gone
    auto get_resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a");
    EXPECT_EQ(get_resp.http_status, 404);

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, DeleteNonexistentReturns404) {
    auto ctx = StartAdminServer();

    auto resp =
        HttpDelete(ctx.http_port, "/tenants/quota/query?tenant_id=nonexistent");
    EXPECT_EQ(resp.http_status, 404);

    ctx.admin_server->Stop();
}

// --- GET/PUT /tenants/quota/_default ---

TEST_F(TenantQuotaHttpTest, GetDefaultQuota) {
    auto ctx = StartAdminServer();

    auto resp = HttpGet(ctx.http_port, "/tenants/quota/_default");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"tenant_id\":\"_default\""), std::string::npos);
    // Default is unlimited (0)
    EXPECT_NE(resp.body.find("\"max_bytes\":0"), std::string::npos);

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutDefaultQuota) {
    auto ctx = StartAdminServer();

    auto put_resp = HttpPut(ctx.http_port, "/tenants/quota/_default",
                            R"({"max_bytes": 5000})");
    EXPECT_EQ(put_resp.http_status, 200);
    EXPECT_NE(put_resp.body.find("\"max_bytes\":5000"), std::string::npos);

    // Verify via GET
    auto get_resp = HttpGet(ctx.http_port, "/tenants/quota/_default");
    EXPECT_NE(get_resp.body.find("\"max_bytes\":5000"), std::string::npos);

    ctx.admin_server->Stop();
}

// --- Service unavailable ---

TEST_F(TenantQuotaHttpTest, ReturnsServiceUnavailableWhenNoService) {
    const int http_port = getFreeTcpPort();
    MasterAdminServer admin_server(static_cast<uint16_t>(http_port),
                                   /*enable_metric_reporting=*/false);
    ASSERT_TRUE(admin_server.Start());
    // Do NOT set service delegate or availability

    auto resp = HttpGet(http_port, "/tenants/quota");
    EXPECT_EQ(resp.http_status, 503);
    EXPECT_NE(resp.body.find("service plane is not active"), std::string::npos);

    admin_server.Stop();
}

// --- Multiple tenants ---

TEST_F(TenantQuotaHttpTest, MultipleTenantsCRUD) {
    auto ctx = StartAdminServer();

    // Create 3 tenants
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=alpha",
            R"({"max_bytes": 100})");
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=beta",
            R"({"max_bytes": 200})");
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=gamma",
            R"({"max_bytes": 300})");

    // List all
    auto list_resp = HttpGet(ctx.http_port, "/tenants/quota");
    EXPECT_EQ(list_resp.http_status, 200);
    EXPECT_NE(list_resp.body.find("alpha"), std::string::npos);
    EXPECT_NE(list_resp.body.find("beta"), std::string::npos);
    EXPECT_NE(list_resp.body.find("gamma"), std::string::npos);

    // Delete one
    HttpDelete(ctx.http_port, "/tenants/quota/query?tenant_id=beta");

    // List again - beta should be gone
    auto list_resp2 = HttpGet(ctx.http_port, "/tenants/quota");
    EXPECT_EQ(list_resp2.http_status, 200);
    EXPECT_NE(list_resp2.body.find("alpha"), std::string::npos);
    EXPECT_EQ(list_resp2.body.find("beta"), std::string::npos);
    EXPECT_NE(list_resp2.body.find("gamma"), std::string::npos);

    ctx.admin_server->Stop();
}

// --- Zero means unlimited ---

TEST_F(TenantQuotaHttpTest, ZeroMaxBytesMeansUnlimited) {
    auto ctx = StartAdminServer();

    auto resp = HttpPut(ctx.http_port,
                        "/tenants/quota/query?tenant_id=tenant_unlimited",
                        R"({"max_bytes": 0})");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"max_bytes\":0"), std::string::npos);

    ctx.admin_server->Stop();
}

// --- Metrics endpoint includes tenant quota metrics ---

TEST_F(TenantQuotaHttpTest, MetricsEndpointIncludesTenantQuotaAfterPut) {
    auto ctx = StartAdminServer();

    // Set a quota via HTTP
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=metrics_test",
            R"({"max_bytes": 999999})");

    // Check /metrics for tenant quota gauges with correct label and value.
    auto resp = HttpGet(ctx.http_port, "/metrics");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(
        resp.body.find(
            "mooncake_tenant_quota_max_bytes{tenant_id=\"metrics_test\"}"),
        std::string::npos)
        << "max_bytes metric should have tenant_id label; body=" << resp.body;
    EXPECT_NE(resp.body.find("999999"), std::string::npos)
        << "max_bytes metric value should be 999999; body=" << resp.body;

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, DeleteTenantClearsMetrics) {
    auto ctx = StartAdminServer();

    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=del_metric",
            R"({"max_bytes": 123456})");

    // Verify metric appears.
    auto before = HttpGet(ctx.http_port, "/metrics");
    EXPECT_NE(before.body.find(
                  "mooncake_tenant_quota_max_bytes{tenant_id=\"del_metric\"}"),
              std::string::npos);

    // Delete the tenant.
    HttpDelete(ctx.http_port, "/tenants/quota/query?tenant_id=del_metric");

    // After delete, max_bytes should be reset to 0.
    auto after = HttpGet(ctx.http_port, "/metrics");
    // The label still appears (Prometheus gauges are not removed), but
    // the value should be 0 rather than the old 123456.
    EXPECT_EQ(after.body.find("123456"), std::string::npos)
        << "max_bytes value should be cleared after delete; body="
        << after.body;

    ctx.admin_server->Stop();
}

// --- State fields in response ---

TEST_F(TenantQuotaHttpTest, ResponseIncludesStateFields) {
    auto ctx = StartAdminServer();

    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=state_test",
            R"({"max_bytes": 5000})");

    auto resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=state_test");
    EXPECT_EQ(resp.http_status, 200);
    // State fields should be present (all zeros for fresh tenant)
    EXPECT_NE(resp.body.find("\"used_bytes\""), std::string::npos);
    EXPECT_NE(resp.body.find("\"reserved_bytes\""), std::string::npos);
    EXPECT_NE(resp.body.find("\"committed_count\""), std::string::npos);

    ctx.admin_server->Stop();
}

// =====================================================================
// End-to-end tests: PutStart/PutEnd/Remove + HTTP quota query
// =====================================================================

// After PutStart + PutEnd, the HTTP query should reflect used_bytes.
// After Remove, used_bytes should drop back to zero.
TEST_F(TenantQuotaHttpTest, PutEndUpdatesUsedBytesVisibleViaHttp) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kValueLen = 1024 * 1024;  // 1 MiB

    // Set a quota for the tenant first.
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=e2e_tenant",
            R"({"max_bytes": 67108864})");

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "e2e_tenant";

    // PutStart + PutEnd: commit 1 MiB.
    ASSERT_TRUE(ctx.service->PutStart(ctx.client_id, "k1", kValueLen, config)
                    .has_value());
    ASSERT_TRUE(ctx.service->PutEnd(ctx.client_id, "k1", ReplicaType::MEMORY)
                    .has_value());

    // HTTP query should show used_bytes == kValueLen.
    auto resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=e2e_tenant");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"used_bytes\":" + std::to_string(kValueLen)),
              std::string::npos)
        << "used_bytes should reflect committed object; body=" << resp.body;
    EXPECT_NE(resp.body.find("\"committed_count\":1"), std::string::npos);

    // Remove the object, used_bytes should drop to 0.
    ASSERT_TRUE(ctx.service->Remove("k1").has_value());

    auto resp2 =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=e2e_tenant");
    EXPECT_EQ(resp2.http_status, 200);
    EXPECT_NE(resp2.body.find("\"used_bytes\":0"), std::string::npos)
        << "used_bytes should be 0 after Remove; body=" << resp2.body;

    ctx.admin_server->Stop();
}

// PutRevoke (abort before PutEnd) should release bytes visible via HTTP.
TEST_F(TenantQuotaHttpTest, PutRevokeReleasesBytes) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kValueLen = 2 * 1024 * 1024;  // 2 MiB

    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=revoke_tenant",
            R"({"max_bytes": 67108864})");

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "revoke_tenant";

    ASSERT_TRUE(
        ctx.service->PutStart(ctx.client_id, "k_revoke", kValueLen, config)
            .has_value());

    // After PutStart, bytes are charged (reserve -> commit happens
    // atomically inside AllocateAndInsertMetadata).
    auto mid_resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=revoke_tenant");
    EXPECT_EQ(mid_resp.http_status, 200);
    EXPECT_NE(mid_resp.body.find("\"used_bytes\":" + std::to_string(kValueLen)),
              std::string::npos)
        << "used_bytes should be charged after PutStart; body="
        << mid_resp.body;

    // Revoke (client decided not to write the data).
    ASSERT_TRUE(
        ctx.service->PutRevoke(ctx.client_id, "k_revoke", ReplicaType::MEMORY)
            .has_value());

    auto after_resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=revoke_tenant");
    EXPECT_EQ(after_resp.http_status, 200);
    EXPECT_NE(after_resp.body.find("\"used_bytes\":0"), std::string::npos)
        << "used_bytes should be 0 after PutRevoke; body=" << after_resp.body;

    ctx.admin_server->Stop();
}

// Multiple objects from the same tenant accumulate used_bytes correctly.
TEST_F(TenantQuotaHttpTest, MultipleObjectsAccumulateUsedBytes) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kObjSize = 512 * 1024;  // 512 KiB
    constexpr int kNumObjects = 4;

    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=accum_tenant",
            R"({"max_bytes": 67108864})");

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "accum_tenant";

    for (int i = 0; i < kNumObjects; ++i) {
        std::string key = "accum_key_" + std::to_string(i);
        ASSERT_TRUE(ctx.service->PutStart(ctx.client_id, key, kObjSize, config)
                        .has_value());
        ASSERT_TRUE(ctx.service->PutEnd(ctx.client_id, key, ReplicaType::MEMORY)
                        .has_value());
    }

    auto resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=accum_tenant");
    EXPECT_EQ(resp.http_status, 200);
    uint64_t expected_used = kObjSize * kNumObjects;
    EXPECT_NE(resp.body.find("\"used_bytes\":" + std::to_string(expected_used)),
              std::string::npos)
        << "used_bytes should be " << expected_used << "; body=" << resp.body;
    EXPECT_NE(
        resp.body.find("\"committed_count\":" + std::to_string(kNumObjects)),
        std::string::npos);

    // Remove half, verify partial release.
    for (int i = 0; i < kNumObjects / 2; ++i) {
        std::string key = "accum_key_" + std::to_string(i);
        ASSERT_TRUE(ctx.service->Remove(key).has_value());
    }

    auto resp2 =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=accum_tenant");
    uint64_t expected_after_remove = kObjSize * (kNumObjects / 2);
    EXPECT_NE(resp2.body.find("\"used_bytes\":" +
                              std::to_string(expected_after_remove)),
              std::string::npos)
        << "used_bytes should be " << expected_after_remove
        << " after partial remove; body=" << resp2.body;

    ctx.admin_server->Stop();
}

// Cross-tenant isolation: PutStart for tenant_a does not affect tenant_b
// usage visible via HTTP.
TEST_F(TenantQuotaHttpTest, CrossTenantIsolationViaHttp) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kObjSize = 1024 * 1024;

    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=iso_a",
            R"({"max_bytes": 67108864})");
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=iso_b",
            R"({"max_bytes": 67108864})");

    ReplicateConfig config_a;
    config_a.replica_num = 1;
    config_a.tenant_id = "iso_a";

    ReplicateConfig config_b;
    config_b.replica_num = 1;
    config_b.tenant_id = "iso_b";

    // Put 2 objects for tenant_a.
    for (int i = 0; i < 2; ++i) {
        std::string key = "iso_a_key_" + std::to_string(i);
        ASSERT_TRUE(
            ctx.service->PutStart(ctx.client_id, key, kObjSize, config_a)
                .has_value());
        ASSERT_TRUE(ctx.service->PutEnd(ctx.client_id, key, ReplicaType::MEMORY)
                        .has_value());
    }

    // Put 1 object for tenant_b.
    ASSERT_TRUE(
        ctx.service->PutStart(ctx.client_id, "iso_b_key_0", kObjSize, config_b)
            .has_value());
    ASSERT_TRUE(
        ctx.service->PutEnd(ctx.client_id, "iso_b_key_0", ReplicaType::MEMORY)
            .has_value());

    // Verify tenant_a shows 2 MiB used.
    auto resp_a =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=iso_a");
    EXPECT_NE(
        resp_a.body.find("\"used_bytes\":" + std::to_string(kObjSize * 2)),
        std::string::npos)
        << "tenant_a should have 2 MiB used; body=" << resp_a.body;

    // Verify tenant_b shows 1 MiB used, unaffected by tenant_a.
    auto resp_b =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=iso_b");
    EXPECT_NE(resp_b.body.find("\"used_bytes\":" + std::to_string(kObjSize)),
              std::string::npos)
        << "tenant_b should have 1 MiB used; body=" << resp_b.body;

    // List should show both tenants.
    auto list_resp = HttpGet(ctx.http_port, "/tenants/quota");
    EXPECT_NE(list_resp.body.find("iso_a"), std::string::npos);
    EXPECT_NE(list_resp.body.find("iso_b"), std::string::npos);

    ctx.admin_server->Stop();
}

// Quota exhaustion: PutStart should fail when tenant quota is exceeded
// and all objects are hard-pinned (nothing evictable).
TEST_F(TenantQuotaHttpTest, QuotaExhaustionRejectsViaHttp) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kObjSize = 1024 * 1024;
    constexpr uint64_t kTenantMax = 2 * kObjSize;

    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=exhaust_tenant",
            R"({"max_bytes": 2097152})");

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "exhaust_tenant";
    config.with_hard_pin = true;  // prevent eviction

    // Fill the quota with 2 hard-pinned objects.
    for (int i = 0; i < 2; ++i) {
        std::string key = "exhaust_key_" + std::to_string(i);
        ASSERT_TRUE(ctx.service->PutStart(ctx.client_id, key, kObjSize, config)
                        .has_value());
        ASSERT_TRUE(ctx.service->PutEnd(ctx.client_id, key, ReplicaType::MEMORY)
                        .has_value());
    }

    // Verify quota is full via HTTP.
    auto resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=exhaust_tenant");
    EXPECT_NE(resp.body.find("\"used_bytes\":" + std::to_string(kTenantMax)),
              std::string::npos)
        << "tenant should be at quota; body=" << resp.body;

    // Third PutStart should fail (quota exhausted, nothing evictable).
    auto overflow_result =
        ctx.service->PutStart(ctx.client_id, "overflow_key", kObjSize, config);
    EXPECT_FALSE(overflow_result.has_value())
        << "PutStart should fail when quota is exhausted";

    // used_bytes should remain unchanged after the rejected put.
    auto resp2 =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=exhaust_tenant");
    EXPECT_NE(resp2.body.find("\"used_bytes\":" + std::to_string(kTenantMax)),
              std::string::npos)
        << "used_bytes should not change after rejected put; body="
        << resp2.body;

    // Reject counter should be visible in /metrics.
    auto metrics_resp = HttpGet(ctx.http_port, "/metrics");
    EXPECT_NE(metrics_resp.body.find("mooncake_tenant_quota_reject_total"
                                     "{tenant_id=\"exhaust_tenant\"}"),
              std::string::npos)
        << "reject counter should have exhaust_tenant label; body="
        << metrics_resp.body;

    ctx.admin_server->Stop();
}

// Metrics endpoint should reflect used_bytes after real writes.
TEST_F(TenantQuotaHttpTest, MetricsReflectRealWriteUsage) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kObjSize = 1024 * 1024;

    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=metric_write_tenant",
            R"({"max_bytes": 67108864})");

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "metric_write_tenant";

    ASSERT_TRUE(ctx.service->PutStart(ctx.client_id, "mw_key", kObjSize, config)
                    .has_value());
    ASSERT_TRUE(
        ctx.service->PutEnd(ctx.client_id, "mw_key", ReplicaType::MEMORY)
            .has_value());

    auto resp = HttpGet(ctx.http_port, "/metrics");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("mooncake_tenant_quota_used_bytes"
                             "{tenant_id=\"metric_write_tenant\"}"),
              std::string::npos)
        << "used_bytes metric should have correct tenant_id label; body="
        << resp.body;
    EXPECT_NE(resp.body.find("mooncake_tenant_quota_max_bytes"
                             "{tenant_id=\"metric_write_tenant\"}"),
              std::string::npos)
        << "max_bytes metric should have correct tenant_id label; body="
        << resp.body;

    ctx.admin_server->Stop();
}

// Delete policy while tenant still has alive objects: metrics should
// reflect actual usage (not be zeroed out), and max_bytes should show
// the effective (default) policy.
TEST_F(TenantQuotaHttpTest, DeletePolicyWithAliveObjectsPreservesMetrics) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kObjSize = 1024 * 1024;  // 1 MiB

    // Set explicit quota for the tenant.
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=alive_tenant",
            R"({"max_bytes": 67108864})");

    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "alive_tenant";

    // Write an object so the tenant has nonzero used_bytes.
    ASSERT_TRUE(
        ctx.service->PutStart(ctx.client_id, "alive_key", kObjSize, config)
            .has_value());
    ASSERT_TRUE(
        ctx.service->PutEnd(ctx.client_id, "alive_key", ReplicaType::MEMORY)
            .has_value());

    // Confirm used_bytes via HTTP before delete.
    auto before =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=alive_tenant");
    EXPECT_EQ(before.http_status, 200);
    EXPECT_NE(before.body.find("\"used_bytes\":" + std::to_string(kObjSize)),
              std::string::npos);

    // Delete the explicit policy. The entry should survive because
    // used_bytes > 0, and policy falls back to default (max_bytes=0).
    auto del_resp = HttpDelete(ctx.http_port,
                               "/tenants/quota/query?tenant_id=alive_tenant");
    EXPECT_EQ(del_resp.http_status, 200);

    // HTTP query should still return the tenant (entry preserved).
    auto after =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=alive_tenant");
    EXPECT_EQ(after.http_status, 200);
    // used_bytes must still reflect the alive object.
    EXPECT_NE(after.body.find("\"used_bytes\":" + std::to_string(kObjSize)),
              std::string::npos)
        << "used_bytes should be preserved after policy delete; body="
        << after.body;
    // max_bytes should be 0 (default policy = unlimited).
    EXPECT_NE(after.body.find("\"max_bytes\":0"), std::string::npos)
        << "max_bytes should fall back to default (0); body=" << after.body;

    // /metrics should also reflect real used_bytes, not zero.
    auto metrics_resp = HttpGet(ctx.http_port, "/metrics");
    EXPECT_NE(metrics_resp.body.find("mooncake_tenant_quota_used_bytes"
                                     "{tenant_id=\"alive_tenant\"}"),
              std::string::npos)
        << "used_bytes metric label should still exist; body="
        << metrics_resp.body;
    // The used_bytes metric value must NOT be zero.
    // Search for the specific line and verify.
    auto used_pos = metrics_resp.body.find(
        "mooncake_tenant_quota_used_bytes"
        "{tenant_id=\"alive_tenant\"}");
    if (used_pos != std::string::npos) {
        // Extract the value after the label (space + number + newline).
        auto val_start = metrics_resp.body.find(' ', used_pos);
        ASSERT_NE(val_start, std::string::npos);
        auto val_end = metrics_resp.body.find('\n', val_start);
        std::string val_str =
            metrics_resp.body.substr(val_start + 1, val_end - val_start - 1);
        EXPECT_NE(val_str, "0")
            << "used_bytes metric should NOT be 0 while object is alive; "
               "got="
            << val_str;
    }

    // Now remove the object. After that, used_bytes should be 0.
    ASSERT_TRUE(ctx.service->Remove("alive_key").has_value());

    auto final_resp =
        HttpGet(ctx.http_port, "/tenants/quota/query?tenant_id=alive_tenant");
    // Entry may or may not exist after Remove (depends on whether
    // ReleaseTenantBytes triggers cleanup). Either 200 with used_bytes=0
    // or 404 is acceptable.
    if (final_resp.http_status == 200) {
        EXPECT_NE(final_resp.body.find("\"used_bytes\":0"), std::string::npos)
            << "used_bytes should be 0 after Remove; body=" << final_resp.body;
    }

    ctx.admin_server->Stop();
}

// =====================================================================
// Body validation: PUT must contain "max_bytes" field
// =====================================================================

TEST_F(TenantQuotaHttpTest, PutEmptyBodyReturnsBadRequest) {
    auto ctx = StartAdminServer();

    auto resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a", "{}");
    EXPECT_EQ(resp.http_status, 400);
    EXPECT_NE(resp.body.find("max_bytes"), std::string::npos)
        << "error should mention max_bytes; body=" << resp.body;

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutDefaultEmptyBodyReturnsBadRequest) {
    auto ctx = StartAdminServer();

    auto resp = HttpPut(ctx.http_port, "/tenants/quota/_default", "{}");
    EXPECT_EQ(resp.http_status, 400);
    EXPECT_NE(resp.body.find("max_bytes"), std::string::npos)
        << "error should mention max_bytes; body=" << resp.body;

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutEmptyStringBodyReturnsBadRequest) {
    auto ctx = StartAdminServer();

    auto resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a", "");
    EXPECT_EQ(resp.http_status, 400);

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutNestedMaxBytesReturnsBadRequest) {
    auto ctx = StartAdminServer();

    // "max_bytes" exists only inside a nested object, not at top level.
    auto resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
                R"({"nested":{"max_bytes":1}})");
    EXPECT_EQ(resp.http_status, 400);
    EXPECT_NE(resp.body.find("max_bytes"), std::string::npos)
        << "body=" << resp.body;

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutMaxBytesInStringValueReturnsBadRequest) {
    auto ctx = StartAdminServer();

    // "max_bytes" appears only inside a string value, not as a field name.
    auto resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
                R"({"comment":"\"max_bytes\""})");
    EXPECT_EQ(resp.http_status, 400);
    EXPECT_NE(resp.body.find("max_bytes"), std::string::npos)
        << "body=" << resp.body;

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutMaxBytesNullReturnsBadRequest) {
    auto ctx = StartAdminServer();

    auto resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
                R"({"max_bytes":null})");
    EXPECT_EQ(resp.http_status, 400);
    EXPECT_NE(resp.body.find("max_bytes"), std::string::npos)
        << "body=" << resp.body;

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, PutMaxBytesStringTypeReturnsBadRequest) {
    auto ctx = StartAdminServer();

    auto resp =
        HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=tenant_a",
                R"({"max_bytes":"123"})");
    EXPECT_EQ(resp.http_status, 400);

    ctx.admin_server->Stop();
}

// =====================================================================
// Default quota metrics sync
// =====================================================================

TEST_F(TenantQuotaHttpTest, PutDefaultQuotaUpdatesMetrics) {
    auto ctx = StartAdminServer();

    HttpPut(ctx.http_port, "/tenants/quota/_default",
            R"({"max_bytes": 8888888})");

    auto resp = HttpGet(ctx.http_port, "/metrics");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(
        resp.body.find(
            "mooncake_tenant_quota_max_bytes{tenant_id=\"_default\"} 8888888"),
        std::string::npos)
        << "_default max_bytes metric should be 8888888; body=" << resp.body;

    ctx.admin_server->Stop();
}

TEST_F(TenantQuotaHttpTest, ChangeDefaultPolicySyncsInheritingTenantMetrics) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kObjSize = 1024 * 1024;

    // Create a tenant via a real write (no explicit policy → inherits
    // default).
    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "inherit_tenant";

    ASSERT_TRUE(
        ctx.service->PutStart(ctx.client_id, "inherit_key", kObjSize, config)
            .has_value());
    ASSERT_TRUE(
        ctx.service->PutEnd(ctx.client_id, "inherit_key", ReplicaType::MEMORY)
            .has_value());

    // Change default policy via HTTP.
    HttpPut(ctx.http_port, "/tenants/quota/_default",
            R"({"max_bytes": 7777777})");

    // The inheriting tenant's max_bytes gauge should now reflect
    // the new default.
    auto resp = HttpGet(ctx.http_port, "/metrics");
    EXPECT_NE(resp.body.find("mooncake_tenant_quota_max_bytes"
                             "{tenant_id=\"inherit_tenant\"} 7777777"),
              std::string::npos)
        << "inheriting tenant max_bytes should sync to new default; body="
        << resp.body;

    // A tenant with explicit policy should NOT be affected.
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=explicit_tenant",
            R"({"max_bytes": 1111111})");
    HttpPut(ctx.http_port, "/tenants/quota/_default",
            R"({"max_bytes": 9999999})");

    auto resp2 = HttpGet(ctx.http_port, "/metrics");
    EXPECT_NE(resp2.body.find("mooncake_tenant_quota_max_bytes"
                              "{tenant_id=\"explicit_tenant\"} 1111111"),
              std::string::npos)
        << "explicit tenant should keep its own max_bytes; body=" << resp2.body;

    ctx.admin_server->Stop();
}

// Upsert should refresh all three gauge metrics from snapshot.
TEST_F(TenantQuotaHttpTest, UpsertRefreshesFullSnapshotMetrics) {
    auto ctx = StartAdminServerWithSegment();
    constexpr uint64_t kObjSize = 1024 * 1024;

    // Write an object to create usage first (inherits default policy).
    ReplicateConfig config;
    config.replica_num = 1;
    config.tenant_id = "upsert_metric_t";

    ASSERT_TRUE(ctx.service->PutStart(ctx.client_id, "um_key", kObjSize, config)
                    .has_value());
    ASSERT_TRUE(
        ctx.service->PutEnd(ctx.client_id, "um_key", ReplicaType::MEMORY)
            .has_value());

    // Now upsert an explicit policy.
    HttpPut(ctx.http_port, "/tenants/quota/query?tenant_id=upsert_metric_t",
            R"({"max_bytes": 5555555})");

    // All three gauges should be present with correct values.
    auto resp = HttpGet(ctx.http_port, "/metrics");
    EXPECT_NE(resp.body.find("mooncake_tenant_quota_max_bytes"
                             "{tenant_id=\"upsert_metric_t\"} 5555555"),
              std::string::npos)
        << "max_bytes should be 5555555; body=" << resp.body;
    EXPECT_NE(resp.body.find("mooncake_tenant_quota_used_bytes"
                             "{tenant_id=\"upsert_metric_t\"} " +
                             std::to_string(kObjSize)),
              std::string::npos)
        << "used_bytes should reflect alive object; body=" << resp.body;

    ctx.admin_server->Stop();
}

}  // namespace mooncake::test
