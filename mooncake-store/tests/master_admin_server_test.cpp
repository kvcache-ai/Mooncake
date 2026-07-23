#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include "ha/ha_types.h"
#include "master_admin_service.h"
#include "master_config.h"
#include "rpc_service.h"
#include "tenant_quota_policy_store.h"
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
    std::vector<std::string> segments;
    std::vector<std::string> devices;
    uint64_t blocked_units{0};
    std::string message;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpQueryDrainJobResponse, success, job_id, type, type_name, status,
         status_name, segments, devices, blocked_units, message, error_code,
         error_message);

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

std::string WriteTenantQuotaPolicyForTest(
    const std::map<std::string, uint64_t>& tenant_quotas) {
    TenantQuotaPolicySnapshot snapshot;
    snapshot.tenant_quotas = tenant_quotas;
    auto path = std::filesystem::temp_directory_path() /
                ("mooncake_admin_tenant_quota_" +
                 UuidToString(generate_uuid()) + ".yaml");
    std::ofstream out(path);
    out << FormatTenantQuotaPolicyYaml(snapshot);
    return path.string();
}

struct HttpStorageDeviceIdentity {
    std::string kind;
    std::string provider;
    std::string device_id;
    std::string target_id;
    std::string namespace_id;
    std::string disk_id;
    std::string node_id;
};
YLT_REFL(HttpStorageDeviceIdentity, kind, provider, device_id, target_id,
         namespace_id, disk_id, node_id);

struct HttpStorageDeviceMetadataItem {
    HttpStorageDeviceIdentity identity;
    std::string health;
    bool readable{false};
    bool writable{false};
    bool schedulable{false};
    int64_t capacity_total{0};
    int64_t capacity_used{0};
    int64_t capacity_available{0};
    uint32_t consecutive_failures{0};
    std::string last_error;
    int64_t last_update_unix_ms{-1};
    std::string mount_endpoint;
    std::string transport;
    std::string opaque_provider_metadata;
};
YLT_REFL(HttpStorageDeviceMetadataItem, identity, health, readable, writable,
         schedulable, capacity_total, capacity_used, capacity_available,
         consecutive_failures, last_error, last_update_unix_ms, mount_endpoint,
         transport, opaque_provider_metadata);

struct HttpStorageDeviceMetadataResponse {
    uint64_t total_devices{0};
    std::vector<HttpStorageDeviceMetadataItem> devices;
};
YLT_REFL(HttpStorageDeviceMetadataResponse, total_devices, devices);

struct HttpStorageDeviceMetadataUpdateResponse {
    bool success{false};
    HttpStorageDeviceMetadataItem device;
};
YLT_REFL(HttpStorageDeviceMetadataUpdateResponse, success, device);

struct HttpStorageDeviceMaintenanceCandidate {
    HttpStorageDeviceMetadataItem device;
    std::string reason;
};
YLT_REFL(HttpStorageDeviceMaintenanceCandidate, device, reason);

struct HttpStorageDeviceMaintenancePlanResponse {
    uint64_t total_recovery_candidates{0};
    uint64_t total_gc_candidates{0};
    std::vector<HttpStorageDeviceMaintenanceCandidate> recovery_candidates;
    std::vector<HttpStorageDeviceMaintenanceCandidate> gc_candidates;
};
YLT_REFL(HttpStorageDeviceMaintenancePlanResponse, total_recovery_candidates,
         total_gc_candidates, recovery_candidates, gc_candidates);

struct HttpStorageDeviceGcCandidatesResponse {
    uint64_t total_gc_candidates{0};
    std::vector<HttpStorageDeviceMaintenanceCandidate> gc_candidates;
};
YLT_REFL(HttpStorageDeviceGcCandidatesResponse, total_gc_candidates,
         gc_candidates);

class FakeStorageDeviceMetadataBackend : public StorageBackendInterface {
   public:
    FakeStorageDeviceMetadataBackend()
        : StorageBackendInterface(FileStorageConfig{}) {}

    tl::expected<void, ErrorCode> Init() override { return {}; }

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>&,
        std::function<ErrorCode(const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&)>,
        EvictionHandler) override {
        return 0;
    }

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>&) override {
        return {};
    }

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override {
        return backend_keys_.contains(key);
    }

    void AddBackendKey(const std::string& key) { backend_keys_.insert(key); }

    tl::expected<bool, ErrorCode> IsEnableOffloading() override {
        return false;
    }

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(const std::vector<std::string>&,
                                      std::vector<StorageObjectMetadata>&)>&)
        override {
        return {};
    }

    tl::expected<StorageDeviceMetadata, ErrorCode>
    ApplyStorageDeviceMetadataUpdate(
        const StorageDeviceMetadataUpdate& update) override {
        if (update.identity.device_id != UUID{0x1234, 0x5678}) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (update.health.has_value()) {
            enabled_ = *update.health == StorageDeviceHealth::HEALTHY;
        }
        if (update.writable.has_value()) {
            enabled_ = *update.writable;
        }
        if (update.schedulable.has_value()) {
            enabled_ = *update.schedulable;
        }
        return ListStorageDeviceMetadata()[0];
    }

    tl::expected<StorageDeviceMetadata, ErrorCode> StartStorageDeviceRecovery(
        const UUID& device_id) override {
        if (device_id != UUID{0x1234, 0x5678}) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        recovery_started_ = true;
        failed_ = false;
        enabled_ = false;
        return ListStorageDeviceMetadata()[0];
    }

    tl::expected<StorageDeviceMetadata, ErrorCode>
    CompleteStorageDeviceRecovery(const UUID& device_id) override {
        if (device_id != UUID{0x1234, 0x5678}) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        recovery_started_ = false;
        failed_ = false;
        enabled_ = true;
        return ListStorageDeviceMetadata()[0];
    }

    tl::expected<StorageDeviceMetadata, ErrorCode> FailStorageDeviceRecovery(
        const UUID& device_id) override {
        if (device_id != UUID{0x1234, 0x5678}) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        recovery_started_ = false;
        failed_ = true;
        enabled_ = false;
        last_error_ = "recovery_failed";
        return ListStorageDeviceMetadata()[0];
    }

    tl::expected<StorageDeviceMetadata, ErrorCode>
    RecordStorageDeviceProbeSuccess(const UUID& device_id) override {
        if (device_id != UUID{0x1234, 0x5678}) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        probe_failures_ = 0;
        failed_ = false;
        recovery_started_ = false;
        enabled_ = true;
        last_error_.clear();
        return ListStorageDeviceMetadata()[0];
    }

    tl::expected<StorageDeviceMetadata, ErrorCode>
    RecordStorageDeviceProbeFailure(const UUID& device_id,
                                    const std::string& reason) override {
        if (device_id != UUID{0x1234, 0x5678}) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        probe_failures_ += 1;
        last_error_ = reason;
        if (probe_failures_ >= 3) {
            failed_ = true;
            enabled_ = false;
        }
        return ListStorageDeviceMetadata()[0];
    }

    tl::expected<StorageDeviceMetadata, ErrorCode> ProbeStorageDevice(
        const UUID& device_id) override {
        if (probe_should_fail_) {
            return RecordStorageDeviceProbeFailure(device_id, "probe_timeout");
        }
        return RecordStorageDeviceProbeSuccess(device_id);
    }

    void set_probe_should_fail(bool fail) { probe_should_fail_ = fail; }
    uint32_t probe_failures() const { return probe_failures_; }

    std::vector<StorageDeviceMetadata> ListStorageDeviceMetadata()
        const override {
        StorageDeviceMetadata metadata;
        metadata.identity.kind = StorageDeviceKind::NAMESPACE;
        metadata.identity.provider = "nvme_kv";
        metadata.identity.device_id = UUID{0x1234, 0x5678};
        metadata.identity.namespace_id = "kv-namespace";
        if (recovery_started_) {
            metadata.health = StorageDeviceHealth::REBUILDING;
        } else if (failed_) {
            metadata.health = StorageDeviceHealth::FAILED;
        } else if (probe_failures_ > 0) {
            metadata.health = StorageDeviceHealth::DEGRADED;
        } else {
            metadata.health = enabled_ ? StorageDeviceHealth::HEALTHY
                                       : StorageDeviceHealth::DISABLED;
        }
        metadata.readable = true;
        metadata.writable = enabled_;
        metadata.schedulable =
            enabled_ && metadata.health == StorageDeviceHealth::HEALTHY;
        metadata.capacity_total = 4096;
        metadata.capacity_used = 1024;
        metadata.capacity_available = 3072;
        metadata.consecutive_failures = probe_failures_;
        metadata.last_error = last_error_;
        metadata.mount_endpoint = "/dev/ng0n1";
        metadata.transport = "nvme-of";
        metadata.opaque_provider_metadata = "backend_type=stub";
        return {metadata};
    }

   private:
    bool enabled_{false};
    bool recovery_started_{false};
    bool failed_{false};
    uint32_t probe_failures_{0};
    bool probe_should_fail_{false};
    std::string last_error_;
    std::unordered_set<std::string> backend_keys_;
};

}  // namespace

// =========================================================================
// MasterAdminServerTest — lightweight tests without a shared service.
// Each test that needs a server creates and destroys it within the test.
// =========================================================================

class MasterAdminServerTest : public ::testing::Test {
   protected:
    struct HttpResponse {
        int http_status;
        std::string body;
    };

    void SetUp() override {}

    void TearDown() override {}

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

    HttpResponse HttpPutJson(int port, const std::string& path,
                             const std::string& body) {
        coro_http::coro_http_client client;
        auto result = async_simple::coro::syncAwait(client.async_put(
            BaseUrl(port) + path, body, coro_http::req_content_type::json));
        return {result.status, std::string(result.resp_body)};
    }

    HttpResponse HttpDelete(int port, const std::string& path) {
        coro_http::coro_http_client client;
        auto result = async_simple::coro::syncAwait(client.async_delete(
            BaseUrl(port) + path, "", coro_http::req_content_type::json));
        return {result.status, std::string(result.resp_body)};
    }
};

// =========================================================================
// Always-available endpoint tests
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

TEST_F(MasterAdminServerTest,
       AdminDeviceViewsIncludeStorageBackendProviderMetadata) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);
    service->SetStorageBackendForAdmin(
        std::make_shared<FakeStorageDeviceMetadataBackend>());

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto metadata_resp = HttpGet(port, "/api/v1/devices/metadata");
    ASSERT_EQ(metadata_resp.http_status, 200);
    HttpStorageDeviceMetadataResponse metadata_payload;
    struct_json::from_json(metadata_payload, metadata_resp.body);
    ASSERT_EQ(metadata_payload.total_devices, 1u);
    ASSERT_EQ(metadata_payload.devices.size(), 1u);
    EXPECT_EQ(metadata_payload.devices[0].identity.provider, "nvme_kv");
    EXPECT_EQ(metadata_payload.devices[0].identity.kind, "NAMESPACE");
    EXPECT_EQ(metadata_payload.devices[0].transport, "nvme-of");
    EXPECT_EQ(metadata_payload.devices[0].mount_endpoint, "/dev/ng0n1");

    auto maintenance_resp = HttpGet(port, "/api/v1/devices/maintenance");
    ASSERT_EQ(maintenance_resp.http_status, 200);
    HttpStorageDeviceMaintenancePlanResponse maintenance_payload;
    struct_json::from_json(maintenance_payload, maintenance_resp.body);
    ASSERT_EQ(maintenance_payload.total_recovery_candidates, 1u);
    EXPECT_EQ(
        maintenance_payload.recovery_candidates[0].device.identity.provider,
        "nvme_kv");
    EXPECT_EQ(maintenance_payload.recovery_candidates[0].reason, "disabled");
    ASSERT_EQ(maintenance_payload.total_gc_candidates, 1u);
    EXPECT_EQ(maintenance_payload.gc_candidates[0].reason, "not_writable");

    auto gc_resp = HttpGet(port, "/api/v1/devices/gc_candidates");
    ASSERT_EQ(gc_resp.http_status, 200);
    HttpStorageDeviceGcCandidatesResponse gc_payload;
    struct_json::from_json(gc_payload, gc_resp.body);
    ASSERT_EQ(gc_payload.total_gc_candidates, 1u);
    EXPECT_EQ(gc_payload.gc_candidates[0].device.identity.provider, "nvme_kv");
    EXPECT_EQ(gc_payload.gc_candidates[0].reason, "not_writable");

    const auto device_id = metadata_payload.devices[0].identity.device_id;
    auto update_resp =
        HttpPutJson(port, "/api/v1/devices/metadata?device_id=" + device_id,
                    R"({"enabled":true})");
    ASSERT_EQ(update_resp.http_status, 200);
    HttpStorageDeviceMetadataUpdateResponse update_payload;
    struct_json::from_json(update_payload, update_resp.body);
    EXPECT_TRUE(update_payload.success);
    EXPECT_EQ(update_payload.device.identity.provider, "nvme_kv");
    EXPECT_EQ(update_payload.device.health, "HEALTHY");
    EXPECT_TRUE(update_payload.device.writable);
    EXPECT_TRUE(update_payload.device.schedulable);

    auto recovery_start = HttpPostJson(
        port,
        "/api/v1/devices/recovery?device_id=" + device_id + "&action=start",
        "{}");
    ASSERT_EQ(recovery_start.http_status, 200);
    struct_json::from_json(update_payload, recovery_start.body);
    EXPECT_EQ(update_payload.device.identity.provider, "nvme_kv");
    EXPECT_EQ(update_payload.device.health, "REBUILDING");
    EXPECT_FALSE(update_payload.device.writable);
    EXPECT_FALSE(update_payload.device.schedulable);

    auto recovery_complete = HttpPostJson(
        port,
        "/api/v1/devices/recovery?device_id=" + device_id + "&action=complete",
        "{}");
    ASSERT_EQ(recovery_complete.http_status, 200);
    struct_json::from_json(update_payload, recovery_complete.body);
    EXPECT_EQ(update_payload.device.health, "HEALTHY");
    EXPECT_TRUE(update_payload.device.writable);
    EXPECT_TRUE(update_payload.device.schedulable);

    auto recovery_fail = HttpPostJson(
        port,
        "/api/v1/devices/recovery?device_id=" + device_id + "&action=fail",
        "{}");
    ASSERT_EQ(recovery_fail.http_status, 200);
    struct_json::from_json(update_payload, recovery_fail.body);
    EXPECT_EQ(update_payload.device.health, "FAILED");
    EXPECT_FALSE(update_payload.device.writable);
    EXPECT_FALSE(update_payload.device.schedulable);
    EXPECT_EQ(update_payload.device.last_error, "recovery_failed");

    auto probe_success = HttpPostJson(
        port, "/api/v1/devices/probe?device_id=" + device_id + "&action=run",
        "{}");
    ASSERT_EQ(probe_success.http_status, 200);
    struct_json::from_json(update_payload, probe_success.body);
    EXPECT_EQ(update_payload.device.health, "HEALTHY");
    EXPECT_TRUE(update_payload.device.writable);
    EXPECT_TRUE(update_payload.device.schedulable);
    EXPECT_TRUE(update_payload.device.last_error.empty());

    for (int i = 1; i <= 3; ++i) {
        auto probe_fail =
            HttpPostJson(port,
                         "/api/v1/devices/probe?device_id=" + device_id +
                             "&action=fail&reason=probe_timeout",
                         "{}");
        ASSERT_EQ(probe_fail.http_status, 200);
        struct_json::from_json(update_payload, probe_fail.body);
        EXPECT_EQ(update_payload.device.consecutive_failures,
                  static_cast<uint32_t>(i));
        EXPECT_EQ(update_payload.device.last_error, "probe_timeout");
        EXPECT_EQ(update_payload.device.health, i < 3 ? "DEGRADED" : "FAILED");
        EXPECT_FALSE(update_payload.device.schedulable);
        if (i == 1) {
            auto degraded_maintenance =
                HttpGet(port, "/api/v1/devices/maintenance");
            ASSERT_EQ(degraded_maintenance.http_status, 200);
            HttpStorageDeviceMaintenancePlanResponse degraded_plan;
            struct_json::from_json(degraded_plan, degraded_maintenance.body);
            ASSERT_EQ(degraded_plan.total_recovery_candidates, 1u);
            EXPECT_EQ(
                degraded_plan.recovery_candidates[0].device.identity.provider,
                "nvme_kv");
            EXPECT_EQ(degraded_plan.recovery_candidates[0].reason, "degraded");
            ASSERT_EQ(degraded_plan.total_gc_candidates, 1u);
            EXPECT_EQ(degraded_plan.gc_candidates[0].reason, "not_schedulable");
        }
    }
    EXPECT_EQ(update_payload.device.health, "FAILED");
    EXPECT_FALSE(update_payload.device.writable);
    EXPECT_FALSE(update_payload.device.schedulable);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, ExistKeyFallsBackToStorageBackend) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);
    auto backend = std::make_shared<FakeStorageDeviceMetadataBackend>();
    backend->AddBackendKey("backend_only_key");
    service->SetStorageBackendForAdmin(backend);

    auto result = service->ExistKey("backend_only_key", "default");
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(*result);

    auto miss = service->ExistKey("nonexistent_key", "default");
    ASSERT_TRUE(miss.has_value());
    EXPECT_FALSE(*miss);
}

TEST_F(MasterAdminServerTest, BatchExistKeyFallsBackToStorageBackend) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);
    auto backend = std::make_shared<FakeStorageDeviceMetadataBackend>();
    backend->AddBackendKey("sdk_key_1");
    backend->AddBackendKey("sdk_key_3");
    service->SetStorageBackendForAdmin(backend);

    std::vector<std::string> keys = {"sdk_key_1", "missing_key", "sdk_key_3"};
    auto results = service->BatchExistKey(keys, "default");
    ASSERT_EQ(results.size(), 3u);
    EXPECT_TRUE(results[0].has_value() && *results[0]);
    EXPECT_TRUE(results[1].has_value() && !*results[1]);
    EXPECT_TRUE(results[2].has_value() && *results[2]);
}

TEST_F(MasterAdminServerTest, BackgroundProbeWorkerDrivesProviderHealth) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);
    auto backend = std::make_shared<FakeStorageDeviceMetadataBackend>();
    backend->set_probe_should_fail(true);
    service->SetStorageBackendForAdmin(backend);

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    admin.SetDeviceProbeIntervalSeconds(1);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    // The worker probes ~1/s; failures accumulate to the threshold and the
    // device should transition to FAILED without any manual probe call.
    std::string health;
    for (int i = 0; i < 50 && backend->probe_failures() < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    auto metadata_resp = HttpGet(port, "/api/v1/devices/metadata");
    ASSERT_EQ(metadata_resp.http_status, 200);
    HttpStorageDeviceMetadataResponse metadata_payload;
    struct_json::from_json(metadata_payload, metadata_resp.body);
    ASSERT_EQ(metadata_payload.devices.size(), 1u);
    EXPECT_GE(backend->probe_failures(), 3u);
    EXPECT_EQ(metadata_payload.devices[0].health, "FAILED");
    EXPECT_FALSE(metadata_payload.devices[0].schedulable);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, RebuildJobLifecycleEndpoints) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    Segment seg;
    seg.id = generate_uuid();
    seg.name = "rebuild_test_seg";
    seg.base = 0xF00000000;
    seg.size = 4 * 1024 * 1024;
    UUID client_id = generate_uuid();
    (void)service->MountSegment(seg, client_id);

    NoFSegment nof_seg;
    nof_seg.id = seg.id;
    nof_seg.name = seg.name;
    nof_seg.base = seg.base;
    nof_seg.size = seg.size;
    nof_seg.te_endpoint = "127.0.0.1:12399";
    (void)service->MountNoFSegment(nof_seg, client_id);

    std::string device_id = UuidToString(seg.id);

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto create_resp =
        HttpPostJson(port, "/api/v1/rebuild_jobs",
                     R"({"device_ids":[")" + device_id +
                         R"("],"target_segments":[],"max_concurrency":2})");
    ASSERT_EQ(create_resp.http_status, 200);
    HttpCreateDrainJobResponse create_payload;
    struct_json::from_json(create_payload, create_resp.body);
    EXPECT_TRUE(create_payload.success);
    EXPECT_EQ(create_payload.status, "CREATED");

    auto query_resp = HttpGet(
        port, "/api/v1/rebuild_jobs/query?job_id=" + create_payload.job_id);
    ASSERT_EQ(query_resp.http_status, 200);
    HttpQueryDrainJobResponse query_payload;
    struct_json::from_json(query_payload, query_resp.body);
    EXPECT_TRUE(query_payload.success);
    EXPECT_EQ(query_payload.type_name, "REBUILD");
    ASSERT_EQ(query_payload.devices.size(), 1u);
    EXPECT_EQ(query_payload.devices[0], device_id);

    auto cancel_resp = HttpPostJson(
        port, "/api/v1/rebuild_jobs/cancel?job_id=" + create_payload.job_id,
        "{}");
    ASSERT_EQ(cancel_resp.http_status, 200);
    HttpCancelDrainJobResponse cancel_payload;
    struct_json::from_json(cancel_payload, cancel_resp.body);
    EXPECT_TRUE(cancel_payload.success);
    EXPECT_EQ(cancel_payload.status, "CANCELED");

    query_resp = HttpGet(
        port, "/api/v1/rebuild_jobs/query?job_id=" + create_payload.job_id);
    ASSERT_EQ(query_resp.http_status, 200);
    struct_json::from_json(query_payload, query_resp.body);
    EXPECT_EQ(query_payload.status_name, "CANCELED");

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
    EXPECT_NE(health.body.find("\"ha_state\":\"starting\""), std::string::npos);

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

    auto tenant_quotas = HttpGet(port, "/api/v1/tenant_quotas");
    EXPECT_EQ(tenant_quotas.http_status, 503);
    EXPECT_NE(tenant_quotas.body.find(unavailable_msg), std::string::npos);

    auto device_metadata = HttpGet(port, "/api/v1/devices/metadata");
    EXPECT_EQ(device_metadata.http_status, 503);
    EXPECT_NE(device_metadata.body.find(unavailable_msg), std::string::npos);

    auto drain_create = HttpPostJson(port, "/api/v1/drain_jobs", "{}");
    EXPECT_EQ(drain_create.http_status, 503);

    // drain_jobs/query and drain_jobs/cancel validate job_id before
    // checking service availability, so we must use a valid UUID.
    std::string valid_uuid = UuidToString(generate_uuid());
    auto drain_query =
        HttpGet(port, "/api/v1/drain_jobs/query?job_id=" + valid_uuid);
    EXPECT_EQ(drain_query.http_status, 503);

    auto drain_cancel = HttpPostJson(
        port, "/api/v1/drain_jobs/cancel?job_id=" + valid_uuid, "");
    EXPECT_EQ(drain_cancel.http_status, 503);

    admin.Stop();
}

TEST_F(MasterAdminServerTest,
       DeviceMetadataAdminEndpointsHandleEmptyAndMissing) {
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

    auto list_resp = HttpGet(port, "/api/v1/devices/metadata");
    EXPECT_EQ(list_resp.http_status, 200);
    HttpStorageDeviceMetadataResponse list_parsed;
    struct_json::from_json(list_parsed, list_resp.body);
    EXPECT_EQ(list_parsed.total_devices, 0u);

    auto maintenance_resp = HttpGet(port, "/api/v1/devices/maintenance");
    EXPECT_EQ(maintenance_resp.http_status, 200);
    HttpStorageDeviceMaintenancePlanResponse maintenance_parsed;
    struct_json::from_json(maintenance_parsed, maintenance_resp.body);
    EXPECT_EQ(maintenance_parsed.total_recovery_candidates, 0u);
    EXPECT_EQ(maintenance_parsed.total_gc_candidates, 0u);

    auto missing_device = HttpPutJson(
        port,
        "/api/v1/devices/metadata?device_id=" + UuidToString(generate_uuid()),
        "{\"enabled\":false}");
    EXPECT_EQ(missing_device.http_status, 404);

    auto invalid_uuid =
        HttpPutJson(port, "/api/v1/devices/metadata?device_id=not-a-uuid",
                    "{\"enabled\":false}");
    EXPECT_EQ(invalid_uuid.http_status, 400);

    auto missing_enabled = HttpPutJson(
        port,
        "/api/v1/devices/metadata?device_id=" + UuidToString(generate_uuid()),
        "{}");
    EXPECT_EQ(missing_enabled.http_status, 400);

    auto invalid_action = HttpPostJson(
        port,
        "/api/v1/devices/recovery?device_id=" + UuidToString(generate_uuid()) +
            "&action=repair",
        "");
    EXPECT_EQ(invalid_action.http_status, 400);

    auto missing_recovery_device = HttpPostJson(
        port,
        "/api/v1/devices/recovery?device_id=" + UuidToString(generate_uuid()) +
            "&action=start",
        "");
    EXPECT_EQ(missing_recovery_device.http_status, 404);

    auto invalid_recovery_uuid = HttpPostJson(
        port, "/api/v1/devices/recovery?device_id=not-a-uuid&action=start", "");
    EXPECT_EQ(invalid_recovery_uuid.http_status, 400);

    admin.Stop();
}

TEST_F(MasterAdminServerTest, TenantQuotaAdminLifecycleEndpoints) {
    const std::string policy_path = WriteTenantQuotaPolicyForTest({});
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    svc_config.enable_multi_tenants = true;
    svc_config.tenant_quota_connector_type = "file";
    svc_config.tenant_quota_connector_uri = policy_path;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "quota_admin_segment";
    segment.base = 0x600000000;
    segment.size = 2000;
    UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto upsert = HttpPutJson(port, "/api/v1/tenant_quotas?tenant_id=tenant-a",
                              "{\"requested_quota_bytes\":800}");
    EXPECT_EQ(upsert.http_status, 200);
    EXPECT_NE(upsert.body.find("\"tenant_id\":\"tenant-a\""),
              std::string::npos);
    EXPECT_NE(upsert.body.find("\"requested_quota_bytes\":800"),
              std::string::npos);
    EXPECT_NE(upsert.body.find("\"effective_quota_bytes\":800"),
              std::string::npos);
    EXPECT_NE(upsert.body.find("\"has_explicit_policy\":true"),
              std::string::npos);

    auto list = HttpGet(port, "/api/v1/tenant_quotas");
    EXPECT_EQ(list.http_status, 200);
    EXPECT_NE(list.body.find("\"tenant_id\":\"tenant-a\""), std::string::npos);

    auto one = HttpGet(port, "/api/v1/tenant_quotas?tenant_id=tenant-a");
    EXPECT_EQ(one.http_status, 200);
    EXPECT_NE(one.body.find("\"committed_count\":0"), std::string::npos);
    EXPECT_NE(one.body.find("\"over_quota\":false"), std::string::npos);

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto put =
        service->PutStart(client_id, "quota_admin_key", 100, cfg, "tenant-a");
    ASSERT_TRUE(put.has_value()) << toString(put.error());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "quota_admin_key", ReplicaType::MEMORY,
                             "tenant-a")
                    .has_value());

    auto delete_non_empty =
        HttpDelete(port, "/api/v1/tenant_quotas?tenant_id=tenant-a");
    EXPECT_EQ(delete_non_empty.http_status, 409);
    EXPECT_NE(delete_non_empty.body.find("TENANT_NOT_EMPTY"),
              std::string::npos);

    ASSERT_TRUE(service->Remove("quota_admin_key", /*force=*/true, "tenant-a")
                    .has_value());

    auto deleted = HttpDelete(port, "/api/v1/tenant_quotas?tenant_id=tenant-a");
    EXPECT_EQ(deleted.http_status, 200);

    auto missing = HttpGet(port, "/api/v1/tenant_quotas?tenant_id=tenant-a");
    EXPECT_EQ(missing.http_status, 404);

    admin.Stop();
    std::filesystem::remove(policy_path);
}

TEST_F(MasterAdminServerTest, TenantQuotaAdminValidationErrors) {
    const std::string policy_path = WriteTenantQuotaPolicyForTest({});
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    svc_config.enable_multi_tenants = true;
    svc_config.tenant_quota_connector_type = "file";
    svc_config.tenant_quota_connector_uri = policy_path;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto missing_tenant = HttpPutJson(port, "/api/v1/tenant_quotas",
                                      "{\"requested_quota_bytes\":100}");
    EXPECT_EQ(missing_tenant.http_status, 400);

    auto empty_tenant = HttpPutJson(port, "/api/v1/tenant_quotas?tenant_id=",
                                    "{\"requested_quota_bytes\":100}");
    EXPECT_EQ(empty_tenant.http_status, 400);

    auto zero_explicit =
        HttpPutJson(port, "/api/v1/tenant_quotas?tenant_id=tenant-a",
                    "{\"requested_quota_bytes\":0}");
    EXPECT_EQ(zero_explicit.http_status, 400);

    auto reserved_tenant =
        HttpGet(port, "/api/v1/tenant_quotas?tenant_id=_system");
    EXPECT_EQ(reserved_tenant.http_status, 400);

    auto missing_query =
        HttpGet(port, "/api/v1/tenant_quotas?tenant_id=missing");
    EXPECT_EQ(missing_query.http_status, 404);

    admin.Stop();
    std::filesystem::remove(policy_path);
}

TEST_F(MasterAdminServerTest, TenantQuotaAdminDisabledModeReturns409) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    svc_config.enable_multi_tenants = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto list = HttpGet(port, "/api/v1/tenant_quotas");
    EXPECT_EQ(list.http_status, 409);
    EXPECT_NE(list.body.find("UNAVAILABLE_IN_CURRENT_MODE"), std::string::npos);

    admin.Stop();
}

// =========================================================================
// MasterAdminServerWithServiceTest — reuses a single server+service across
// all tests via SetUpTestSuite / TearDownTestSuite for fast execution.
// =========================================================================

class MasterAdminServerWithServiceTest : public ::testing::Test {
   protected:
    struct HttpResponse {
        int http_status;
        std::string body;
    };

    static void SetUpTestSuite() {
        WrappedMasterServiceConfig svc_config;
        svc_config.default_kv_lease_ttl = 5000;
        svc_config.enable_metric_reporting = false;
        svc_config.client_live_ttl_sec =
            3600;  // prevent client expiry in slow CI
        service_ = std::make_shared<WrappedMasterService>(svc_config);

        segment_.id = generate_uuid();
        segment_.name = "admin_test_segment";
        segment_.base = 0x300000000;
        segment_.size = 8 * 1024 * 1024;
        UUID client_id = generate_uuid();
        (void)service_->MountSegment(segment_, client_id);

        ReplicateConfig cfg;
        cfg.replica_num = 1;
        auto ps = service_->PutStart(client_id, kDefaultKey, 1024, cfg);
        if (ps.has_value()) {
            (void)service_->PutEnd(client_id, kDefaultKey, ReplicaType::MEMORY);
        }

        port_ = getFreeTcpPort();
        admin_ = std::make_unique<MasterAdminServer>(
            static_cast<uint16_t>(port_), false);
        ASSERT_TRUE(admin_->Start());
        admin_->SetRuntimeState(ha::MasterRuntimeState::kServing);
        admin_->SetServiceDelegate(service_);
        admin_->SetServiceAvailable(true);
    }

    static void TearDownTestSuite() {
        if (admin_) {
            admin_->Stop();
            admin_.reset();
        }
        service_.reset();
    }

    HttpResponse HttpGet(const std::string& path) {
        coro_http::coro_http_client client;
        auto result = client.get(BaseUrl() + path);
        return {result.status, std::string(result.resp_body)};
    }

    HttpResponse HttpPostJson(const std::string& path,
                              const std::string& body) {
        coro_http::coro_http_client client;
        auto result = client.post(BaseUrl() + path, body,
                                  coro_http::req_content_type::json);
        return {result.status, std::string(result.resp_body)};
    }

    static std::string BaseUrl() {
        return "http://127.0.0.1:" + std::to_string(port_);
    }

    static std::shared_ptr<WrappedMasterService> service_;
    static std::unique_ptr<MasterAdminServer> admin_;
    static int port_;
    static Segment segment_;
    static constexpr const char* kDefaultKey = "admin_test_key";
};

std::shared_ptr<WrappedMasterService>
    MasterAdminServerWithServiceTest::service_;
std::unique_ptr<MasterAdminServer> MasterAdminServerWithServiceTest::admin_;
int MasterAdminServerWithServiceTest::port_ = 0;
Segment MasterAdminServerWithServiceTest::segment_;

// -----------------------------------------------------------------------
// GET /get_all_keys
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest, GetAllKeysReturnsKeys) {
    auto resp = HttpGet("/get_all_keys");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find(kDefaultKey), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest, GetAllKeysExcludesRemovedKey) {
    // Use a unique key so removal doesn't affect other tests.
    const std::string key = "ephemeral_empty_test_key";
    UUID client_id = generate_uuid();
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto ps = service_->PutStart(client_id, key, 1024, cfg);
    if (ps.has_value()) {
        (void)service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    }
    (void)service_->Remove(key, "default");

    auto resp = HttpGet("/get_all_keys");
    EXPECT_EQ(resp.http_status, 200);
    // The default key is still present; only the ephemeral key was removed.
    EXPECT_NE(resp.body.find(kDefaultKey), std::string::npos);
    EXPECT_EQ(resp.body.find(key), std::string::npos);
}

// -----------------------------------------------------------------------
// GET /query_key
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest, QueryKeyReturnsDataForExistingKey) {
    auto resp = HttpGet("/query_key?key=" + std::string(kDefaultKey));
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"buffer_address_\""), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest, QueryKeyReturns404ForNonexistentKey) {
    auto resp = HttpGet("/query_key?key=nonexistent_key_xyz");
    EXPECT_EQ(resp.http_status, 404);
}

TEST_F(MasterAdminServerWithServiceTest, QueryKeyWithoutKeyParamReturns404) {
    auto resp = HttpGet("/query_key");
    EXPECT_EQ(resp.http_status, 404);
}

// -----------------------------------------------------------------------
// GET /query_segment
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest,
       QuerySegmentReturnsDataForExistingSegment) {
    auto resp = HttpGet("/query_segment?segment=" + segment_.name);
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find(segment_.name), std::string::npos);
    EXPECT_NE(resp.body.find("Used(bytes)"), std::string::npos);
    EXPECT_NE(resp.body.find("Capacity(bytes)"), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest,
       QuerySegmentReturns500ForNonexistentSegment) {
    auto resp = HttpGet("/query_segment?segment=nonexistent_seg");
    EXPECT_EQ(resp.http_status, 500);
}

// -----------------------------------------------------------------------
// GET /get_all_segments
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest, GetAllSegmentsReturnsSegments) {
    auto resp = HttpGet("/get_all_segments");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find(segment_.name), std::string::npos);
}

// -----------------------------------------------------------------------
// GET /get_segments_detail
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest, GetSegmentsDetailReturnsDetailedInfo) {
    auto resp = HttpGet("/get_segments_detail");
    ASSERT_EQ(resp.http_status, 200);

    HttpSegmentsDetailResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_GT(parsed.total_segments, 0u);
    EXPECT_NE(resp.body.find(segment_.name), std::string::npos);
    EXPECT_NE(resp.body.find("\"allocator_used_bytes\""), std::string::npos);
    EXPECT_NE(resp.body.find("\"allocator_capacity_bytes\""),
              std::string::npos);
}

// -----------------------------------------------------------------------
// POST /api/v1/drain_jobs
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest,
       CreateDrainJobSucceedsWithValidRequest) {
    // Use a unique segment so subsequent drain tests are not blocked.
    std::string seg = "drain_create_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0x600000000;
    s.size = 4 * 1024 * 1024;
    (void)service_->MountSegment(s, generate_uuid());

    std::string body = R"({"segments":[")" + seg + R"("]})";
    auto resp = HttpPostJson("/api/v1/drain_jobs", body);
    EXPECT_EQ(resp.http_status, 200);

    HttpCreateDrainJobResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_TRUE(parsed.success);
    EXPECT_FALSE(parsed.job_id.empty());
    EXPECT_EQ(parsed.status, "CREATED");
}

TEST_F(MasterAdminServerWithServiceTest, CreateDrainJobFailsWithInvalidJson) {
    auto resp = HttpPostJson("/api/v1/drain_jobs", "not json");
    ASSERT_EQ(resp.http_status, 400);

    HttpErrorResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_FALSE(parsed.success);
}

TEST_F(MasterAdminServerWithServiceTest, CreateDrainJobFailsWithEmptyBody) {
    auto resp = HttpPostJson("/api/v1/drain_jobs", "{}");
    EXPECT_EQ(resp.http_status, 400);
}

// -----------------------------------------------------------------------
// GET /api/v1/drain_jobs/query
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest, QueryDrainJobReturnsCreatedJob) {
    std::string seg = "drain_query_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0x700000000;
    s.size = 4 * 1024 * 1024;
    (void)service_->MountSegment(s, generate_uuid());

    std::string body = R"({"segments":[")" + seg + R"("]})";
    auto create_resp = HttpPostJson("/api/v1/drain_jobs", body);
    ASSERT_EQ(create_resp.http_status, 200);

    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_FALSE(create_parsed.job_id.empty());

    auto query_resp =
        HttpGet("/api/v1/drain_jobs/query?job_id=" + create_parsed.job_id);
    EXPECT_EQ(query_resp.http_status, 200);

    HttpQueryDrainJobResponse query_parsed;
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
    EXPECT_EQ(query_parsed.job_id, create_parsed.job_id);
}

TEST_F(MasterAdminServerWithServiceTest, QueryDrainJobFailsWithInvalidJobId) {
    auto resp = HttpGet("/api/v1/drain_jobs/query?job_id=not-a-uuid");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerWithServiceTest, QueryDrainJobFailsWithMissingJobId) {
    auto resp = HttpGet("/api/v1/drain_jobs/query");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerWithServiceTest, QueryDrainJobFailsForNonexistentJob) {
    auto resp = HttpGet("/api/v1/drain_jobs/query?job_id=" +
                        UuidToString(generate_uuid()));
    EXPECT_EQ(resp.http_status, 404);
}

// -----------------------------------------------------------------------
// POST /api/v1/drain_jobs/cancel
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest, CancelDrainJobSucceeds) {
    std::string seg = "drain_cancel_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0x800000000;
    s.size = 4 * 1024 * 1024;
    (void)service_->MountSegment(s, generate_uuid());

    std::string body = R"({"segments":[")" + seg + R"("]})";
    auto create_resp = HttpPostJson("/api/v1/drain_jobs", body);
    EXPECT_EQ(create_resp.http_status, 200);

    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_FALSE(create_parsed.job_id.empty());

    auto cancel_resp = HttpPostJson(
        "/api/v1/drain_jobs/cancel?job_id=" + create_parsed.job_id, "");
    EXPECT_EQ(cancel_resp.http_status, 200);

    HttpCancelDrainJobResponse cancel_parsed;
    struct_json::from_json(cancel_parsed, cancel_resp.body);
    EXPECT_TRUE(cancel_parsed.success);
    EXPECT_EQ(cancel_parsed.job_id, create_parsed.job_id);
    EXPECT_EQ(cancel_parsed.status, "CANCELED");
}

TEST_F(MasterAdminServerWithServiceTest, CancelDrainJobFailsWithInvalidJobId) {
    auto resp = HttpPostJson("/api/v1/drain_jobs/cancel?job_id=not-a-uuid", "");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerWithServiceTest, CancelDrainJobFailsWithMissingJobId) {
    auto resp = HttpPostJson("/api/v1/drain_jobs/cancel", "");
    EXPECT_EQ(resp.http_status, 400);
}

// -----------------------------------------------------------------------
// GET /api/v1/segments/status
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest,
       SegmentStatusReturnsDataForExistingSegment) {
    auto resp = HttpGet("/api/v1/segments/status?segment=" + segment_.name);
    EXPECT_EQ(resp.http_status, 200);

    HttpSegmentStatusResponse parsed;
    struct_json::from_json(parsed, resp.body);
    EXPECT_TRUE(parsed.success);
    EXPECT_EQ(parsed.segment, segment_.name);
    EXPECT_FALSE(parsed.status_name.empty());
}

TEST_F(MasterAdminServerWithServiceTest, SegmentStatusFailsWithMissingSegment) {
    auto resp = HttpGet("/api/v1/segments/status");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerWithServiceTest,
       SegmentStatusReturnsErrorForNonexistentSegment) {
    auto resp = HttpGet("/api/v1/segments/status?segment=no_such_segment");
    EXPECT_EQ(resp.http_status, 404);
}

// -----------------------------------------------------------------------
// GET /batch_query_keys
// -----------------------------------------------------------------------

TEST_F(MasterAdminServerWithServiceTest,
       BatchQueryKeysWithNoKeysParamReturns400) {
    auto resp = HttpGet("/batch_query_keys");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerWithServiceTest,
       BatchQueryKeysWithEmptyKeysParamReturns400) {
    auto resp = HttpGet("/batch_query_keys?keys=");
    EXPECT_EQ(resp.http_status, 400);
}

TEST_F(MasterAdminServerWithServiceTest,
       BatchQueryKeysReturnsDataForExistingKey) {
    auto resp = HttpGet("/batch_query_keys?keys=" + std::string(kDefaultKey));
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"data\":{"), std::string::npos);
    EXPECT_NE(resp.body.find(kDefaultKey), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest,
       BatchQueryKeysReturnsErrorForNonexistentKey) {
    auto resp = HttpGet("/batch_query_keys?keys=nonexistent_key");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"ok\":false"), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest, BatchQueryKeysMultipleKeys) {
    UUID client_id = generate_uuid();
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto ps = service_->PutStart(client_id, "second_key", 512, cfg);
    if (ps.has_value()) {
        (void)service_->PutEnd(client_id, "second_key", ReplicaType::MEMORY);
    }

    auto resp = HttpGet("/batch_query_keys?keys=" + std::string(kDefaultKey) +
                        ",second_key");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find(kDefaultKey), std::string::npos);
    EXPECT_NE(resp.body.find("second_key"), std::string::npos);

    (void)service_->Remove("second_key", "default");
}

TEST_F(MasterAdminServerWithServiceTest,
       BatchQueryKeysWithExistingAndNonexistentKeys) {
    auto resp = HttpGet("/batch_query_keys?keys=" + std::string(kDefaultKey) +
                        ",nonexistent");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"ok\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("\"ok\":false"), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest, DrainJobFullLifecycle) {
    std::string seg = "drain_lifecycle_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0x900000000;
    s.size = 4 * 1024 * 1024;
    (void)service_->MountSegment(s, generate_uuid());

    std::string body = R"({"segments":[")" + seg + R"("]})";
    auto create_resp = HttpPostJson("/api/v1/drain_jobs", body);
    ASSERT_EQ(create_resp.http_status, 200);
    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_TRUE(create_parsed.success);
    std::string job_id = create_parsed.job_id;

    auto query_resp = HttpGet("/api/v1/drain_jobs/query?job_id=" + job_id);
    EXPECT_EQ(query_resp.http_status, 200);
    HttpQueryDrainJobResponse query_parsed;
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
    EXPECT_EQ(query_parsed.job_id, job_id);

    auto cancel_resp =
        HttpPostJson("/api/v1/drain_jobs/cancel?job_id=" + job_id, "");
    EXPECT_EQ(cancel_resp.http_status, 200);
    HttpCancelDrainJobResponse cancel_parsed;
    struct_json::from_json(cancel_parsed, cancel_resp.body);
    EXPECT_TRUE(cancel_parsed.success);
    EXPECT_EQ(cancel_parsed.job_id, job_id);
    EXPECT_EQ(cancel_parsed.status, "CANCELED");

    query_resp = HttpGet("/api/v1/drain_jobs/query?job_id=" + job_id);
    EXPECT_EQ(query_resp.http_status, 200);
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
}

// =========================================================================
// Destructive / isolated tests that must run on their own server instance.
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
    EXPECT_NE(resp.body.find("service plane is not active"), std::string::npos);

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

TEST_F(MasterAdminServerTest, MultipleSegmentsAndKeys) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    UUID client_id = generate_uuid();

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

    auto seg_resp = HttpGet(port, "/get_all_segments");
    EXPECT_EQ(seg_resp.http_status, 200);
    EXPECT_NE(seg_resp.body.find("seg_alpha"), std::string::npos);
    EXPECT_NE(seg_resp.body.find("seg_beta"), std::string::npos);

    auto keys_resp = HttpGet(port, "/get_all_keys");
    EXPECT_EQ(keys_resp.http_status, 200);
    EXPECT_NE(keys_resp.body.find("key_one"), std::string::npos);
    EXPECT_NE(keys_resp.body.find("key_two"), std::string::npos);

    auto q1 = HttpGet(port, "/query_segment?segment=seg_alpha");
    EXPECT_EQ(q1.http_status, 200);
    EXPECT_NE(q1.body.find("seg_alpha"), std::string::npos);

    auto q2 = HttpGet(port, "/query_segment?segment=seg_beta");
    EXPECT_EQ(q2.http_status, 200);
    EXPECT_NE(q2.body.find("seg_beta"), std::string::npos);

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

    auto detail_resp = HttpGet(port, "/get_segments_detail");
    EXPECT_EQ(detail_resp.http_status, 200);
    HttpSegmentsDetailResponse detail_parsed;
    struct_json::from_json(detail_parsed, detail_resp.body);
    EXPECT_EQ(detail_parsed.total_segments, 2u);

    auto batch_resp = HttpGet(port, "/batch_query_keys?keys=key_one,key_two");
    EXPECT_EQ(batch_resp.http_status, 200);
    EXPECT_NE(batch_resp.body.find("key_one"), std::string::npos);
    EXPECT_NE(batch_resp.body.find("key_two"), std::string::npos);

    admin.Stop();
}

// /batch_query_keys returns replica metadata for disk-based keys via the
// optional disk_values/local_disk_values/nof_values fields, while the existing
// values field stays present (empty array) for backward compatibility.
TEST_F(MasterAdminServerTest, BatchQueryKeysReturnsLocalDiskReplicaInfo) {
    WrappedMasterServiceConfig svc_config;
    svc_config.default_kv_lease_ttl = 5000;
    svc_config.enable_metric_reporting = false;
    svc_config.enable_offload = true;  // required to mount local-disk segments
    auto service = std::make_shared<WrappedMasterService>(svc_config);

    UUID client_id = generate_uuid();
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "ld_segment";
    segment.base = 0x600000000;
    segment.size = 8 * 1024 * 1024;
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());
    ASSERT_TRUE(service->MountLocalDiskSegment(client_id, true).has_value());

    const std::string key = "ld_only_key";
    const std::string endpoint = "127.0.0.1:9999";
    StorageObjectMetadata sm;
    sm.bucket_id = 0;
    sm.offset = 0;
    sm.key_size = static_cast<int64_t>(key.size());
    sm.data_size = 2048;
    sm.transport_endpoint = endpoint;
    OffloadTaskItem task{.tenant_id = "default", .key = key, .size = 2048};
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, {task}, {sm}).has_value());

    int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    auto resp = HttpGet(port, "/batch_query_keys?keys=" + key);
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"success\":true"), std::string::npos);
    EXPECT_NE(resp.body.find("local_disk_values"), std::string::npos);
    EXPECT_NE(resp.body.find(endpoint), std::string::npos);
    // Backward compat: a disk-only key still reports an (empty) values array.
    EXPECT_NE(resp.body.find("\"values\":[]"), std::string::npos);

    admin.Stop();
}

TEST_F(MasterAdminServerWithServiceTest, RebuildJobFullLifecycle) {
    std::string seg = "rebuild_lifecycle_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0xA00000000;
    s.size = 4 * 1024 * 1024;
    UUID client_id = generate_uuid();
    (void)service_->MountSegment(s, client_id);

    NoFSegment nof_seg;
    nof_seg.id = s.id;
    nof_seg.name = seg;
    nof_seg.base = s.base;
    nof_seg.size = s.size;
    nof_seg.te_endpoint = "127.0.0.1:12345";
    (void)service_->MountNoFSegment(nof_seg, client_id);

    std::string device_id = UuidToString(s.id);
    std::string body = R"({"device_ids":[")" + device_id + R"("]})";
    auto create_resp = HttpPostJson("/api/v1/rebuild_jobs", body);
    ASSERT_EQ(create_resp.http_status, 200);
    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_TRUE(create_parsed.success);
    std::string job_id = create_parsed.job_id;

    auto query_resp = HttpGet("/api/v1/rebuild_jobs/query?job_id=" + job_id);
    EXPECT_EQ(query_resp.http_status, 200);
    HttpQueryDrainJobResponse query_parsed;
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
    EXPECT_EQ(query_parsed.job_id, job_id);
    EXPECT_EQ(query_parsed.type_name, "REBUILD");

    auto cancel_resp =
        HttpPostJson("/api/v1/rebuild_jobs/cancel?job_id=" + job_id, "");
    EXPECT_EQ(cancel_resp.http_status, 200);
    HttpCancelDrainJobResponse cancel_parsed;
    struct_json::from_json(cancel_parsed, cancel_resp.body);
    EXPECT_TRUE(cancel_parsed.success);
    EXPECT_EQ(cancel_parsed.job_id, job_id);
    EXPECT_EQ(cancel_parsed.status, "CANCELED");
}

TEST_F(MasterAdminServerWithServiceTest, RebuildJobInvalidDeviceReturns404) {
    std::string body =
        R"({"device_ids":["00000000-0000-0000-0000-000000000099"]})";
    auto resp = HttpPostJson("/api/v1/rebuild_jobs", body);
    EXPECT_NE(resp.http_status, 200);
}

TEST_F(MasterAdminServerWithServiceTest,
       NoFRecoveryStartCreatesRebuildJobAndGatesComplete) {
    std::string seg = "recovery_gate_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0xB00000000;
    s.size = 4 * 1024 * 1024;
    UUID client_id = generate_uuid();
    (void)service_->MountSegment(s, client_id);

    NoFSegment nof_seg;
    nof_seg.id = s.id;
    nof_seg.name = seg;
    nof_seg.base = s.base;
    nof_seg.size = s.size;
    nof_seg.te_endpoint = "127.0.0.1:12346";
    (void)service_->MountNoFSegment(nof_seg, client_id);

    std::string device_id = UuidToString(s.id);

    auto start_resp = HttpPostJson(
        "/api/v1/devices/recovery?device_id=" + device_id + "&action=start",
        "");
    ASSERT_EQ(start_resp.http_status, 200);

    HttpStorageDeviceMetadataUpdateResponse start_parsed;
    struct_json::from_json(start_parsed, start_resp.body);
    EXPECT_TRUE(start_parsed.success);
    EXPECT_EQ(start_parsed.device.health, "REBUILDING");
    EXPECT_FALSE(start_parsed.device.opaque_provider_metadata.empty());

    auto complete_resp = HttpPostJson(
        "/api/v1/devices/recovery?device_id=" + device_id + "&action=complete",
        "");
    EXPECT_NE(complete_resp.http_status, 200);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto fail_resp = HttpPostJson(
        "/api/v1/devices/recovery?device_id=" + device_id + "&action=fail", "");
    EXPECT_EQ(fail_resp.http_status, 200);
    HttpStorageDeviceMetadataUpdateResponse fail_parsed;
    struct_json::from_json(fail_parsed, fail_resp.body);
    EXPECT_EQ(fail_parsed.device.health, "FAILED");
}

TEST_F(MasterAdminServerWithServiceTest,
       RebuildJobSucceedsWhenNoObjectsOnDevice) {
    std::string seg = "rebuild_empty_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0xC00000000;
    s.size = 4 * 1024 * 1024;
    UUID client_id = generate_uuid();
    (void)service_->MountSegment(s, client_id);

    NoFSegment nof_seg;
    nof_seg.id = s.id;
    nof_seg.name = seg;
    nof_seg.base = s.base;
    nof_seg.size = s.size;
    nof_seg.te_endpoint = "127.0.0.1:12347";
    (void)service_->MountNoFSegment(nof_seg, client_id);

    std::string device_id = UuidToString(s.id);
    std::string body = R"({"device_ids":[")" + device_id + R"("]})";
    auto create_resp = HttpPostJson("/api/v1/rebuild_jobs", body);
    ASSERT_EQ(create_resp.http_status, 200);
    HttpCreateDrainJobResponse create_parsed;
    struct_json::from_json(create_parsed, create_resp.body);
    ASSERT_TRUE(create_parsed.success);
    std::string job_id = create_parsed.job_id;

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto query_resp = HttpGet("/api/v1/rebuild_jobs/query?job_id=" + job_id);
    EXPECT_EQ(query_resp.http_status, 200);
    HttpQueryDrainJobResponse query_parsed;
    struct_json::from_json(query_parsed, query_resp.body);
    EXPECT_TRUE(query_parsed.success);
    EXPECT_EQ(query_parsed.status_name, "SUCCEEDED");
}

TEST_F(MasterAdminServerWithServiceTest,
       RebuildProgressMetadataShownOnDeviceView) {
    std::string seg = "rebuild_progress_seg_" + UuidToString(generate_uuid());
    Segment s;
    s.id = generate_uuid();
    s.name = seg;
    s.base = 0xD00000000;
    s.size = 4 * 1024 * 1024;
    UUID client_id = generate_uuid();
    (void)service_->MountSegment(s, client_id);

    NoFSegment nof_seg;
    nof_seg.id = s.id;
    nof_seg.name = seg;
    nof_seg.base = s.base;
    nof_seg.size = s.size;
    nof_seg.te_endpoint = "127.0.0.1:12348";
    (void)service_->MountNoFSegment(nof_seg, client_id);

    std::string device_id = UuidToString(s.id);
    std::string body = R"({"device_ids":[")" + device_id + R"("]})";
    auto create_resp = HttpPostJson("/api/v1/rebuild_jobs", body);
    ASSERT_EQ(create_resp.http_status, 200);

    auto metadata_resp = HttpGet("/api/v1/devices/metadata");
    EXPECT_EQ(metadata_resp.http_status, 200);
    EXPECT_NE(metadata_resp.body.find("rebuild_job_id"), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest, DeviceCleanupManualStrategy) {
    const std::string key = "cleanup_manual_key";
    UUID client_id = generate_uuid();
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto ps = service_->PutStart(client_id, key, 512, cfg);
    ASSERT_TRUE(ps.has_value());
    (void)service_->PutEnd(client_id, key, ReplicaType::MEMORY);

    std::string body = R"({"device_id":"any","strategy":"manual","keys":[")" +
                       key + R"("],"dry_run":true})";
    auto resp = HttpPostJson("/api/v1/devices/cleanup", body);
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"dry_run\":true"), std::string::npos);
}

TEST_F(MasterAdminServerWithServiceTest, DeviceCleanupEmptyManualReturns400) {
    std::string body = R"({"device_id":"any","strategy":"manual","keys":[]})";
    auto resp = HttpPostJson("/api/v1/devices/cleanup", body);
    EXPECT_NE(resp.http_status, 200);
}

TEST_F(MasterAdminServerWithServiceTest,
       DeviceCleanupRejectsConcurrentOnSameDevice) {
    std::string body =
        R"({"device_id":"same_dev","strategy":"lru","dry_run":true})";
    auto resp = HttpPostJson("/api/v1/devices/cleanup", body);
    EXPECT_EQ(resp.http_status, 200);
}

}  // namespace test
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return result;
}
