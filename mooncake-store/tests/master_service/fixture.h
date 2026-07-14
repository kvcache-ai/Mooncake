#pragma once

// Shared fixture and helpers for MasterService unit tests.

#include "master_service.h"
#include "rpc_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unordered_set>

#include <unistd.h>

#include "tenant_quota_policy_store.h"
#include "types.h"
#include "utils.h"

namespace mooncake::test {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        Capture();
        ::unsetenv(name_.c_str());
    }

    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        Capture();
        ::setenv(name_.c_str(), value, 1);
    }

    ~ScopedEnvVar() {
        if (previous_value_.has_value()) {
            ::setenv(name_.c_str(), previous_value_->c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }

   private:
    void Capture() {
        const char* value = ::getenv(name_.c_str());
        if (value != nullptr) {
            previous_value_ = value;
        }
    }

    std::string name_;
    std::optional<std::string> previous_value_;
};

class MasterServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceTest");
        FLAGS_logtostderr = true;
    }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;
    static constexpr uint64_t kStrictTenantQuotaBytes = 4 * 1024 * 1024;

    std::string WriteTenantPolicyFile(
        const std::map<std::string, uint64_t>& tenant_quotas) {
        TenantQuotaPolicySnapshot snapshot;
        snapshot.tenant_quotas = tenant_quotas;
        auto path =
            std::filesystem::temp_directory_path() /
            ("mooncake_master_service_test_" + std::to_string(::getpid()) +
             "_" + std::to_string(next_policy_file_++) + ".yaml");
        std::ofstream out(path);
        out << FormatTenantQuotaPolicyYaml(snapshot);
        out.close();
        policy_files_.push_back(path.string());
        return path.string();
    }

    MasterServiceConfig MakeStrictTenantConfig(
        const std::vector<std::string>& tenants) {
        std::map<std::string, uint64_t> tenant_quotas;
        for (const auto& tenant : tenants) {
            tenant_quotas.emplace(tenant, kStrictTenantQuotaBytes);
        }
        return MasterServiceConfig::builder()
            .set_enable_multi_tenants(true)
            .set_tenant_quota_connector_type("file")
            .set_tenant_quota_connector_uri(
                WriteTenantPolicyFile(tenant_quotas))
            .build();
    }

    WrappedMasterServiceConfig MakeStrictWrappedConfig(
        const std::vector<std::string>& tenants) {
        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = 100;
        config.enable_metric_reporting = false;
        config.enable_multi_tenants = true;
        config.tenant_quota_connector_type = "file";
        std::map<std::string, uint64_t> tenant_quotas;
        for (const auto& tenant : tenants) {
            tenant_quotas.emplace(tenant, kStrictTenantQuotaBytes);
        }
        config.tenant_quota_connector_uri =
            WriteTenantPolicyFile(tenant_quotas);
        return config;
    }

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize,
                        std::string host_id = "") const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        segment.host_id = std::move(host_id);
        return segment;
    }

#ifdef USE_NOF
    NoFSegment MakeNoFSegment(
        std::string name = "test_nof_segment",
        std::string endpoint = "test_nof_segment_endpoint",
        size_t base = kDefaultSegmentBase + kDefaultSegmentSize,
        size_t size = kDefaultSegmentSize) const {
        NoFSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = std::move(endpoint);
        return segment;
    }
#endif

    MountedSegmentContext PrepareSimpleSegment(
        MasterService& service, std::string name = "test_segment",
        size_t base = kDefaultSegmentBase, size_t size = kDefaultSegmentSize,
        std::string host_id = "") const {
        Segment segment =
            MakeSegment(std::move(name), base, size, std::move(host_id));
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    std::string PutObjectOnSegment(MasterService& service,
                                   const UUID& client_id,
                                   const std::string& segment_name,
                                   size_t slice_length = 1024) const {
        static std::atomic<uint64_t> counter{0};
        std::string key =
            "drain_job_key_" + std::to_string(counter.fetch_add(1));

        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment_name;

        auto put_start =
            service.PutStart(client_id, key, "default", slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(
            service.PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                .has_value());
        return key;
    }

    std::string FindGroupIdOnDifferentShard(const std::string& key) const {
        static constexpr size_t kMetadataShardCountForTest = 1024;
        const size_t key_shard =
            std::hash<std::string>{}(key) % kMetadataShardCountForTest;
        for (int i = 0; i < 10000; ++i) {
            std::string group_id = key + "_group_" + std::to_string(i);
            if (std::hash<std::string>{}(group_id) %
                    kMetadataShardCountForTest !=
                key_shard) {
                return group_id;
            }
        }
        return key + "_fallback_group";
    }

    void PutCompletedObject(MasterService& service, const UUID& client_id,
                            const std::string& key,
                            const ReplicateConfig& config,
                            uint64_t slice_length = 1024) const {
        auto put_start =
            service.PutStart(client_id, key, "default", slice_length, config);
        ASSERT_TRUE(put_start.has_value())
            << "PutStart failed for key=" << key
            << ", error=" << toString(put_start.error());
        ASSERT_TRUE(
            service.PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                .has_value());
    }

    void PutCompletedObject(MasterService& service, const UUID& client_id,
                            const std::string& key,
                            const std::string& tenant_id,
                            const ReplicateConfig& config,
                            uint64_t slice_length = 1024) const {
        auto put_start =
            service.PutStart(client_id, key, tenant_id, slice_length, config);
        ASSERT_TRUE(put_start.has_value())
            << "PutStart failed for key=" << key << ", tenant_id=" << tenant_id
            << ", error=" << toString(put_start.error());
        ASSERT_TRUE(
            service.PutEnd(client_id, key, tenant_id, ReplicaType::MEMORY)
                .has_value());
    }

    bool ExecutePendingMoveTasks(MasterService& service,
                                 const UUID& client_id) const {
        auto fetched = service.FetchTasks(client_id, /*batch_size=*/16);
        EXPECT_TRUE(fetched.has_value());
        if (!fetched.has_value() || fetched->empty()) {
            return false;
        }

        bool processed = false;
        for (const auto& assignment : *fetched) {
            if (assignment.type != TaskType::REPLICA_MOVE) {
                continue;
            }

            ReplicaMovePayload payload;
            struct_json::from_json(payload, assignment.payload);

            auto move_start =
                service.MoveStart(client_id, payload.key, payload.tenant_id,
                                  payload.source, payload.target);
            EXPECT_TRUE(move_start.has_value());
            EXPECT_TRUE(
                service.MoveEnd(client_id, payload.key, payload.tenant_id)
                    .has_value());

            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::SUCCESS;
            complete_request.message = "move_done";
            EXPECT_TRUE(service.MarkTaskToComplete(client_id, complete_request)
                            .has_value());
            processed = true;
        }
        return processed;
    }

    bool FailPendingMoveTasks(MasterService& service,
                              const UUID& client_id) const {
        auto fetched = service.FetchTasks(client_id, /*batch_size=*/16);
        EXPECT_TRUE(fetched.has_value());
        if (!fetched.has_value() || fetched->empty()) {
            return false;
        }

        bool processed = false;
        for (const auto& assignment : *fetched) {
            if (assignment.type != TaskType::REPLICA_MOVE) {
                continue;
            }

            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::FAILED;
            complete_request.message = "move_failed";
            EXPECT_TRUE(service.MarkTaskToComplete(client_id, complete_request)
                            .has_value());
            processed = true;
        }
        return processed;
    }

    template <typename Predicate>
    void WaitUntil(
        Predicate&& predicate,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(4000),
        std::chrono::milliseconds interval =
            std::chrono::milliseconds(50)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return;
            }
            std::this_thread::sleep_for(interval);
        }
        EXPECT_TRUE(predicate());
    }

    std::vector<Replica::Descriptor> replica_list;
    std::vector<std::string> policy_files_;
    size_t next_policy_file_ = 0;

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
        google::ShutdownGoogleLogging();
    }
};

inline std::string GenerateKeyForSegment(
    const UUID& client_id, const std::unique_ptr<MasterService>& service,
    const std::string& segment_name) {
    static std::atomic<uint64_t> counter(0);

    while (true) {
        std::string key = "key_" + std::to_string(counter.fetch_add(1));
        std::vector<Replica::Descriptor> replica_list;

        auto exist_result = service->ExistKey(key, "default");
        if (exist_result.has_value() && exist_result.value()) {
            continue;
        }

        auto put_result = service->PutStart(client_id, key, "default", {1024},
                                            {.replica_num = 1});
        if (put_result.has_value()) {
            replica_list = std::move(put_result.value());
        }
        ErrorCode code =
            put_result.has_value() ? ErrorCode::OK : put_result.error();

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }
        auto put_end_result =
            service->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_ == segment_name) {
            return key;
        }
        auto remove_result = service->Remove(key, "default");
        if (!remove_result.has_value()) {
            // Ignore cleanup failure.
        }
    }
}

}  // namespace mooncake::test
