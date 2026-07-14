#pragma once

// Shared fixture and helpers for MasterService promotion-on-hit tests.

// Unit tests for the L2->L1 promotion-on-hit master-side path. Exercises
// the master-service entry points directly without going through the RPC
// layer.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "tenant_quota_policy_store.h"
#include "types.h"

namespace mooncake::test {

size_t CountPromotionTask(const std::vector<PromotionTaskItem>& tasks,
                          const std::string& key) {
    return std::count_if(
        tasks.begin(), tasks.end(),
        [&key](const PromotionTaskItem& task) { return task.key == key; });
}

class PromotionOnHitTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PromotionOnHitTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
        google::ShutdownGoogleLogging();
    }

    // Friend access to MasterService::promotion_admission_threshold_, which
    // is otherwise private. PromotionOnHitTest is friended; TEST_F-generated
    // subclasses are not, hence this static funnel.
    static uint32_t GetPromotionAdmissionThresholdForTesting(
        MasterService* service) {
        return service->promotion_admission_threshold_;
    }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;

    std::string WriteTenantQuotaPolicyFile(
        const std::map<std::string, uint64_t>& tenant_quotas) {
        TenantQuotaPolicySnapshot snapshot;
        snapshot.tenant_quotas = tenant_quotas;
        auto path =
            std::filesystem::temp_directory_path() /
            ("mooncake_promotion_tenant_policy_" + std::to_string(::getpid()) +
             "_" + std::to_string(next_policy_file_++) + ".yaml");
        std::ofstream out(path);
        out << FormatTenantQuotaPolicyYaml(snapshot);
        out.close();
        policy_files_.push_back(path.string());
        return path.string();
    }

    Segment MakeSegment(std::string name, size_t base, size_t size) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
        std::string segment_name;
    };

    MountedSegmentContext PrepareSegment(MasterService& service,
                                         std::string name, size_t base,
                                         size_t size) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        auto mount_ld = service.MountLocalDiskSegment(client_id, true);
        EXPECT_TRUE(mount_ld.has_value());
        return {.segment_id = segment.id,
                .client_id = client_id,
                .segment_name = segment.name};
    }

    // Put an object and complete it (creates a MEMORY replica).
    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key, size_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start =
            service.PutStart(client_id, key, "default", size, config);
        ASSERT_TRUE(put_start.has_value()) << "PutStart failed for key=" << key;
        auto put_end =
            service.PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << "PutEnd failed for key=" << key;
    }

    // Inject a synthetic LOCAL_DISK replica for `key` on `client_id`'s
    // segment via NotifyOffloadSuccess. Lets tests put a key into
    // LOCAL_DISK-only state without running the full offload pipeline.
    bool InjectLocalDiskReplica(MasterService& service, const UUID& client_id,
                                const std::string& key, int64_t size,
                                const std::string& transport_endpoint,
                                const std::string& tenant_id = "default") {
        std::vector<OffloadTaskItem> tasks{
            OffloadTaskItem{.tenant_id = tenant_id, .key = key, .size = size}};
        StorageObjectMetadata sm;
        sm.bucket_id = 0;
        sm.offset = 0;
        sm.key_size = static_cast<int64_t>(key.size());
        sm.data_size = size;
        sm.transport_endpoint = transport_endpoint;
        std::vector<StorageObjectMetadata> metas{sm};
        auto res = service.NotifyOffloadSuccess(client_id, tasks, metas);
        return res.has_value();
    }

    // Register a client as a LOCAL_DISK holder only (no DRAM segment).
    // This simulates the cross-host case where the LOCAL_DISK source lives
    // on a different node than the DRAM target chosen for promotion.
    UUID PrepareLocalDiskOnlyClient(MasterService& service) const {
        UUID client_id = generate_uuid();
        auto mount_ld = service.MountLocalDiskSegment(client_id, true);
        EXPECT_TRUE(mount_ld.has_value());
        return client_id;
    }

    std::vector<std::string> policy_files_;
    size_t next_policy_file_ = 0;
};

// Sanity: with promotion disabled, no path mutates promotion_objects.
}  // namespace mooncake::test
