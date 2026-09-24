// Unit tests for the master-side SSD prefetch entry point
// MasterService::RegisterPrefetchTask and the from_prefetch lease grant in
// NotifyPromotionSuccess. Exercises MasterService directly, no RPC layer.
//
// RegisterPrefetchTask records a promotion task without the promotion-on-hit
// admission gates (frequency sketch, DRAM watermark) and without pushing to
// the holder's heartbeat mailbox; the prefetch caller drives the execution
// chain itself.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include <unistd.h>

#include "types.h"

namespace mooncake::test {

class PrefetchTaskMasterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PrefetchTaskMasterTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;

    Segment MakeSegment(std::string name, size_t base, size_t size) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    // Mount a DRAM segment plus the LOCAL_DISK segment that
    // NotifyOffloadSuccess requires for replica registration.
    UUID PrepareHolderClient(MasterService& service,
                             const std::string& segment_name) const {
        Segment segment =
            MakeSegment(segment_name, kDefaultSegmentBase, kDefaultSegmentSize);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        auto mount_ld = service.MountLocalDiskSegment(client_id, true);
        EXPECT_TRUE(mount_ld.has_value());
        return client_id;
    }

    // Inject a synthetic LOCAL_DISK-only key, the same way
    // promotion_on_hit_test does, without running the offload pipeline.
    bool InjectLocalDiskOnlyKey(MasterService& service, const UUID& client_id,
                                const std::string& key, int64_t size,
                                const std::string& transport_endpoint) {
        std::vector<OffloadTaskItem> tasks{
            OffloadTaskItem{.tenant_id = std::string(TenantId::kDefaultValue),
                            .key = key,
                            .size = size}};
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

    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key, size_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start =
            service.PutStart(client_id, key, TenantId::Default(), size, config);
        ASSERT_TRUE(put_start.has_value()) << "PutStart failed for key=" << key;
        auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                      ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << "PutEnd failed for key=" << key;
    }

    // Friend funnels (PrefetchTaskMasterTest is friended on MasterService).
    static bool HasPromotionTaskForTesting(MasterService* service,
                                           const TenantId& tenant_id,
                                           const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        const auto* tenant_state = accessor.GetTenantState();
        return tenant_state != nullptr &&
               tenant_state->promotion_tasks.contains(key);
    }

    static bool IsLeaseAliveForTesting(MasterService* service,
                                       const TenantId& tenant_id,
                                       const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        if (!accessor.Exists()) {
            return false;
        }
        return !accessor.Get().IsLeaseExpired();
    }
};

TEST_F(PrefetchTaskMasterTest, RegisterSuccessThenDuplicateSkips) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_1");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pk1", 1024,
                                       "prefetch_seg_1"));

    auto res =
        service->RegisterPrefetchTask(holder, "pk1", TenantId::Default());
    ASSERT_TRUE(res.has_value());
    EXPECT_TRUE(
        HasPromotionTaskForTesting(service.get(), TenantId::Default(), "pk1"));

    // Duplicate registration is a normal best-effort outcome, reported
    // distinctly so the caller skips silently.
    auto dup =
        service->RegisterPrefetchTask(holder, "pk1", TenantId::Default());
    ASSERT_FALSE(dup.has_value());
    EXPECT_EQ(dup.error(), ErrorCode::PROMOTION_ALREADY_EXISTS);

    // Cleanup: holder aborts the task.
    auto release =
        service->NotifyPromotionFailure(holder, "pk1", TenantId::Default());
    ASSERT_TRUE(release.has_value());
    EXPECT_FALSE(
        HasPromotionTaskForTesting(service.get(), TenantId::Default(), "pk1"));

    service->RemoveAll();
}

TEST_F(PrefetchTaskMasterTest, MissingKeyNotFound) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_2");
    auto res = service->RegisterPrefetchTask(holder, "no_such_key",
                                             TenantId::Default());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::OBJECT_NOT_FOUND);

    service->RemoveAll();
}

TEST_F(PrefetchTaskMasterTest, NonHolderRejected) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_3");
    UUID other = PrepareHolderClient(*service, "prefetch_seg_3b");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pk3", 1024,
                                       "prefetch_seg_3"));

    auto res = service->RegisterPrefetchTask(other, "pk3", TenantId::Default());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(
        HasPromotionTaskForTesting(service.get(), TenantId::Default(), "pk3"));

    service->RemoveAll();
}

TEST_F(PrefetchTaskMasterTest, MemoryReplicaPresentSkips) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_4");
    PutObject(*service, holder, "pk4", 1024);

    auto res =
        service->RegisterPrefetchTask(holder, "pk4", TenantId::Default());
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::PROMOTION_ALREADY_EXISTS);

    service->RemoveAll();
}

TEST_F(PrefetchTaskMasterTest, NotifyPromotionSuccessGrantsLeaseForPrefetch) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;  // ms
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_5");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pk5", 1024,
                                       "prefetch_seg_5"));

    // The prefetch execution chain, master side only (no data movement):
    // register -> alloc start -> commit.
    ASSERT_TRUE(
        service->RegisterPrefetchTask(holder, "pk5", TenantId::Default())
            .has_value());
    const std::vector<std::string> preferred_segments;
    auto alloc = service->PromotionAllocStart(
        holder, "pk5", TenantId::Default(), 1024, preferred_segments);
    ASSERT_TRUE(alloc.has_value());
    auto commit =
        service->NotifyPromotionSuccess(holder, "pk5", TenantId::Default());
    ASSERT_TRUE(commit.has_value());

    // A prefetch-committed replica carries a fresh read lease so the DRAM
    // copy survives until the follow-up get().
    EXPECT_TRUE(
        IsLeaseAliveForTesting(service.get(), TenantId::Default(), "pk5"));

    // The memory replica is now visible to readers.
    auto list = service->GetReplicaListForAdmin("pk5", TenantId::Default());
    ASSERT_TRUE(list.has_value());
    bool has_memory = false;
    for (const auto& replica : list->replicas) {
        if (replica.is_memory_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_memory = true;
        }
    }
    EXPECT_TRUE(has_memory);

    service->RemoveAll();
}

}  // namespace mooncake::test
