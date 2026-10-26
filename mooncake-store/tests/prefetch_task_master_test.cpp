// Unit tests for the master-side SSD prefetch entry point
// MasterService::RegisterPrefetchTask and the from_prefetch lease grant in
// NotifyPromotionSuccess. Exercises MasterService directly, no RPC layer.
//
// RegisterPrefetchTask records a promotion task without the promotion-on-hit
// admission gates (frequency sketch, DRAM watermark) and without pushing to
// the holder's heartbeat mailbox; the prefetch caller drives the execution
// chain itself, naming the task by the task id the registration returned.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
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

    // Mount a DRAM segment plus the LOCAL_DISK segment that disk replicas
    // require for registration.
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

    // Inject a synthetic LOCAL_DISK-only key via AddReplica, the same way
    // promotion_on_hit_test does, without running the offload pipeline
    // (NotifyOffloadSuccess only registers the completion of an offload
    // task the client took).
    bool InjectLocalDiskOnlyKey(MasterService& service, const UUID& client_id,
                                const std::string& key, int64_t size,
                                const std::string& transport_endpoint) {
        Replica replica(client_id, static_cast<uint64_t>(size),
                        transport_endpoint, ReplicaStatus::COMPLETE);
        auto res =
            service.AddReplica(client_id, key, TenantId::Default(), replica);
        return res.has_value();
    }

    // The item of a prefetch task, as the holder builds it from the task id
    // its registration returned.
    static PromotionTaskItem PrefetchItem(const std::string& key,
                                          const L4TaskId& task_id,
                                          int64_t size = 1024) {
        return PromotionTaskItem{
            .tenant_id = std::string(TenantId::kDefaultValue),
            .key = key,
            .size = size,
            .task_id = task_id};
    }

    // The id of the key's in-flight task, if any.
    static std::optional<L4TaskId> TaskIdForTesting(MasterService* service,
                                                    const TenantId& tenant_id,
                                                    const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        if (!accessor.Exists() ||
            !accessor.GetState().promotion_task.has_value()) {
            return std::nullopt;
        }
        return accessor.GetState().promotion_task->task_id;
    }

    // The status of the key's memory replica, if it has one.
    static std::optional<ReplicaStatus> MemoryReplicaStatusForTesting(
        MasterService* service, const TenantId& tenant_id,
        const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        if (!accessor.Exists()) {
            return std::nullopt;
        }
        std::optional<ReplicaStatus> status;
        accessor.Get().VisitReplicas(
            &Replica::fn_is_memory_replica,
            [&status](const Replica& replica) { status = replica.status(); });
        return status;
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
    // In-flight tasks live on the object's runtime state.
    static bool HasPromotionTaskForTesting(MasterService* service,
                                           const TenantId& tenant_id,
                                           const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        return accessor.Exists() &&
               accessor.GetState().promotion_task.has_value();
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

    // The candidate's execution-failure count if a candidate exists.
    static std::optional<uint32_t> CandidateFailuresForTesting(
        MasterService* service, const TenantId& tenant_id,
        const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        if (!accessor.Exists() ||
            !accessor.GetState().promotion_candidate.has_value()) {
            return std::nullopt;
        }
        return accessor.GetState().promotion_candidate->execution_failures;
    }

    // The in-flight task's execution-failure count if a task exists.
    static std::optional<uint32_t> TaskFailuresForTesting(
        MasterService* service, const TenantId& tenant_id,
        const std::string& key) {
        MasterService::MetadataAccessorRO accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        if (!accessor.Exists() ||
            !accessor.GetState().promotion_task.has_value()) {
            return std::nullopt;
        }
        return accessor.GetState().promotion_task->execution_failures;
    }

    // Seed an on-hit candidate carrying a prior execution-failure history,
    // the way the retry pipeline leaves it after a failed chain.
    static void SeedCandidateForTesting(MasterService* service,
                                        const TenantId& tenant_id,
                                        const std::string& key,
                                        uint32_t failures) {
        MasterService::MetadataAccessorRW accessor(
            service, MasterService::ObjectIdentity{.tenant_id = tenant_id,
                                                   .user_key = key});
        if (!accessor.Exists()) {
            return;
        }
        service->promotion_candidates_.RecordLocked(
            tenant_id, key, accessor.GetState(), /*sketch_score=*/0,
            PromotionCandidateReason::kExecutionFailed, ErrorCode::OK,
            failures);
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
    // The registration returns the id of the task it recorded.
    EXPECT_EQ(TaskIdForTesting(service.get(), TenantId::Default(), "pk1"),
              std::optional<L4TaskId>(*res));

    // Duplicate registration is a normal best-effort outcome, reported
    // distinctly so the caller skips silently.
    auto dup =
        service->RegisterPrefetchTask(holder, "pk1", TenantId::Default());
    ASSERT_FALSE(dup.has_value());
    EXPECT_EQ(dup.error(), ErrorCode::PROMOTION_ALREADY_EXISTS);

    // Cleanup: holder aborts the task.
    auto release = service->NotifyPromotionTaskFailure(
        holder, PrefetchItem("pk1", *res), TenantId::Default());
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
    auto task_id =
        service->RegisterPrefetchTask(holder, "pk5", TenantId::Default());
    ASSERT_TRUE(task_id.has_value());
    const std::vector<std::string> preferred_segments;
    auto alloc = service->PromotionTaskAllocStart(
        holder, PrefetchItem("pk5", *task_id), TenantId::Default(),
        preferred_segments);
    ASSERT_TRUE(alloc.has_value());
    auto commit = service->NotifyPromotionTaskSuccess(
        holder, PrefetchItem("pk5", *task_id), TenantId::Default());
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

TEST_F(PrefetchTaskMasterTest, PrefetchFailureDoesNotRecordCandidate) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_leak");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pkl", 1024,
                                       "prefetch_seg_leak"));
    auto task_id =
        service->RegisterPrefetchTask(holder, "pkl", TenantId::Default());
    ASSERT_TRUE(task_id.has_value());
    ASSERT_TRUE(service
                    ->NotifyPromotionTaskFailure(holder,
                                                 PrefetchItem("pkl", *task_id),
                                                 TenantId::Default())
                    .has_value());

    // A from_prefetch failure must not re-record an on-hit candidate: with
    // promotion_on_hit off the retry sweeper never consumes it, so the
    // entry would leak and pin the tenant state permanently.
    EXPECT_FALSE(
        CandidateFailuresForTesting(service.get(), TenantId::Default(), "pkl")
            .has_value());

    service->RemoveAll();
}

TEST_F(PrefetchTaskMasterTest, RegisterPropagatesCandidateFailureCount) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_prop");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pkp", 1024,
                                       "prefetch_seg_prop"));
    // A previous chain (e.g. on-hit retries) already failed twice on this
    // key; the candidate carries that history.
    SeedCandidateForTesting(service.get(), TenantId::Default(), "pkp", 2);

    auto task_id =
        service->RegisterPrefetchTask(holder, "pkp", TenantId::Default());
    ASSERT_TRUE(task_id.has_value());
    // The candidate is consumed and its failure count moves onto the task —
    // hardcoding zero here would re-arm a persistently failing key on every
    // probe, defeating kMaxPromotionExecutionFailures.
    EXPECT_EQ(TaskFailuresForTesting(service.get(), TenantId::Default(), "pkp"),
              std::optional<uint32_t>(2));
    EXPECT_FALSE(
        CandidateFailuresForTesting(service.get(), TenantId::Default(), "pkp")
            .has_value());

    // A prefetch failure then ends the chain instead of re-recording.
    ASSERT_TRUE(service
                    ->NotifyPromotionTaskFailure(holder,
                                                 PrefetchItem("pkp", *task_id),
                                                 TenantId::Default())
                    .has_value());
    EXPECT_FALSE(
        CandidateFailuresForTesting(service.get(), TenantId::Default(), "pkp")
            .has_value());

    service->RemoveAll();
}

// Every prefetch registration records a task with a new id, so a later
// prefetch of the key is a different task from an earlier one.
TEST_F(PrefetchTaskMasterTest, EachRegistrationGetsANewTaskId) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_ids");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pki", 1024,
                                       "prefetch_seg_ids"));
    auto first =
        service->RegisterPrefetchTask(holder, "pki", TenantId::Default());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(service
                    ->NotifyPromotionTaskFailure(holder,
                                                 PrefetchItem("pki", *first),
                                                 TenantId::Default())
                    .has_value());
    auto second =
        service->RegisterPrefetchTask(holder, "pki", TenantId::Default());
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(*first, *second);

    service->RemoveAll();
}

// The key-only calls cannot name a prefetch task, so they never stage or
// commit its buffer (the holder of an older store degrades to no prefetch).
TEST_F(PrefetchTaskMasterTest, KeyOnlyCallsDoNotRunAPrefetchTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_keyonly");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pkk", 1024,
                                       "prefetch_seg_keyonly"));
    auto task_id =
        service->RegisterPrefetchTask(holder, "pkk", TenantId::Default());
    ASSERT_TRUE(task_id.has_value());

    const std::vector<std::string> preferred_segments;
    EXPECT_FALSE(service
                     ->PromotionAllocStart(holder, "pkk", TenantId::Default(),
                                           1024, preferred_segments)
                     .has_value());
    EXPECT_FALSE(
        MemoryReplicaStatusForTesting(service.get(), TenantId::Default(), "pkk")
            .has_value());

    ASSERT_TRUE(
        service
            ->PromotionTaskAllocStart(holder, PrefetchItem("pkk", *task_id),
                                      TenantId::Default(), preferred_segments)
            .has_value());
    EXPECT_FALSE(
        service->NotifyPromotionSuccess(holder, "pkk", TenantId::Default())
            .has_value());
    EXPECT_EQ(MemoryReplicaStatusForTesting(service.get(), TenantId::Default(),
                                            "pkk"),
              std::optional<ReplicaStatus>(ReplicaStatus::PROCESSING));

    service->RemoveAll();
}

// A late success of an earlier prefetch of the key (its task ended, and a
// newer prefetch staged its own buffer) must not commit the newer buffer:
// its holder may still be writing it, so readers would get torn bytes.
TEST_F(PrefetchTaskMasterTest, LateSuccessOfAnEarlierPrefetchCommitsNothing) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_late_ok");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pks", 1024,
                                       "prefetch_seg_late_ok"));
    const std::vector<std::string> preferred_segments;

    // The earlier prefetch stages a buffer, then its task ends.
    auto earlier =
        service->RegisterPrefetchTask(holder, "pks", TenantId::Default());
    ASSERT_TRUE(earlier.has_value());
    ASSERT_TRUE(
        service
            ->PromotionTaskAllocStart(holder, PrefetchItem("pks", *earlier),
                                      TenantId::Default(), preferred_segments)
            .has_value());
    ASSERT_TRUE(service
                    ->NotifyPromotionTaskFailure(holder,
                                                 PrefetchItem("pks", *earlier),
                                                 TenantId::Default())
                    .has_value());

    // The newer prefetch stages its own buffer.
    auto newer =
        service->RegisterPrefetchTask(holder, "pks", TenantId::Default());
    ASSERT_TRUE(newer.has_value());
    ASSERT_TRUE(
        service
            ->PromotionTaskAllocStart(holder, PrefetchItem("pks", *newer),
                                      TenantId::Default(), preferred_segments)
            .has_value());

    // The earlier prefetch's success arrives late: refused, nothing commits.
    EXPECT_FALSE(service
                     ->NotifyPromotionTaskSuccess(holder,
                                                  PrefetchItem("pks", *earlier),
                                                  TenantId::Default())
                     .has_value());
    EXPECT_EQ(MemoryReplicaStatusForTesting(service.get(), TenantId::Default(),
                                            "pks"),
              std::optional<ReplicaStatus>(ReplicaStatus::PROCESSING));
    EXPECT_EQ(TaskIdForTesting(service.get(), TenantId::Default(), "pks"),
              std::optional<L4TaskId>(*newer));

    // The newer prefetch's own success commits its buffer.
    ASSERT_TRUE(service
                    ->NotifyPromotionTaskSuccess(holder,
                                                 PrefetchItem("pks", *newer),
                                                 TenantId::Default())
                    .has_value());
    EXPECT_EQ(MemoryReplicaStatusForTesting(service.get(), TenantId::Default(),
                                            "pks"),
              std::optional<ReplicaStatus>(ReplicaStatus::COMPLETE));

    service->RemoveAll();
}

// A late failure of an earlier prefetch of the key must not free the buffer
// a newer prefetch staged: its holder is still writing into it, and the
// range could be handed to another put meanwhile.
TEST_F(PrefetchTaskMasterTest, LateFailureOfAnEarlierPrefetchFreesNothing) {
    MasterServiceConfig config;
    config.enable_offload = true;
    auto service = std::make_unique<MasterService>(config);

    UUID holder = PrepareHolderClient(*service, "prefetch_seg_late_fail");
    ASSERT_TRUE(InjectLocalDiskOnlyKey(*service, holder, "pkf", 1024,
                                       "prefetch_seg_late_fail"));
    const std::vector<std::string> preferred_segments;

    // The earlier prefetch stages a buffer, then its task ends.
    auto earlier =
        service->RegisterPrefetchTask(holder, "pkf", TenantId::Default());
    ASSERT_TRUE(earlier.has_value());
    ASSERT_TRUE(
        service
            ->PromotionTaskAllocStart(holder, PrefetchItem("pkf", *earlier),
                                      TenantId::Default(), preferred_segments)
            .has_value());
    ASSERT_TRUE(service
                    ->NotifyPromotionTaskFailure(holder,
                                                 PrefetchItem("pkf", *earlier),
                                                 TenantId::Default())
                    .has_value());

    // The newer prefetch stages its own buffer.
    auto newer =
        service->RegisterPrefetchTask(holder, "pkf", TenantId::Default());
    ASSERT_TRUE(newer.has_value());
    ASSERT_TRUE(
        service
            ->PromotionTaskAllocStart(holder, PrefetchItem("pkf", *newer),
                                      TenantId::Default(), preferred_segments)
            .has_value());

    // The earlier prefetch's failure arrives late: a no-op.
    EXPECT_TRUE(service
                    ->NotifyPromotionTaskFailure(holder,
                                                 PrefetchItem("pkf", *earlier),
                                                 TenantId::Default())
                    .has_value());
    EXPECT_EQ(TaskIdForTesting(service.get(), TenantId::Default(), "pkf"),
              std::optional<L4TaskId>(*newer));
    EXPECT_EQ(MemoryReplicaStatusForTesting(service.get(), TenantId::Default(),
                                            "pkf"),
              std::optional<ReplicaStatus>(ReplicaStatus::PROCESSING));

    // The newer prefetch still completes.
    ASSERT_TRUE(service
                    ->NotifyPromotionTaskSuccess(holder,
                                                 PrefetchItem("pkf", *newer),
                                                 TenantId::Default())
                    .has_value());
    EXPECT_EQ(MemoryReplicaStatusForTesting(service.get(), TenantId::Default(),
                                            "pkf"),
              std::optional<ReplicaStatus>(ReplicaStatus::COMPLETE));

    service->RemoveAll();
}

}  // namespace mooncake::test
