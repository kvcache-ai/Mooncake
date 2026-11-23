#include "promotion_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "allocator.h"
#include "client_liveness.h"
#include "master_metric_manager.h"
#include "object_metadata.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace {

const TenantId kTenant = TenantId::Default();
constexpr std::chrono::seconds kDefaultTimeout{60};

ClientSessionSharedPtr MakeActiveSession(const UUID& client_id) {
    return std::make_shared<const ClientLivenessRecord>(
        client_id, ClientLivenessRecord::Clock::now());
}

ClientSessionSharedPtr MakeNonServingSession(const UUID& client_id) {
    const auto now = ClientLivenessRecord::Clock::now();
    auto rec = std::make_shared<ClientLivenessRecord>(client_id, now);
    (void)rec->Evaluate(now + std::chrono::seconds(10), std::chrono::seconds(1),
                        std::chrono::seconds(1));
    return rec;
}

PromotionConfig MakeDefaultConfig() {
    PromotionConfig cfg;
    cfg.promotion_on_hit = true;
    cfg.promotion_admission_threshold = 2;
    cfg.promotion_queue_limit = 64;
    cfg.promotion_max_per_heartbeat = 2;
    return cfg;
}

std::unique_ptr<ObjectMetadata> MakeDiskObject(
    const UUID& holder_id, const ClientSessionSharedPtr& session,
    const TenantId& tenant_id, const std::string& key, uint64_t size = 256) {
    std::vector<Replica> reps;
    reps.emplace_back(holder_id, size, "tcp://127.0.0.1:9201",
                      ReplicaStatus::COMPLETE, session);
    return std::make_unique<ObjectMetadata>(
        holder_id, std::chrono::system_clock::now(), size, std::move(reps),
        std::nullopt, false, ObjectDataType::UNKNOWN, std::string{}, tenant_id,
        key);
}

Replica* FirstLocalDiskReplica(ObjectMetadata& metadata) {
    Replica* found = nullptr;
    metadata.VisitReplicas(&Replica::fn_is_local_disk_replica,
                           [&found](Replica& r) {
                               if (found == nullptr) {
                                   found = &r;
                               }
                           });
    return found;
}

ReplicaID AddStagedMemoryReplica(
    ObjectMetadata& metadata,
    const std::shared_ptr<OffsetBufferAllocator>& alloc, size_t size = 256) {
    std::vector<Replica> reps;
    reps.emplace_back(alloc->allocate(size), ReplicaStatus::PROCESSING);
    const ReplicaID id = reps.back().id();
    metadata.AddReplicas(std::move(reps));
    return id;
}

TEST(PromotionManagerTest, ValidatesConfigAndRespectsDisabledFlag) {
    LocalSsdManager local_ssd;

    {
        auto cfg = MakeDefaultConfig();
        cfg.promotion_on_hit = false;
        PromotionManager manager(local_ssd, cfg, kDefaultTimeout);
        EXPECT_FALSE(manager.HasPendingCandidates());
        auto gate = manager.EvaluateReadHitGate(kTenant, "k", 0.1, 0.8, false);
        EXPECT_FALSE(gate.passed);
        EXPECT_EQ(gate.result, PromotionQueueResult::kDisabled);
    }

    {
        auto cfg = MakeDefaultConfig();
        cfg.promotion_admission_threshold = 0;
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(PromotionManager(local_ssd, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }
    {
        auto cfg = MakeDefaultConfig();
        cfg.promotion_admission_threshold = 256;
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(PromotionManager(local_ssd, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }
    {
        auto cfg = MakeDefaultConfig();
        cfg.promotion_max_per_heartbeat = 0;
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(PromotionManager(local_ssd, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }
}

TEST(PromotionManagerTest, ShouldPromoteOnReadHitChecksMediaMix) {
    LocalSsdManager local_ssd;
    PromotionManager manager(local_ssd, MakeDefaultConfig(), kDefaultTimeout);
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);

    Replica disk_rep(client_id, 128, "tcp://127.0.0.1:9201",
                     ReplicaStatus::COMPLETE, session);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_p0", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9201", "tcp");
    Replica mem_rep(alloc->allocate(128), ReplicaStatus::COMPLETE);

    std::vector<Replica::Descriptor> disk_only{disk_rep.get_descriptor()};
    std::vector<Replica::Descriptor> mem_and_disk{mem_rep.get_descriptor(),
                                                  disk_rep.get_descriptor()};
    std::vector<Replica::Descriptor> mem_only{mem_rep.get_descriptor()};

    EXPECT_TRUE(manager.ShouldPromoteOnReadHit(disk_only));
    EXPECT_FALSE(manager.ShouldPromoteOnReadHit(mem_and_disk));
    EXPECT_FALSE(manager.ShouldPromoteOnReadHit(mem_only));
}

TEST(PromotionManagerTest, ReadHitGateAndCandidateRetryLifecycle) {
    LocalSsdManager local_ssd;
    PromotionManager manager(local_ssd, MakeDefaultConfig(), kDefaultTimeout);
    const UUID holder_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    // Threshold is 2: first hit rejected by frequency.
    auto g1 = manager.EvaluateReadHitGate(kTenant, "k_hot", 0.2, 0.8, false);
    EXPECT_FALSE(g1.passed);
    EXPECT_EQ(g1.result, PromotionQueueResult::kFrequencyRejected);

    // Second hit rejected by watermark; record candidate internally.
    auto g2 = manager.EvaluateReadHitGate(kTenant, "k_hot", 0.85, 0.8, false);
    EXPECT_FALSE(g2.passed);
    EXPECT_EQ(g2.result, PromotionQueueResult::kWatermarkRejected);
    manager.RecordWatermarkCandidate(kTenant, "k_hot", g2.freq);
    EXPECT_TRUE(manager.HasPendingCandidates());

    auto due = manager.TakeDueCandidates(std::chrono::steady_clock::now());
    ASSERT_EQ(due.size(), 1u);
    EXPECT_EQ(due[0].key, "k_hot");

    // Admit consumes the candidate and pins the source replica.
    auto obj = MakeDiskObject(holder_id, session, kTenant, "k_hot", 256);
    Replica* src = FirstLocalDiskReplica(*obj);
    EXPECT_EQ(
        manager.AdmitPromotion(kTenant, "k_hot", *src, g2.freq, false, false),
        PromotionQueueResult::kQueued);
    EXPECT_EQ(src->get_refcnt(), 1u);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_hot"));
    EXPECT_FALSE(manager.HasPendingCandidates());

    // Force bypasses frequency gate, while watermark still applies.
    auto g_force_wm = manager.EvaluateReadHitGate(kTenant, "k_cold", 0.85, 0.8,
                                                  /*force=*/true);
    EXPECT_FALSE(g_force_wm.passed);
    EXPECT_EQ(g_force_wm.result, PromotionQueueResult::kWatermarkRejected);
    auto g_force_ok = manager.EvaluateReadHitGate(kTenant, "k_cold", 0.2, 0.8,
                                                  /*force=*/true);
    EXPECT_TRUE(g_force_ok.passed);
}

TEST(PromotionManagerTest, AdmitAllocCommitAndPrefetchLifecycle) {
    LocalSsdManager local_ssd;
    PromotionManager manager(local_ssd, MakeDefaultConfig(), kDefaultTimeout);

    const UUID holder_id = generate_uuid();
    const UUID other_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    auto obj = MakeDiskObject(holder_id, session, kTenant, "k_promo", 512);
    Replica* src = FirstLocalDiskReplica(*obj);
    ASSERT_EQ(manager.AdmitPromotion(kTenant, "k_promo", *src, 2, true, true),
              PromotionQueueResult::kQueued);
    EXPECT_EQ(src->get_refcnt(), 1u);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_promo"));

    // Heartbeat drains queued task.
    auto pending = manager.TakePendingPromotions(holder_id);
    ASSERT_TRUE(pending.has_value());
    ASSERT_EQ(pending->size(), 1u);
    EXPECT_EQ((*pending)[0].key, "k_promo");

    // BeginAlloc validates holder and size, and prevents concurrent double
    // reservation.
    EXPECT_EQ(manager.BeginAlloc(other_id, kTenant, "k_promo", 512).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(manager.BeginAlloc(holder_id, kTenant, "k_promo", 256).error(),
              ErrorCode::INVALID_PARAMS);
    {
        auto reservation =
            manager.BeginAlloc(holder_id, kTenant, "k_promo", 512);
        ASSERT_TRUE(reservation.has_value());
        EXPECT_EQ(
            manager.BeginAlloc(holder_id, kTenant, "k_promo", 512).error(),
            ErrorCode::REPLICA_IS_NOT_READY);
        // Dropping `reservation` without Commit automatically rolls back.
    }

    auto reservation = manager.BeginAlloc(holder_id, kTenant, "k_promo", 512);
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(reservation->Commit(200).has_value());

    // GetStagedAllocId enforces holder check and returns alloc_id.
    EXPECT_EQ(manager.GetStagedAllocId(other_id, kTenant, "k_promo").error(),
              ErrorCode::INVALID_PARAMS);
    auto staged_id = manager.GetStagedAllocId(holder_id, kTenant, "k_promo");
    ASSERT_TRUE(staged_id.has_value());
    EXPECT_EQ(*staged_id, 200u);

    // CompletePromotion removes task, unpins source, and returns from_prefetch.
    auto complete =
        manager.CompletePromotion(holder_id, kTenant, "k_promo", true, *obj);
    ASSERT_TRUE(complete.has_value());
    EXPECT_FALSE(*complete);
    EXPECT_EQ(src->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasTask(kTenant, "k_promo"));

    // Prefetch task validates holder, pins source, and returns
    // from_prefetch=true on complete.
    auto pf_obj = MakeDiskObject(holder_id, session, kTenant, "k_pf", 256);
    Replica* pf_src = FirstLocalDiskReplica(*pf_obj);
    EXPECT_EQ(manager.RegisterPrefetchTask(other_id, kTenant, "k_pf", *pf_src)
                  .error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(pf_src->get_refcnt(), 0u);

    ASSERT_TRUE(
        manager.RegisterPrefetchTask(holder_id, kTenant, "k_pf", *pf_src)
            .has_value());
    EXPECT_EQ(pf_src->get_refcnt(), 1u);
    {
        auto pf_res = manager.BeginAlloc(holder_id, kTenant, "k_pf", 256);
        ASSERT_TRUE(pf_res.has_value());
        ASSERT_TRUE(pf_res->Commit(201).has_value());
    }
    auto pf_done =
        manager.CompletePromotion(holder_id, kTenant, "k_pf", true, *pf_obj);
    ASSERT_TRUE(pf_done.has_value());
    EXPECT_TRUE(*pf_done);
    EXPECT_EQ(pf_src->get_refcnt(), 0u);
}

TEST(PromotionManagerTest,
     FailPromotionBoundsRetriesPopsStagedAndSkipsPrefetch) {
    LocalSsdManager local_ssd;
    PromotionManager manager(local_ssd, MakeDefaultConfig(), kDefaultTimeout);
    const UUID holder_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_pfail", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9201", "tcp");
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    auto obj = MakeDiskObject(holder_id, session, kTenant, "k_fail", 256);

    for (uint32_t attempt = 0;
         attempt <= PromotionManager::kMaxPromotionExecutionFailures;
         ++attempt) {
        Replica* src = FirstLocalDiskReplica(*obj);
        ASSERT_NE(src, nullptr);
        ASSERT_EQ(
            manager.AdmitPromotion(kTenant, "k_fail", *src, 2, true, true),
            PromotionQueueResult::kQueued);
        EXPECT_EQ(src->get_refcnt(), 1u);
        auto res = manager.BeginAlloc(holder_id, kTenant, "k_fail", 256);
        ASSERT_TRUE(res.has_value());
        const ReplicaID staged_id = AddStagedMemoryReplica(*obj, alloc, 256);
        ASSERT_TRUE(res->Commit(staged_id).has_value());
        ASSERT_TRUE(obj->HasReplica(&Replica::fn_is_memory_replica));

        ASSERT_TRUE(manager.FailPromotion(holder_id, kTenant, "k_fail", *obj)
                        .has_value());
        EXPECT_EQ(FirstLocalDiskReplica(*obj)->get_refcnt(), 0u);
        EXPECT_FALSE(obj->HasReplica(&Replica::fn_is_memory_replica));
        EXPECT_FALSE(manager.HasTask(kTenant, "k_fail"));
        if (attempt < PromotionManager::kMaxPromotionExecutionFailures) {
            EXPECT_TRUE(manager.HasPendingCandidates());
        } else {
            EXPECT_FALSE(manager.HasPendingCandidates());
        }
    }

    // Prefetch failures must NOT record an on-hit retry candidate.
    auto pf_obj = MakeDiskObject(holder_id, session, kTenant, "k_pf_fail", 256);
    Replica* pf_src = FirstLocalDiskReplica(*pf_obj);
    ASSERT_TRUE(
        manager.RegisterPrefetchTask(holder_id, kTenant, "k_pf_fail", *pf_src)
            .has_value());
    EXPECT_EQ(pf_src->get_refcnt(), 1u);
    ASSERT_TRUE(manager.FailPromotion(holder_id, kTenant, "k_pf_fail", *pf_obj)
                    .has_value());
    EXPECT_EQ(pf_src->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasPendingCandidates());
}

TEST(PromotionManagerTest, CancelRemovedReplicasEraseAndDiscardExpiredTasks) {
    LocalSsdManager local_ssd;
    PromotionManager manager(local_ssd, MakeDefaultConfig(),
                             std::chrono::seconds(60));
    const UUID holder_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_pexp", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9201", "tcp");
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    // CancelTaskForRemovedReplicas triggers only when alloc_id is removed, and
    // unpins source.
    {
        auto obj = MakeDiskObject(holder_id, session, kTenant, "k_cancel", 256);
        Replica* src = FirstLocalDiskReplica(*obj);
        ASSERT_EQ(
            manager.AdmitPromotion(kTenant, "k_cancel", *src, 2, true, true),
            PromotionQueueResult::kQueued);
        EXPECT_EQ(src->get_refcnt(), 1u);
        {
            auto res = manager.BeginAlloc(holder_id, kTenant, "k_cancel", 256);
            ASSERT_TRUE(res.has_value());
            ASSERT_TRUE(res->Commit(600).has_value());
        }

        const std::vector<ReplicaID> other_ids{601};
        EXPECT_FALSE(manager.CancelTaskForRemovedReplicas(kTenant, "k_cancel",
                                                          other_ids, *obj));
        EXPECT_EQ(src->get_refcnt(), 1u);
        EXPECT_TRUE(manager.HasTask(kTenant, "k_cancel"));

        const std::vector<ReplicaID> matching_ids{600};
        EXPECT_TRUE(manager.CancelTaskForRemovedReplicas(kTenant, "k_cancel",
                                                         matching_ids, *obj));
        EXPECT_EQ(src->get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_cancel"));
    }

    // OnObjectErased drops task, unpins source, and drops retry candidate.
    {
        auto obj = MakeDiskObject(holder_id, session, kTenant, "k_erase", 256);
        Replica* src = FirstLocalDiskReplica(*obj);
        ASSERT_EQ(
            manager.AdmitPromotion(kTenant, "k_erase", *src, 2, true, true),
            PromotionQueueResult::kQueued);
        manager.RecordWatermarkCandidate(kTenant, "k_erase", 3);
        EXPECT_TRUE(manager.HasPendingCandidates());
        manager.OnObjectErased(kTenant, "k_erase", *obj);
        EXPECT_EQ(src->get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_erase"));
        EXPECT_FALSE(manager.HasPendingCandidates());
    }

    // DiscardExpiredTasks: non-expired untouched; completed task skipped;
    // expired tasks unpinned and staged replicas popped; orphaned nullptr
    // metadata tasks still cleared.
    const TenantId tenant_b("tenant-b");
    auto obj_a = MakeDiskObject(holder_id, session, kTenant, "k_exp_a", 256);
    auto obj_b = MakeDiskObject(holder_id, session, tenant_b, "k_exp_b", 256);
    auto obj_c = MakeDiskObject(holder_id, session, kTenant, "k_exp_gone", 256);
    Replica* src_a = FirstLocalDiskReplica(*obj_a);
    Replica* src_b = FirstLocalDiskReplica(*obj_b);
    Replica* src_c = FirstLocalDiskReplica(*obj_c);

    ASSERT_EQ(manager.AdmitPromotion(kTenant, "k_exp_a", *src_a, 2, true, true),
              PromotionQueueResult::kQueued);
    {
        auto res = manager.BeginAlloc(holder_id, kTenant, "k_exp_a", 256);
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res->Commit(701).has_value());
    }
    ASSERT_EQ(
        manager.AdmitPromotion(tenant_b, "k_exp_b", *src_b, 2, true, true),
        PromotionQueueResult::kQueued);
    {
        auto res = manager.BeginAlloc(holder_id, tenant_b, "k_exp_b", 256);
        ASSERT_TRUE(res.has_value());
        const ReplicaID staged_b = AddStagedMemoryReplica(*obj_b, alloc, 256);
        ASSERT_TRUE(res->Commit(staged_b).has_value());
    }
    ASSERT_EQ(
        manager.AdmitPromotion(kTenant, "k_exp_gone", *src_c, 2, true, true),
        PromotionQueueResult::kQueued);

    auto lock_cb = [&](const TenantId& t, std::string_view k, auto&& fn) {
        if (t == kTenant && k == "k_exp_a") {
            fn(obj_a.get());
        } else if (t == tenant_b && k == "k_exp_b") {
            fn(obj_b.get());
        } else {
            fn(nullptr);
        }
    };

    const auto now = std::chrono::system_clock::now();
    manager.DiscardExpiredTasks(now, lock_cb);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_exp_a"));
    EXPECT_TRUE(manager.HasTask(tenant_b, "k_exp_b"));
    EXPECT_TRUE(manager.HasTask(kTenant, "k_exp_gone"));

    // Completing k_exp_a before expiry runs makes its expiry a no-op.
    ASSERT_TRUE(
        manager.CompletePromotion(holder_id, kTenant, "k_exp_a", true, *obj_a)
            .has_value());
    EXPECT_EQ(src_a->get_refcnt(), 0u);

    const auto future = now + std::chrono::seconds(120);
    manager.DiscardExpiredTasks(future, lock_cb);
    EXPECT_FALSE(manager.HasTask(tenant_b, "k_exp_b"));
    EXPECT_EQ(FirstLocalDiskReplica(*obj_b)->get_refcnt(), 0u);
    EXPECT_FALSE(obj_b->HasReplica(&Replica::fn_is_memory_replica));
    EXPECT_FALSE(manager.HasTask(kTenant, "k_exp_gone"));
}

TEST(PromotionManagerTest, ShouldPromoteOnReadHitDisabledAndEmpty) {
    LocalSsdManager local_ssd;
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    Replica disk_rep(client_id, 128, "tcp://127.0.0.1:9202",
                     ReplicaStatus::COMPLETE, session);
    std::vector<Replica::Descriptor> disk_only{disk_rep.get_descriptor()};
    std::vector<Replica::Descriptor> empty;

    {
        auto cfg = MakeDefaultConfig();
        cfg.promotion_on_hit = false;
        PromotionManager disabled(local_ssd, cfg, kDefaultTimeout);
        EXPECT_FALSE(disabled.ShouldPromoteOnReadHit(disk_only));
        EXPECT_FALSE(disabled.ShouldPromoteOnReadHit(empty));
        disabled.RecordWatermarkCandidate(kTenant, "k_dis", 2);
        EXPECT_FALSE(disabled.HasPendingCandidates());
        EXPECT_TRUE(disabled.TakeDueCandidates(std::chrono::steady_clock::now())
                        .empty());
    }

    {
        PromotionManager enabled(local_ssd, MakeDefaultConfig(),
                                 kDefaultTimeout);
        EXPECT_FALSE(enabled.ShouldPromoteOnReadHit(empty));
    }
}

TEST(PromotionManagerTest, AdmitPromotionRejectionBranchesAndRecencyTouch) {
    LocalSsdManager local_ssd;
    auto cfg = MakeDefaultConfig();
    cfg.promotion_queue_limit = 2;
    cfg.promotion_max_per_heartbeat = 1;
    PromotionManager manager(local_ssd, cfg, kDefaultTimeout);

    const UUID holder_id = generate_uuid();
    const UUID unknown_holder = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    auto offline_session = MakeNonServingSession(holder_id);
    auto unknown_session = MakeActiveSession(unknown_holder);
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    auto& metrics = MasterMetricManager::instance();
    const int64_t admitted_base = metrics.get_promotion_admitted();
    const int64_t in_flight_base = metrics.get_promotion_in_flight();
    const int64_t rejected_cap_base = metrics.get_promotion_rejected_cap();

    auto obj1 = MakeDiskObject(holder_id, session, kTenant, "k1", 128);
    auto obj2 = MakeDiskObject(holder_id, session, kTenant, "k2", 128);
    auto obj3 = MakeDiskObject(holder_id, session, kTenant, "k3", 128);
    Replica* src1 = FirstLocalDiskReplica(*obj1);
    Replica* src2 = FirstLocalDiskReplica(*obj2);
    Replica* src3 = FirstLocalDiskReplica(*obj3);

    ASSERT_EQ(manager.AdmitPromotion(kTenant, "k1", *src1, 2, true, false),
              PromotionQueueResult::kQueued);
    ASSERT_EQ(manager.AdmitPromotion(kTenant, "k2", *src2, 2, true, false),
              PromotionQueueResult::kQueued);
    EXPECT_EQ(src1->get_refcnt(), 1u);
    EXPECT_EQ(src2->get_refcnt(), 1u);
    EXPECT_EQ(metrics.get_promotion_admitted() - admitted_base, 2);
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 2);

    manager.RecordWatermarkCandidate(kTenant, "k1", 2);
    ASSERT_TRUE(manager.HasPendingCandidates());
    EXPECT_EQ(manager.AdmitPromotion(kTenant, "k1", *src1, 2, true, false),
              PromotionQueueResult::kAlreadyInFlight);
    EXPECT_EQ(src1->get_refcnt(), 1u);
    EXPECT_FALSE(manager.HasPendingCandidates());

    // Queue cap (2) rejects k3 without pinning; records candidate only when
    // record_candidate && !force.
    EXPECT_EQ(manager.AdmitPromotion(kTenant, "k3", *src3, 2,
                                     /*record_candidate=*/false,
                                     /*force=*/false),
              PromotionQueueResult::kQueueCapRejected);
    EXPECT_EQ(src3->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasPendingCandidates());

    EXPECT_EQ(manager.AdmitPromotion(kTenant, "k3", *src3, 2,
                                     /*record_candidate=*/true, /*force=*/true),
              PromotionQueueResult::kQueueCapRejected);
    EXPECT_FALSE(manager.HasPendingCandidates());

    EXPECT_EQ(manager.AdmitPromotion(kTenant, "k3", *src3, 2,
                                     /*record_candidate=*/true,
                                     /*force=*/false),
              PromotionQueueResult::kQueueCapRejected);
    EXPECT_EQ(src3->get_refcnt(), 0u);
    EXPECT_TRUE(manager.HasPendingCandidates());
    EXPECT_EQ(metrics.get_promotion_rejected_cap() - rejected_cap_base, 3);
    manager.EraseCandidate(kTenant, "k3");
    EXPECT_FALSE(manager.HasPendingCandidates());

    // TakePendingPromotions respects promotion_max_per_heartbeat (1) and
    // returns the touched k1 before k2.
    auto hb1 = manager.TakePendingPromotions(holder_id);
    ASSERT_TRUE(hb1.has_value());
    ASSERT_EQ(hb1->size(), 1u);
    EXPECT_EQ((*hb1)[0].key, "k1");

    auto hb2 = manager.TakePendingPromotions(holder_id);
    ASSERT_TRUE(hb2.has_value());
    ASSERT_EQ(hb2->size(), 1u);
    EXPECT_EQ((*hb2)[0].key, "k2");

    auto hb_unknown = manager.TakePendingPromotions(unknown_holder);
    ASSERT_FALSE(hb_unknown.has_value());
    EXPECT_EQ(hb_unknown.error(), ErrorCode::SEGMENT_NOT_FOUND);

    // Free task slots so subsequent gate checks pass the cap check.
    manager.OnObjectErased(kTenant, "k1", *obj1);
    manager.OnObjectErased(kTenant, "k2", *obj2);
    EXPECT_EQ(src1->get_refcnt(), 0u);
    EXPECT_EQ(src2->get_refcnt(), 0u);
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 0);

    // Missing session -> kNoLocalDiskSource.
    {
        Replica no_session(holder_id, 128, "tcp://127.0.0.1:9201",
                           ReplicaStatus::COMPLETE, nullptr);
        EXPECT_EQ(manager.AdmitPromotion(kTenant, "k_nosess", no_session, 2,
                                         true, false),
                  PromotionQueueResult::kNoLocalDiskSource);
        EXPECT_EQ(no_session.get_refcnt(), 0u);
    }

    // Offline session (not serving) -> kPushFailed; candidate recorded only
    // when record_candidate && !force.
    {
        Replica unserv(holder_id, 128, "tcp://127.0.0.1:9201",
                       ReplicaStatus::COMPLETE, offline_session);
        EXPECT_EQ(
            manager.AdmitPromotion(kTenant, "k_unserv", unserv, 2, true, true),
            PromotionQueueResult::kPushFailed);
        EXPECT_EQ(unserv.get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasPendingCandidates());

        EXPECT_EQ(
            manager.AdmitPromotion(kTenant, "k_unserv", unserv, 2, true, false),
            PromotionQueueResult::kPushFailed);
        EXPECT_EQ(unserv.get_refcnt(), 0u);
        EXPECT_TRUE(manager.HasPendingCandidates());
        manager.EraseCandidate(kTenant, "k_unserv");
    }

    // Unregistered holder (SEGMENT_NOT_FOUND) -> kNoLocalDiskSource and erases
    // any stale candidate.
    {
        Replica unk(unknown_holder, 128, "tcp://127.0.0.1:9201",
                    ReplicaStatus::COMPLETE, unknown_session);
        manager.RecordWatermarkCandidate(kTenant, "k_no_seg", 2);
        EXPECT_EQ(
            manager.AdmitPromotion(kTenant, "k_no_seg", unk, 2, true, false),
            PromotionQueueResult::kNoLocalDiskSource);
        EXPECT_EQ(unk.get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasPendingCandidates());
    }

    // Pre-existing mailbox entry (OBJECT_ALREADY_EXISTS) -> kAlreadyInFlight
    // and erases candidate.
    ASSERT_EQ(local_ssd.EnqueuePromotion(
                  holder_id, PromotionTaskItem{.tenant_id = kTenant.value(),
                                               .key = "k_dup_mb",
                                               .size = 128}),
              ErrorCode::OK);
    manager.RecordWatermarkCandidate(kTenant, "k_dup_mb", 2);
    EXPECT_EQ(
        manager.AdmitPromotion(kTenant, "k_dup_mb", *src3, 2, true, false),
        PromotionQueueResult::kAlreadyInFlight);
    EXPECT_EQ(src3->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasPendingCandidates());
}

TEST(PromotionManagerTest, RegisterPrefetchLimitsAndCandidateConsumption) {
    LocalSsdManager local_ssd;
    auto cfg = MakeDefaultConfig();
    cfg.promotion_queue_limit = 1;
    PromotionManager manager(local_ssd, cfg, kDefaultTimeout);

    const UUID holder_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    manager.RecordWatermarkCandidate(kTenant, "pf_a", 3);
    ASSERT_TRUE(manager.HasPendingCandidates());

    auto& metrics = MasterMetricManager::instance();
    const int64_t in_flight_base = metrics.get_promotion_in_flight();

    auto obj_a = MakeDiskObject(holder_id, session, kTenant, "pf_a", 256);
    auto obj_b = MakeDiskObject(holder_id, session, kTenant, "pf_b", 256);
    Replica* src_a = FirstLocalDiskReplica(*obj_a);
    Replica* src_b = FirstLocalDiskReplica(*obj_b);

    ASSERT_TRUE(manager.RegisterPrefetchTask(holder_id, kTenant, "pf_a", *src_a)
                    .has_value());
    EXPECT_EQ(src_a->get_refcnt(), 1u);
    EXPECT_FALSE(manager.HasPendingCandidates());
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 1);

    // Duplicate registration -> PROMOTION_ALREADY_EXISTS without double pin.
    auto dup = manager.RegisterPrefetchTask(holder_id, kTenant, "pf_a", *src_a);
    ASSERT_FALSE(dup.has_value());
    EXPECT_EQ(dup.error(), ErrorCode::PROMOTION_ALREADY_EXISTS);
    EXPECT_EQ(src_a->get_refcnt(), 1u);

    // Exceeding promotion_queue_limit -> KEYS_ULTRA_LIMIT without pinning.
    auto over =
        manager.RegisterPrefetchTask(holder_id, kTenant, "pf_b", *src_b);
    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error(), ErrorCode::KEYS_ULTRA_LIMIT);
    EXPECT_EQ(src_b->get_refcnt(), 0u);

    manager.OnObjectErased(kTenant, "pf_a", *obj_a);
    EXPECT_EQ(src_a->get_refcnt(), 0u);
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 0);
}

TEST(PromotionManagerTest, AllocReservationMovesAndLifecycleErrorPaths) {
    LocalSsdManager local_ssd;
    PromotionManager manager(local_ssd, MakeDefaultConfig(), kDefaultTimeout);
    const UUID holder_id = generate_uuid();
    const UUID other_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_pmv", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9201", "tcp");
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    // Default-constructed reservation fails Commit cleanly.
    {
        PromotionManager::AllocReservation empty_res;
        auto commit = empty_res.Commit(999);
        ASSERT_FALSE(commit.has_value());
        EXPECT_EQ(commit.error(), ErrorCode::REPLICA_IS_NOT_READY);
    }

    auto missing_obj = MakeDiskObject(holder_id, session, kTenant, "missing");
    EXPECT_EQ(manager.BeginAlloc(holder_id, kTenant, "missing", 128).error(),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(manager.GetStagedAllocId(holder_id, kTenant, "missing").error(),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(manager
                  .CompletePromotion(holder_id, kTenant, "missing", true,
                                     *missing_obj)
                  .error(),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_TRUE(
        manager.FailPromotion(holder_id, kTenant, "missing", *missing_obj)
            .has_value());

    auto obj1 = MakeDiskObject(holder_id, session, kTenant, "k_mv1", 256);
    auto obj2 = MakeDiskObject(holder_id, session, kTenant, "k_mv2", 256);
    Replica* src1 = FirstLocalDiskReplica(*obj1);
    Replica* src2 = FirstLocalDiskReplica(*obj2);
    ASSERT_EQ(manager.AdmitPromotion(kTenant, "k_mv1", *src1, 2, true, true),
              PromotionQueueResult::kQueued);
    ASSERT_EQ(manager.AdmitPromotion(kTenant, "k_mv2", *src2, 2, true, true),
              PromotionQueueResult::kQueued);

    // Unstaged task (alloc_id == 0) rejects GetStagedAllocId and
    // CompletePromotion.
    EXPECT_EQ(manager.GetStagedAllocId(holder_id, kTenant, "k_mv1").error(),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(
        manager.CompletePromotion(holder_id, kTenant, "k_mv1", true, *obj1)
            .error(),
        ErrorCode::REPLICA_IS_NOT_READY);

    // Move construction and move assignment release the old reservation and
    // transfer ownership cleanly.
    {
        auto res1 = manager.BeginAlloc(holder_id, kTenant, "k_mv1", 256);
        ASSERT_TRUE(res1.has_value());
        PromotionManager::AllocReservation moved_ctor(std::move(*res1));
        EXPECT_EQ(res1->Commit(1001).error(), ErrorCode::REPLICA_IS_NOT_READY);

        auto res2 = manager.BeginAlloc(holder_id, kTenant, "k_mv2", 256);
        ASSERT_TRUE(res2.has_value());
        moved_ctor = std::move(*res2);

        auto res1_again = manager.BeginAlloc(holder_id, kTenant, "k_mv1", 256);
        ASSERT_TRUE(res1_again.has_value());
        ASSERT_TRUE(res1_again->Commit(2001).has_value());
        EXPECT_EQ(res1_again->Commit(2002).error(),
                  ErrorCode::REPLICA_IS_NOT_READY);
        EXPECT_EQ(manager.BeginAlloc(holder_id, kTenant, "k_mv1", 256).error(),
                  ErrorCode::REPLICA_IS_NOT_READY);

        ASSERT_TRUE(moved_ctor.Commit(2002).has_value());
    }

    // Wrong holder rejected on CompletePromotion and FailPromotion without
    // removing the task.
    EXPECT_EQ(manager.CompletePromotion(other_id, kTenant, "k_mv1", true, *obj1)
                  .error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(manager.FailPromotion(other_id, kTenant, "k_mv1", *obj1).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_mv1"));
    EXPECT_EQ(src1->get_refcnt(), 1u);

    auto& metrics = MasterMetricManager::instance();
    const int64_t comp_base = metrics.get_promotion_completed();
    const int64_t comp_bytes_base = metrics.get_promotion_completed_bytes();
    const int64_t cancelled_base = metrics.get_promotion_cancelled();

    auto ok_done =
        manager.CompletePromotion(holder_id, kTenant, "k_mv1", true, *obj1);
    ASSERT_TRUE(ok_done.has_value());
    EXPECT_EQ(src1->get_refcnt(), 0u);
    EXPECT_EQ(metrics.get_promotion_completed() - comp_base, 1);
    EXPECT_EQ(metrics.get_promotion_completed_bytes() - comp_bytes_base, 256);

    auto cancel_done =
        manager.CompletePromotion(holder_id, kTenant, "k_mv2", false, *obj2);
    ASSERT_TRUE(cancel_done.has_value());
    EXPECT_EQ(src2->get_refcnt(), 0u);
    EXPECT_EQ(metrics.get_promotion_completed() - comp_base, 1);
    EXPECT_EQ(metrics.get_promotion_cancelled() - cancelled_base, 1);

    // Commit fails if the task was removed while reservation was held;
    // FailPromotion with an already-COMPLETE memory replica does not record a
    // retry candidate.
    auto raced_obj =
        MakeDiskObject(holder_id, session, kTenant, "k_raced", 128);
    Replica* raced_src = FirstLocalDiskReplica(*raced_obj);
    ASSERT_EQ(
        manager.AdmitPromotion(kTenant, "k_raced", *raced_src, 2, true, true),
        PromotionQueueResult::kQueued);
    auto raced_res = manager.BeginAlloc(holder_id, kTenant, "k_raced", 128);
    ASSERT_TRUE(raced_res.has_value());
    {
        std::vector<Replica> complete_mem;
        complete_mem.emplace_back(alloc->allocate(128),
                                  ReplicaStatus::COMPLETE);
        raced_obj->AddReplicas(std::move(complete_mem));
    }
    ASSERT_TRUE(manager.FailPromotion(holder_id, kTenant, "k_raced", *raced_obj)
                    .has_value());
    EXPECT_EQ(FirstLocalDiskReplica(*raced_obj)->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasPendingCandidates());
    EXPECT_EQ(raced_res->Commit(3001).error(), ErrorCode::REPLICA_IS_NOT_READY);
}

TEST(PromotionManagerTest, CandidateOutcomesEraseActiveTaskAndMultiTenant) {
    LocalSsdManager local_ssd;
    PromotionManager manager(local_ssd, MakeDefaultConfig(), kDefaultTimeout);
    const UUID holder_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    auto& metrics = MasterMetricManager::instance();
    const int64_t cand_admitted_base =
        metrics.get_promotion_candidate_admitted();
    const int64_t cand_rejected_base =
        metrics.get_promotion_candidate_admission_rejected();
    const int64_t in_flight_base = metrics.get_promotion_in_flight();
    const int64_t cancelled_base = metrics.get_promotion_cancelled();

    manager.RecordWatermarkCandidate(kTenant, "c1", 2);
    manager.ReportCandidateOutcome(kTenant, "c1",
                                   PromotionQueueResult::kQueued);
    EXPECT_EQ(metrics.get_promotion_candidate_admitted() - cand_admitted_base,
              1);

    manager.ReportCandidateOutcome(kTenant, "c1",
                                   PromotionQueueResult::kWatermarkRejected);
    EXPECT_EQ(metrics.get_promotion_candidate_admission_rejected() -
                  cand_rejected_base,
              1);
    EXPECT_TRUE(
        manager.TakeDueCandidates(std::chrono::steady_clock::now()).empty());
    EXPECT_EQ(manager
                  .TakeDueCandidates(std::chrono::steady_clock::now() +
                                     std::chrono::seconds(1))
                  .size(),
              1u);

    manager.ReportCandidateOutcome(kTenant, "c1",
                                   PromotionQueueResult::kMemoryReplicaPresent);
    EXPECT_FALSE(manager.HasPendingCandidates());

    // CancelTaskForRemovedReplicas: empty list or unstaged task (alloc_id==0)
    // is a no-op.
    auto unstaged =
        MakeDiskObject(holder_id, session, kTenant, "k_unstaged", 128);
    Replica* unstaged_src = FirstLocalDiskReplica(*unstaged);
    ASSERT_EQ(manager.AdmitPromotion(kTenant, "k_unstaged", *unstaged_src, 2,
                                     true, true),
              PromotionQueueResult::kQueued);
    EXPECT_FALSE(manager.CancelTaskForRemovedReplicas(kTenant, "k_unstaged", {},
                                                      *unstaged));
    const std::vector<ReplicaID> zero_id{0};
    EXPECT_FALSE(manager.CancelTaskForRemovedReplicas(kTenant, "k_unstaged",
                                                      zero_id, *unstaged));
    EXPECT_TRUE(manager.HasTask(kTenant, "k_unstaged"));

    manager.RecordWatermarkCandidate(kTenant, "k_unstaged", 2);
    manager.OnObjectErased(kTenant, "k_unstaged", *unstaged);
    EXPECT_EQ(unstaged_src->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasTask(kTenant, "k_unstaged"));
    EXPECT_FALSE(manager.HasPendingCandidates());
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 0);
    EXPECT_EQ(metrics.get_promotion_cancelled() - cancelled_base, 1);
    EXPECT_TRUE(manager.TakePendingPromotions(holder_id)->empty());

    // Multi-tenant isolation with the same key.
    const TenantId tenant_a("tenant-a");
    const TenantId tenant_b("tenant-b");
    auto obj_a = MakeDiskObject(holder_id, session, tenant_a, "shared", 128);
    auto obj_b = MakeDiskObject(holder_id, session, tenant_b, "shared", 256);
    ASSERT_EQ(
        manager.AdmitPromotion(tenant_a, "shared",
                               *FirstLocalDiskReplica(*obj_a), 2, true, true),
        PromotionQueueResult::kQueued);
    ASSERT_EQ(
        manager.AdmitPromotion(tenant_b, "shared",
                               *FirstLocalDiskReplica(*obj_b), 2, true, true),
        PromotionQueueResult::kQueued);
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 2);

    manager.OnObjectErased(tenant_a, "shared", *obj_a);
    EXPECT_EQ(FirstLocalDiskReplica(*obj_a)->get_refcnt(), 0u);
    EXPECT_EQ(FirstLocalDiskReplica(*obj_b)->get_refcnt(), 1u);
    EXPECT_FALSE(manager.HasTask(tenant_a, "shared"));
    EXPECT_TRUE(manager.HasTask(tenant_b, "shared"));
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 1);

    manager.OnObjectErased(tenant_b, "shared", *obj_b);
    EXPECT_EQ(FirstLocalDiskReplica(*obj_b)->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasTask(tenant_b, "shared"));
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 0);
}

TEST(PromotionManagerTest,
     ConcurrentSameKeyContentionPreservesInFlightAndRefcntBalance) {
    LocalSsdManager local_ssd;
    PromotionConfig cfg = MakeDefaultConfig();
    cfg.promotion_queue_limit = 50000;
    PromotionManager manager(local_ssd, cfg, std::chrono::seconds(1));

    const UUID holder_id = generate_uuid();
    auto session = MakeActiveSession(holder_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_phot", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9201", "tcp");
    ASSERT_EQ(local_ssd.RegisterClient(holder_id, true), ErrorCode::OK);

    auto& metrics = MasterMetricManager::instance();
    const int64_t in_flight_base = metrics.get_promotion_in_flight();

    constexpr int kHotKeys = 4;
    constexpr int kThreads = 8;
    constexpr int kIterations = 250;

    struct KeySlot {
        std::mutex mu;
        std::string key;
        std::unique_ptr<ObjectMetadata> obj;
    };
    std::vector<std::unique_ptr<KeySlot>> slots;
    slots.reserve(kHotKeys);
    for (int i = 0; i < kHotKeys; ++i) {
        auto slot = std::make_unique<KeySlot>();
        slot->key = "hot_promo_" + std::to_string(i);
        slot->obj = MakeDiskObject(holder_id, session, kTenant, slot->key, 256);
        slots.push_back(std::move(slot));
    }

    std::atomic<uint64_t> admitted{0};
    std::atomic<bool> start{false};

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            while (!start.load(std::memory_order_acquire)) {
            }
            for (int i = 0; i < kIterations; ++i) {
                auto& slot = *slots[(t + i) % kHotKeys];
                switch ((t + i) % 6) {
                    case 0:
                    case 1: {
                        std::lock_guard lock(slot.mu);
                        if (manager.AdmitPromotion(
                                kTenant, slot.key,
                                *FirstLocalDiskReplica(*slot.obj), 2, false,
                                true) == PromotionQueueResult::kQueued) {
                            admitted.fetch_add(1, std::memory_order_relaxed);
                        }
                        break;
                    }
                    case 2: {
                        std::lock_guard lock(slot.mu);
                        auto res = manager.BeginAlloc(holder_id, kTenant,
                                                      slot.key, 256);
                        if (res.has_value() && (i % 2 == 0)) {
                            const ReplicaID aid =
                                AddStagedMemoryReplica(*slot.obj, alloc, 256);
                            (void)res->Commit(aid);
                        }
                        break;
                    }
                    case 3: {
                        std::lock_guard lock(slot.mu);
                        (void)manager.CompletePromotion(
                            holder_id, kTenant, slot.key, false, *slot.obj);
                        break;
                    }
                    case 4: {
                        std::lock_guard lock(slot.mu);
                        (void)manager.FailPromotion(holder_id, kTenant,
                                                    slot.key, *slot.obj);
                        break;
                    }
                    case 5: {
                        if (i % 2 == 0) {
                            std::lock_guard lock(slot.mu);
                            auto staged = manager.GetStagedAllocId(
                                holder_id, kTenant, slot.key);
                            if (staged.has_value()) {
                                const std::vector<ReplicaID> ids{*staged};
                                (void)manager.CancelTaskForRemovedReplicas(
                                    kTenant, slot.key, ids, *slot.obj);
                            }
                        } else {
                            manager.DiscardExpiredTasks(
                                std::chrono::system_clock::now() +
                                    std::chrono::hours(1),
                                [&](const TenantId&, std::string_view k,
                                    auto&& fn) {
                                    for (auto& s : slots) {
                                        if (s->key == k) {
                                            std::lock_guard lock(s->mu);
                                            fn(s->obj.get());
                                            return;
                                        }
                                    }
                                    fn(nullptr);
                                });
                        }
                        break;
                    }
                }
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    // Retire any surviving tasks and verify exact refcnt == 0 & metric balance.
    for (auto& slot : slots) {
        std::lock_guard lock(slot->mu);
        ASSERT_TRUE(
            manager.FailPromotion(holder_id, kTenant, slot->key, *slot->obj)
                .has_value());
        EXPECT_FALSE(manager.HasTask(kTenant, slot->key));
        EXPECT_EQ(FirstLocalDiskReplica(*slot->obj)->get_refcnt(), 0u);
    }
    EXPECT_GT(admitted.load(), 0u);
    EXPECT_EQ(metrics.get_promotion_in_flight() - in_flight_base, 0);
}

}  // namespace
}  // namespace mooncake
