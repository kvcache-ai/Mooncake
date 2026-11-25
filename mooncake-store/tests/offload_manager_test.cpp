#include "offload_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
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

OffloadConfig MakeDefaultConfig() {
    OffloadConfig cfg;
    cfg.enable_offload = true;
    cfg.offload_cap_ratio = 0.5;
    cfg.offloading_queue_limit = 1000;
    return cfg;
}

class FakeHostView final : public OffloadManager::HostView {
   public:
    void RegisterSegment(std::string segment_name, const UUID& owner) {
        std::lock_guard lock(mutex_);
        owners_[std::move(segment_name)] = owner;
    }

    void SetReadable(bool readable) {
        readable_.store(readable, std::memory_order_relaxed);
    }

    bool IsReplicaReadable(const Replica& replica) const override {
        return readable_.load(std::memory_order_relaxed) &&
               replica.is_completed() && replica.is_available();
    }

    bool ResolveSegmentOwners(std::span<const std::string_view> segment_names,
                              std::span<UUID> out_owners) const override {
        if (out_owners.size() < segment_names.size()) {
            return false;
        }
        std::lock_guard lock(mutex_);
        for (size_t i = 0; i < segment_names.size(); ++i) {
            auto it = owners_.find(std::string(segment_names[i]));
            if (it == owners_.end()) {
                return false;
            }
            out_owners[i] = it->second;
        }
        return true;
    }

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, UUID> owners_;
    std::atomic<bool> readable_{true};
};

// Creates a serving, completed MEMORY replica backed by `alloc`.
Replica MakeMemoryReplica(const std::shared_ptr<OffsetBufferAllocator>& alloc,
                          const ClientSessionSharedPtr& session,
                          size_t size = 512) {
    auto buf = alloc->allocate(size);
    EXPECT_NE(buf, nullptr);
    if (session) {
        buf->bindSegmentLifetime(SegmentLifetime(session));
    }
    return Replica(std::move(buf), ReplicaStatus::COMPLETE);
}

std::unique_ptr<ObjectMetadata> MakeObject(const UUID& client_id,
                                           const TenantId& tenant_id,
                                           const std::string& key,
                                           std::vector<Replica> replicas,
                                           size_t value_length = 512) {
    return std::make_unique<ObjectMetadata>(
        client_id, std::chrono::system_clock::now(), value_length,
        std::move(replicas), std::nullopt, false, ObjectDataType::UNKNOWN,
        std::string{}, tenant_id, key);
}

Replica* FirstMemoryReplica(ObjectMetadata& metadata) {
    Replica* found = nullptr;
    metadata.VisitReplicas(&Replica::fn_is_memory_replica,
                           [&found](Replica& r) {
                               if (found == nullptr) {
                                   found = &r;
                               }
                           });
    return found;
}

TEST(OffloadManagerTest, ValidatesConfigAndComputesCap) {
    LocalSsdManager local_ssd;
    FakeHostView host;

    {
        auto cfg = MakeDefaultConfig();
        cfg.offload_cap_ratio = -0.1;
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(OffloadManager(local_ssd, host, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }
    {
        auto cfg = MakeDefaultConfig();
        cfg.offload_cap_ratio = 1.1;
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(OffloadManager(local_ssd, host, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }
    {
        auto cfg = MakeDefaultConfig();
        cfg.offload_cap_ratio = std::numeric_limits<double>::quiet_NaN();
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(OffloadManager(local_ssd, host, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }
    {
        auto cfg = MakeDefaultConfig();
        cfg.offloading_queue_limit = 0;
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(OffloadManager(local_ssd, host, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }
    {
        auto cfg = MakeDefaultConfig();
        cfg.offloading_queue_limit = 100'000'001ULL;
        EXPECT_THROW(cfg.Validate(), std::invalid_argument);
        EXPECT_THROW(OffloadManager(local_ssd, host, cfg, kDefaultTimeout),
                     std::invalid_argument);
    }

    auto cfg = MakeDefaultConfig();
    cfg.offload_on_evict = true;
    cfg.offload_force_evict = true;
    cfg.offload_cap_ratio = 0.25;
    cfg.offloading_queue_limit = 200;
    EXPECT_NO_THROW(cfg.Validate());
    EXPECT_EQ(cfg.OffloadCap(), 50);
    EXPECT_FALSE(cfg.ShouldOffloadOnPutEnd());
    EXPECT_TRUE(cfg.ShouldOffloadOnEvict());
    EXPECT_NO_THROW(OffloadManager(local_ssd, host, cfg, kDefaultTimeout));

    cfg.offload_on_evict = false;
    EXPECT_TRUE(cfg.ShouldOffloadOnPutEnd());
    EXPECT_FALSE(cfg.ShouldOffloadOnEvict());

    cfg.enable_offload = false;
    cfg.offload_on_evict = true;
    EXPECT_FALSE(cfg.ShouldOffloadOnPutEnd());
    EXPECT_FALSE(cfg.ShouldOffloadOnEvict());
}

TEST(OffloadManagerTest, DisabledConfigSkipsStartAndPutEnd) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_dis", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    host.RegisterSegment("seg_dis", client_id);
    ASSERT_EQ(local_ssd.RegisterClient(client_id, true), ErrorCode::OK);

    OffloadConfig disabled_cfg = MakeDefaultConfig();
    disabled_cfg.enable_offload = false;
    OffloadManager disabled(local_ssd, host, disabled_cfg, kDefaultTimeout);

    std::vector<Replica> reps;
    reps.push_back(MakeMemoryReplica(alloc, session, 512));
    auto obj = MakeObject(client_id, kTenant, "k_disabled", std::move(reps));
    Replica* mem = FirstMemoryReplica(*obj);

    EXPECT_FALSE(disabled.StartOffload(kTenant, "k_disabled", *mem));
    disabled.OnPutEnd(kTenant, "k_disabled", ReplicaType::MEMORY, *obj);
    EXPECT_FALSE(disabled.HasTask(kTenant, "k_disabled"));
    EXPECT_EQ(mem->get_refcnt(), 0u);

    Replica disk(client_id, 512, "tcp://127.0.0.1:9100",
                 ReplicaStatus::COMPLETE, session);
    EXPECT_EQ(
        disabled.CompleteOffload(client_id, kTenant, "k_disabled", *obj, disk),
        OffloadManager::CompleteStatus::kNotTracked);
}

TEST(OffloadManagerTest, ApplyLocalDiskReplicaInsertsOnceWithoutDuplicating) {
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_apply", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    std::vector<Replica> reps;
    reps.push_back(MakeMemoryReplica(alloc, session, 512));
    auto obj = MakeObject(client_id, kTenant, "k_apply", std::move(reps));

    Replica disk1(client_id, 512, "tcp://127.0.0.1:9100",
                  ReplicaStatus::COMPLETE, session);
    EXPECT_TRUE(obj->ApplyLocalDiskReplica(client_id, disk1));
    EXPECT_TRUE(obj->HasReplica(&Replica::fn_is_local_disk_replica));
    EXPECT_TRUE(obj->disk_cache_total_accounted);

    Replica disk2(client_id, 1024, "tcp://127.0.0.1:9200",
                  ReplicaStatus::COMPLETE, session);
    EXPECT_FALSE(obj->ApplyLocalDiskReplica(client_id, disk2));

    size_t local_disk_count = 0;
    obj->VisitReplicas(&Replica::fn_is_local_disk_replica,
                       [&](const Replica&) { ++local_disk_count; });
    EXPECT_EQ(local_disk_count, 1u);
}

TEST(OffloadManagerTest, StartCancelAndHeartbeatDrainLifecycle) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    OffloadManager manager(local_ssd, host, MakeDefaultConfig(),
                           kDefaultTimeout);
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_lc", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    host.RegisterSegment("seg_lc", client_id);
    ASSERT_EQ(local_ssd.RegisterClient(client_id, true), ErrorCode::OK);

    std::vector<Replica> reps;
    reps.push_back(MakeMemoryReplica(alloc, session, 512));
    auto obj = MakeObject(client_id, kTenant, "k_offload", std::move(reps));
    Replica* mem = FirstMemoryReplica(*obj);
    ASSERT_NE(mem, nullptr);
    EXPECT_EQ(mem->get_refcnt(), 0u);

    // StartOffload (eviction::OffloadQueue) pins source replica and records
    // task.
    EXPECT_TRUE(manager.StartOffload(kTenant, "k_offload", *mem));
    EXPECT_EQ(mem->get_refcnt(), 1u);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_offload"));
    EXPECT_TRUE(manager.HasInFlight(kTenant, "k_offload"));

    // Duplicate StartOffload while task is active returns false without
    // double-pinning.
    EXPECT_FALSE(manager.StartOffload(kTenant, "k_offload", *mem));
    EXPECT_EQ(mem->get_refcnt(), 1u);

    // While still queued in the mailbox, TryCancelQueuedOffload succeeds and
    // unpins.
    EXPECT_TRUE(manager.TryCancelQueuedOffload(kTenant, "k_offload"));
    EXPECT_EQ(mem->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasTask(kTenant, "k_offload"));

    // Re-enqueue, then disable offloading via heartbeat: drained task is
    // cleared and source replica is unpinned automatically via ReplicaPin.
    ASSERT_TRUE(manager.StartOffload(kTenant, "k_offload", *mem));
    EXPECT_EQ(mem->get_refcnt(), 1u);
    auto disabled_hb =
        manager.HandleOffloadHeartbeat(client_id, /*enable_offloading=*/false);
    ASSERT_TRUE(disabled_hb.has_value());
    EXPECT_TRUE(disabled_hb->empty());
    EXPECT_EQ(mem->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasTask(kTenant, "k_offload"));

    // Re-enable offloading, enqueue, drain via heartbeat: cancel must fail and
    // keep source pinned.
    ASSERT_TRUE(manager.HandleOffloadHeartbeat(client_id, true).has_value());
    ASSERT_TRUE(manager.StartOffload(kTenant, "k_offload", *mem));
    EXPECT_EQ(mem->get_refcnt(), 1u);
    auto drained = manager.HandleOffloadHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    EXPECT_EQ((*drained)[0].key, "k_offload");

    EXPECT_FALSE(manager.TryCancelQueuedOffload(kTenant, "k_offload"));
    EXPECT_EQ(mem->get_refcnt(), 1u);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_offload"));
}

TEST(OffloadManagerTest, CompleteNackAndUnmountedRefusal) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    OffloadManager manager(local_ssd, host, MakeDefaultConfig(),
                           kDefaultTimeout);
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_cmp", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    host.RegisterSegment("seg_cmp", client_id);
    ASSERT_EQ(local_ssd.RegisterClient(client_id, true), ErrorCode::OK);

    // FailOffload (NACK) clears task and unpins source replica.
    {
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc, session, 256));
        auto obj =
            MakeObject(client_id, kTenant, "k_nack", std::move(reps), 256);
        Replica* mem = FirstMemoryReplica(*obj);
        ASSERT_TRUE(manager.StartOffload(kTenant, "k_nack", *mem));
        EXPECT_EQ(mem->get_refcnt(), 1u);
        EXPECT_TRUE(manager.FailOffload(client_id, kTenant, "k_nack"));
        EXPECT_EQ(mem->get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_nack"));
        EXPECT_FALSE(manager.FailOffload(client_id, kTenant, "k_nack"));
    }

    // Complete when mounted clears task, unpins source, inserts LOCAL_DISK
    // replica, and records usage.
    {
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc, session, 256));
        auto obj =
            MakeObject(client_id, kTenant, "k_mounted", std::move(reps), 256);
        Replica* mem = FirstMemoryReplica(*obj);
        ASSERT_TRUE(manager.StartOffload(kTenant, "k_mounted", *mem));
        EXPECT_EQ(mem->get_refcnt(), 1u);

        Replica disk(client_id, 256, "tcp://127.0.0.1:9100",
                     ReplicaStatus::COMPLETE, session);
        EXPECT_EQ(manager.CompleteOffload(client_id, kTenant, "k_mounted", *obj,
                                          disk),
                  OffloadManager::CompleteStatus::kCommitted);
        EXPECT_EQ(mem->get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_mounted"));
        EXPECT_TRUE(obj->HasReplica(&Replica::fn_is_local_disk_replica));
        auto usage = local_ssd.GetUsage(client_id);
        ASSERT_TRUE(usage.has_value());
        EXPECT_EQ(usage->used_bytes, 256);
    }

    // Complete after client unmount still clears task & unpins source, but
    // refuses replica registration.
    {
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc, session, 256));
        auto obj =
            MakeObject(client_id, kTenant, "k_unmounted", std::move(reps), 256);
        Replica* mem = FirstMemoryReplica(*obj);
        ASSERT_TRUE(manager.StartOffload(kTenant, "k_unmounted", *mem));
        EXPECT_EQ(mem->get_refcnt(), 1u);

        (void)local_ssd.UnregisterClient(client_id);
        Replica disk(client_id, 256, "tcp://127.0.0.1:9100",
                     ReplicaStatus::COMPLETE, session);
        EXPECT_EQ(manager.CompleteOffload(client_id, kTenant, "k_unmounted",
                                          *obj, disk),
                  OffloadManager::CompleteStatus::kRefusedUnmounted);
        EXPECT_EQ(mem->get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_unmounted"));
        EXPECT_FALSE(obj->HasReplica(&Replica::fn_is_local_disk_replica));
    }
}

TEST(OffloadManagerTest, OnObjectErasedAndDiscardExpiredTasks) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    OffloadManager manager(local_ssd, host, MakeDefaultConfig(),
                           std::chrono::seconds(60));
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_exp", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    host.RegisterSegment("seg_exp", client_id);
    ASSERT_EQ(local_ssd.RegisterClient(client_id, true), ErrorCode::OK);

    // OnObjectErased removes active task, unpins source, and drops pending
    // mailbox entry (LOCAL_DISK usage release is handled by MasterService).
    {
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc, session, 512));
        reps.emplace_back(client_id, 512, "tcp://127.0.0.1:9100",
                          ReplicaStatus::COMPLETE, session);
        auto obj = MakeObject(client_id, kTenant, "k_erase", std::move(reps));
        Replica* mem = FirstMemoryReplica(*obj);
        ASSERT_TRUE(manager.StartOffload(kTenant, "k_erase", *mem));
        EXPECT_EQ(mem->get_refcnt(), 1u);

        manager.OnObjectErased(kTenant, "k_erase");
        EXPECT_EQ(mem->get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_erase"));
        auto remaining = manager.HandleOffloadHeartbeat(client_id, true);
        ASSERT_TRUE(remaining.has_value());
        EXPECT_TRUE(remaining->empty());
    }

    // DiscardExpiredTasks: non-expired tasks untouched; expired tasks removed
    // and unpinned via ReplicaPin (even if the object was already destroyed).
    const TenantId tenant_b("tenant-b");
    std::vector<Replica> ra;
    ra.push_back(MakeMemoryReplica(alloc, session, 512));
    auto obj_a = MakeObject(client_id, kTenant, "k_exp_a", std::move(ra));
    Replica* mem_a = FirstMemoryReplica(*obj_a);

    std::vector<Replica> rb;
    rb.push_back(MakeMemoryReplica(alloc, session, 512));
    auto obj_b = MakeObject(client_id, tenant_b, "k_exp_b", std::move(rb));
    Replica* mem_b = FirstMemoryReplica(*obj_b);

    std::vector<Replica> rc;
    rc.push_back(MakeMemoryReplica(alloc, session, 512));
    auto obj_c = MakeObject(client_id, kTenant, "k_exp_gone", std::move(rc));
    Replica* mem_c = FirstMemoryReplica(*obj_c);

    ASSERT_TRUE(manager.StartOffload(kTenant, "k_exp_a", *mem_a));
    ASSERT_TRUE(manager.StartOffload(tenant_b, "k_exp_b", *mem_b));
    ASSERT_TRUE(manager.StartOffload(kTenant, "k_exp_gone", *mem_c));
    // Destroy obj_c before expiry to verify ReplicaPin outlives the Replica
    // safely without use-after-free.
    obj_c.reset();

    const auto now = std::chrono::system_clock::now();
    manager.DiscardExpiredTasks(now);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_exp_a"));
    EXPECT_TRUE(manager.HasTask(tenant_b, "k_exp_b"));
    EXPECT_TRUE(manager.HasTask(kTenant, "k_exp_gone"));

    // Completing k_exp_a before expiry runs makes its expiry a no-op.
    Replica disk_a(client_id, 512, "tcp://127.0.0.1:9100",
                   ReplicaStatus::COMPLETE, session);
    EXPECT_EQ(
        manager.CompleteOffload(client_id, kTenant, "k_exp_a", *obj_a, disk_a),
        OffloadManager::CompleteStatus::kCommitted);
    EXPECT_EQ(mem_a->get_refcnt(), 0u);

    const auto future = now + std::chrono::seconds(120);
    manager.DiscardExpiredTasks(future);
    EXPECT_FALSE(manager.HasTask(tenant_b, "k_exp_b"));
    EXPECT_EQ(mem_b->get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasTask(kTenant, "k_exp_gone"));
}

TEST(OffloadManagerTest, OnPutEndOffloadAndMultiMirrorCancel) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    const UUID client_a = generate_uuid();
    const UUID client_b = generate_uuid();
    auto session_a = MakeActiveSession(client_a);
    auto session_b = MakeActiveSession(client_b);
    auto alloc_a = std::make_shared<OffsetBufferAllocator>(
        "seg_pe_a", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9101", "tcp");
    auto alloc_b = std::make_shared<OffsetBufferAllocator>(
        "seg_pe_b", 0x200000, 64 * 1024 * 1024, "tcp://127.0.0.1:9102", "tcp");
    host.RegisterSegment("seg_pe_a", client_a);
    host.RegisterSegment("seg_pe_b", client_b);

    // Disabled / offload-on-evict skips OnPutEnd.
    {
        OffloadConfig cfg = MakeDefaultConfig();
        cfg.offload_on_evict = true;
        OffloadManager evict_only(local_ssd, host, cfg, kDefaultTimeout);
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc_a, session_a, 256));
        auto obj =
            MakeObject(client_a, kTenant, "k_skip", std::move(reps), 256);
        evict_only.OnPutEnd(kTenant, "k_skip", ReplicaType::MEMORY, *obj);
        EXPECT_FALSE(evict_only.HasTask(kTenant, "k_skip"));
        EXPECT_EQ(FirstMemoryReplica(*obj)->get_refcnt(), 0u);
    }

    OffloadManager manager(local_ssd, host, MakeDefaultConfig(),
                           kDefaultTimeout);
    ASSERT_EQ(local_ssd.RegisterClient(client_a, true), ErrorCode::OK);
    ASSERT_EQ(local_ssd.RegisterClient(client_b, true), ErrorCode::OK);

    // DFS PutEnd is ignored.
    {
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc_a, session_a, 256));
        auto obj = MakeObject(client_a, kTenant, "k_dfs", std::move(reps), 256);
        manager.OnPutEnd(kTenant, "k_dfs", ReplicaType::DFS, *obj);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_dfs"));
    }

    // Two completed memory replicas on client_a & client_b: OnPutEnd queues to
    // both mirrors and pins the first enqueued memory replica.
    std::vector<Replica> reps;
    reps.push_back(MakeMemoryReplica(alloc_a, session_a, 256));
    reps.push_back(MakeMemoryReplica(alloc_b, session_b, 256));
    auto obj =
        MakeObject(client_a, kTenant, "k_mirrored", std::move(reps), 256);
    manager.OnPutEnd(kTenant, "k_mirrored", ReplicaType::MEMORY, *obj);
    EXPECT_TRUE(manager.HasTask(kTenant, "k_mirrored"));

    uint32_t total_pins = 0;
    obj->VisitReplicas(&Replica::fn_is_memory_replica,
                       [&](const Replica& r) { total_pins += r.get_refcnt(); });
    EXPECT_EQ(total_pins, 1u);

    // Draining only client_a prevents TryCancelQueuedOffload from cancelling
    // and leaves client_b's pending mirror untouched.
    auto hb_a = manager.HandleOffloadHeartbeat(client_a, true);
    ASSERT_TRUE(hb_a.has_value());
    ASSERT_EQ(hb_a->size(), 1u);
    EXPECT_EQ((*hb_a)[0].key, "k_mirrored");

    EXPECT_FALSE(manager.TryCancelQueuedOffload(kTenant, "k_mirrored"));
    EXPECT_TRUE(manager.HasTask(kTenant, "k_mirrored"));

    auto hb_b = manager.HandleOffloadHeartbeat(client_b, true);
    ASSERT_TRUE(hb_b.has_value());
    ASSERT_EQ(hb_b->size(), 1u);
    EXPECT_EQ((*hb_b)[0].key, "k_mirrored");

    // Atomic cancel succeeds when ALL mirrors are still pending, clearing both
    // mailboxes and unpinning the source.
    std::vector<Replica> reps2;
    reps2.push_back(MakeMemoryReplica(alloc_a, session_a, 128));
    reps2.push_back(MakeMemoryReplica(alloc_b, session_b, 128));
    auto obj2 =
        MakeObject(client_a, kTenant, "k_cancel_both", std::move(reps2), 128);
    manager.OnPutEnd(kTenant, "k_cancel_both", ReplicaType::MEMORY, *obj2);
    ASSERT_TRUE(manager.HasTask(kTenant, "k_cancel_both"));

    EXPECT_TRUE(manager.TryCancelQueuedOffload(kTenant, "k_cancel_both"));
    EXPECT_FALSE(manager.HasTask(kTenant, "k_cancel_both"));
    total_pins = 0;
    obj2->VisitReplicas(&Replica::fn_is_memory_replica, [&](const Replica& r) {
        total_pins += r.get_refcnt();
    });
    EXPECT_EQ(total_pins, 0u);
    EXPECT_TRUE(manager.HandleOffloadHeartbeat(client_a, true)->empty());
    EXPECT_TRUE(manager.HandleOffloadHeartbeat(client_b, true)->empty());
}

TEST(OffloadManagerTest,
     ConcurrentStartOffloadAndDisableHeartbeatNoLeakedTasks) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    OffloadConfig cfg = MakeDefaultConfig();
    cfg.offloading_queue_limit = 50000;
    OffloadManager manager(local_ssd, host, cfg, kDefaultTimeout);
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_race", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    host.RegisterSegment("seg_race", client_id);
    ASSERT_EQ(local_ssd.RegisterClient(client_id, true), ErrorCode::OK);

    constexpr int kRounds = 200;
    for (int r = 0; r < kRounds; ++r) {
        std::mutex obj_mu;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>> objs;
        std::vector<std::string> keys;
        for (int i = 0; i < 8; ++i) {
            std::string k =
                "race_" + std::to_string(r) + "_" + std::to_string(i);
            std::vector<Replica> reps;
            reps.push_back(MakeMemoryReplica(alloc, session, 64));
            objs.emplace(
                k, MakeObject(client_id, kTenant, k, std::move(reps), 64));
            keys.push_back(std::move(k));
        }

        ASSERT_TRUE(
            manager.HandleOffloadHeartbeat(client_id, true).has_value());
        std::atomic<bool> start{false};

        std::thread producer([&]() {
            while (!start.load(std::memory_order_acquire)) {
            }
            for (const auto& k : keys) {
                std::lock_guard lock(obj_mu);
                Replica* mem = FirstMemoryReplica(*objs[k]);
                (void)manager.StartOffload(kTenant, k, *mem);
            }
        });
        std::thread disabler([&]() {
            while (!start.load(std::memory_order_acquire)) {
            }
            (void)manager.HandleOffloadHeartbeat(client_id, false);
        });
        start.store(true, std::memory_order_release);
        producer.join();
        disabler.join();

        // After a final disable heartbeat, no queued key may remain orphaned in
        // tasks_ and every replica refcnt must return to 0.
        (void)manager.HandleOffloadHeartbeat(client_id, false);
        for (const auto& k : keys) {
            EXPECT_FALSE(manager.HasTask(kTenant, k))
                << "Orphaned task leaked for key=" << k;
            EXPECT_EQ(FirstMemoryReplica(*objs[k])->get_refcnt(), 0u)
                << "Leaked refcnt for key=" << k;
        }
    }
}

TEST(OffloadManagerTest, StartOffloadValidationAndQueueLimit) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    OffloadConfig cfg = MakeDefaultConfig();
    cfg.offloading_queue_limit = 2;
    OffloadManager manager(local_ssd, host, cfg, kDefaultTimeout);

    const UUID client_a = generate_uuid();
    const UUID disabled_client = generate_uuid();
    const UUID unknown_client = generate_uuid();
    auto session_a = MakeActiveSession(client_a);
    auto session_dis = MakeActiveSession(disabled_client);
    auto session_unk = MakeActiveSession(unknown_client);

    ASSERT_EQ(local_ssd.RegisterClient(client_a, true), ErrorCode::OK);
    ASSERT_EQ(local_ssd.RegisterClient(disabled_client, false), ErrorCode::OK);

    auto alloc_a = std::make_shared<OffsetBufferAllocator>(
        "seg_lim_a", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9101", "tcp");
    auto alloc_dis = std::make_shared<OffsetBufferAllocator>(
        "seg_lim_dis", 0x200000, 64 * 1024 * 1024, "tcp://127.0.0.1:9102",
        "tcp");
    auto alloc_unk = std::make_shared<OffsetBufferAllocator>(
        "seg_lim_unk", 0x300000, 64 * 1024 * 1024, "tcp://127.0.0.1:9103",
        "tcp");
    auto alloc_no_owner = std::make_shared<OffsetBufferAllocator>(
        "seg_lim_missing", 0x400000, 64 * 1024 * 1024, "tcp://127.0.0.1:9104",
        "tcp");

    host.RegisterSegment("seg_lim_a", client_a);
    host.RegisterSegment("seg_lim_dis", disabled_client);
    host.RegisterSegment("seg_lim_unk", unknown_client);

    // Non-memory, unbound session, non-readable, missing owner, unregistered
    // client, and disabled client all fail without pinning.
    {
        Replica disk(client_a, 128, "tcp://127.0.0.1:9101",
                     ReplicaStatus::COMPLETE, session_a);
        EXPECT_FALSE(manager.StartOffload(kTenant, "k_disk", disk));
        EXPECT_EQ(disk.get_refcnt(), 0u);
    }
    {
        Replica no_session = MakeMemoryReplica(alloc_a, nullptr, 128);
        EXPECT_FALSE(manager.StartOffload(kTenant, "k_nosess", no_session));
        EXPECT_EQ(no_session.get_refcnt(), 0u);
    }
    {
        Replica mem = MakeMemoryReplica(alloc_a, session_a, 128);
        host.SetReadable(false);
        EXPECT_FALSE(manager.StartOffload(kTenant, "k_unread", mem));
        EXPECT_EQ(mem.get_refcnt(), 0u);
        host.SetReadable(true);
    }
    {
        Replica no_owner = MakeMemoryReplica(alloc_no_owner, session_a, 128);
        EXPECT_FALSE(manager.StartOffload(kTenant, "k_noowner", no_owner));
        EXPECT_EQ(no_owner.get_refcnt(), 0u);
    }
    {
        Replica unreg = MakeMemoryReplica(alloc_unk, session_unk, 128);
        EXPECT_FALSE(manager.StartOffload(kTenant, "k_unreg", unreg));
        EXPECT_EQ(unreg.get_refcnt(), 0u);
    }
    {
        Replica dis = MakeMemoryReplica(alloc_dis, session_dis, 128);
        EXPECT_FALSE(manager.StartOffload(kTenant, "k_dis", dis));
        EXPECT_EQ(dis.get_refcnt(), 0u);
    }

    // Queue limit (2): first two succeed and pin; third fails without pinning.
    Replica r1 = MakeMemoryReplica(alloc_a, session_a, 128);
    Replica r2 = MakeMemoryReplica(alloc_a, session_a, 128);
    Replica r3 = MakeMemoryReplica(alloc_a, session_a, 128);
    EXPECT_TRUE(manager.StartOffload(kTenant, "k_1", r1));
    EXPECT_TRUE(manager.StartOffload(kTenant, "k_2", r2));
    EXPECT_FALSE(manager.StartOffload(kTenant, "k_3", r3));
    EXPECT_EQ(r1.get_refcnt(), 1u);
    EXPECT_EQ(r2.get_refcnt(), 1u);
    EXPECT_EQ(r3.get_refcnt(), 0u);
    EXPECT_FALSE(manager.HasTask(kTenant, "k_3"));
}

TEST(OffloadManagerTest, CompleteEdgeCasesAndMultiTenantIsolation) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    OffloadManager manager(local_ssd, host, MakeDefaultConfig(),
                           kDefaultTimeout);
    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_edge", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    host.RegisterSegment("seg_edge", client_id);
    ASSERT_EQ(local_ssd.RegisterClient(client_id, true), ErrorCode::OK);

    // CompleteOffload with no active task returns kNotTracked and touches
    // nothing.
    {
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc, session, 512));
        auto obj = MakeObject(client_id, kTenant, "missing", std::move(reps));
        Replica disk(client_id, 512, "tcp://127.0.0.1:9100",
                     ReplicaStatus::COMPLETE, session);
        EXPECT_EQ(
            manager.CompleteOffload(client_id, kTenant, "missing", *obj, disk),
            OffloadManager::CompleteStatus::kNotTracked);
        EXPECT_EQ(local_ssd.GetUsage(client_id)->used_bytes, 0);
        EXPECT_FALSE(obj->HasReplica(&Replica::fn_is_local_disk_replica));
    }

    // CompleteOffload when LOCAL_DISK replica already exists clears the task
    // and unpins without adding a duplicate replica or double-counting usage.
    {
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc, session, 512));
        reps.emplace_back(client_id, 512, "tcp://127.0.0.1:9100",
                          ReplicaStatus::COMPLETE, session);
        auto obj =
            MakeObject(client_id, kTenant, "k_existing_disk", std::move(reps));
        Replica* mem = FirstMemoryReplica(*obj);
        ASSERT_TRUE(manager.StartOffload(kTenant, "k_existing_disk", *mem));

        Replica updated_disk(client_id, 512, "tcp://127.0.0.1:9200",
                             ReplicaStatus::COMPLETE, session);
        EXPECT_EQ(manager.CompleteOffload(client_id, kTenant, "k_existing_disk",
                                          *obj, updated_disk),
                  OffloadManager::CompleteStatus::kCommitted);
        EXPECT_EQ(mem->get_refcnt(), 0u);
        EXPECT_FALSE(manager.HasTask(kTenant, "k_existing_disk"));
        EXPECT_EQ(local_ssd.GetUsage(client_id)->used_bytes, 0);

        size_t local_disk_count = 0;
        obj->VisitReplicas(&Replica::fn_is_local_disk_replica,
                           [&](const Replica&) { ++local_disk_count; });
        EXPECT_EQ(local_disk_count, 1u);
    }

    // Same key in different tenants must be isolated.
    const TenantId tenant_a("tenant-a");
    const TenantId tenant_b("tenant-b");
    std::vector<Replica> ra;
    ra.push_back(MakeMemoryReplica(alloc, session, 128));
    auto obj_a =
        MakeObject(client_id, tenant_a, "shared_key", std::move(ra), 128);
    std::vector<Replica> rb;
    rb.push_back(MakeMemoryReplica(alloc, session, 256));
    auto obj_b =
        MakeObject(client_id, tenant_b, "shared_key", std::move(rb), 256);

    ASSERT_TRUE(manager.StartOffload(tenant_a, "shared_key",
                                     *FirstMemoryReplica(*obj_a)));
    ASSERT_TRUE(manager.StartOffload(tenant_b, "shared_key",
                                     *FirstMemoryReplica(*obj_b)));
    EXPECT_TRUE(manager.HasTask(tenant_a, "shared_key"));
    EXPECT_TRUE(manager.HasTask(tenant_b, "shared_key"));

    EXPECT_TRUE(manager.FailOffload(client_id, tenant_a, "shared_key"));
    EXPECT_EQ(FirstMemoryReplica(*obj_a)->get_refcnt(), 0u);
    EXPECT_EQ(FirstMemoryReplica(*obj_b)->get_refcnt(), 1u);
    EXPECT_FALSE(manager.HasTask(tenant_a, "shared_key"));
    EXPECT_TRUE(manager.HasTask(tenant_b, "shared_key"));
}

TEST(OffloadManagerTest, ConcurrentSameKeyContentionPreservesRefcntBalance) {
    LocalSsdManager local_ssd;
    FakeHostView host;
    OffloadConfig cfg = MakeDefaultConfig();
    cfg.offloading_queue_limit = 50000;
    OffloadManager manager(local_ssd, host, cfg, std::chrono::seconds(1));

    const UUID client_id = generate_uuid();
    auto session = MakeActiveSession(client_id);
    auto alloc = std::make_shared<OffsetBufferAllocator>(
        "seg_hot", 0x100000, 64 * 1024 * 1024, "tcp://127.0.0.1:9100", "tcp");
    host.RegisterSegment("seg_hot", client_id);
    ASSERT_EQ(local_ssd.RegisterClient(client_id, true), ErrorCode::OK);

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
        slot->key = "hot_offload_" + std::to_string(i);
        std::vector<Replica> reps;
        reps.push_back(MakeMemoryReplica(alloc, session, 64));
        slot->obj =
            MakeObject(client_id, kTenant, slot->key, std::move(reps), 64);
        slots.push_back(std::move(slot));
    }

    std::atomic<uint64_t> starts{0};
    std::atomic<bool> go{false};

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            while (!go.load(std::memory_order_acquire)) {
            }
            for (int i = 0; i < kIterations; ++i) {
                auto& slot = *slots[(t + i) % kHotKeys];
                switch ((t + i) % 6) {
                    case 0:
                    case 1: {
                        std::lock_guard lock(slot.mu);
                        if (manager.StartOffload(
                                kTenant, slot.key,
                                *FirstMemoryReplica(*slot.obj))) {
                            starts.fetch_add(1, std::memory_order_relaxed);
                        }
                        break;
                    }
                    case 2: {
                        std::lock_guard lock(slot.mu);
                        (void)manager.TryCancelQueuedOffload(kTenant, slot.key);
                        break;
                    }
                    case 3: {
                        std::lock_guard lock(slot.mu);
                        Replica disk(client_id, 64, "tcp://127.0.0.1:9100",
                                     ReplicaStatus::COMPLETE, session);
                        (void)manager.CompleteOffload(
                            client_id, kTenant, slot.key, *slot.obj, disk);
                        break;
                    }
                    case 4: {
                        std::lock_guard lock(slot.mu);
                        (void)manager.FailOffload(client_id, kTenant, slot.key);
                        break;
                    }
                    case 5: {
                        if (i % 2 == 0) {
                            std::lock_guard lock(slot.mu);
                            manager.OnObjectErased(kTenant, slot.key);
                        } else {
                            manager.DiscardExpiredTasks(
                                std::chrono::system_clock::now() +
                                std::chrono::hours(1));
                        }
                        break;
                    }
                }
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    // Clean up any surviving active task and verify exact refcnt == 0.
    for (auto& slot : slots) {
        std::lock_guard lock(slot->mu);
        manager.OnObjectErased(kTenant, slot->key);
        EXPECT_FALSE(manager.HasTask(kTenant, slot->key));
        EXPECT_EQ(FirstMemoryReplica(*slot->obj)->get_refcnt(), 0u);
    }
    EXPECT_GT(starts.load(), 0u);
}

}  // namespace
}  // namespace mooncake
