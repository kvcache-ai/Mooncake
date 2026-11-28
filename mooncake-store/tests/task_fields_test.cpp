#include <gtest/gtest.h>

#include "allocation_probe.h"
#include "eviction/offloading_reclaimer.h"
#include "master_service/master_service_test_peer.h"

namespace mooncake::test {
namespace {
using Fields = route::ObjectState::TaskFields;
const TenantId tenant = TenantId::Default();
const UUID writer{21, 22};

void Mount(MasterService& service, UUID client = writer,
           const std::string& name = "seg") {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment.te_endpoint = name;
    segment.base = DEFAULT_CXL_BASE;
    segment.size = 16U << 20;
    ASSERT_TRUE(service.MountSegment(segment, client));
}

TEST(TaskFieldsTest, ColdKeyCleanupDoesNotAllocateTasks) {
    MasterServiceConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    config.default_kv_lease_ttl = 0;
    MasterService service(config);
    Mount(service);
    MasterServiceTestPeer peer(service);
    ReplicateConfig replication;
    replication.preferred_segment = "seg";
    ASSERT_TRUE(service.PutStart(writer, "pending", tenant, 128, replication));
    ASSERT_TRUE(service.PutStart(writer, "done", tenant, 128, replication));
    ASSERT_TRUE(service.PutEnd(writer, "done", tenant, ReplicaType::MEMORY));

    auto without_tasks = [](auto&& operation) {
        AllocationProbe probe(SIZE_MAX, sizeof(Fields));
        auto ok = operation();
        return std::pair{static_cast<bool>(ok), probe.calls};
    };
    auto expected = std::pair{true, size_t{0}};
    EXPECT_EQ(without_tasks([&] {
                  peer.DiscardExpiredProcessingReplicas(
                      tenant, std::chrono::system_clock::now());
                  return true;
              }),
              expected);
    for (int i = 0; i < 2; ++i)
        EXPECT_EQ(without_tasks([&] {
                      return service.NotifyPromotionFailure(writer, "done",
                                                            tenant);
                  }),
                  expected);
    EXPECT_EQ(without_tasks([&] {
                  return service.UpsertStart(writer, "done", tenant, 128,
                                             replication);
              }),
              expected);
    ASSERT_TRUE(service.PutEnd(writer, "done", tenant, ReplicaType::MEMORY));
    EXPECT_EQ(
        without_tasks([&] { return service.Remove("done", tenant, true); }),
        expected);
}

TEST(TaskFieldsTest, CandidateSlotAndBothIndexesSurviveEveryAllocationFailure) {
    const std::string key(80, 'k');
    for (size_t nth = 0;; ++nth) {
        SCOPED_TRACE(nth);
        PromotionCandidateTracker tracker;
        route::ObjectState state;
        auto record = [&] {
            tracker.RecordLocked(tenant, key, state, 5,
                                 PromotionCandidateReason::kWatermark,
                                 ErrorCode::OK);
        };
        try {
            AllocationProbe fail(nth);
            record();
        } catch (const std::bad_alloc&) {
            EXPECT_TRUE(tracker.Empty());
            EXPECT_TRUE(
                tracker.Due(std::chrono::steady_clock::now(), 10).empty());
            EXPECT_FALSE(state.Tasks().promotion_candidate);
            record();
            ASSERT_EQ(tracker.Due(std::chrono::steady_clock::now(), 10).size(),
                      1u);
            tracker.EraseLocked(tenant, key, state);
            EXPECT_TRUE(tracker.Empty());
            continue;
        }
        EXPECT_TRUE(state.Tasks().promotion_candidate);
        break;
    }
}

TEST(TaskFieldsTest, MirrorInsertionFailureLeavesNeitherMailboxPublished) {
    const UUID second{22, 23};
    const std::string key(80, 'k');
    std::vector<LocalSsdManager::OffloadMirror> mirrors{{writer, 128},
                                                        {second, 128}};
    LocalSsdManager manager;
    ASSERT_EQ(manager.RegisterClient(writer, true), ErrorCode::OK);
    ASSERT_EQ(manager.RegisterClient(second, true), ErrorCode::OK);
    auto enqueue = [&] {
        return manager.EnqueueOffloadMirrors(mirrors, tenant, key, 10);
    };
    for (size_t nth = 0;; ++nth) {
        ErrorCode result;
        try {
            AllocationProbe fail(nth);
            result = enqueue();
        } catch (const std::bad_alloc&) {
            for (const auto& id : {writer, second})
                EXPECT_TRUE(manager.SetOffloadingAndTakePending(id, true)
                                .value()
                                .empty());
            continue;
        }
        ASSERT_EQ(result, ErrorCode::OK);
        break;
    }
    for (const auto& id : {writer, second}) {
        auto items = manager.SetOffloadingAndTakePending(id, true).value();
        ASSERT_EQ(items.size(), 1u);
        EXPECT_EQ(items.front().key, key);
    }

    ASSERT_EQ(
        manager.EnqueueOffload(second, {tenant.value(), "occupied", 128}, 1),
        ErrorCode::OK);
    EXPECT_EQ(manager.EnqueueOffloadMirrors(mirrors, tenant, key, 1),
              ErrorCode::KEYS_ULTRA_LIMIT);
    EXPECT_TRUE(
        manager.SetOffloadingAndTakePending(writer, true).value().empty());
    auto remaining = manager.SetOffloadingAndTakePending(second, true).value();
    ASSERT_EQ(remaining.size(), 1u);
    EXPECT_EQ(remaining.front().key, "occupied");
    EXPECT_EQ(enqueue(), ErrorCode::OK);
}

TEST(TaskFieldsTest, PutEndOffloadPreparationFailureCanBeRetried) {
    MasterServiceConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    config.enable_offload = true;
    config.offload_on_evict = false;
    MasterService service(config);
    Mount(service);
    const UUID second{22, 23};
    Mount(service, second, "other");
    for (auto id : {writer, second}) {
        ASSERT_TRUE(service.MountLocalDiskSegment(id, true));
    }
    MasterServiceTestPeer peer(service);
    auto& ns = peer.GetOrCreateNamespace(tenant);
    ReplicateConfig replication;
    replication.replica_num = 2;
    replication.session_ids = std::vector<std::string>{"write-session"};
    replication.preferred_segments = {"seg", "other"};
    for (const auto size : {sizeof(Fields), sizeof(OffloadingTask)}) {
        auto key = std::to_string(size);
        ASSERT_TRUE(service.PutStart(writer, key, tenant, 128, replication));
        auto finish = [&] {
            return service.PutEnd(writer, key, tenant, ReplicaType::MEMORY);
        };
        auto failed = [&] {
            AllocationProbe fail(0, size);
            return finish();
        }();
        ASSERT_FALSE(failed);
        EXPECT_EQ(failed.error(), ErrorCode::INTERNAL_ERROR);
        for (auto id : {writer, second}) {
            EXPECT_TRUE(peer.LocalSsdManager(service)
                            .SetOffloadingAndTakePending(id, true)
                            .value()
                            .empty());
        }
        {
            auto held = ns.objects.Read(key);
            ASSERT_TRUE(held);
            EXPECT_FALSE(held->state().Tasks().offloading_task);
            EXPECT_TRUE(held->metadata().SessionId().empty());
            ASSERT_EQ(held->metadata().GetAllReplicas().size(), 2u);
            for (const auto& replica : held->metadata().GetAllReplicas()) {
                EXPECT_EQ(replica.get_refcnt(), 0);
                EXPECT_TRUE(replica.is_processing());
            }
        }
        peer.DiscardExpiredProcessingReplicas(tenant,
                                              std::chrono::system_clock::now());
        ASSERT_TRUE(finish());
        for (auto id : {writer, second}) {
            auto items = peer.LocalSsdManager(service)
                             .SetOffloadingAndTakePending(id, true)
                             .value();
            ASSERT_EQ(items.size(), 1u);
            EXPECT_EQ(items.front().key, key);
        }
        auto held = ns.objects.Read(key);
        const auto& task = held->state().Tasks().offloading_task;
        ASSERT_TRUE(task);
        EXPECT_EQ(held->metadata().SessionId(), "write-session");
        EXPECT_EQ(task->mirror_clients.size(), 2u);
        for (const auto& replica : held->metadata().GetAllReplicas()) {
            EXPECT_EQ(replica.get_refcnt(),
                      replica.id() == task->source_id ? 1 : 0);
        }
    }
}

TEST(TaskFieldsTest, EvictionPreparesTaskBeforeQueueAndPin) {
    metadata::Namespace ns;
    auto allocator = std::make_shared<DummyBufferAllocator>("seg", "endpoint");
    std::vector<Replica> replicas;
    replicas.emplace_back(
        std::make_unique<AllocatedBuffer>(allocator, nullptr, 128),
        ReplicaStatus::COMPLETE);
    const ReplicaID id = replicas.front().id();
    {
        auto hold = ns.objects.WriteKey("key");
        hold.Publish(UUID{1, 2}, std::chrono::system_clock::now(), 128,
                     std::move(replicas), std::nullopt, false,
                     ObjectDataType::UNKNOWN, "", TenantId::Default(),
                     hold.key());
    }
    struct Queue : eviction::OffloadQueue {
        bool pushed = false;
        bool Push(const TenantId&, const std::string&, Replica&,
                  std::vector<UUID>*) override {
            pushed = true;
            return true;
        }
    } queue;
    struct Inner : eviction::Reclaimer {
        tl::expected<eviction::Effect, ErrorCode> Apply(
            metadata::Namespace&, const route::WriteGuard&,
            const eviction::Plan&, const eviction::Report&) override {
            return eviction::Effect{};
        }
        void TearDown(metadata::Namespace&, const route::WriteGuard&) override {
        }
    } inner;
    eviction::OffloadingReclaimer reclaimer(inner, queue, {});
    eviction::Plan plan;
    plan.release.push_back(id);
    for (const auto size : {sizeof(Fields), sizeof(OffloadingTask)}) {
        {
            auto hold = ns.objects.Write("key");
            auto failed = [&] {
                AllocationProbe fail(0, size);
                return reclaimer.Apply(ns, *hold, plan, {});
            }();
            ASSERT_FALSE(failed);
            EXPECT_EQ(failed.error(), ErrorCode::INTERNAL_ERROR);
        }
        EXPECT_FALSE(queue.pushed);
        EXPECT_TRUE(ns.InFlightKeys().empty());
        auto read = ns.objects.Read("key");
        EXPECT_FALSE(read->state().Tasks().offloading_task);
        EXPECT_EQ(read->metadata().GetAllReplicas().front().get_refcnt(), 0);
    }
    {
        auto hold = ns.objects.Write("key");
        ASSERT_TRUE(reclaimer.Apply(ns, *hold, plan, {}));
    }
    EXPECT_TRUE(queue.pushed);
    EXPECT_EQ(ns.InFlightKeys().size(), 1u);
    auto read = ns.objects.Read("key");
    EXPECT_TRUE(read->state().Tasks().offloading_task);
    EXPECT_EQ(read->metadata().GetAllReplicas().front().get_refcnt(), 1);
}
}  // namespace
}  // namespace mooncake::test
