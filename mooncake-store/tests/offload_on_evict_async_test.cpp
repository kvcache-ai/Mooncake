// Async offload-on-evict (--offload_on_evict_async): eviction queues each
// victim's memory for a copy to its owner's SSD, counts it toward the round's
// goal, bounds the copies in flight per owner, and frees the memory as the
// copy lands. The eviction thread is stopped; each test drives the rounds,
// the store heartbeats and the landings by hand.

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <deque>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "eviction/offload_ledger.h"
#include "master_metric_manager.h"
#include "types.h"

namespace mooncake::test {
namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;
using Tick = MasterServiceTestPeer::AsyncEvictionTick;

constexpr size_t kObject = 1 << 20;
constexpr size_t kSegment = 32 << 20;

struct Owner {
    UUID client_id;
    std::string segment;
    UUID segment_id;
};

struct Held {
    bool exists = false;
    size_t memory = 0;
    size_t disk = 0;
    bool offloading = false;
};

class OffloadOnEvictAsyncTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OffloadOnEvictAsyncTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static MasterServiceConfig AsyncConfig() {
        MasterServiceConfig config;
        config.enable_offload = true;
        config.offload_on_evict = true;
        config.offload_on_evict_async = true;
        // Room for 32 copies in flight per owner.
        config.offloading_queue_limit = 64;
        config.offload_cap_ratio = 0.5;
        // A read holds the key for a minute.
        config.default_kv_lease_ttl = 60'000;
        config.eviction_high_watermark_ratio = 0.5;
        config.eviction_ratio = 0.1;
        return config;
    }

    // A service whose eviction thread is stopped.
    static std::unique_ptr<MasterService> Start(
        const MasterServiceConfig& config = AsyncConfig()) {
        auto service = std::make_unique<MasterService>(config);
        MasterServiceTestPeer(*service).StopEvictionThreadForTesting();
        return service;
    }

    // A client with a memory segment and a local disk.
    static Owner MountOwner(MasterService& service, const std::string& name,
                            size_t base) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = name;
        segment.base = base;
        segment.size = kSegment;
        segment.te_endpoint = name;
        const UUID client_id = generate_uuid();
        EXPECT_TRUE(service.MountSegment(segment, client_id).has_value());
        EXPECT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
        return {client_id, name, segment.id};
    }

    // Puts `key` in `owner`'s memory, its lease run out `age` ago (PutEnd
    // leaves none), so keys put together still have an order.
    static void Put(MasterService& service, const Owner& owner,
                    const std::string& key, seconds age) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {owner.segment};
        auto start = service.PutStart(owner.client_id, key, TenantId::Default(),
                                      kObject, config);
        ASSERT_TRUE(start.has_value())
            << key << ": " << toString(start.error());
        ASSERT_TRUE(service
                        .PutEnd(owner.client_id, key, TenantId::Default(),
                                ReplicaType::MEMORY)
                        .has_value());
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            &service,
            MasterServiceTestPeer::ObjectIdentity{TenantId::Default(), key});
        ASSERT_TRUE(accessor.Exists());
        SpinLocker locker(&accessor.Get().lock);
        accessor.Get().lease_->SetDeadline(std::chrono::system_clock::now() -
                                           age);
    }

    // The reader's lease on `key` runs out now, as it would once the lease
    // TTL passed; the release refiles the key by it.
    static void ExpireLease(MasterService& service, const std::string& key) {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            &service,
            MasterServiceTestPeer::ObjectIdentity{TenantId::Default(), key});
        ASSERT_TRUE(accessor.Exists());
        SpinLocker locker(&accessor.Get().lock);
        accessor.Get().lease_->SetDeadline(std::chrono::system_clock::now() -
                                           milliseconds(1));
    }

    // Puts `key` in `owner`'s memory as a writer would: its lease runs out at
    // once. False when there was no room.
    static bool TryPut(MasterService& service, const Owner& owner,
                       const std::string& key) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {owner.segment};
        return service
                   .PutStart(owner.client_id, key, TenantId::Default(), kObject,
                             config)
                   .has_value() &&
               service
                   .PutEnd(owner.client_id, key, TenantId::Default(),
                           ReplicaType::MEMORY)
                   .has_value();
    }

    // Puts `count` keys named prefix0.., the first the oldest.
    static std::vector<std::string> PutAll(MasterService& service,
                                           const Owner& owner,
                                           const std::string& prefix,
                                           int count) {
        std::vector<std::string> keys;
        for (int i = 0; i < count; ++i) {
            keys.push_back(prefix + std::to_string(i));
            Put(service, owner, keys.back(), seconds(10 * (count - i)));
        }
        return keys;
    }

    // The keys the owner's next heartbeat hands its store, in that order.
    // Remembers each copy's token, which Land reports back as a store does.
    std::vector<std::string> Take(MasterService& service, const Owner& owner) {
        auto tasks = service.OffloadObjectHeartbeat(owner.client_id, true);
        EXPECT_TRUE(tasks.has_value());
        std::vector<std::string> keys;
        if (tasks) {
            for (const auto& task : *tasks) {
                keys.push_back(task.key);
                tokens_[task.key] = task.token.value_or(0);
            }
        }
        return keys;
    }

    static std::vector<std::string> Sorted(std::vector<std::string> keys) {
        std::sort(keys.begin(), keys.end());
        return keys;
    }

    // The store reports `keys` written to its disk, or failed, each with the
    // token it was last handed with.
    void Land(MasterService& service, const Owner& owner,
              const std::vector<std::string>& keys, bool ok = true) {
        for (const auto& key : keys) {
            LandAs(service, owner, key, tokens_[key], ok);
        }
    }

    // The store reports `key` as the copy queued with `token` (0: none).
    static void LandAs(MasterService& service, const Owner& owner,
                       const std::string& key, uint64_t token, bool ok = true) {
        OffloadTaskItem task{
            .tenant_id = "default", .key = key, .size = kObject, .token = {}};
        if (token != 0) {
            task.token = token;
        }
        const StorageObjectMetadata metadata{
            .bucket_id = 1,
            .offset = 0,
            .key_size = static_cast<int64_t>(key.size()),
            .data_size = ok ? static_cast<int64_t>(kObject) : -1,
            .transport_endpoint = owner.segment + ":rpc"};
        ASSERT_TRUE(
            service.NotifyOffloadSuccess(owner.client_id, {task}, {metadata}));
    }

    std::unordered_map<std::string, uint64_t> tokens_;

    static Held Inspect(MasterService& service, const std::string& key) {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service, TenantId::Default(), key);
        Held held;
        if (!accessor.Exists()) {
            return held;
        }
        held.exists = true;
        held.memory =
            accessor.Get().CountReplicas(&Replica::fn_is_memory_replica);
        held.disk =
            accessor.Get().CountReplicas(&Replica::fn_is_local_disk_replica);
        held.offloading = accessor.GetState().offloading_task.has_value();
        return held;
    }

    // Where `key`'s memory replica lives.
    static uint64_t SourceAddress(MasterService& service,
                                  const std::string& key) {
        auto listed =
            service.BatchGetReplicaListForAdmin({key}, TenantId::Default());
        EXPECT_TRUE(listed[0].has_value());
        if (!listed[0]) {
            return 0;
        }
        for (const auto& replica : listed[0]->replicas) {
            if (replica.is_memory_replica()) {
                return replica.get_memory_descriptor()
                    .buffer_descriptor.buffer_address_;
            }
        }
        return 0;
    }

    static MasterMetricManager& Metrics() {
        return MasterMetricManager::instance();
    }
};

TEST_F(OffloadOnEvictAsyncTest, ARoundQueuesItsShareOldestFirst) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    ASSERT_TRUE(peer.OffloadAsync());
    EXPECT_EQ(peer.OffloadOwnerRoom(), 32u);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 8);

    peer.RunBatchEvictForTesting(0.25, 0.25);

    EXPECT_EQ(Sorted(Take(*service, owner)),
              (std::vector<std::string>{"k0", "k1"}));
    EXPECT_EQ(peer.OffloadLedger().objects(owner.client_id), 2u);
    EXPECT_EQ(peer.OffloadLedger().bytes(), 2 * kObject);
    EXPECT_EQ(peer.OffloadLedger().bytes(TenantId::Default()), 2 * kObject);
    // Queued, not freed: the source is pinned until the copy lands.
    EXPECT_EQ(Inspect(*service, "k0").memory, 1u);
    EXPECT_TRUE(Inspect(*service, "k0").offloading);
    EXPECT_FALSE(Inspect(*service, "k2").offloading);
}

// --eviction_worker_threads does not reach offload-on-evict: its reclaimer
// keeps every memory eviction run serial, pool rounds and tenant runs alike,
// so a round still queues its share oldest first. Without offload-on-evict
// the same setting lets the runs use several workers.
TEST_F(OffloadOnEvictAsyncTest, EvictionWorkersLeaveOffloadRunsSerial) {
    auto config = AsyncConfig();
    config.eviction_worker_threads = 4;
    {
        auto service = Start(config);
        MasterServiceTestPeer peer(*service);
        ASSERT_TRUE(peer.OffloadAsync());
        const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
        PutAll(*service, owner, "k", 8);
        EXPECT_EQ(peer.MemoryEvictionWorkersForTesting(TenantId::Default()),
                  1u);

        peer.RunBatchEvictForTesting(0.25, 0.25);
        EXPECT_EQ(Sorted(Take(*service, owner)),
                  (std::vector<std::string>{"k0", "k1"}));

        peer.EvictNamespaceMemory(TenantId::Default(), 4 * kObject);
        EXPECT_EQ(Sorted(Take(*service, owner)),
                  (std::vector<std::string>{"k2", "k3"}));
    }
    // Plain offload-on-evict stays serial too; without it, 4 workers.
    config.offload_on_evict_async = false;
    for (const bool offload_on_evict : {true, false}) {
        config.offload_on_evict = offload_on_evict;
        auto service = Start(config);
        MasterServiceTestPeer peer(*service);
        EXPECT_FALSE(peer.OffloadAsync());
        const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
        PutAll(*service, owner, "k", 1);
        EXPECT_EQ(peer.MemoryEvictionWorkersForTesting(TenantId::Default()),
                  offload_on_evict ? 1u : 4u)
            << offload_on_evict;
    }
}

// The legacy mode queues every expired key it meets for the same goal.
TEST_F(OffloadOnEvictAsyncTest, WithoutTheKnobARoundQueuesEveryExpiredKey) {
    auto config = AsyncConfig();
    config.offload_on_evict_async = false;
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    ASSERT_FALSE(peer.OffloadAsync());
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 8);

    peer.RunBatchEvictForTesting(0.25, 0.25);

    EXPECT_EQ(Take(*service, owner).size(), 8u);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
}

TEST_F(OffloadOnEvictAsyncTest, NoRoundWhileTheCopiesInFlightCoverTheExcess) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 20);  // 20 of 32 MiB, watermark 0.5

    // Target max(0.1, 0.625 - 0.5 + 0.1) of 20 keys: 5 queued.
    ASSERT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    EXPECT_EQ(peer.OffloadLedger().objects(), 5u);
    // 15 of 32 MiB once they land: under the watermark, nothing to do.
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kIdle);
    EXPECT_EQ(peer.OffloadLedger().objects(), 5u);

    const auto taken = Take(*service, owner);
    ASSERT_EQ(taken.size(), 5u);
    Land(*service, owner, taken);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kIdle);
}

TEST_F(OffloadOnEvictAsyncTest, ALandedCopyFreesTheMemory) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));
    const int64_t freed = Metrics().get_offload_evict_freed();
    const int64_t freed_bytes = Metrics().get_offload_evict_freed_bytes();
    const int64_t writes = Metrics().get_offload_evict_write_latency_count();
    const int64_t frees = Metrics().get_offload_evict_free_latency_count();
    const int64_t lease_waits = Metrics().get_offload_evict_lease_wait();

    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    Land(*service, owner, {"k"});

    const Held held = Inspect(*service, "k");
    EXPECT_TRUE(held.exists);
    EXPECT_EQ(held.memory, 0u);
    EXPECT_EQ(held.disk, 1u);
    EXPECT_FALSE(held.offloading);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    EXPECT_EQ(peer.OffloadLedger().bytes(), 0u);
    EXPECT_EQ(Metrics().get_offload_evict_freed(), freed + 1);
    EXPECT_EQ(Metrics().get_offload_evict_freed_bytes(),
              freed_bytes + static_cast<int64_t>(kObject));
    // One write and one free timed from the queueing; no lease to wait for.
    EXPECT_EQ(Metrics().get_offload_evict_write_latency_count(), writes + 1);
    EXPECT_EQ(Metrics().get_offload_evict_free_latency_count(), frees + 1);
    EXPECT_EQ(Metrics().get_offload_evict_lease_wait(), lease_waits);
    // Written once: a later round has nothing left to queue.
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_TRUE(Take(*service, owner).empty());
    // And the key reads from disk.
    auto read = service->GetReplicaList("k", TenantId::Default());
    ASSERT_TRUE(read.has_value());
    ASSERT_EQ(read->replicas.size(), 1u);
    EXPECT_TRUE(read->replicas[0].is_local_disk_replica());
}

// A reader that took a lease while the copy was in flight keeps the memory
// until its lease runs out; eviction then frees it without another copy.
TEST_F(OffloadOnEvictAsyncTest, AReaderDuringTheCopyKeepsTheMemoryForItsLease) {
    // The read's lease (a minute) outlasts the landing however slow the
    // machine; the test ends it by hand.
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));
    const int64_t kept = Metrics().get_offload_evict_kept("read_again");
    const int64_t writes = Metrics().get_offload_evict_write_latency_count();
    const int64_t frees = Metrics().get_offload_evict_free_latency_count();
    const int64_t lease_waits = Metrics().get_offload_evict_lease_wait();

    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    ASSERT_TRUE(service->GetReplicaList("k", TenantId::Default()));
    Land(*service, owner, {"k"});

    Held held = Inspect(*service, "k");
    EXPECT_EQ(held.memory, 1u);
    EXPECT_EQ(held.disk, 1u);
    EXPECT_FALSE(held.offloading);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    EXPECT_EQ(Metrics().get_offload_evict_kept("read_again"), kept + 1);
    // The copy was written, but its memory waits for the reader's lease.
    EXPECT_EQ(Metrics().get_offload_evict_write_latency_count(), writes + 1);
    EXPECT_EQ(Metrics().get_offload_evict_free_latency_count(), frees);
    EXPECT_EQ(Metrics().get_offload_evict_lease_wait(), lease_waits + 1);

    ExpireLease(*service, "k");
    peer.RunBatchEvictForTesting(1.0, 1.0);
    held = Inspect(*service, "k");
    EXPECT_EQ(held.memory, 0u);
    EXPECT_EQ(held.disk, 1u);
    EXPECT_TRUE(Take(*service, owner).empty());
}

TEST_F(OffloadOnEvictAsyncTest, AFailedCopyReleasesItsCreditAndWaits) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));

    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    Land(*service, owner, {"k"}, /*ok=*/false);

    const Held held = Inspect(*service, "k");
    EXPECT_EQ(held.memory, 1u);
    EXPECT_EQ(held.disk, 0u);
    EXPECT_FALSE(held.offloading);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    // It waits: the next round does not hand it straight back.
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_TRUE(Take(*service, owner).empty());
    EXPECT_FALSE(Inspect(*service, "k").offloading);
}

// The wait after a failed copy keeps the key's rank and does not hide it from
// a round that may drop.
TEST_F(OffloadOnEvictAsyncTest, AFailedCopyCanStillBeDroppedUnderPressure) {
    auto config = AsyncConfig();
    config.offload_force_evict = true;
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    Land(*service, owner, {"k"}, /*ok=*/false);

    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_FALSE(Inspect(*service, "k").exists);
    EXPECT_TRUE(Take(*service, owner).empty());
}

TEST_F(OffloadOnEvictAsyncTest, AFullOwnerHoldsBackOnlyItsOwnObjects) {
    auto config = AsyncConfig();
    config.offloading_queue_limit = 4;  // room 2
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner a = MountOwner(*service, "seg_a", 0x300000000);
    const Owner b = MountOwner(*service, "seg_b", 0x400000000);
    // a's keys are the oldest, so its room fills first.
    for (int i = 0; i < 4; ++i) {
        Put(*service, a, "a" + std::to_string(i), seconds(100 - i));
    }
    Put(*service, b, "b0", seconds(10));
    const int64_t no_room = Metrics().get_offload_evict_no_room();

    peer.RunBatchEvictForTesting(1.0, 1.0);

    EXPECT_EQ(Sorted(Take(*service, a)),
              (std::vector<std::string>{"a0", "a1"}));
    EXPECT_EQ(Take(*service, b), std::vector<std::string>{"b0"});
    EXPECT_EQ(peer.OffloadLedger().objects(a.client_id), 2u);
    EXPECT_EQ(peer.OffloadLedger().objects(b.client_id), 1u);
    EXPECT_EQ(Metrics().get_offload_evict_no_room(), no_room + 2);
    // Kept, not dropped.
    EXPECT_EQ(Inspect(*service, "a3").memory, 1u);
    EXPECT_FALSE(Inspect(*service, "a3").offloading);
}

// The trigger holds a round back while no owner has room, and after a round
// that got nothing done until the backoff passes or an offload ends.
TEST_F(OffloadOnEvictAsyncTest, TheTriggerWaitsForRoomAndBacksOff) {
    auto config = AsyncConfig();
    config.offload_on_evict_backoff_ms = 600'000;
    config.offloading_queue_limit = 4;  // room 2
    config.eviction_high_watermark_ratio = 0.0;
    config.eviction_ratio = 1.0;
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 3);
    // Memory no round may take, so usage stays over the watermark.
    Put(*service, owner, "held", seconds(-600));

    ASSERT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    EXPECT_EQ(peer.OffloadLedger().objects(owner.client_id), 2u);
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kNoRoom);
    ASSERT_EQ(Sorted(Take(*service, owner)),
              (std::vector<std::string>{"k0", "k1"}));

    // Room again: the key kept for want of it goes next.
    Land(*service, owner, {"k0"});
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    EXPECT_EQ(Take(*service, owner), std::vector<std::string>{"k2"});
    // A round that finds nothing it may take (k2 is pinned by its copy) ...
    Land(*service, owner, {"k1"});
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    // ... holds the next one back; a new put does not lift that ...
    Put(*service, owner, "k3", seconds(1));
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kBackoff);
    // ... but an offload that ends does.
    Land(*service, owner, {"k2"});
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    EXPECT_EQ(Take(*service, owner), std::vector<std::string>{"k3"});
}

// A failed allocation with enough copies in flight to cover a round waits for
// them to land instead of queueing more.
TEST_F(OffloadOnEvictAsyncTest, AFailedAllocationWaitsForTheLandingsInFlight) {
    auto config = AsyncConfig();
    config.eviction_ratio = 0.05;  // a round's worth: 1.6 MiB
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 4);
    peer.RunBatchEvictForTesting(0.5, 0.5);
    ASSERT_EQ(peer.OffloadLedger().objects(), 2u);  // 2 MiB in flight

    const int64_t waits = Metrics().get_offload_evict_waited_for_frees();
    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(),
              Tick::kWaitForLandings);
    EXPECT_FALSE(MasterServiceTestPeer::NeedMemEviction(*service).load());
    EXPECT_EQ(peer.OffloadLedger().objects(), 2u);
    EXPECT_EQ(Metrics().get_offload_evict_waited_for_frees(), waits + 1);

    // With less in flight than a round frees, the failed allocation runs one.
    Land(*service, owner, Take(*service, owner));
    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    EXPECT_GT(peer.OffloadLedger().objects(), 0u);
    EXPECT_EQ(Metrics().get_offload_evict_waited_for_frees(), waits + 1);
}

// One owner's store took its copies and hangs. Once it has made no progress
// for --offload_on_evict_stall_ms its copies stop counting as memory about
// to be freed and it gets no more, so eviction goes on with the healthy owner
// and its puts keep succeeding.
TEST_F(OffloadOnEvictAsyncTest, AStuckOwnerDoesNotStallThePool) {
    auto config = AsyncConfig();
    // Long enough that the healthy owner, which lands every iteration, never
    // looks stalled however slow the machine.
    config.offload_on_evict_stall_ms = 2000;
    config.eviction_high_watermark_ratio = 0.9;
    config.eviction_ratio = 0.05;
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner a = MountOwner(*service, "seg_a", 0x300000000);
    const Owner b = MountOwner(*service, "seg_b", 0x400000000);
    for (int i = 0; i < 30; ++i) {
        Put(*service, b, "b" + std::to_string(i), seconds(1000 - i));
    }
    for (int i = 0; i < 26; ++i) {
        Put(*service, a, "a" + std::to_string(i), seconds(100 - i));
    }
    // A's writer fills it; the rounds queue B's oldest keys, and B's store
    // takes them and never reports.
    int fresh = 0;
    auto drive = [&](int puts) {
        int ok = 0;
        for (int i = 0; i < puts; ++i) {
            ok += TryPut(*service, a, "x" + std::to_string(fresh++));
            peer.RunAsyncOffloadEvictionTickForTesting();
            const auto landed = Take(*service, a);
            if (!landed.empty()) {
                Land(*service, a, landed);
            }
            Take(*service, b);
        }
        return ok;
    };
    drive(20);
    const uint64_t stuck = peer.OffloadLedger().objects(b.client_id);
    ASSERT_GT(stuck, 0u);

    std::this_thread::sleep_for(milliseconds(2100));
    EXPECT_GE(drive(100), 95);
    EXPECT_EQ(peer.OffloadLedger().objects(b.client_id), stuck);
    EXPECT_GT(Metrics().get_offload_evict_freed(), 0);
    // A tenant run does not count B's stuck copies either.
    const uint64_t before = peer.OffloadLedger().objects(a.client_id);
    peer.EvictNamespaceMemory(TenantId::Default(), kObject);
    EXPECT_EQ(peer.OffloadLedger().objects(a.client_id), before + 1);
}

// An owner that keeps landing, however slowly, stays live: its copies in
// flight keep counting, and they stop counting once it stalls.
TEST_F(OffloadOnEvictAsyncTest, ASlowOwnerCountsUntilItStalls) {
    auto config = AsyncConfig();
    config.offload_on_evict_stall_ms = 1000;
    config.offloading_queue_limit = 8;  // room 4
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 20);  // 20 of 32 MiB, watermark 0.5

    ASSERT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    auto taken = Take(*service, owner);
    ASSERT_EQ(taken.size(), 4u);
    // 16 of 32 MiB once the 4 copies land.
    for (int i = 0; i < 2; ++i) {
        std::this_thread::sleep_for(milliseconds(100));
        Land(*service, owner, {taken[i]});
        EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kIdle)
            << i;
    }
    // Quiet past the window: 18 MiB used, nothing counted as on its way out,
    // and no room on a stalled owner.
    std::this_thread::sleep_for(milliseconds(1200));
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kNoRoom);
    peer.PublishOffloadLedgerGaugesForTesting();
    EXPECT_EQ(Metrics().get_offload_evict_stalled_bytes(),
              static_cast<int64_t>(2 * kObject));
    // A landing is progress again.
    Land(*service, owner, {taken[2]});
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kIdle);
}

// While waiting for the copies in flight, a round still frees the memory of
// objects that are already on disk.
TEST_F(OffloadOnEvictAsyncTest, WaitingForLandingsStillFreesWhatIsOnDisk) {
    auto config = AsyncConfig();
    config.eviction_ratio = 0.05;  // a round's worth: 1.6 MiB
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    // On disk and in memory: read while its copy was in flight.
    Put(*service, owner, "on_disk", seconds(10));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"on_disk"});
    ASSERT_TRUE(service->GetReplicaList("on_disk", TenantId::Default()));
    Land(*service, owner, {"on_disk"});
    ASSERT_EQ(Inspect(*service, "on_disk").memory, 1u);
    // 2 MiB in flight.
    Put(*service, owner, "k0", seconds(100));
    Put(*service, owner, "k1", seconds(99));
    peer.RunBatchEvictForTesting(0.6, 0.6);
    ASSERT_EQ(peer.OffloadLedger().objects(), 2u);
    ExpireLease(*service, "on_disk");  // the read's lease ends

    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(),
              Tick::kWaitForLandings);
    EXPECT_EQ(Inspect(*service, "on_disk").memory, 0u);
    EXPECT_EQ(Inspect(*service, "on_disk").disk, 1u);
    EXPECT_EQ(peer.OffloadLedger().objects(), 2u);
}

// A failed allocation waits only on stores that are landing now: one that
// has ended no copy for max(5 x backoff, 1 s) is not waited on.
TEST_F(OffloadOnEvictAsyncTest, AWaitWithNoLandingsGivesUp) {
    auto config = AsyncConfig();
    config.offload_on_evict_backoff_ms = 20;  // wait at most 1 s
    config.eviction_ratio = 0.05;
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 4);
    peer.RunBatchEvictForTesting(0.5, 0.5);
    ASSERT_EQ(peer.OffloadLedger().objects(), 2u);

    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(),
              Tick::kWaitForLandings);
    std::this_thread::sleep_for(milliseconds(1100));
    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
    EXPECT_GT(peer.OffloadLedger().objects(), 2u);
}

// A copy whose task ended (here it expired) is reported late: its source may
// have been freed and reused meanwhile, so its bytes are not registered.
TEST_F(OffloadOnEvictAsyncTest, ALateReportOfAnEndedCopyIsNotRegistered) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    ASSERT_NE(tokens_["k"], 0u);
    peer.DiscardExpiredForTesting(std::chrono::system_clock::now() +
                                  std::chrono::hours(1));
    const int64_t stale = Metrics().get_offload_evict_stale_landing();

    Land(*service, owner, {"k"});

    EXPECT_EQ(Metrics().get_offload_evict_stale_landing(), stale + 1);
    const Held held = Inspect(*service, "k");
    EXPECT_EQ(held.memory, 1u);
    EXPECT_EQ(held.disk, 0u);
}

// Only the copy the task waits for counts: the report of an earlier copy of
// the same key neither registers nor ends the current one.
TEST_F(OffloadOnEvictAsyncTest, AReportOfAnotherCopyLeavesTheCurrentOneAlone) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    const uint64_t first = tokens_["k"];
    peer.DiscardExpiredForTesting(std::chrono::system_clock::now() +
                                  std::chrono::hours(1));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    ASSERT_NE(tokens_["k"], first);

    LandAs(*service, owner, "k", first);
    EXPECT_TRUE(Inspect(*service, "k").offloading);
    EXPECT_EQ(Inspect(*service, "k").disk, 0u);
    LandAs(*service, owner, "k", first, /*ok=*/false);
    EXPECT_TRUE(Inspect(*service, "k").offloading);

    Land(*service, owner, {"k"});
    EXPECT_FALSE(Inspect(*service, "k").offloading);
    EXPECT_EQ(Inspect(*service, "k").disk, 1u);
    EXPECT_EQ(Inspect(*service, "k").memory, 0u);
}

// force_evict drops an object it cannot queue only under pressure; a
// watermark round keeps it for a later copy.
TEST_F(OffloadOnEvictAsyncTest, ForceEvictDropsOnlyUnderPressure) {
    auto config = AsyncConfig();
    config.offloading_queue_limit = 2;  // room 1
    config.offload_force_evict = true;
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 3);

    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_TRUE(Inspect(*service, "k0").offloading);
    EXPECT_EQ(Inspect(*service, "k1").memory, 1u);
    EXPECT_EQ(Inspect(*service, "k2").memory, 1u);

    // The kept keys kept their rank: the next round under pressure meets
    // them at once.
    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_TRUE(Inspect(*service, "k0").offloading);
    EXPECT_FALSE(Inspect(*service, "k1").exists);
    EXPECT_FALSE(Inspect(*service, "k2").exists);
}

TEST_F(OffloadOnEvictAsyncTest, WithoutForceEvictPressureNeverDrops) {
    auto config = AsyncConfig();
    config.offloading_queue_limit = 2;  // room 1
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 2);

    MasterServiceTestPeer::NeedMemEviction(*service) = true;
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_TRUE(Inspect(*service, "k0").offloading);
    EXPECT_EQ(Inspect(*service, "k1").memory, 1u);
}

// A remove cancels a copy still queued. Once a store took it, the remove
// (forced or not, one key or all) still goes through at once, but the
// source's memory is parked until the copy is reported, so it is not reused
// while the store reads it, and the report brings nothing back.
TEST_F(OffloadOnEvictAsyncTest, ARemoveCancelsAQueuedCopyAndParksATakenOne) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 3);
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(peer.OffloadLedger().objects(), 3u);

    EXPECT_TRUE(service->Remove("k0", TenantId::Default(), false));
    EXPECT_EQ(peer.OffloadLedger().objects(), 2u);
    EXPECT_EQ(Sorted(Take(*service, owner)),
              (std::vector<std::string>{"k1", "k2"}));
    const uint64_t source = SourceAddress(*service, "k1");
    const int64_t parked = Metrics().get_offload_evict_parked();

    EXPECT_TRUE(service->Remove("k1", TenantId::Default(), true));
    EXPECT_FALSE(Inspect(*service, "k1").exists);
    EXPECT_FALSE(service->GetReplicaList("k1", TenantId::Default()));
    // Still charged to the owner: its store is still copying.
    EXPECT_EQ(peer.OffloadLedger().objects(owner.client_id), 2u);
    // An upsert of a key whose copy is taken waits, as before.
    auto upsert = service->UpsertStart(owner.client_id, "k2",
                                       TenantId::Default(), kObject, {});
    ASSERT_FALSE(upsert.has_value());
    EXPECT_EQ(upsert.error(), ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    EXPECT_EQ(service->RemoveAll(true), 1);
    EXPECT_FALSE(Inspect(*service, "k2").exists);
    EXPECT_EQ(Metrics().get_offload_evict_parked(), parked + 2);

    // The parked memory is not handed to the next write ...
    Put(*service, owner, "k1", seconds(1));
    EXPECT_NE(SourceAddress(*service, "k1"), source);
    // ... and the copies' reports neither register nor resurrect anything.
    Land(*service, owner, {"k1"});
    Land(*service, owner, {"k2"}, /*ok=*/false);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    const Held k1 = Inspect(*service, "k1");
    EXPECT_EQ(k1.memory, 1u);
    EXPECT_EQ(k1.disk, 0u);
    EXPECT_FALSE(Inspect(*service, "k2").exists);
    auto batch = service->BatchRemove({"k1"}, TenantId::Default(), false);
    ASSERT_EQ(batch.size(), 1u);
    EXPECT_TRUE(batch[0].has_value());
}

// BatchReplicaClear takes no lease into account but the object's own, which
// a lease-free copy does not hold: it parks the source of a taken copy too,
// whether it clears the whole object or the copy's segment.
TEST_F(OffloadOnEvictAsyncTest, ABatchReplicaClearParksTheSourceOfATakenCopy) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 2);
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner).size(), 2u);
    const uint64_t source = SourceAddress(*service, "k0");

    auto cleared = service->BatchReplicaClear({"k0"}, owner.client_id, "");
    ASSERT_TRUE(cleared.has_value());
    EXPECT_EQ(*cleared, std::vector<std::string>{"k0"});
    cleared =
        service->BatchReplicaClear({"k1"}, owner.client_id, owner.segment);
    ASSERT_TRUE(cleared.has_value());
    EXPECT_EQ(*cleared, std::vector<std::string>{"k1"});
    EXPECT_FALSE(Inspect(*service, "k0").exists);
    EXPECT_FALSE(Inspect(*service, "k1").exists);
    EXPECT_EQ(peer.OffloadLedger().objects(), 2u);

    Put(*service, owner, "other", seconds(1));
    EXPECT_NE(SourceAddress(*service, "other"), source);
    Land(*service, owner, {"k0", "k1"});
    EXPECT_FALSE(Inspect(*service, "k0").exists);
    EXPECT_FALSE(Inspect(*service, "k1").exists);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
}

// A parked source whose copy is never reported goes when the copy would have
// expired, or when its owner's disk goes.
TEST_F(OffloadOnEvictAsyncTest, AParkedSourceGoesWithTheReaperOrTheDisk) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 2);
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner).size(), 2u);
    ASSERT_TRUE(service->Remove("k0", TenantId::Default(), false));
    peer.DiscardExpiredForTesting(std::chrono::system_clock::now() +
                                  std::chrono::hours(1));
    // k1's task expired too; k0's parked source went with it.
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);

    Put(*service, owner, "k2", seconds(5));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner).size(), 2u);  // k1 again, and k2
    ASSERT_TRUE(service->Remove("k2", TenantId::Default(), false));
    ASSERT_TRUE(service->UnmountLocalDiskSegment(owner.client_id));
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
}

TEST_F(OffloadOnEvictAsyncTest, UnmountingTheDiskEndsTheCopiesQueuedToIt) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 2);
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner).size(), 2u);

    ASSERT_TRUE(service->UnmountLocalDiskSegment(owner.client_id));

    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    EXPECT_EQ(peer.OffloadLedger().objects(owner.client_id), 0u);
    for (const char* key : {"k0", "k1"}) {
        const Held held = Inspect(*service, key);
        EXPECT_EQ(held.memory, 1u) << key;
        EXPECT_FALSE(held.offloading) << key;
    }
    // Unpinned: a remove no longer waits for a copy.
    EXPECT_TRUE(service->Remove("k0", TenantId::Default(), false));
}

TEST_F(OffloadOnEvictAsyncTest, TheReaperReleasesACopyThatNeverReports) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, owner).size(), 1u);

    peer.DiscardExpiredForTesting(std::chrono::system_clock::now() +
                                  std::chrono::hours(1));

    EXPECT_FALSE(Inspect(*service, "k").offloading);
    EXPECT_EQ(Inspect(*service, "k").memory, 1u);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
}

// A copy its store never takes ends with the reaper, and its mailbox entry
// with it: the next round queues the key again and the store gets only that
// copy. Left behind, the entry would refuse every push of the key
// (OBJECT_ALREADY_EXISTS) and hand the store a copy no task waits for.
TEST_F(OffloadOnEvictAsyncTest, ACopyNeverTakenCanBeQueuedAgainAfterItExpires) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    Put(*service, owner, "k", seconds(10));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_TRUE(Inspect(*service, "k").offloading);

    peer.DiscardExpiredForTesting(std::chrono::system_clock::now() +
                                  std::chrono::hours(1));
    ASSERT_FALSE(Inspect(*service, "k").offloading);
    ASSERT_EQ(peer.OffloadLedger().objects(), 0u);

    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_TRUE(Inspect(*service, "k").offloading);
    EXPECT_EQ(peer.OffloadLedger().objects(owner.client_id), 1u);
    ASSERT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
    Land(*service, owner, {"k"});
    const Held held = Inspect(*service, "k");
    EXPECT_EQ(held.memory, 0u);
    EXPECT_EQ(held.disk, 1u);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
}

// A tenant's own eviction counts what the tenant already has in flight.
TEST_F(OffloadOnEvictAsyncTest, ATenantRunCountsWhatItHasInFlight) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 6);
    peer.RunBatchEvictForTesting(0.3, 0.3);
    ASSERT_EQ(peer.OffloadLedger().bytes(TenantId::Default()), 2 * kObject);

    peer.EvictNamespaceMemory(TenantId::Default(), 2 * kObject);
    EXPECT_EQ(peer.OffloadLedger().objects(), 2u);

    peer.EvictNamespaceMemory(TenantId::Default(), 3 * kObject);
    EXPECT_EQ(peer.OffloadLedger().objects(), 3u);
    EXPECT_EQ(Sorted(Take(*service, owner)),
              (std::vector<std::string>{"k0", "k1", "k2"}));
}

// A round ends once every owner is full: the keys it did not reach keep
// their rank, and the walk does not go on through keys it cannot queue.
TEST_F(OffloadOnEvictAsyncTest, ARoundEndsOnceEveryOwnerIsFull) {
    auto config = AsyncConfig();
    config.offloading_queue_limit = 2;  // room 1
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    PutAll(*service, owner, "k", 24);
    const int64_t no_room = Metrics().get_offload_evict_no_room();

    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_EQ(Metrics().get_offload_evict_no_room(), no_room + 1);
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_EQ(Metrics().get_offload_evict_no_room(), no_room + 2);
    EXPECT_EQ(peer.OffloadLedger().objects(), 1u);
    // The room that frees goes to the oldest key left.
    Land(*service, owner, Take(*service, owner));
    Put(*service, owner, "newer", seconds(1));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_EQ(Take(*service, owner), std::vector<std::string>{"k1"});
}

// A writer that outruns the store: whatever room frees goes to the oldest
// keys still in memory, never to the newer ones written meanwhile.
TEST_F(OffloadOnEvictAsyncTest, UnderSustainedOverloadTheOldestKeysLeaveFirst) {
    auto config = AsyncConfig();
    config.offloading_queue_limit = 16;  // room 8
    config.eviction_ratio = 0.05;
    auto service = Start(config);
    MasterServiceTestPeer peer(*service);
    const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
    constexpr int kOld = 20;
    PutAll(*service, owner, "old", kOld);  // 20 of 32 MiB, watermark 0.5

    std::deque<std::string> store;
    std::vector<std::string> landed;
    // The tick each key was handed to the store at. A store gets a
    // heartbeat's keys in no particular order, so the order is told by tick.
    std::unordered_map<std::string, int> taken_at;
    int fresh = 0;
    for (int tick = 0; tick < 60; ++tick) {
        // Two writes per tick ...
        for (int i = 0; i < 2; ++i) {
            if (TryPut(*service, owner, "fresh" + std::to_string(fresh))) {
                ++fresh;
            }
        }
        peer.RunAsyncOffloadEvictionTickForTesting();
        for (auto& key : Take(*service, owner)) {
            taken_at.emplace(key, tick);
            store.push_back(std::move(key));
        }
        // ... one landing.
        if (!store.empty()) {
            Land(*service, owner, {store.front()});
            landed.push_back(store.front());
            store.pop_front();
        }
    }

    ASSERT_GE(landed.size(), static_cast<size_t>(kOld));
    // Every old key was handed to the store no later than the first fresh
    // one, and its memory went as its copy landed.
    int last_old = -1;
    for (int i = 0; i < kOld; ++i) {
        const auto it = taken_at.find("old" + std::to_string(i));
        ASSERT_NE(it, taken_at.end()) << i;
        last_old = std::max(last_old, it->second);
        EXPECT_EQ(Inspect(*service, "old" + std::to_string(i)).memory, 0u) << i;
    }
    for (const auto& [key, tick] : taken_at) {
        if (key.rfind("fresh", 0) == 0) {
            EXPECT_GE(tick, last_old) << key;
        }
    }
    EXPECT_GT(fresh, 0);
}

// With force_evict, usage above (1 + watermark) / 2 is pressure too: the
// tick drops what it cannot queue. Between the watermark and that line it
// only queues, and keeps the rest.
TEST_F(OffloadOnEvictAsyncTest, ForceEvictDropsOnlyAboveThePressureLine) {
    for (const int keys : {26, 22}) {  // 0.81 and 0.69 of the pool
        auto config = AsyncConfig();
        config.offload_force_evict = true;
        config.offloading_queue_limit = 2;  // room 1
        auto service = Start(config);
        MasterServiceTestPeer peer(*service);
        const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
        PutAll(*service, owner, "k", keys);

        EXPECT_EQ(peer.RunAsyncOffloadEvictionTickForTesting(), Tick::kRan);
        int alive = 0;
        int queued = 0;
        for (int i = 0; i < keys; ++i) {
            const Held held = Inspect(*service, "k" + std::to_string(i));
            alive += held.exists;
            queued += held.offloading;
        }
        EXPECT_EQ(queued, 1) << keys;
        if (keys == 26) {
            // A target of 0.1 + 0.81 - 0.5 of the keys, all released.
            EXPECT_EQ(alive, 26 - 11);
        } else {
            EXPECT_EQ(alive, 22);
        }
    }
}

// Offboarding an owner ends the copies queued to it, here by freeing the
// parked source of the object its offboarding erased.
TEST_F(OffloadOnEvictAsyncTest, OffboardingAnOwnerEndsItsCopies) {
    auto service = Start();
    MasterServiceTestPeer peer(*service);
    const Owner a = MountOwner(*service, "seg_a", 0x300000000);
    const Owner b = MountOwner(*service, "seg_b", 0x400000000);
    Put(*service, a, "a0", seconds(20));
    Put(*service, b, "b0", seconds(10));
    peer.RunBatchEvictForTesting(1.0, 1.0);
    ASSERT_EQ(Take(*service, a), std::vector<std::string>{"a0"});
    ASSERT_EQ(Take(*service, b), std::vector<std::string>{"b0"});

    ClientOffboardingJob job;
    job.client_id = a.client_id;
    job.retired_session = peer.FindClientRecord(a.client_id);
    ASSERT_TRUE(job.retired_session);
    bool done = false;
    for (int i = 0; i < 20 && !done; ++i) {
        done = peer.ProcessClientOffboardingJob(job);
    }
    ASSERT_TRUE(done);

    EXPECT_EQ(peer.OffloadLedger().objects(a.client_id), 0u);
    EXPECT_EQ(peer.OffloadLedger().objects(b.client_id), 1u);
    EXPECT_FALSE(Inspect(*service, "a0").exists);
    EXPECT_TRUE(Inspect(*service, "b0").offloading);
}

TEST_F(OffloadOnEvictAsyncTest, TheKnobNeedsOffloadOnEvict) {
    {
        auto config = AsyncConfig();
        config.offload_on_evict = false;
        auto service = Start(config);
        MasterServiceTestPeer peer(*service);
        EXPECT_FALSE(peer.OffloadAsync());
        // Write-through, as configured.
        const Owner owner = MountOwner(*service, "seg_a", 0x300000000);
        Put(*service, owner, "k", seconds(10));
        EXPECT_EQ(Take(*service, owner), std::vector<std::string>{"k"});
        EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    }
    {
        auto config = AsyncConfig();
        config.enable_offload = false;
        auto service = Start(config);
        EXPECT_FALSE(MasterServiceTestPeer(*service).OffloadAsync());
    }
    for (const double ratio : {0.0, 0.001, std::nan("")}) {
        auto config = AsyncConfig();
        config.offload_cap_ratio = ratio;
        EXPECT_THROW(MasterService service(config), std::invalid_argument)
            << ratio;
    }
    {
        auto config = AsyncConfig();
        config.offload_on_evict_stall_ms = 0;
        EXPECT_THROW(MasterService service(config), std::invalid_argument);
    }
    {
        // HA with leader election and no OpLog keeps the mode.
        auto config = AsyncConfig();
        config.enable_ha = true;
        auto service = Start(config);
        EXPECT_TRUE(MasterServiceTestPeer(*service).OffloadAsync());
    }
}

// Every path at once, with the eviction thread running: writers that also
// remove, readers whose leases race the landings, stores that land, fail or
// drop their copies, and disks that come and go. Nothing may crash or hang,
// and once the reaper has run every credit is given back and no task, parked
// source or invalid object is left.
class OffloadOnEvictAsyncStressTest
    : public OffloadOnEvictAsyncTest,
      public ::testing::WithParamInterface<bool> {};

TEST_P(OffloadOnEvictAsyncStressTest, ConcurrentPutsLandingsRemovesUnmounts) {
    constexpr size_t kSmall = 256 << 10;
    constexpr int kOwners = 3;
    constexpr int kWriters = 6;
    constexpr int kKeysPerWriter = 4000;
    auto config = AsyncConfig();
    config.offload_on_evict_backoff_ms = 20;
    config.offload_force_evict = GetParam();
    config.default_kv_lease_ttl = 20;
    config.eviction_high_watermark_ratio = 0.7;
    config.put_start_discard_timeout_sec = 1;
    config.put_start_release_timeout_sec = 2;
    config.offloading_queue_limit = 32;  // room 16
    auto service = std::make_unique<MasterService>(config);
    MasterServiceTestPeer peer(*service);
    ASSERT_TRUE(peer.OffloadAsync());
    std::vector<Owner> owners;
    for (int i = 0; i < kOwners; ++i) {
        owners.push_back(MountOwner(*service, "seg_" + std::to_string(i),
                                    0x300000000ull + (uint64_t(i) << 32)));
    }
    auto key_of = [](int w, int i) {
        return "w" + std::to_string(w) + "_" + std::to_string(i);
    };
    auto put = [&](const Owner& owner, const std::string& key) {
        ReplicateConfig replicate;
        replicate.replica_num = 1;
        replicate.preferred_segments = {owner.segment};
        return service
                   ->PutStart(owner.client_id, key, TenantId::Default(), kSmall,
                              replicate)
                   .has_value() &&
               service
                   ->PutEnd(owner.client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                   .has_value();
    };
    auto report = [&](const Owner& owner, std::vector<OffloadTaskItem> items,
                      bool ok) {
        std::vector<StorageObjectMetadata> metadatas;
        for (const auto& item : items) {
            metadatas.push_back(StorageObjectMetadata{
                .bucket_id = 1,
                .offset = 0,
                .key_size = static_cast<int64_t>(item.key.size()),
                .data_size = ok ? item.size : -1,
                .transport_endpoint = owner.segment + ":rpc"});
        }
        return service->NotifyOffloadSuccess(owner.client_id, items, metadatas);
    };

    std::atomic<bool> stop_load{false};
    std::atomic<bool> stop_stores{false};
    std::atomic<uint64_t> puts_ok{0};
    std::atomic<uint64_t> landed{0};
    std::vector<std::atomic<int>> written(kWriters);
    std::vector<std::thread> threads;
    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w] {
            std::mt19937 rng(1234 + w);
            int i = 0;
            while (!stop_load && i < kKeysPerWriter) {
                if (put(owners[rng() % kOwners], key_of(w, i))) {
                    ++puts_ok;
                    written[w] = ++i;
                } else {
                    std::this_thread::sleep_for(milliseconds(1));
                }
                if (i > 8 && rng() % 8 == 0) {
                    service->Remove(key_of(w, rng() % i), TenantId::Default(),
                                    rng() % 2 == 0);
                }
                if (i > 8 && rng() % 64 == 0) {
                    service->BatchRemove(
                        {key_of(w, rng() % i), key_of(w, rng() % i)},
                        TenantId::Default(), false);
                }
                if (i > 8 && rng() % 256 == 0) {
                    service->BatchReplicaClear({key_of(w, rng() % i)},
                                               owners[0].client_id, "");
                }
            }
        });
    }
    for (int r = 0; r < 2; ++r) {
        threads.emplace_back([&, r] {
            std::mt19937 rng(99 + r);
            while (!stop_load) {
                const int w = rng() % kWriters;
                const int n = written[w].load();
                if (n > 0) {
                    service->GetReplicaList(key_of(w, rng() % n),
                                            TenantId::Default());
                }
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        });
    }
    threads.emplace_back([&] {
        while (!stop_stores) {
            for (const auto& owner : owners) {
                service->Ping(owner.client_id);
            }
            std::this_thread::sleep_for(milliseconds(200));
        }
    });
    for (int o = 0; o < kOwners; ++o) {
        threads.emplace_back([&, o] {
            std::mt19937 rng(7 + o);
            const Owner& owner = owners[o];
            while (!stop_stores) {
                auto tasks =
                    service->OffloadObjectHeartbeat(owner.client_id, true);
                if (!tasks || tasks->empty()) {
                    std::this_thread::sleep_for(milliseconds(2));
                    continue;
                }
                std::vector<OffloadTaskItem> ok, failed;
                for (auto& task : *tasks) {
                    const unsigned dice = rng() % 100;
                    if (dice < 3) {
                        continue;  // never reported: the reaper's
                    }
                    (dice < 12 ? failed : ok).push_back(std::move(task));
                }
                std::this_thread::sleep_for(milliseconds(rng() % 6));
                const size_t count = ok.size();
                if (!ok.empty() && report(owner, std::move(ok), true)) {
                    landed += count;
                }
                if (!failed.empty()) {
                    report(owner, std::move(failed), false);
                }
            }
        });
    }
    threads.emplace_back([&] {
        std::mt19937 rng(5);
        while (!stop_load) {
            std::this_thread::sleep_for(milliseconds(700));
            const Owner& owner = owners[rng() % kOwners];
            service->UnmountLocalDiskSegment(owner.client_id);
            std::this_thread::sleep_for(milliseconds(30));
            service->MountLocalDiskSegment(owner.client_id, true);
        }
    });

    std::this_thread::sleep_for(seconds(5));
    stop_load = true;
    std::this_thread::sleep_for(seconds(1));  // the stores drain
    stop_stores = true;
    for (auto& thread : threads) {
        thread.join();
    }
    // What the stores dropped is the reaper's; what is still queued is
    // taken and failed.
    peer.StopEvictionThreadForTesting();
    peer.DiscardExpiredForTesting(std::chrono::system_clock::now() +
                                  std::chrono::hours(1));
    for (const auto& owner : owners) {
        auto rest = service->OffloadObjectHeartbeat(owner.client_id, true);
        if (rest && !rest->empty()) {
            report(owner, std::move(*rest), false);
        }
    }

    EXPECT_GT(puts_ok.load(), 1000u);
    EXPECT_GT(landed.load(), 100u);
    EXPECT_EQ(peer.OffloadLedger().objects(), 0u);
    EXPECT_EQ(peer.OffloadLedger().bytes(), 0u);
    for (const auto& owner : owners) {
        EXPECT_EQ(peer.OffloadLedger().objects(owner.client_id), 0u);
    }
    size_t tasks_left = 0;
    size_t invalid = 0;
    for (int w = 0; w < kWriters; ++w) {
        for (int i = 0; i < written[w]; ++i) {
            const Held held = Inspect(*service, key_of(w, i));
            if (held.exists) {
                tasks_left += held.offloading;
                invalid += held.memory + held.disk == 0;
            }
        }
    }
    EXPECT_EQ(tasks_left, 0u);
    EXPECT_EQ(invalid, 0u);
}

INSTANTIATE_TEST_SUITE_P(ForceEvict, OffloadOnEvictAsyncStressTest,
                         ::testing::Bool());

}  // namespace
}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
