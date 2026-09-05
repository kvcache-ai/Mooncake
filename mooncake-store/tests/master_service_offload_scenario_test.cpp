#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "master_service/dsl/scenario.h"

namespace mooncake::test {
namespace {

MasterServiceConfig OffloadConfig(uint64_t lease_ttl_ms = 2000) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = lease_ttl_ms;
    return config;
}

MasterServiceConfig OffloadOnEvictConfig() {
    MasterServiceConfig config = OffloadConfig();
    config.offload_on_evict = true;
    return config;
}

MasterServiceConfig OffloadForceEvictConfig() {
    MasterServiceConfig config = OffloadOnEvictConfig();
    config.offload_force_evict = true;
    return config;
}

// Matches the local-disk fleet the direct tests build: offloading enabled, no
// leases in the way, and the allocator ranking segments by SSD free ratio.
MasterServiceConfig SsdAwareOffloadConfig() {
    MasterServiceConfig config = OffloadConfig(/*lease_ttl_ms=*/0);
    config.allocation_strategy_type =
        AllocationStrategyType::SSD_FREE_RATIO_FIRST;
    return config;
}

std::string HeartbeatKey(size_t index) {
    return "offload_hb_" + std::to_string(index);
}

std::vector<std::string> HeartbeatKeys(size_t begin, size_t end) {
    std::vector<std::string> keys;
    keys.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
        keys.push_back(HeartbeatKey(index));
    }
    return keys;
}

}  // namespace

TEST(MasterServiceOffloadScenarioTest, HeartbeatReturnsObjectsPutSinceEnable) {
    // Spread enough keys across the metadata shards that the heartbeat sweep
    // covers all of them, as the direct test did with 3000 keys per phase.
    constexpr size_t kBatch = 3000;
    MasterScenario scenario(
        "the heartbeat hands back objects put while offloading was enabled",
        OffloadConfig());
    scenario.Given(MemoryNode("node").Capacity(16384_KB));
    scenario.When(MountLocalDisk("node").OffloadingDisabled());
    scenario.Given(Objects(0, kBatch)
                       .NamedBy(HeartbeatKey)
                       .Size(1_KB)
                       .By("node")
                       .CompleteOn("node"));
    scenario.When(OffloadHeartbeat("node").ExpectNoTasks());
    scenario.Given(Objects(kBatch, 2 * kBatch)
                       .NamedBy(HeartbeatKey)
                       .Size(1_KB)
                       .By("node")
                       .CompleteOn("node"));
    scenario.When(OffloadHeartbeat("node").ExpectTasks(
        HeartbeatKeys(kBatch, 2 * kBatch), 1024));
    scenario.Given(Objects(2 * kBatch, 3 * kBatch)
                       .NamedBy(HeartbeatKey)
                       .Size(1_KB)
                       .By("node")
                       .CompleteOn("node"));
    scenario.When(OffloadHeartbeat("node").ExpectTasks(
        HeartbeatKeys(2 * kBatch, 3 * kBatch), 1024));
}

TEST(MasterServiceOffloadScenarioTest, PutEndQueuesOffloadByDefault) {
    MasterScenario("the default mode queues offload work at PutEnd",
                   OffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .Given(Objects({"key_a1", "key_a2", "key_a3"})
                   .Size(1_KB)
                   .By("node")
                   .CompleteOn("node"))
        .When(OffloadHeartbeat("node").ExpectTasks(
            {"key_a1", "key_a2", "key_a3"}, 1024));
}

TEST(MasterServiceOffloadScenarioTest, OffloadOnEvictSkipsQueueingAtPutEnd) {
    MasterScenario("offload_on_evict defers queueing to eviction",
                   OffloadOnEvictConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .Given(Objects({"key_b1", "key_b2", "key_b3"})
                   .Size(1_KB)
                   .By("node")
                   .CompleteOn("node"))
        .When(OffloadHeartbeat("node").ExpectNoTasks());
}

TEST(MasterServiceOffloadScenarioTest,
     OffloadOnEvictWithForceEvictSkipsQueueingAtPutEnd) {
    MasterScenario("offload_force_evict does not restore PutEnd queueing",
                   OffloadForceEvictConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .Given(Objects({"key_c1", "key_c2"})
                   .Size(1_KB)
                   .By("node")
                   .CompleteOn("node"))
        .When(OffloadHeartbeat("node").ExpectNoTasks());
}

TEST(MasterServiceOffloadScenarioTest, ForceEvictAloneKeepsDefaultQueueing) {
    MasterServiceConfig config = OffloadConfig();
    config.offload_force_evict = true;  // on_evict is false, so it is ignored
    MasterScenario("offload_force_evict alone behaves like the default mode",
                   config)
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .Given(Objects({"key_d1", "key_d2"})
                   .Size(1_KB)
                   .By("node")
                   .CompleteOn("node"))
        .When(OffloadHeartbeat("node").ExpectTasks({"key_d1", "key_d2"}, 1024));
}

TEST(MasterServiceOffloadScenarioTest, BatchRemoveDropsQueuedOffloadMirrors) {
    // EraseMetadata must drop the mirror entry in the LocalDisk segment's
    // offloading_objects; a stale entry would be drained back to the client
    // and produce an orphan bucket on SSD.
    MasterScenario("a removed key leaves nothing in the offload queue",
                   OffloadConfig(/*lease_ttl_ms=*/0))
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .Given(Objects({"key_r1", "key_r2", "key_r3"})
                   .Size(1_KB)
                   .By("node")
                   .CompleteOn("node"))
        .When(BatchRemove({"key_r1", "key_r2", "key_r3"}).Force())
        .When(OffloadHeartbeat("node").ExpectNoTasks());
}

// An offload task passes through two observable states in
// offloading_tasks[key]: QUEUED (mirror still present, worker has not seen
// the task; UpsertStart cancels it in place) and IN-FLIGHT (mirror drained by
// the heartbeat; UpsertStart returns OBJECT_HAS_REPLICATION_TASK and the
// caller retries after the worker's completion).

TEST(MasterServiceOffloadScenarioTest, UpsertPreemptsQueuedOffload) {
    MasterScenario(
        "an upsert cancels a queued offload and leaves no stale "
        "queue entry",
        OffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .When(PutStart("upsert_over_queued_offload", 1_KB)
                  .By("node")
                  .ExpectReplicas(1))
        .When(PutEnd("upsert_over_queued_offload").By("node"))
        .When(UpsertStart("upsert_over_queued_offload", 1_KB)
                  .By("node")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .When(OffloadHeartbeat("node").ExpectNoTasks())
        .When(PutEnd("upsert_over_queued_offload").By("node"));
}

TEST(MasterServiceOffloadScenarioTest, BatchUpsertPreemptsQueuedOffloads) {
    MasterScenario("a batch upsert preempts every queued offload it covers",
                   OffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .Given(Objects({"batch_k1", "batch_k2", "batch_k3"})
                   .Size(1_KB)
                   .By("node")
                   .CompleteOn("node"))
        .When(BatchUpsertStart(
                  {{"batch_k1", 1_KB}, {"batch_k2", 1_KB}, {"batch_k3", 1_KB}})
                  .By("node"))
        .When(OffloadHeartbeat("node").ExpectNoTasks());
}

TEST(MasterServiceOffloadScenarioTest, UpsertIsRejectedWhileOffloadInFlight) {
    MasterScenario("an upsert cannot displace an offload the worker owns",
                   OffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .When(PutStart("upsert_over_inflight_offload", 1_KB)
                  .By("node")
                  .ExpectReplicas(1))
        .When(PutEnd("upsert_over_inflight_offload").By("node"))
        .When(OffloadHeartbeat("node").ExpectTasks(
            {"upsert_over_inflight_offload"}, 1024))
        .When(UpsertStart("upsert_over_inflight_offload", 1_KB)
                  .By("node")
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK));
}

TEST(MasterServiceOffloadScenarioTest,
     RejectedUpsertLeavesTheOtherMirrorInPlace) {
    // A key replicated onto two clients gets a mirror on each of their
    // LocalDisk segments, both covered by a single offloading_tasks entry.
    // Draining one client's queue hands that worker the task, so the upsert
    // must be rejected and the other client's mirror must survive for the
    // worker's completion.
    MasterScenario("a rejected upsert does not consume the second mirror",
                   OffloadConfig())
        .Given(MemoryNode("node-a"))
        .Given(MemoryNode("node-b"))
        .When(MountLocalDisk("node-a"))
        .When(MountLocalDisk("node-b"))
        .When(PutStart("upsert_over_partially_drained_offload", 1_KB)
                  .By("node-a")
                  .Replicas(2)
                  .OnNodes({"node-a", "node-b"})
                  .ExpectReplicas(2))
        .When(PutEnd("upsert_over_partially_drained_offload").By("node-a"))
        .When(OffloadHeartbeat("node-a").ExpectTasks(
            {"upsert_over_partially_drained_offload"}, 1024))
        .When(UpsertStart("upsert_over_partially_drained_offload", 1_KB)
                  .By("node-a")
                  .Replicas(2)
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(OffloadHeartbeat("node-b").ExpectTasks(
            {"upsert_over_partially_drained_offload"}, 1024));
}

TEST(MasterServiceOffloadScenarioTest, RemoveReleasesLocalDiskUsage) {
    // Both stores report 1000 bytes of SSD. Offloading 800 bytes to disk-1
    // leaves it 20% free against disk-2's 90%, so disk-2 would win the next
    // allocation; removing the heavy object must hand disk-1 its bytes back.
    MasterScenario("Remove returns an offloaded object's SSD bytes",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("disk-1"))
        .Given(MemoryNode("disk-2"))
        .When(MountLocalDisk("disk-1"))
        .When(ReportSsdCapacity("disk-1", 1000))
        .When(MountLocalDisk("disk-2"))
        .When(ReportSsdCapacity("disk-2", 1000))
        .When(PutStart("ssd_remove_released", 800).By("disk-1"))
        .When(PutEnd("ssd_remove_released").By("disk-1"))
        .When(CompleteOffload({"ssd_remove_released"})
                  .By("disk-1")
                  .OnNode("disk-1"))
        .When(PutStart("ssd_remove_baseline", 100).By("disk-2"))
        .When(PutEnd("ssd_remove_baseline").By("disk-2"))
        .When(CompleteOffload({"ssd_remove_baseline"})
                  .By("disk-2")
                  .OnNode("disk-2"))
        .When(Remove("ssd_remove_released"))
        .When(PutStart("ssd_remove_probe", 64)
                  .By("disk-1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"disk-1"}));
}

TEST(MasterServiceOffloadScenarioTest, ReplicaClearReleasesLocalDiskUsage) {
    MasterScenario("BatchReplicaClear returns an offloaded object's SSD bytes",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("disk-1"))
        .Given(MemoryNode("disk-2"))
        .When(MountLocalDisk("disk-1"))
        .When(ReportSsdCapacity("disk-1", 1000))
        .When(MountLocalDisk("disk-2"))
        .When(ReportSsdCapacity("disk-2", 1000))
        .When(PutStart("ssd_clear_released", 800).By("disk-1"))
        .When(PutEnd("ssd_clear_released").By("disk-1"))
        .When(CompleteOffload({"ssd_clear_released"})
                  .By("disk-1")
                  .OnNode("disk-1"))
        .When(PutStart("ssd_clear_baseline", 100).By("disk-2"))
        .When(PutEnd("ssd_clear_baseline").By("disk-2"))
        .When(CompleteOffload({"ssd_clear_baseline"})
                  .By("disk-2")
                  .OnNode("disk-2"))
        .When(ClearReplicas({"ssd_clear_released"})
                  .By("disk-1")
                  .ExpectCleared({"ssd_clear_released"}))
        .When(PutStart("ssd_clear_probe", 64)
                  .By("disk-1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"disk-1"}));
}

TEST(MasterServiceOffloadScenarioTest, AllocationPrefersTheFresherSsd) {
    MasterScenario("the allocator follows SSD free ratio after offloads",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("disk-1"))
        .Given(MemoryNode("disk-2"))
        .When(MountLocalDisk("disk-1"))
        .When(ReportSsdCapacity("disk-1", 1000))
        .When(MountLocalDisk("disk-2"))
        .When(ReportSsdCapacity("disk-2", 1000))
        .When(PutStart("ssd_fresher_heavy", 800).By("disk-1"))
        .When(PutEnd("ssd_fresher_heavy").By("disk-1"))
        .When(CompleteOffload({"ssd_fresher_heavy"})
                  .By("disk-1")
                  .OnNode("disk-1"))
        .When(PutStart("ssd_fresher_light", 100).By("disk-2"))
        .When(PutEnd("ssd_fresher_light").By("disk-2"))
        .When(CompleteOffload({"ssd_fresher_light"})
                  .By("disk-2")
                  .OnNode("disk-2"))
        .When(PutStart("ssd_fresher_probe", 64)
                  .By("disk-2")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"disk-2"}));
}

TEST(MasterServiceOffloadScenarioTest, EvictDiskReplicaReleasesLocalDiskUsage) {
    MasterScenario("evicting a LOCAL_DISK replica returns its SSD bytes",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("disk-1"))
        .Given(MemoryNode("disk-2"))
        .When(MountLocalDisk("disk-1"))
        .When(ReportSsdCapacity("disk-1", 1000))
        .When(MountLocalDisk("disk-2"))
        .When(ReportSsdCapacity("disk-2", 1000))
        .When(PutStart("ssd_evict_dec_heavy", 800).By("disk-1"))
        .When(PutEnd("ssd_evict_dec_heavy").By("disk-1"))
        .When(CompleteOffload({"ssd_evict_dec_heavy"})
                  .By("disk-1")
                  .OnNode("disk-1"))
        .When(PutStart("ssd_evict_dec_light", 100).By("disk-2"))
        .When(PutEnd("ssd_evict_dec_light").By("disk-2"))
        .When(CompleteOffload({"ssd_evict_dec_light"})
                  .By("disk-2")
                  .OnNode("disk-2"))
        .When(PutStart("ssd_evict_dec_probe1", 64)
                  .By("disk-2")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"disk-2"}))
        .When(EvictDiskReplica("ssd_evict_dec_heavy")
                  .By("disk-1")
                  .OfType(ReplicaType::LOCAL_DISK))
        .When(PutStart("ssd_evict_dec_probe2", 64)
                  .By("disk-1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"disk-1"}));
}

// A LOCAL_DISK segment used to leave the master only when its client expired,
// so a store that was shutting down stayed advertised as the owner of its
// offloaded keys for up to one client_ttl. These cover the operation that
// lets it deregister itself instead.

TEST(MasterServiceOffloadScenarioTest, UnmountLocalDiskRequiresOffloadMode) {
    MasterScenario(
        "deregistration is refused when offload is not enabled",
        MasterServiceConfig::builder().set_root_fs_dir("/mnt/ssd").build())
        .Given(MemoryNode("node"))
        .When(UnmountLocalDisk("node").ExpectError(ErrorCode::UNABLE_OFFLOAD));
}

TEST(MasterServiceOffloadScenarioTest, UnmountLocalDiskIsIdempotent) {
    MasterScenario("repeated and never-mounted deregistrations succeed",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .When(ReportSsdCapacity("node", 1000))
        .When(UnmountLocalDisk("node"))
        .When(UnmountLocalDisk("node"))
        .When(UnmountLocalDisk("never-mounted"));
}

TEST(MasterServiceOffloadScenarioTest, UnmountLocalDiskStopsOffloadWork) {
    // The master hands a deregistered client no further offload work. The
    // client reads this as its cue to re-mount, which is why a draining store
    // must latch offloading off before calling.
    MasterScenario("a deregistered client's heartbeat is refused",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .When(ReportSsdCapacity("node", 1000))
        .When(OffloadHeartbeat("node"))
        .When(UnmountLocalDisk("node"))
        .When(
            OffloadHeartbeat("node").ExpectError(ErrorCode::SEGMENT_NOT_FOUND));
}

TEST(MasterServiceOffloadScenarioTest, UnmountLocalDiskDropsOnlyItsReplicas) {
    // Neither client re-mounts, so neither is in the master's alive set. That
    // is the case that catches a deregistration which sweeps by liveness
    // instead of by owner: it would take the staying store's replicas too.
    MasterScenario("deregistration sweeps by owner, not by liveness",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("leaving"))
        .Given(MemoryNode("staying"))
        .When(MountLocalDisk("leaving"))
        .When(ReportSsdCapacity("leaving", 1000))
        .When(MountLocalDisk("staying"))
        .When(ReportSsdCapacity("staying", 1000))
        .When(PutStart("ssd_unmount_leaving_key", 1_KB).By("leaving"))
        .When(PutEnd("ssd_unmount_leaving_key").By("leaving"))
        .When(CompleteOffload({"ssd_unmount_leaving_key"})
                  .By("leaving")
                  .OnNode("leaving"))
        .When(PutStart("ssd_unmount_staying_key", 1_KB).By("staying"))
        .When(PutEnd("ssd_unmount_staying_key").By("staying"))
        .When(CompleteOffload({"ssd_unmount_staying_key"})
                  .By("staying")
                  .OnNode("staying"))
        .When(UnmountLocalDisk("leaving"))
        .Then(Object("ssd_unmount_leaving_key")
                  .HasReplicas(1)
                  .HasMemoryReplicas(1))
        .Then(Object("ssd_unmount_staying_key")
                  .HasReplicas(2)
                  .HasLocalDiskReplicas(1));
}

TEST(MasterServiceOffloadScenarioTest, UnmountLocalDiskKeepsReAdoptionWorking) {
    // A key whose only replica is the disk is what a store's own files look
    // like to the master after it re-registers them. Deregistration erases it
    // the same as client expiry would, and a store that comes back re-adopts
    // its files, so deregistering costs a restart nothing.
    MasterScenario("a disk-only key is erased and can be re-adopted",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .When(ReportSsdCapacity("node", 1000))
        .When(CompleteOffload({"ssd_unmount_disk_only_key"})
                  .By("node")
                  .OnNode("node")
                  .OfSize(1024))
        .Then(Object("ssd_unmount_disk_only_key").IsReadable())
        .When(UnmountLocalDisk("node"))
        .Then(Object("ssd_unmount_disk_only_key").DoesNotExist())
        .When(MountLocalDisk("node"))
        .When(CompleteOffload({"ssd_unmount_disk_only_key"})
                  .By("node")
                  .OnNode("node")
                  .OfSize(1024))
        .Then(Object("ssd_unmount_disk_only_key")
                  .HasReplicas(1)
                  .HasLocalDiskReplicas(1));
}

TEST(MasterServiceOffloadScenarioTest, CompleteOffloadAfterUnmountIsRefused) {
    // A registration that arrives after the deregistration -- an in-flight
    // rescan batch, or an offload completion racing the drain -- must be
    // refused, so it cannot land after the sweep and leave the master
    // advertising a departed owner. The orphan re-adoption path is refused
    // the same way.
    MasterScenario("late offload completions cannot revive a swept segment",
                   SsdAwareOffloadConfig())
        .Given(MemoryNode("node"))
        .When(MountLocalDisk("node"))
        .When(ReportSsdCapacity("node", 1000))
        .When(PutStart("ssd_gate_key", 1_KB).By("node"))
        .When(PutEnd("ssd_gate_key").By("node"))
        .When(CompleteOffload({"ssd_gate_key"}).By("node").OnNode("node"))
        .When(UnmountLocalDisk("node"))
        .When(CompleteOffload({"ssd_gate_key"})
                  .By("node")
                  .OnNode("node")
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .Then(Object("ssd_gate_key").HasReplicas(1).HasMemoryReplicas(1))
        .When(CompleteOffload({"ssd_gate_orphan_key"})
                  .By("node")
                  .OnNode("node")
                  .OfSize(1024)
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .Then(Object("ssd_gate_orphan_key").DoesNotExist());
}

}  // namespace mooncake::test
