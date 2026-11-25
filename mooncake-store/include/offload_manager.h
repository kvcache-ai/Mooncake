#pragma once

// Offload management: downward MEMORY -> LOCAL_DISK offloading task state
// machine.
//
// Self-contained and thread-safe: OffloadManager owns its active offload task
// table (1024 cache-line-aligned shards with lock-free negative presence
// filters) and holds RAII ReplicaPin handles on pinned source replicas.
// LocalSSD client lifecycle, capacity/usage, and mailbox state live in
// LocalSsdManager. OffloadManager never acquires MasterService locks or calls
// back into MasterService to lock keys for unpinning.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "common/concurrent_sharded_map.h"
#include "config/offload_config.h"
#include "eviction/offloading_reclaimer.h"
#include "local_ssd/manager.h"
#include "object_metadata.h"
#include "replica.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

class OffloadManager final : public eviction::OffloadQueue {
   public:
    // Minimal read-only host dependencies needed to resolve a memory replica's
    // offload targets without exposing SegmentManager or MasterService.
    class HostView {
       public:
        virtual ~HostView() = default;
        virtual bool IsReplicaReadable(const Replica& replica) const = 0;
        // Resolves every segment name in `segment_names` under a single read
        // guard, writing the owning client UUIDs into `out_owners` (whose size
        // matches `segment_names.size()`). Returns false if any segment has no
        // owner.
        virtual bool ResolveSegmentOwners(
            std::span<const std::string_view> segment_names,
            std::span<UUID> out_owners) const = 0;
    };

    enum class CompleteStatus {
        kNotTracked,        // No active offload task for (tenant, key).
        kRefusedUnmounted,  // Task cleared & unpinned; client unmounted.
        kCommitted,         // Task cleared & unpinned; LOCAL_DISK applied.
    };

    OffloadManager(LocalSsdManager& local_ssd, const HostView& host,
                   const OffloadConfig& config,
                   std::chrono::seconds task_timeout);

    OffloadManager(const OffloadManager&) = delete;
    OffloadManager& operator=(const OffloadManager&) = delete;

    // --- Self-contained offload task state machine -------------------------

    bool HasTask(const TenantId& tenant_id, std::string_view key) const;

    // eviction::OffloadQueue
    bool HasInFlight(const TenantId& tenant_id,
                     const std::string& key) const override {
        return HasTask(tenant_id, key);
    }
    bool StartOffload(const TenantId& tenant_id, const std::string& key,
                      Replica& source) override;

    // Called from PutEnd under the key write lock. No-op unless PutEnd
    // offloading is enabled and `metadata` has eligible completed memory
    // replicas (and no processing DFS replica).
    void OnPutEnd(const TenantId& tenant_id, const std::string& key,
                  ReplicaType replica_type, ObjectMetadata& metadata);

    // Returns true if no blocking offload remains (either absent or cancelled
    // while still queued in every mirror mailbox, automatically releasing its
    // ReplicaPin). Returns false if a worker has already claimed the task.
    bool TryCancelQueuedOffload(const TenantId& tenant_id,
                                std::string_view key);

    // Processes a client's offload heartbeat. If `enable_offloading` is false,
    // drains and clears pending tasks; erasing each task automatically releases
    // its ReplicaPin without requiring any per-key metadata lock.
    tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
    HandleOffloadHeartbeat(const UUID& client_id, bool enable_offloading);

    // Handles a worker NACK: clears active task (automatically releasing its
    // ReplicaPin) and updates failure metrics. Does not require the key lock.
    bool FailOffload(const UUID& client_id, const TenantId& tenant_id,
                     std::string_view key);

    // Handles a worker ACK under the key write lock: clears active task
    // (releasing its ReplicaPin), checks registration admission, applies the
    // LOCAL_DISK replica to `metadata`, and records metrics/usage.
    CompleteStatus CompleteOffload(const UUID& client_id,
                                   const TenantId& tenant_id,
                                   std::string_view key,
                                   ObjectMetadata& metadata,
                                   Replica& local_disk_replica);

    // Called from EraseMetadata: drops any active offload task (including
    // pending mailbox entries) and automatically releases its ReplicaPin.
    void OnObjectErased(const TenantId& tenant_id, std::string_view key);

    // Single-pass expiry: scans non-empty task shards, removes expired tasks
    // and their pending mailbox entries, and automatically releases their
    // ReplicaPin handles without acquiring any per-key metadata lock.
    void DiscardExpiredTasks(const std::chrono::system_clock::time_point& now);

   private:
    struct OffloadTarget {
        ReplicaID source_id{0};
        UUID client_id{};
        int64_t size{0};
    };

    struct OffloadingTask {
        ReplicaPin source_pin;
        std::chrono::system_clock::time_point start_time;
        std::vector<UUID> mirror_clients;
    };

    using TargetList = boost::container::small_vector<OffloadTarget, 4>;

    bool CollectTargets(const Replica& replica, TargetList& out) const;

    bool StartOffloadTargets(
        const TenantId& tenant_id, const std::string& key,
        std::span<const OffloadTarget> targets,
        const std::function<ReplicaPin(ReplicaID)>& pin_source);

    LocalSsdManager& local_ssd_;
    const HostView& host_;
    const OffloadConfig config_;
    const std::chrono::seconds task_timeout_;

    ConcurrentShardedMap<OffloadingTask> tasks_;
};

}  // namespace mooncake
