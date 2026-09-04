#pragma once

// Shared per-object runtime task types. These are tenant- and object-scoped
// bookkeeping structs that both MasterService and the tenant module
// (mooncake::tenant::ObjectEntry) need to reference. They are deliberately
// lifted out of MasterService so the tenant module can stay independent of
// MasterService internals.
//
// Each struct is a small per-key state record, not a container.

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

struct DynamicReplicaPending {
    UUID proposal_id{};
    UUID lease_id{};
    std::string source_segment;
    std::string target_segment;
    std::string target_domain;
    uint64_t version_epoch{0};
    int64_t expire_at_ms_epoch{0};
    UUID task_id{};
};

struct ReplicationTask {
    UUID client_id;
    std::chrono::system_clock::time_point start_time;
    enum class Type {
        COPY,
        MOVE,
    } type;
    ReplicaID source_id;
    std::vector<ReplicaID> replica_ids;
    uint64_t pending_quota_charge_bytes{0};
    UUID dynamic_replication_lease_id{};
    uint64_t dynamic_replication_version_epoch{0};
    bool durable_cleanup_pending{false};
};

struct OffloadingTask {
    ReplicaID source_id;
    std::chrono::system_clock::time_point start_time;
    // Clients whose LocalDiskSegment::offloading_objects hold a mirror
    // for this key. One marker can cover several mirrors, since the
    // offload is pushed once per completed MEMORY replica and those
    // replicas may live on different clients.
    std::vector<UUID> mirror_clients;
};

// Tracks an in-flight LOCAL_DISK -> MEMORY copy. The source
// LOCAL_DISK replica is refcnt-pinned for the duration of the task
// so it cannot be evicted.
enum class PromotionQueueResult {
    kQueued,
    kDisabled,
    kFrequencyRejected,
    kWatermarkRejected,
    kQueueCapRejected,
    kAlreadyInFlight,
    kMemoryReplicaPresent,
    kNoLocalDiskSource,
    kNotFound,
    kPushFailed,
};

enum class PromotionCandidateReason {
    kWatermark,
    kQueueCap,
    kPushFailed,
    kExecutionFailed,
};

struct PromotionCandidate {
    uint8_t sketch_score{0};
    std::chrono::steady_clock::time_point first_seen;
    std::chrono::steady_clock::time_point last_seen;
    std::chrono::steady_clock::time_point retry_after;
    PromotionCandidateReason last_reason{PromotionCandidateReason::kQueueCap};
    ErrorCode last_error{ErrorCode::OK};
    uint32_t retry_count{0};
    // Execution failures in this admission chain (AllocStart / TE-write /
    // SSD failures reported via NotifyPromotionFailure). Propagated into
    // PromotionTask at admission so the bound survives the candidate's
    // consumption; reset only when a genuinely new chain starts (fresh
    // insert with 0, e.g. after a give-up or a success).
    uint32_t execution_failures{0};
};

// alloc_id is the new MEMORY replica staged by AllocStart;
// NotifyPromotionSuccess commits it so a concurrent Put on the same key cannot
// be confused with ours. start_time anchors the reaper deadline and is reset at
// AllocStart so each phase (queue-wait and active transfer) gets its own full
// timeout window. holder_id is the only client authorized to commit/abort the
// task (the source LOCAL_DISK owner); without it another client could flip the
// staged PROCESSING replica to COMPLETE before the holder's RDMA write landed.
struct PromotionTask {
    ReplicaID source_id;    // the LOCAL_DISK replica being promoted
    ReplicaID alloc_id{0};  // the new MEMORY replica staged by AllocStart
    uint64_t object_size;
    uint64_t pending_quota_charge_bytes{0};
    std::chrono::system_clock::time_point start_time;
    UUID holder_id;  // owner of source LOCAL_DISK; only Notifier allowed
    // Execution failures so far in this admission chain. Read by
    // NotifyPromotionFailure before the task is erased and re-recorded as
    // execution_failures+1 until kMaxPromotionExecutionFailures. Note the
    // asymmetry with PromotionCandidate::execution_failures: admission
    // copies candidate -> task verbatim, failure re-record writes
    // task+1 -> candidate.
    uint32_t execution_failures{0};
};

}  // namespace mooncake
