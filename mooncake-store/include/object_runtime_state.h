#pragma once

// Per-key runtime state records: the in-flight replication, offloading and
// promotion bookkeeping that belongs to one object key. Moved out of
// MasterService unchanged; each record is small state, not a container.

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "replica.h"
#include "types.h"

namespace mooncake {

// One producer's contiguous byte contribution within an in-flight RangeTarget
// assembly. Lives only in ObjectState while writing and disappears on publish.
struct RangeSpan {
    uint64_t offset{0};
    uint64_t length{0};
    UUID writer{};
    bool completed{false};
};

// Normalized placement and policy spec captured when the first producer opens
// a RangeTarget round; joining producers compare against it directly.
struct RangeWriteSpec {
    uint64_t total_size{0};
    size_t replica_num{0};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    bool hard_pinned{false};
    std::string group_id;
    std::string residency_affinity_id;
    std::string session_id;
    SoftPinAction soft_pin_action{SoftPinAction::PRESERVE};
    std::chrono::milliseconds soft_pin_ttl{0};

    bool operator==(const RangeWriteSpec&) const = default;
};

// Runtime assembly progress for a RangeTarget (PiecePut) write. Ordinary
// ObjectMetadata owns the fixed full-size MEMORY replicas, TotalLength(), and
// put_start_time.
struct RangeWriteState {
    static constexpr size_t kMaxSpans = 128;

    std::string write_id;
    RangeWriteSpec spec;
    uint64_t completed_bytes{0};
    std::vector<ReplicaID> replica_ids;
    boost::container::small_vector<RangeSpan, 8> spans;

    bool HasOverlap(uint64_t offset, uint64_t length) const noexcept {
        for (const auto& span : spans) {
            if (offset < span.offset + span.length &&
                span.offset < offset + length) {
                return true;
            }
        }
        return false;
    }

    RangeSpan* FindSpanByOffset(uint64_t offset) noexcept {
        for (auto& span : spans) {
            if (span.offset == offset) {
                return &span;
            }
        }
        return nullptr;
    }
};

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
    UUID dynamic_replication_lease_id{};
    uint64_t dynamic_replication_version_epoch{0};
    bool durable_cleanup_pending{false};
    std::optional<ReplicaID> existing_move_target{};
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
//
// alloc_id pins down which staged PROCESSING MEMORY replica
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

// NotifyPromotionSuccess should commit, so a concurrent Put on the
// same key cannot be confused with ours. 0 until
// PromotionAllocStart records the new replica.
//
// start_time is the reaper deadline anchor. Set at task admission
// and reset at PromotionAllocStart so each phase (queue-wait and
// active-transfer) gets its own full put_start_release_timeout_sec_
// window. Without the reset a backlogged task could enter active
// transfer with little TTL left, and the reaper could erase the
// staged replica by its allocation id mid-RDMA-write.
//
// holder_id is the client owning the source LOCAL_DISK segment and
// the only one authorized to commit (NotifyPromotionSuccess) or
// abort (NotifyPromotionFailure) the task. Without it, any client
// knowing the key could flip the staged PROCESSING replica to
// COMPLETE before the holder's RDMA write landed, exposing torn
// data to readers.
struct PromotionTask {
    ReplicaID source_id;    // the LOCAL_DISK replica being promoted
    ReplicaID alloc_id{0};  // the new MEMORY replica staged by AllocStart
    uint64_t object_size;
    std::chrono::system_clock::time_point start_time;
    UUID holder_id;  // owner of source LOCAL_DISK; only Notifier allowed
    // Execution failures so far in this admission chain. Read by
    // NotifyPromotionFailure before the task is erased and re-recorded as
    // execution_failures+1 until kMaxPromotionExecutionFailures. Note the
    // asymmetry with PromotionCandidate::execution_failures: admission
    // copies candidate -> task verbatim, failure re-record writes
    // task+1 -> candidate.
    uint32_t execution_failures{0};
    // SSD prefetch admission. NotifyPromotionSuccess grants a read lease
    // so the promoted replica survives until the follow-up get.
    bool from_prefetch{false};
};

}  // namespace mooncake
