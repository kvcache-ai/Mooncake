#pragma once

// Dynamic MEMORY replication: which hot objects earn one more MEMORY replica,
// and where it goes.
//
// The controller owns the heat windows, the admission queue and the thread
// draining it, and the rules a proposal and its placement follow. It owns no
// object and takes no object lock: the caller reads an object under its own
// lock and hands the controller what it decides on.
//
// The heat lock and the queue lock are leaves, and the admission thread takes
// neither while it submits.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "client_liveness.h"
#include "object_metadata.h"
#include "rpc_types.h"
#include "segment.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}  // namespace test

class DynamicReplicationController {
   public:
    // Where one more MEMORY replica of an object would be copied from and to.
    struct Plan {
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
        std::shared_ptr<ClientLivenessRecord> source_liveness;
    };

    // Runs on the admission thread once per queued object.
    using SubmitFn =
        std::function<void(const TenantId& tenant_id, const std::string& key)>;

    // How long an object waits after a dynamic copy before the next proposal,
    // and how long a client may take to act on a lease.
    static constexpr std::chrono::milliseconds kActionCooldown{30000};
    static constexpr std::chrono::milliseconds kLeaseTtl{30000};

    // `mode` is "off", "observe" or "enforce"; anything else means off.
    DynamicReplicationController(const std::string& mode,
                                 uint32_t heat_window_seconds,
                                 double admission_qps_threshold,
                                 size_t max_memory_replicas);
    ~DynamicReplicationController();

    DynamicReplicationController(const DynamicReplicationController&) = delete;
    DynamicReplicationController& operator=(
        const DynamicReplicationController&) = delete;

    // Starts the admission thread. Stop() joins it and is idempotent; the
    // owner calls it before anything `submit` reaches is torn down.
    void Start(SubmitFn submit);
    void Stop();

    bool Enabled() const { return mode_ != Mode::kOff; }
    bool Enforcing() const { return mode_ == Mode::kEnforce; }

    // Whether an object holding `memory_replicas` readable MEMORY replicas is
    // eligible for one more.
    bool WantsAnotherMemoryReplica(size_t memory_replicas) const;

    // --- Heat admission ----------------------------------------------------

    // Counts one read of the object and, once it turns hot, queues it for a
    // proposal (enforce) or logs that it would have (observe).
    void RecordAccess(const TenantId& tenant_id, const std::string& key);
    // Counts one read; true on the read that makes the object hot.
    bool ObserveAccess(const TenantId& tenant_id, const std::string& key);
    // Whether the object is hot in its current window.
    bool HeatAdmitted(const TenantId& tenant_id, const std::string& key);
    uint32_t AdmissionMinHits() const;

    // --- Proposals ---------------------------------------------------------

    // The proposal the master raises on its own for a hot object.
    ReplicaActionProposal MakeAutoProposal(const TenantId& tenant_id,
                                           const std::string& key) const;
    // The checks that need no object: mode, shape and deadline.
    tl::expected<void, ErrorCode> ValidateProposal(
        const ReplicaActionProposal& proposal, int64_t now_ms) const;
    // Picks a source among the readable MEMORY replicas and a target segment
    // with room to spare, preferring another host.
    std::optional<Plan> SelectPlan(
        ScopedSegmentAccess& segment_access, const ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& is_readable,
        const std::optional<std::string>& preferred_target_segment,
        std::string target_domain) const;
    // Forgets removed dynamic replicas and holds off recreating them.
    size_t RecordReplicaRemoval(
        ObjectMetadata& metadata,
        const std::vector<ReplicaID>& replica_ids) const;

    // The object version a lease is bound to.
    static uint64_t VersionEpoch(const ObjectMetadata& metadata);
    static int64_t NowMs();

   private:
    friend class test::MasterServiceTestPeer;

    enum class Mode { kOff, kObserve, kEnforce };
    struct Window {
        std::chrono::steady_clock::time_point window_start{};
        uint32_t hits{0};
    };
    struct QueuedObject {
        TenantId tenant_id;
        std::string key;
    };

    static constexpr std::chrono::milliseconds kRecreateCooldown{60000};
    static constexpr std::chrono::milliseconds kWindowCleanupInterval{1000};
    static constexpr uint64_t kAdmissionThreadSleepMs = 100;
    static constexpr size_t kWindowEntryLimit = 50000;
    static constexpr size_t kWindowCleanupBudget = 256;
    static constexpr size_t kAdmissionQueueLimit = 50000;
    static constexpr size_t kAdmissionBatchSize = 64;
    static constexpr double kTargetHighWatermark = 0.85;

    static uint64_t StableScore(const std::string& key,
                                const std::string& segment);
    void CleanupWindowsLocked(std::chrono::steady_clock::time_point now,
                              std::chrono::seconds window);
    void Enqueue(const TenantId& tenant_id, const std::string& key);
    void AdmissionThreadFunc();

    const Mode mode_;
    const uint32_t heat_window_seconds_;
    const double admission_qps_threshold_;
    const size_t max_memory_replicas_;

    std::mutex heat_mutex_;
    std::unordered_map<std::string, Window> windows_;
    std::deque<std::string> window_order_;
    std::chrono::steady_clock::time_point next_window_cleanup_{};

    std::mutex admission_mutex_;
    std::condition_variable admission_cv_;
    std::queue<QueuedObject> admission_queue_;
    std::unordered_set<std::string> admission_queued_;
    std::atomic<bool> admission_running_{false};
    SubmitFn submit_;
    std::thread admission_thread_;
};

}  // namespace mooncake
