// mooncake-store/include/ha/oplog/p2p_oplog_applier.h
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"

namespace mooncake {

class OpLogStore;

/// P2P-specific OpLog applier.
///
/// Owns ordering, gap handling and failure state for P2P route, client and
/// segment operations. Applies metadata changes to P2PStandbyMetadataStore.
///
class P2POpLogApplier {
   public:
    /// Constructor.
    /// @param p2p_store       The P2PStandbyMetadataStore to apply ops to.
    ///                        Must outlive this applier.
    /// @param cluster_id      Cluster ID for validation.
    /// @param oplog_store     Optional OpLogStore for gap resolution.
    explicit P2POpLogApplier(P2PStandbyMetadataStore* p2p_store,
                             const std::string& cluster_id = std::string(),
                             OpLogStore* oplog_store = nullptr);

    /**
     * @brief Set or replace the OpLogStore used for requesting missing entries
     * @param oplog_store OpLogStore pointer (caller owns the pointer)
     */
    void SetOpLogStore(OpLogStore* oplog_store) { oplog_store_ = oplog_store; }

    /**
     * @brief Apply a single OpLog entry (with ordering checks)
     * @param entry OpLog entry to apply
     * @return true on success, false on failure or ordering violation
     */
    bool ApplyOpLogEntry(const OpLogEntry& entry);

    /**
     * @brief Apply multiple OpLog entries
     * @param entries OpLog entries to apply
     * @return Number of successfully applied entries
     */
    size_t ApplyOpLogEntries(const std::vector<OpLogEntry>& entries);

    /**
     * @brief Get the expected global sequence ID
     * @return Expected global sequence ID
     */
    uint64_t GetExpectedSequenceId() const;

    bool IsHealthy() const { return healthy_.load(); }
    uint64_t GetFailedSequenceId() const { return failed_sequence_id_.load(); }
    int GetFailedOpType() const { return failed_op_type_.load(); }
    std::string GetFailureReason() const;

    /**
     * @brief Recover from a given sequence ID
     * @param last_applied_sequence_id Last applied sequence ID
     */
    void Recover(uint64_t last_applied_sequence_id);

    /**
     * @brief Process pending entries (entries with non-continuous sequence IDs)
     * @return Number of entries processed
     */
    size_t ProcessPendingEntries();

    // Mark sequence IDs that the store scanned but did not contain. Confirmed
    // holes can be skipped without waiting for the generic gap timeout.
    void ConfirmMissingSequenceIds(
        const std::vector<uint64_t>& missing_sequence_ids);

    // Promotion helper:
    // Try to resolve current gaps ONCE (no waiting) by fetching missing/skipped
    // sequence_ids from the store. If an entry arrives late:
    // - P2P delete-like entries: apply the matching operation
    // - publish entries: discard
    //
    // This is used during Standby promotion so we don't block promotion on
    // gaps, but still best-effort clean up potentially stale metadata.
    struct GapResolveResult {
        size_t attempted{0};
        size_t fetched{0};
        size_t applied_deletes{0};
    };
    GapResolveResult TryResolveGapsOnceForPromotion(size_t max_ids = 1024);

   private:
    bool ApplyOpLogEntryInternal(const OpLogEntry& entry);
    bool IsBestEffortOpLogEntry(const OpLogEntry& entry) const;
    bool IsLateSkippedDeleteLikeOpLogEntry(
        const OpLogEntry& entry) const;

    // Apply individual P2P OpTypes. Return true on success.
    bool ApplyPublishRoute(const OpLogEntry& entry);
    bool ApplyWithdrawRoute(const OpLogEntry& entry);
    bool ApplyMountSegment(const OpLogEntry& entry);
    bool ApplyUnmountSegment(const OpLogEntry& entry);
    bool ApplyRegisterClient(const OpLogEntry& entry);
    bool ApplyUnregisterClient(const OpLogEntry& entry);

    /**
     * @brief Request missing OpLog entry from the store
     * @param missing_seq_id Missing sequence ID
     * @return true if entry was found and applied, false otherwise
     */
    bool RequestMissingOpLog(uint64_t missing_seq_id);

    bool HandleApplyFailure(const OpLogEntry& entry, const char* reason);

    // OpLogStore for requesting missing OpLog entries (optional, not owned)
    std::string cluster_id_;
    OpLogStore* oplog_store_{nullptr};

    // Note: key_sequence_map_ has been removed.
    // Global sequence_id is sufficient for ordering guarantee.

    // Track pending entries (entries with non-continuous sequence IDs)
    mutable std::mutex pending_mutex_;
    std::map<uint64_t, OpLogEntry> pending_entries_;

    // Track missing sequence IDs that we're waiting for
    std::map<uint64_t, std::chrono::steady_clock::time_point>
        missing_sequence_ids_;

    std::set<uint64_t> confirmed_missing_sequence_ids_;

    // Sequence IDs we chose to skip (gap-timeout). If the late entry arrives:
    // - delete-like entries: apply to avoid stale metadata
    // - publish entries: discard to avoid resurrecting stale state
    std::map<uint64_t, std::chrono::steady_clock::time_point>
        skipped_sequence_ids_;

    // Next expected global sequence_id. Read frequently from monitoring thread,
    // updated by watch/apply thread. Use atomic to avoid data races.
    std::atomic<uint64_t> expected_sequence_id_{1};
    std::atomic<bool> healthy_{true};
    std::atomic<uint64_t> failed_sequence_id_{0};
    std::atomic<int> failed_op_type_{-1};
    mutable std::mutex failure_mutex_;
    std::string failure_reason_;

    // Constants for missing entry handling
    // IMPORTANT: request must happen BEFORE skip, otherwise we will never
    // request.
    static constexpr int kMissingEntryRequestSeconds =
        1;  // request from the store after 1s
    static constexpr int kMissingEntrySkipSeconds =
        3;  // skip after 3s (avoid global stall)
    static constexpr int kMaxPendingEntries =
        1000;  // Max pending entries before giving up

    P2PStandbyMetadataStore* p2p_store_;
};

}  // namespace mooncake
