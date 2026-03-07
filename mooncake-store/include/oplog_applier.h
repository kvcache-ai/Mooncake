#pragma once

#include <chrono>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "oplog_manager.h"
#include "metadata_store.h"

namespace mooncake {

// Forward declaration
class EtcdOpLogStore;

/**
 * @brief Apply OpLog entries to Standby metadata store with ordering guarantee
 *
 * This class applies OpLog entries to the Standby metadata store,
 * ensuring both global and per-key ordering.
 */
class OpLogApplier {
   public:
    /**
     * @brief Constructor
     * @param metadata_store Metadata store to apply changes to
     * @param cluster_id Cluster ID for accessing etcd OpLog (optional, for
     * requesting missing OpLog)
     */
    explicit OpLogApplier(MetadataStore* metadata_store,
                          const std::string& cluster_id = std::string());

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
     * @brief Get the current sequence ID for a key (DEPRECATED)
     * @param key Object key
     * @return Always returns 0 - global sequence_id is used for ordering
     * @deprecated Use global sequence_id for ordering
     */
    uint64_t GetKeySequenceId(const std::string& key) const;

    /**
     * @brief Get the expected global sequence ID
     * @return Expected global sequence ID
     */
    uint64_t GetExpectedSequenceId() const;

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

    // Promotion helper:
    // Try to resolve current gaps ONCE (no waiting) by fetching missing/skipped
    // sequence_ids from etcd. If an entry arrives late:
    // - REMOVE / PUT_REVOKE: delete the key
    // - PUT_END: discard
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
    /**
     * @brief Check if the entry's sequence order is valid
     * @param entry OpLog entry
     * @return true if order is valid, false otherwise
     */
    bool CheckSequenceOrder(const OpLogEntry& entry);

    /**
     * @brief Apply PUT_END operation
     * @param entry OpLog entry
     */
    void ApplyPutEnd(const OpLogEntry& entry);

    /**
     * @brief Apply PUT_REVOKE operation
     * @param entry OpLog entry
     */
    void ApplyPutRevoke(const OpLogEntry& entry);

    /**
     * @brief Apply REMOVE operation
     * @param entry OpLog entry
     */
    void ApplyRemove(const OpLogEntry& entry);

    /**
     * @brief Request missing OpLog entry from etcd
     * @param missing_seq_id Missing sequence ID
     * @return true if entry was found and applied, false otherwise
     */
    bool RequestMissingOpLog(uint64_t missing_seq_id);

    /**
     * @brief Schedule wait for missing entries
     * @param missing_seq_id Missing sequence ID
     */
    void ScheduleWaitForMissingEntries(uint64_t missing_seq_id);

    MetadataStore* metadata_store_;

    // EtcdOpLogStore for requesting missing OpLog entries (optional)
    std::string cluster_id_;
    mutable std::mutex etcd_oplog_store_mutex_;
    mutable std::unique_ptr<EtcdOpLogStore> etcd_oplog_store_;

    /**
     * @brief Get or create EtcdOpLogStore instance (lazy initialization)
     * @return Pointer to EtcdOpLogStore, or nullptr if cluster_id is not set
     */
    EtcdOpLogStore* GetEtcdOpLogStore() const;

    // Note: key_sequence_map_ has been removed.
    // Global sequence_id is sufficient for ordering guarantee.

    // Track pending entries (entries with non-continuous sequence IDs)
    mutable std::mutex pending_mutex_;
    std::map<uint64_t, OpLogEntry> pending_entries_;

    // Track missing sequence IDs that we're waiting for
    std::map<uint64_t, std::chrono::steady_clock::time_point>
        missing_sequence_ids_;

    // Sequence IDs we chose to skip (gap-timeout). If the late entry arrives:
    // - REMOVE / PUT_REVOKE: delete the key (safe)
    // - PUT_END: discard (do not resurrect potentially stale metadata)
    std::map<uint64_t, std::chrono::steady_clock::time_point>
        skipped_sequence_ids_;

    // Next expected global sequence_id. Read frequently from monitoring thread,
    // updated by watch/apply thread. Use atomic to avoid data races.
    std::atomic<uint64_t> expected_sequence_id_{1};

    // Constants for missing entry handling
    // IMPORTANT: request must happen BEFORE skip, otherwise we will never
    // request.
    static constexpr int kMissingEntryRequestSeconds =
        1;  // request from etcd after 1s
    static constexpr int kMissingEntrySkipSeconds =
        3;  // skip after 3s (avoid global stall)
    static constexpr int kMaxPendingEntries =
        1000;  // Max pending entries before giving up
};

}  // namespace mooncake
