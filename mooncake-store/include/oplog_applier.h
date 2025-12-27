#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "oplog_manager.h"
#include "metadata_store.h"

namespace mooncake {

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
     */
    explicit OpLogApplier(MetadataStore* metadata_store);

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
     * @brief Get the current sequence ID for a key
     * @param key Object key
     * @return Current sequence ID, or 0 if key not found
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

    // Track per-key sequence ID for ordering guarantee
    mutable std::mutex key_sequence_mutex_;
    std::unordered_map<std::string, uint64_t> key_sequence_map_;

    // Track pending entries (entries with non-continuous sequence IDs)
    mutable std::mutex pending_mutex_;
    std::map<uint64_t, OpLogEntry> pending_entries_;
    uint64_t expected_sequence_id_{1};
};

}  // namespace mooncake

