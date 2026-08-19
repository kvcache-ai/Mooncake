#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "ha/oplog/oplog_types.h"
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
     * @param cluster_id Cluster ID (for validation only)
     */
    explicit OpLogApplier(MetadataStore* metadata_store,
                          const std::string& cluster_id = std::string());

    ~OpLogApplier() = default;

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

    /**
     * @brief Recover from a given sequence ID
     * @param last_applied_sequence_id Last applied sequence ID
     */
    void Recover(uint64_t last_applied_sequence_id);

    const StandbySegmentRegistry& GetSegmentRegistry() const;
    void ApplySegmentMount(const OpLogEntry& entry);
    void ApplySegmentUnmount(const OpLogEntry& entry);
    void ApplySegmentUpdate(const OpLogEntry& entry);

    /**
     * @brief Load segment registry from snapshot baseline.
     * Clears existing registry and replaces with given segments.
     */
    void LoadSegmentRegistry(const std::vector<StandbySegmentInfo>& segments);

   private:
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

    MetadataStore* metadata_store_;

    std::string cluster_id_;

    // Next expected global sequence_id. Read frequently from monitoring thread,
    // updated by watch/apply thread. Use atomic to avoid data races.
    std::atomic<uint64_t> expected_sequence_id_{1};

    // Standby segment registry
    StandbySegmentRegistry segment_registry_;
};

}  // namespace mooncake
