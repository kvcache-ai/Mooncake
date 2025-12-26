#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "oplog_manager.h"

namespace mooncake {

// Forward declaration
class OpLogApplier;

/**
 * @brief Watch etcd for OpLog changes and apply them to Standby
 *
 * This class watches etcd for new OpLog entries and forwards them
 * to OpLogApplier for processing.
 */
class OpLogWatcher {
   public:
    /**
     * @brief Constructor
     * @param etcd_endpoints Comma-separated etcd endpoints
     * @param cluster_id Cluster identifier
     * @param applier OpLog applier to process entries
     */
    OpLogWatcher(const std::string& etcd_endpoints,
                 const std::string& cluster_id, OpLogApplier* applier);

    ~OpLogWatcher();

    /**
     * @brief Start watching etcd for OpLog changes
     */
    void Start();

    /**
     * @brief Stop watching
     */
    void Stop();

    /**
     * @brief Read OpLog entries from etcd since a given sequence ID
     * @param start_seq_id Starting sequence ID (exclusive)
     * @param entries Output vector of OpLog entries
     * @return true on success, false on failure
     */
    bool ReadOpLogSince(uint64_t start_seq_id,
                        std::vector<OpLogEntry>& entries);

    /**
     * @brief Get the last processed sequence ID
     * @return Last processed sequence ID
     */
    uint64_t GetLastProcessedSequenceId() const;

   private:
    /**
     * @brief Watch etcd OpLog changes (runs in background thread)
     */
    void WatchOpLog();

    /**
     * @brief Process a Watch event
     * @param key etcd key
     * @param value etcd value
     * @param revision etcd revision
     */
    void HandleWatchEvent(const std::string& key, const std::string& value,
                          int64_t revision);

    std::string etcd_endpoints_;
    std::string cluster_id_;
    OpLogApplier* applier_;
    std::atomic<bool> running_{false};
    std::thread watch_thread_;
    std::atomic<uint64_t> last_processed_sequence_id_{0};
};

}  // namespace mooncake

