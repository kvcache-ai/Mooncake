// mooncake-store/include/etcd_oplog_change_notifier.h
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ha/oplog/etcd_oplog_store.h"
#include "ha/oplog/oplog_change_notifier.h"
#include "ha/oplog/oplog_manager.h"
#include "types.h"

namespace mooncake {

// Forward declaration
class EtcdOpLogChangeNotifier;

// Shared control block for safe C-style watch callbacks.
// Because the etcd Watch goroutine can deliver callbacks after Stop()
// returns (or even after destruction), we cannot pass a raw `this`
// pointer as the callback context.
struct ChangeNotifierCallbackContext {
    std::mutex mutex;
    EtcdOpLogChangeNotifier* notifier{nullptr};

    ChangeNotifierCallbackContext() = default;
    ChangeNotifierCallbackContext(const ChangeNotifierCallbackContext&) =
        delete;
    ChangeNotifierCallbackContext& operator=(
        const ChangeNotifierCallbackContext&) = delete;
};

// OpLogChangeNotifier implementation backed by etcd Watch.
// Migrated from OpLogReplicator's watch logic.
class EtcdOpLogChangeNotifier : public OpLogChangeNotifier {
   public:
    explicit EtcdOpLogChangeNotifier(const std::string& cluster_id,
                                     EtcdOpLogStore* oplog_store);
    ~EtcdOpLogChangeNotifier();

    ErrorCode Start(uint64_t start_sequence_id, EntryCallback on_entry,
                    ErrorCallback on_error) override;
    void Stop() override;
    bool IsHealthy() const override;

   private:
    // Read historical entries and return the etcd revision for watch resume.
    bool ReadOpLogSince(uint64_t start_seq_id, std::vector<OpLogEntry>& entries,
                        EtcdRevisionId& revision_id);

    // C-style callback for etcd Watch goroutine.
    static void WatchCallback(void* context, const char* key, size_t key_size,
                              const char* value, size_t value_size,
                              int event_type, int64_t mod_revision);

    // Background thread running the watch loop.
    void WatchLoop();

    // Handle a single watch event (PUT/DELETE/BROKEN).
    void HandleWatchEvent(const std::string& key, const std::string& value,
                          int event_type, int64_t mod_revision);

    // Reconnection with exponential backoff.
    void TryReconnect();

    // Sync missed entries after reconnection.
    bool SyncMissedEntries();

    // Read and deliver all entries since start_seq_id. Updates
    // last_processed_sequence_id_ and next_watch_revision_.
    // Returns the number of delivered entries, or -1 on read failure.
    int64_t DeliverHistoricalEntries(uint64_t start_seq_id);

    std::string cluster_id_;
    std::string watch_prefix_;     // "/oplog/{cluster_id}/"
    EtcdOpLogStore* oplog_store_;  // Not owned

    EntryCallback on_entry_;
    ErrorCallback on_error_;

    std::atomic<bool> running_{false};
    std::thread watch_thread_;
    std::atomic<uint64_t> last_processed_sequence_id_{0};
    std::atomic<int64_t> next_watch_revision_{0};

    ChangeNotifierCallbackContext* callback_ctx_{nullptr};

    std::atomic<int> consecutive_errors_{0};
    std::atomic<int> reconnect_count_{0};
    std::atomic<bool> watch_healthy_{false};

    static constexpr int kMaxConsecutiveErrors = 10;
    static constexpr int kReconnectDelayMs = 1000;
    static constexpr int kMaxReconnectDelayMs = 30000;
    static constexpr int kSyncBatchSize = 1000;
};

}  // namespace mooncake
