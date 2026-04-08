// mooncake-store/include/polling_oplog_change_notifier.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "ha/oplog/oplog_change_notifier.h"
#include "ha/oplog/oplog_store.h"

namespace mooncake {

class PollingOpLogChangeNotifier : public OpLogChangeNotifier {
   public:
    PollingOpLogChangeNotifier(OpLogStore* store, int poll_interval_ms);
    ~PollingOpLogChangeNotifier();

    ErrorCode Start(uint64_t start_sequence_id, EntryCallback on_entry,
                    ErrorCallback on_error) override;
    void Stop() override;
    bool IsHealthy() const override;

   private:
    void PollLoop();

    OpLogStore* store_;  // Not owned
    int poll_interval_ms_;

    EntryCallback on_entry_;
    ErrorCallback on_error_;

    std::atomic<bool> running_{false};
    std::thread poll_thread_;
    std::atomic<uint64_t> last_sequence_id_{0};
    std::atomic<bool> healthy_{false};

    // For interruptible sleep on Stop()
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;

    static constexpr size_t kPollBatchSize = 1000;
};

}  // namespace mooncake
