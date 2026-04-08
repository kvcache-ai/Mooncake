#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "ha/oplog/oplog_store.h"
#include "ha/standby/standby_state_machine.h"

namespace mooncake {

class OpLogApplier;

using WatcherStateCallback = std::function<void(StandbyEvent)>;

class OpLogWatcher {
   public:
    OpLogWatcher(std::shared_ptr<ha::OpLogStore> oplog_store,
                 OpLogApplier* applier);
    ~OpLogWatcher();

    void Start();
    bool StartFromSequenceId(uint64_t start_seq_id);
    void Stop();

    uint64_t GetLastProcessedSequenceId() const;

    void SetStateCallback(WatcherStateCallback callback) {
        state_callback_ = std::move(callback);
    }

    bool IsWatchHealthy() const { return watch_healthy_.load(); }

   private:
    void NotifyStateEvent(StandbyEvent event) const;
    bool ApplyPollResult(const ha::OpLogPollResult& result);
    void WatchLoop();

    std::shared_ptr<ha::OpLogStore> oplog_store_;
    OpLogApplier* applier_;
    std::atomic<bool> running_{false};
    std::thread watch_thread_;
    std::atomic<uint64_t> last_processed_sequence_id_{0};
    std::atomic<int> consecutive_errors_{0};
    std::atomic<bool> watch_healthy_{false};
    WatcherStateCallback state_callback_;

    static constexpr int kMaxConsecutiveErrors = 10;
    static constexpr auto kPollTimeout = std::chrono::milliseconds(1000);
    static constexpr int kSyncBatchSize = 1000;
};

}  // namespace mooncake
