#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

namespace mooncake {

// BandwidthTracker tracks read/write bandwidth for client operations.
// Enabled via MC_ENABLE_BANDWIDTH_METRICS=1 environment variable.
// Logging interval is configurable via MC_BANDWIDTH_LOG_INTERVAL_S (default 10
// seconds).
class BandwidthTracker {
   public:
    BandwidthTracker() = default;
    ~BandwidthTracker();

    // Start the bandwidth logging thread using the given client name.
    // No-op if already started. Reads MC_ENABLE_BANDWIDTH_METRICS to decide
    // whether to actually activate; returns immediately if disabled.
    void start(const std::string& name);

    // Stop the logging thread. No-op if not running.
    void stop();

    // Record bytes transferred in a write (put/upsert) operation.
    // No-op when the tracker is not active.
    void record_write(size_t bytes);

    // Record bytes transferred in a read (get) operation.
    // No-op when the tracker is not active.
    void record_read(size_t bytes);

   private:
    void thread_func();

    std::atomic<uint64_t> total_write_bytes_{0};
    std::atomic<uint64_t> total_read_bytes_{0};
    std::atomic<uint64_t> write_ops_{0};
    std::atomic<uint64_t> read_ops_{0};

    std::string name_;
    int interval_s_{10};

    std::thread thread_;
    std::atomic<bool> running_{false};
};

}  // namespace mooncake
