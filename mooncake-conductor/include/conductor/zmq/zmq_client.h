#pragma once

#include <zmq.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "conductor/zmq/event_type.h"

namespace mooncake::conductor::zmq {

class ZMQClientTestPeer;

// EventHandler processes received KV events.
class EventHandler {
   public:
    virtual ~EventHandler() = default;
    // Returns empty string on success, error message otherwise.
    virtual std::string HandleBatch(const DecodedBatch& batch,
                                    const MessageMetadata& metadata) = 0;

    // Called once when this source can no longer be recovered without risking
    // out-of-order mutations. Implementations that maintain an index should
    // stop serving or invalidate entries attributed to this source until a
    // full resynchronization has completed.
    virtual void OnSourceStale(const std::string& cache_pool_key,
                               const MessageMetadata& metadata,
                               const std::string& reason) = 0;
};

struct ZMQClientConfig {
    std::string cache_pool_key;
    std::string endpoint;
    std::string replay_endpoint;
    common::PublisherKind publisher_kind = common::PublisherKind::kVllm;
    std::chrono::milliseconds poll_timeout{100};
    std::chrono::milliseconds replay_timeout{5000};
    // Total time allowed to close a sequence gap, including retries.
    std::chrono::milliseconds replay_recovery_timeout{30000};
    std::chrono::milliseconds reconnect_delay{1000};
    // Application-level recovery buffer limits. These bound live messages held
    // behind a gap as well as replay records waiting to be merged with them.
    size_t max_recovery_buffered_messages = 10000;
    size_t max_recovery_buffered_bytes = 64 * 1024 * 1024;
    // SUB receive high-water mark. Zero leaves the ZeroMQ default unchanged.
    int rcv_hwm = 200000;
};

// Returns empty string when valid, error message otherwise.
std::string ValidateConfig(const ZMQClientConfig& config);

class ZMQClient {
   public:
    ZMQClient(ZMQClientConfig config, std::shared_ptr<EventHandler> handler);
    ~ZMQClient();
    ZMQClient(const ZMQClient&) = delete;
    ZMQClient& operator=(const ZMQClient&) = delete;

    // Establishes the SUB and DEALER sockets. Returns empty string on
    // success. Safe to call when already connected (no-op).
    std::string Connect();

    // Connects and starts the background event loop thread. Returns
    // empty string on success.
    std::string Start();

    // Stops the event loop (stop flag + join) and closes all sockets.
    // Idempotent — stop, wait for the loop to join, then clean up; safe
    // to invoke repeatedly.
    void Stop();

    int64_t GetLastSequence() const;
    bool IsStale() const;
    std::string GetStaleReason() const;

    // Cumulative counts inferred from transport sequence gaps.
    int64_t GetDroppedEvents() const { return dropped_events_.load(); }
    int64_t GetGapCount() const { return gap_count_.load(); }

   private:
    friend class ZMQClientTestPeer;

    void Loop();
    void HandleReconnect();
    bool IsConnected() const;
    void MarkDisconnected();
    // The following require holding mu_ (exclusive):
    void CleanupSocketsLocked();

    std::string Consume();
    std::string ProcessMessage();

    struct BufferedMessage {
        std::string topic;
        int64_t sequence;
        std::string payload;
    };

    enum class ReplayFailure {
        kNone,
        kRetryable,
        kUnrecoverable,
    };

    struct ReplayResult {
        std::vector<BufferedMessage> messages;
        std::string error;
        ReplayFailure failure = ReplayFailure::kNone;

        bool ok() const { return failure == ReplayFailure::kNone; }
    };

    ReplayResult RequestReplay(
        int64_t from_seq, std::optional<int64_t> until_seq,
        std::chrono::steady_clock::time_point recovery_deadline);
    std::string ResetReplaySocket();
    std::string DispatchMessage(const std::string& topic, int64_t sequence,
                                const char* payload, size_t payload_size);
    void UpdateLastSequence(int64_t sequence);
    bool BufferMessage(BufferedMessage message, std::string* error);
    std::string DrainBufferedMessages(bool allow_initial_baseline = false);
    void StartRecovery(const std::string& reason);
    std::string AttemptRecovery();
    void MarkStale(const std::string& reason);

    ZMQClientConfig config_;
    std::shared_ptr<EventHandler> event_handler_;

    ::zmq::context_t zmq_context_{1};
    std::unique_ptr<::zmq::socket_t> sub_socket_;
    std::unique_ptr<::zmq::socket_t> replay_socket_;

    // State management.
    mutable std::shared_mutex mu_;
    bool connected_ = false;
    // Highest sequence applied contiguously. It must never advance across a
    // gap; buffered_messages_ holds live/replay records beyond such a gap.
    int64_t last_seq_ = -1;
    int64_t last_live_seq_ = -1;
    std::map<int64_t, BufferedMessage> buffered_messages_;
    size_t buffered_message_bytes_ = 0;
    bool recovery_in_progress_ = false;
    std::chrono::steady_clock::time_point recovery_deadline_{};
    bool stale_ = false;
    std::string stale_reason_;
    // Counters updated when a transport sequence skips one or more events.
    std::atomic<int64_t> dropped_events_{0};
    std::atomic<int64_t> gap_count_{0};

    // Lifecycle.
    std::atomic<bool> stop_requested_{false};
    std::thread loop_thread_;
    std::mutex stop_mu_;  // serialises concurrent Stop() calls
};

}  // namespace mooncake::conductor::zmq
