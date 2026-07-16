#pragma once

#include <zmq.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>

#include "conductor/zmq/event_type.h"

namespace conductor {
namespace zmq {

class ZMQClientTestPeer;

// EventHandler processes received KV events.
class EventHandler {
   public:
    virtual ~EventHandler() = default;
    // Returns empty string on success, error message otherwise.
    virtual std::string HandleBatch(const DecodedBatch& batch,
                                    const MessageMetadata& metadata) = 0;
};

struct ZMQClientConfig {
    std::string cache_pool_key;
    std::string endpoint;
    std::string replay_endpoint;
    std::string model_name;
    common::PublisherKind publisher_kind = common::PublisherKind::kVllm;
    std::chrono::milliseconds poll_timeout{100};
    std::chrono::milliseconds replay_timeout{5000};
    std::chrono::milliseconds reconnect_delay{1000};
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
    std::string RequestReplay(int64_t from_seq);

    ZMQClientConfig config_;
    std::shared_ptr<EventHandler> event_handler_;

    ::zmq::context_t zmq_context_{1};
    std::unique_ptr<::zmq::socket_t> sub_socket_;
    std::unique_ptr<::zmq::socket_t> replay_socket_;

    // State management.
    mutable std::shared_mutex mu_;
    bool connected_ = false;
    int64_t last_seq_ = -1;
    std::chrono::milliseconds reconnect_delay_;

    // Lifecycle.
    std::atomic<bool> stop_requested_{false};
    std::thread loop_thread_;
    std::mutex stop_mu_;  // serialises concurrent Stop() calls
};

}  // namespace zmq
}  // namespace conductor
