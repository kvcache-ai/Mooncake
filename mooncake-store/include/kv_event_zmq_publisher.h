#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "kv_event.h"

namespace mooncake {

/**
 * @brief Configuration for ZmqKvEventPublisher.
 */
struct ZmqKvEventPublisherConfig {
    // ZMQ endpoint to publish on, e.g. "tcp://0.0.0.0:5557". Use a "*" port
    // (e.g. "tcp://127.0.0.1:*") to let ZMQ choose a free port; the chosen
    // endpoint can be read back via ZmqKvEventPublisher::endpoint().
    std::string endpoint{"tcp://0.0.0.0:5557"};
    // ZMQ topic prefix sent as frame 1. Dynamo's listener matches this against
    // its zmq_topic subscription filter; default is empty (matches all).
    std::string topic{};
    // When true (default) the PUB socket binds the endpoint (Mooncake master is
    // the stable server). When false it connects instead (for relays/proxies).
    bool bind{true};
    // Linger in milliseconds applied on close so a clean shutdown can flush
    // buffered events. -1 keeps the ZMQ default (block forever); 0 drops.
    int linger_ms{0};
    // Send high-water-mark (max queued outbound messages); 0 means unlimited.
    int send_hwm{0};
};

/**
 * @brief KvEventPublisher backed by a single ZMQ PUB socket.
 *
 * Emits the SGLang/vLLM-compatible 3-frame message Dynamo's ZMQ relay expects:
 *   frame 1: topic
 *   frame 2: 8-byte big-endian monotonic sequence number
 *   frame 3: msgpack payload [timestamp, [event], dp_rank]
 *
 * Because Mooncake's master is the single centralized owner of KV state, all
 * workers' events flow through this one PUB stream; the physical owner of each
 * block is carried in the per-event worker_id field (see kv_event.h). Each
 * Publish() call sends a single-event batch.
 *
 * Thread-safety: Publish() is serialized with an internal mutex, so the
 * publisher may be shared across master threads.
 */
class ZmqKvEventPublisher : public KvEventPublisher {
   public:
    explicit ZmqKvEventPublisher(const ZmqKvEventPublisherConfig& config);
    ~ZmqKvEventPublisher() override;

    ZmqKvEventPublisher(const ZmqKvEventPublisher&) = delete;
    ZmqKvEventPublisher& operator=(const ZmqKvEventPublisher&) = delete;

    void Publish(const KvEvent& event) override;

    // The actual bound/connected endpoint. When the config used a wildcard
    // port (e.g. "tcp://127.0.0.1:*"), this returns the concrete endpoint ZMQ
    // selected (e.g. "tcp://127.0.0.1:54321").
    const std::string& endpoint() const;

    // Number of messages successfully handed to ZMQ so far (for diagnostics).
    uint64_t published_count() const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
