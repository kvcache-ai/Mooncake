#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "kv_event/kv_event_config.h"
#include "kv_event/key_util.h"

namespace mooncake {

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS

// Publishes standardized KV cache events (RFC #1527) over ZMQ for indexers.
class KvEventPublisher {
   public:
    explicit KvEventPublisher(KvEventConfig config);
    ~KvEventPublisher();

    KvEventPublisher(const KvEventPublisher&) = delete;
    KvEventPublisher& operator=(const KvEventPublisher&) = delete;

    bool enabled() const { return config_.enabled; }

    // Non-blocking enqueue; never drops events (unbounded queue).
    // tenant_id empty uses config_.tenant_id (RFC envelope default).
    void PublishStored(const std::string& object_key, const std::string& medium,
                       const std::string& tenant_id = "");
    void PublishRemoved(const std::string& object_key,
                        const std::string& medium,
                        const std::string& tenant_id = "");

    struct Stats {
        uint64_t published_batches{0};
        uint64_t published_events{0};
        uint64_t dropped_events{0};
        uint64_t skipped_unparsed_keys{0};
    };
    Stats GetStats() const;

    static std::optional<uint64_t> ParseSeqHashFromObjectKey(
        const std::string& object_key) {
        return mooncake::ParseSeqHashFromObjectKey(object_key);
    }

   private:
    enum class EventKind { kStored, kRemoved };

    struct PendingEvent {
        EventKind kind;
        std::string object_key;
        std::string medium;
        std::string tenant_id;
    };

    void Enqueue(PendingEvent event);
    void WorkerLoop();
    void PublishBatch(const std::vector<PendingEvent>& batch);
    void DrainRemainingQueue(std::vector<PendingEvent>& batch);

    KvEventConfig config_;
    void* zmq_context_{nullptr};
    void* zmq_socket_{nullptr};

    mutable std::mutex queue_mutex_;
    std::deque<PendingEvent> queue_;
    std::condition_variable queue_cv_;
    std::thread worker_;
    std::atomic<bool> stop_{false};

    std::atomic<uint64_t> next_event_id_{1};
    std::atomic<uint64_t> next_zmq_sequence_{1};

    std::atomic<uint64_t> published_batches_{0};
    std::atomic<uint64_t> published_events_{0};
    std::atomic<uint64_t> dropped_events_{0};
    std::atomic<uint64_t> skipped_unparsed_keys_{0};
};

#else

// Stub when mooncake_store is built without libzmq (ENABLE_KV_EVENTS=OFF).
class KvEventPublisher {
   public:
    explicit KvEventPublisher(KvEventConfig config)
        : config_(std::move(config)) {}

    bool enabled() const { return false; }

    void PublishStored(const std::string&, const std::string&,
                       const std::string& = "") {}
    void PublishRemoved(const std::string&, const std::string&,
                        const std::string& = "") {}

    struct Stats {
        uint64_t published_batches{0};
        uint64_t published_events{0};
        uint64_t dropped_events{0};
        uint64_t skipped_unparsed_keys{0};
    };
    Stats GetStats() const { return {}; }

    static std::optional<uint64_t> ParseSeqHashFromObjectKey(
        const std::string& object_key) {
        return mooncake::ParseSeqHashFromObjectKey(object_key);
    }

   private:
    KvEventConfig config_;
};

#endif

}  // namespace mooncake
