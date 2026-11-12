#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "kv_event/kv_event_config.h"

namespace mooncake {

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS

// Publishes standardized KV cache events (RFC #1527) over ZMQ for indexers.
class KvEventPublisher {
   public:
    static constexpr uint8_t kMediumCpu = 1u << 0;
    static constexpr uint8_t kMediumDisk = 1u << 1;

    explicit KvEventPublisher(KvEventConfig config);
    ~KvEventPublisher();

    KvEventPublisher(const KvEventPublisher&) = delete;
    KvEventPublisher& operator=(const KvEventPublisher&) = delete;

    bool enabled() const { return config_.enabled; }

    // Enqueues removed tiers ("cpu", "disk") followed by stored tiers directly
    // from route-tracked bitmasks, into a bounded queue that drops the oldest
    // entry when full.
    void PublishTransition(std::string_view object_key,
                           std::string_view tenant_id,
                           std::string_view group_id,
                           std::string_view session_id, uint8_t removed_mask,
                           uint8_t stored_mask);

    void PublishCleared(std::string_view tenant_id = "");

    // Checks `epoch == expected_epoch` under the queue lock before enqueuing
    // `cleared`, ordering the check against concurrent `PublishTransition`
    // calls that bump `epoch` prior to taking the queue lock.
    bool PublishClearedIfUnchanged(std::string_view tenant_id,
                                   const std::atomic<uint64_t>& epoch,
                                   uint64_t expected_epoch);

    struct Stats {
        uint64_t published_batches{0};
        uint64_t published_events{0};
        uint64_t dropped_events{0};
        uint64_t skipped_keyless_events{0};
    };
    Stats GetStats() const;

   private:
    enum class EventKind { kStored, kRemoved, kCleared };

    struct PendingEvent {
        EventKind kind;
        std::string object_key;
        std::string medium;
        std::string tenant_id;
        std::string group_id;
        std::string session_id{};
    };

    void PushEventLocked(PendingEvent&& event);
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
    std::atomic<uint64_t> skipped_keyless_events_{0};
};

#else

// Stub when mooncake_store is built without libzmq (ENABLE_KV_EVENTS=OFF).
class KvEventPublisher {
   public:
    static constexpr uint8_t kMediumCpu = 1u << 0;
    static constexpr uint8_t kMediumDisk = 1u << 1;

    explicit KvEventPublisher(KvEventConfig config)
        : config_(std::move(config)) {}

    bool enabled() const { return false; }

    void PublishTransition(std::string_view, std::string_view, std::string_view,
                           std::string_view, uint8_t, uint8_t) {}
    void PublishCleared(std::string_view = "") {}
    bool PublishClearedIfUnchanged(std::string_view,
                                   const std::atomic<uint64_t>& epoch,
                                   uint64_t expected_epoch) {
        return epoch.load(std::memory_order_acquire) == expected_epoch;
    }

    struct Stats {
        uint64_t published_batches{0};
        uint64_t published_events{0};
        uint64_t dropped_events{0};
        uint64_t skipped_keyless_events{0};
    };
    Stats GetStats() const { return {}; }

   private:
    KvEventConfig config_;
};

#endif

}  // namespace mooncake
