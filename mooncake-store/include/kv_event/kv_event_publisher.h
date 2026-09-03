#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kv_event/kv_event_config.h"
#include "tenant/tenant_id.h"

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

    // Non-blocking enqueue into a bounded queue; drops oldest when full.
    // The string overloads keep the wire format independent of Store's
    // TenantId wrapper; the TenantId overloads preserve the existing API.
    void PublishStored(const std::string& object_key, const std::string& medium,
                       const std::string& tenant_id = "",
                       const std::string& group_id = "");
    void PublishStored(const std::string& object_key, const std::string& medium,
                       const TenantId& tenant_id,
                       const std::string& group_id = "") {
        PublishStored(object_key, medium, tenant_id.value(), group_id);
    }
    void PublishRemoved(const std::string& object_key,
                        const std::string& medium,
                        const std::string& tenant_id = "",
                        const std::string& group_id = "");
    void PublishRemoved(const std::string& object_key,
                        const std::string& medium, const TenantId& tenant_id,
                        const std::string& group_id = "") {
        PublishRemoved(object_key, medium, tenant_id.value(), group_id);
    }
    void PublishCleared(const std::string& tenant_id = "");

    // The publisher keeps no per-object state: every delta below is computed
    // from the arguments alone. Callers already snapshot the medium set before
    // mutating metadata, so they are the authoritative source for it.

    // Publish a successful Put/Upsert commit. Every currently available medium
    // receives a stored event. A commit is always announced, even when the
    // medium set is unchanged, because the object contents may have changed.
    void PublishCommitted(const std::string& object_key,
                          const std::vector<std::string>& current_media,
                          const std::string& tenant_id = "",
                          const std::string& group_id = "");

    // Synchronize replica availability after an internal metadata mutation.
    // previous_media is the medium set captured before the mutation; media that
    // disappeared get a removed event and media that appeared get a stored one.
    void SyncObjectState(const std::string& object_key,
                         const std::vector<std::string>& current_media,
                         const std::string& tenant_id = "",
                         const std::string& group_id = "",
                         const std::vector<std::string>& previous_media = {});

    // Publish removal of every medium the object had before deletion.
    void PublishObjectRemoved(
        const std::string& object_key, const std::string& tenant_id = "",
        const std::string& group_id = "",
        const std::vector<std::string>& previous_media = {});

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
    };

    void EnqueueBatch(std::vector<PendingEvent> events);
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
    explicit KvEventPublisher(KvEventConfig config)
        : config_(std::move(config)) {}

    bool enabled() const { return false; }

    void PublishStored(const std::string&, const std::string&,
                       const std::string& = "", const std::string& = "") {}
    void PublishStored(const std::string&, const std::string&, const TenantId&,
                       const std::string& = "") {}
    void PublishRemoved(const std::string&, const std::string&,
                        const std::string& = "", const std::string& = "") {}
    void PublishRemoved(const std::string&, const std::string&, const TenantId&,
                        const std::string& = "") {}
    void PublishCleared(const std::string& = "") {}
    void PublishCommitted(const std::string&, const std::vector<std::string>&,
                          const std::string& = "", const std::string& = "") {}
    void SyncObjectState(const std::string&, const std::vector<std::string>&,
                         const std::string& = "", const std::string& = "",
                         const std::vector<std::string>& = {}) {}
    void PublishObjectRemoved(const std::string&, const std::string& = "",
                              const std::string& = "",
                              const std::vector<std::string>& = {}) {}

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
