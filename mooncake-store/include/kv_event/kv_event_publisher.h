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
#include <unordered_map>
#include <unordered_set>
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

    // Non-blocking enqueue into a bounded queue; drops oldest when full.
    // tenant_id empty defaults to "default" on the wire.
    void PublishStored(const std::string& object_key, const std::string& medium,
                       const std::string& tenant_id = "",
                       const std::string& group_id = "");
    void PublishRemoved(const std::string& object_key,
                        const std::string& medium,
                        const std::string& tenant_id = "",
                        const std::string& group_id = "");
    void PublishCleared(const std::string& tenant_id = "");

    // Publish a successful Put/Upsert commit. Every currently available
    // medium receives a stored event, while media absent from the new state
    // receive removed events.
    void PublishCommitted(const std::string& object_key,
                          const std::vector<std::string>& current_media,
                          const std::string& tenant_id = "",
                          const std::string& group_id = "");

    // Synchronize replica availability after an internal metadata mutation.
    // previous_media_hint is used when the publisher has no in-process state,
    // for example after restoring metadata from a snapshot.
    void SyncObjectState(
        const std::string& object_key,
        const std::vector<std::string>& current_media,
        const std::string& tenant_id = "", const std::string& group_id = "",
        const std::vector<std::string>& previous_media_hint = {});

    // Publish removal of every available medium and release cached context.
    void PublishObjectRemoved(
        const std::string& object_key, const std::string& tenant_id = "",
        const std::string& group_id = "",
        const std::vector<std::string>& previous_media_hint = {});

    struct Stats {
        uint64_t published_batches{0};
        uint64_t published_events{0};
        uint64_t dropped_events{0};
        uint64_t skipped_unparsed_keys{0};
        uint64_t invalid_event_hashes{0};
    };
    Stats GetStats() const;

    static std::optional<uint64_t> ParseSeqHashFromObjectKey(
        const std::string& object_key) {
        return mooncake::ParseSeqHashFromObjectKey(object_key);
    }

   private:
    enum class EventKind { kStored, kRemoved, kCleared };

    struct EventContext {
        std::string cache_prefix;
        std::string model_name;
        std::string connector_block_hash;
        std::optional<uint64_t> seq_hash;
        std::optional<int64_t> group_id;
        std::optional<int64_t> tp_rank;
        std::optional<int64_t> head_or_tp_rank;
        std::optional<int64_t> pcp_rank;
        std::optional<int64_t> dcp_rank;
        std::optional<int64_t> pp_rank;
        std::optional<int64_t> layer_id;
        bool has_explicit_block_hash{false};
    };

    struct PendingEvent {
        EventKind kind;
        std::string object_key;
        std::string medium;
        std::string tenant_id;
        std::string group_id;
        EventContext context;
    };

    struct ObjectEventState {
        EventContext context;
        std::unordered_set<std::string> media;
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
    std::atomic<uint64_t> skipped_unparsed_keys_{0};
    std::atomic<uint64_t> invalid_event_hashes_{0};

    mutable std::mutex state_mutex_;
    std::unordered_map<std::string,
                       std::unordered_map<std::string, ObjectEventState>>
        object_states_;

    EventContext BuildEventContext(const std::string& object_key);
    static std::string ResolveGroupId(const std::string& group_id,
                                      const EventContext& context);
    void RecordInvalidHash(const char* field, const std::string& value);
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
    void PublishRemoved(const std::string&, const std::string&,
                        const std::string& = "", const std::string& = "") {}
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
        uint64_t skipped_unparsed_keys{0};
        uint64_t invalid_event_hashes{0};
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
