#include "kv_event/kv_event_publisher.h"

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS

#include <glog/logging.h>
#include <msgpack.hpp>
#include <zmq.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <endian.h>
#include <unordered_set>
#include <vector>

namespace mooncake {
namespace {

constexpr int kZmqSendHwm = 10000;
constexpr size_t kMaxBatchSize = 64;

int64_t CurrentUnixTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void PackOptionalString(msgpack::packer<msgpack::sbuffer>& packer,
                        const std::string& value) {
    if (value.empty()) {
        packer.pack_nil();
    } else {
        packer.pack(value);
    }
}

size_t ComputeEventMapSize(bool is_stored, bool is_cleared, bool emit_legacy,
                           bool emit_object_key) {
    if (is_cleared) {
        // Envelope only: event_id, timestamp, event_type, model_name,
        // block_size, additional_salt, lora_name, tenant_id, backend_id,
        // medium, dp_rank.
        return 11 + (emit_legacy ? 1 : 0);
    }
    // Base envelope: event_id, timestamp, event_type, model_name, block_size,
    // additional_salt, lora_name, tenant_id, backend_id, medium, dp_rank,
    // seq_hashes, group_id.
    constexpr size_t kBaseFields = 13;
    size_t map_size = kBaseFields;
    if (emit_legacy) {
        map_size += 2;  // type, block_hashes
    }
    if (emit_object_key) {
        map_size += 1;  // object_key
    }
    if (is_stored) {
        map_size += 3;  // base_block_idx, parent_hash, token_ids
        if (emit_legacy) {
            map_size += 1;  // parent_block_hash
        }
    } else {
        map_size += 1;  // base_block_idx
    }
    return map_size;
}

std::unordered_set<std::string> NormalizeMedia(
    const std::vector<std::string>& media) {
    std::unordered_set<std::string> result;
    for (const auto& value : media) {
        if (!value.empty()) {
            result.insert(value);
        }
    }
    return result;
}

std::vector<std::string> SortedMedia(
    const std::unordered_set<std::string>& media) {
    std::vector<std::string> result(media.begin(), media.end());
    std::sort(result.begin(), result.end());
    return result;
}

// Events always carry a concrete tenant so subscribers can key on it without
// having to model an unset value.
std::string NormalizeTenant(const std::string& tenant_id) {
    return tenant_id.empty() ? "default" : tenant_id;
}

}  // namespace

KvEventPublisher::KvEventPublisher(KvEventConfig config)
    : config_(std::move(config)) {
    if (!config_.enabled) {
        return;
    }
    if (config_.bind_endpoint.empty()) {
        LOG(ERROR) << "kv_events enabled but bind_endpoint is empty";
        config_.enabled = false;
        return;
    }
    if (config_.backend_id.empty()) {
        LOG(ERROR) << "kv_events enabled but backend_id is empty";
        config_.enabled = false;
        return;
    }
    if (!config_.emit_object_key) {
        // object_key is the only key-identifying field on the wire, so without
        // it a stored/removed event carries nothing a subscriber can act on and
        // is dropped. Say so at startup: the flag name reads like a formatting
        // switch, but it silences the whole per-object stream.
        LOG(WARNING) << "kv_events: emit_object_key=false suppresses all stored "
                        "and removed events; only cleared will be published. "
                        "Suppressed events are counted as skipped_keyless_events";
    }

    zmq_context_ = zmq_ctx_new();
    if (!zmq_context_) {
        LOG(ERROR) << "kv_events: failed to create ZMQ context";
        config_.enabled = false;
        return;
    }
    zmq_socket_ = zmq_socket(zmq_context_, ZMQ_PUB);
    if (!zmq_socket_) {
        LOG(ERROR) << "kv_events: failed to create ZMQ PUB socket: "
                   << zmq_strerror(zmq_errno());
        zmq_ctx_destroy(zmq_context_);
        zmq_context_ = nullptr;
        config_.enabled = false;
        return;
    }
    int hwm = kZmqSendHwm;
    zmq_setsockopt(zmq_socket_, ZMQ_SNDHWM, &hwm, sizeof(hwm));
    int linger_ms = 0;
    zmq_setsockopt(zmq_socket_, ZMQ_LINGER, &linger_ms, sizeof(linger_ms));

    if (zmq_bind(zmq_socket_, config_.bind_endpoint.c_str()) != 0) {
        LOG(ERROR) << "kv_events: zmq_bind failed for " << config_.bind_endpoint
                   << ": " << zmq_strerror(zmq_errno());
        zmq_close(zmq_socket_);
        zmq_ctx_destroy(zmq_context_);
        zmq_socket_ = nullptr;
        zmq_context_ = nullptr;
        config_.enabled = false;
        return;
    }

    worker_ = std::thread(&KvEventPublisher::WorkerLoop, this);
    LOG(INFO) << "kv_events publisher enabled on " << config_.bind_endpoint
              << " backend_id=" << config_.backend_id;
}

KvEventPublisher::~KvEventPublisher() {
    if (!config_.enabled) {
        return;
    }
    stop_.store(true);
    queue_cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
    if (zmq_socket_) {
        zmq_close(zmq_socket_);
        zmq_socket_ = nullptr;
    }
    if (zmq_context_) {
        zmq_ctx_destroy(zmq_context_);
        zmq_context_ = nullptr;
    }
}

void KvEventPublisher::PublishStored(const std::string& object_key,
                                     const std::string& medium,
                                     const std::string& tenant_id,
                                     const std::string& group_id) {
    if (!config_.enabled) {
        return;
    }
    // medium is the only availability field on a stored event, so an empty one
    // would announce a replica the subscriber cannot place on any tier.
    if (medium.empty()) {
        return;
    }
    std::vector<PendingEvent> events;
    events.push_back(PendingEvent{EventKind::kStored, object_key, medium,
                                  NormalizeTenant(tenant_id), group_id});
    EnqueueBatch(std::move(events));
}

void KvEventPublisher::PublishRemoved(const std::string& object_key,
                                      const std::string& medium,
                                      const std::string& tenant_id,
                                      const std::string& group_id) {
    if (!config_.enabled) {
        return;
    }
    // A retraction has to name the tier it retracts; medium=nil would ask the
    // subscriber to drop availability it cannot identify.
    if (medium.empty()) {
        return;
    }
    std::vector<PendingEvent> events;
    events.push_back(PendingEvent{EventKind::kRemoved, object_key, medium,
                                  NormalizeTenant(tenant_id), group_id});
    EnqueueBatch(std::move(events));
}

void KvEventPublisher::PublishCleared(const std::string& tenant_id) {
    if (!config_.enabled) {
        return;
    }
    std::vector<PendingEvent> events;
    events.push_back(PendingEvent{EventKind::kCleared, "", "",
                                  NormalizeTenant(tenant_id), ""});
    EnqueueBatch(std::move(events));
}

void KvEventPublisher::PublishCommitted(
    const std::string& object_key,
    const std::vector<std::string>& current_media, const std::string& tenant_id,
    const std::string& group_id) {
    if (!config_.enabled) {
        return;
    }
    const std::string normalized_tenant = NormalizeTenant(tenant_id);
    std::vector<PendingEvent> events;
    // A commit announces every medium unconditionally. Unlike SyncObjectState
    // this is not a delta: the medium set can be unchanged while the object
    // contents are new, and a subscriber that saw no event would keep serving
    // the previous contents.
    for (const auto& medium : SortedMedia(NormalizeMedia(current_media))) {
        events.push_back(PendingEvent{EventKind::kStored, object_key, medium,
                                      normalized_tenant, group_id});
    }
    EnqueueBatch(std::move(events));
}

void KvEventPublisher::PublishObjectRemoved(
    const std::string& object_key, const std::string& tenant_id,
    const std::string& group_id,
    const std::vector<std::string>& previous_media) {
    if (!config_.enabled) {
        return;
    }
    const std::string normalized_tenant = NormalizeTenant(tenant_id);
    std::vector<PendingEvent> events;
    for (const auto& medium : SortedMedia(NormalizeMedia(previous_media))) {
        events.push_back(PendingEvent{EventKind::kRemoved, object_key, medium,
                                      normalized_tenant, group_id});
    }
    EnqueueBatch(std::move(events));
}

void KvEventPublisher::SyncObjectState(
    const std::string& object_key,
    const std::vector<std::string>& current_media, const std::string& tenant_id,
    const std::string& group_id,
    const std::vector<std::string>& previous_media) {
    if (!config_.enabled) {
        return;
    }
    const std::string normalized_tenant = NormalizeTenant(tenant_id);
    const auto new_set = NormalizeMedia(current_media);
    const auto previous_set = NormalizeMedia(previous_media);

    std::vector<PendingEvent> events;
    for (const auto& medium : SortedMedia(previous_set)) {
        if (!new_set.contains(medium)) {
            events.push_back(PendingEvent{EventKind::kRemoved, object_key,
                                          medium, normalized_tenant, group_id});
        }
    }
    for (const auto& medium : SortedMedia(new_set)) {
        if (!previous_set.contains(medium)) {
            events.push_back(PendingEvent{EventKind::kStored, object_key, medium,
                                          normalized_tenant, group_id});
        }
    }
    EnqueueBatch(std::move(events));
}

KvEventPublisher::Stats KvEventPublisher::GetStats() const {
    Stats stats;
    stats.published_batches = published_batches_.load();
    stats.published_events = published_events_.load();
    stats.dropped_events = dropped_events_.load();
    stats.skipped_keyless_events = skipped_keyless_events_.load();
    return stats;
}

void KvEventPublisher::EnqueueBatch(std::vector<PendingEvent> events) {
    if (events.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& event : events) {
            if (config_.queue_capacity > 0 &&
                queue_.size() >= config_.queue_capacity) {
                queue_.pop_front();
                dropped_events_.fetch_add(1, std::memory_order_relaxed);
                // Reserve a ZMQ sequence gap so consumers can detect loss.
                next_zmq_sequence_.fetch_add(1, std::memory_order_relaxed);
            }
            queue_.push_back(std::move(event));
        }
    }
    queue_cv_.notify_one();
}

void KvEventPublisher::DrainRemainingQueue(std::vector<PendingEvent>& batch) {
    while (true) {
        batch.clear();
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (queue_.empty()) {
                break;
            }
            while (!queue_.empty() && batch.size() < kMaxBatchSize) {
                batch.push_back(std::move(queue_.front()));
                queue_.pop_front();
            }
        }
        PublishBatch(batch);
    }
}

void KvEventPublisher::WorkerLoop() {
    std::vector<PendingEvent> batch;
    batch.reserve(kMaxBatchSize);
    while (!stop_.load()) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock,
                           [this] { return stop_.load() || !queue_.empty(); });
            while (!queue_.empty() && batch.size() < kMaxBatchSize) {
                batch.push_back(std::move(queue_.front()));
                queue_.pop_front();
            }
        }
        if (!batch.empty()) {
            PublishBatch(batch);
            batch.clear();
        }
    }
    DrainRemainingQueue(batch);
}

void KvEventPublisher::PublishBatch(const std::vector<PendingEvent>& batch) {
    struct EncodedEvent {
        PendingEvent pending;
        uint64_t event_id{0};
    };
    std::vector<EncodedEvent> encoded;
    encoded.reserve(batch.size());
    for (const auto& pending : batch) {
        if (pending.kind != EventKind::kCleared &&
            (!config_.emit_object_key || pending.object_key.empty())) {
            skipped_keyless_events_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        encoded.push_back(EncodedEvent{
            pending, next_event_id_.fetch_add(1, std::memory_order_relaxed)});
    }
    if (encoded.empty()) {
        return;
    }

    msgpack::sbuffer payload_buffer;
    msgpack::packer<msgpack::sbuffer> packer(&payload_buffer);

    const int64_t timestamp_ms = CurrentUnixTimeMs();

    packer.pack_array(3);
    packer.pack(timestamp_ms);

    packer.pack_array(encoded.size());
    for (const auto& item : encoded) {
        const bool is_stored = item.pending.kind == EventKind::kStored;
        const bool is_cleared = item.pending.kind == EventKind::kCleared;
        const char* rfc_type =
            is_stored ? "stored" : (is_cleared ? "cleared" : "removed");
        const char* legacy_type =
            is_stored ? "BlockStored"
                      : (is_cleared ? "AllBlocksCleared" : "BlockRemoved");
        // Every enqueue path normalizes an empty tenant to "default" before
        // building the PendingEvent, so no re-check is needed here.
        const std::string& tenant_id = item.pending.tenant_id;

        const size_t map_size = ComputeEventMapSize(
            is_stored, is_cleared, config_.emit_legacy_compat_fields,
            config_.emit_object_key);

        packer.pack_map(map_size);
        packer.pack("event_id");
        packer.pack(item.event_id);
        packer.pack("timestamp");
        packer.pack(timestamp_ms);
        packer.pack("event_type");
        packer.pack(rfc_type);
        if (config_.emit_legacy_compat_fields) {
            packer.pack("type");
            packer.pack(legacy_type);
        }
        packer.pack("model_name");
        PackOptionalString(packer, config_.model_name);
        packer.pack("block_size");
        if (config_.block_size == 0) {
            packer.pack_nil();
        } else {
            packer.pack(config_.block_size);
        }
        packer.pack("additional_salt");
        PackOptionalString(packer, config_.additional_salt);
        packer.pack("lora_name");
        PackOptionalString(packer, config_.lora_name);
        packer.pack("tenant_id");
        packer.pack(tenant_id);
        packer.pack("backend_id");
        packer.pack(config_.backend_id);
        packer.pack("medium");
        PackOptionalString(packer, item.pending.medium);
        packer.pack("dp_rank");
        packer.pack(config_.dp_rank);

        if (is_cleared) {
            continue;
        }

        packer.pack("group_id");
        PackOptionalString(packer, item.pending.group_id);

        if (config_.emit_object_key) {
            packer.pack("object_key");
            packer.pack(item.pending.object_key);
        }

        packer.pack("seq_hashes");
        packer.pack_array(0);

        if (config_.emit_legacy_compat_fields) {
            packer.pack("block_hashes");
            packer.pack_array(0);
        }

        if (is_stored) {
            packer.pack("base_block_idx");
            packer.pack_nil();
            packer.pack("parent_hash");
            packer.pack_nil();
            packer.pack("token_ids");
            packer.pack_nil();
            if (config_.emit_legacy_compat_fields) {
                packer.pack("parent_block_hash");
                packer.pack_nil();
            }
        } else {
            packer.pack("base_block_idx");
            packer.pack_nil();
        }
    }

    packer.pack(config_.dp_rank);

    const uint64_t seq = next_zmq_sequence_.fetch_add(1);
    const uint64_t seq_be = htobe64(seq);

    zmq_msg_t topic_msg;
    zmq_msg_t seq_msg;
    zmq_msg_t payload_msg;
    zmq_msg_init_size(&topic_msg, 0);
    zmq_msg_init_size(&seq_msg, sizeof(seq_be));
    std::memcpy(zmq_msg_data(&seq_msg), &seq_be, sizeof(seq_be));
    zmq_msg_init_size(&payload_msg, payload_buffer.size());
    std::memcpy(zmq_msg_data(&payload_msg), payload_buffer.data(),
                payload_buffer.size());

    const int rc_topic = zmq_sendmsg(zmq_socket_, &topic_msg, ZMQ_SNDMORE);
    const int rc_seq =
        (rc_topic >= 0) ? zmq_sendmsg(zmq_socket_, &seq_msg, ZMQ_SNDMORE) : -1;
    const int rc_payload =
        (rc_seq >= 0) ? zmq_sendmsg(zmq_socket_, &payload_msg, 0) : -1;

    zmq_msg_close(&topic_msg);
    zmq_msg_close(&seq_msg);
    zmq_msg_close(&payload_msg);

    if (rc_topic >= 0 && rc_seq >= 0 && rc_payload >= 0) {
        published_batches_.fetch_add(1, std::memory_order_relaxed);
        published_events_.fetch_add(encoded.size(), std::memory_order_relaxed);
    } else {
        dropped_events_.fetch_add(encoded.size(), std::memory_order_relaxed);
    }
}

}  // namespace mooncake

#endif  // MOONCAKE_ENABLE_KV_EVENTS
