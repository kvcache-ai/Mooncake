#include "kv_event/kv_event_publisher.h"

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS

#include <glog/logging.h>
#include <msgpack.hpp>
#include <zmq.h>

#include <chrono>
#include <cstring>
#include <endian.h>
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

void PackOptionalU32(msgpack::packer<msgpack::sbuffer>& packer, uint32_t value,
                     bool has_value) {
    if (!has_value) {
        packer.pack_nil();
    } else {
        packer.pack(value);
    }
}

size_t ComputeEventMapSize(bool is_stored, bool emit_legacy,
                           bool emit_object_key) {
    // Base envelope: event_id, timestamp, event_type, model_name, block_size,
    // additional_salt, lora_name, tenant_id, backend_id, medium, dp_rank,
    // seq_hashes.
    constexpr size_t kBaseFields = 12;
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
                                     const std::string& tenant_id) {
    if (!config_.enabled) {
        return;
    }
    Enqueue(PendingEvent{EventKind::kStored, object_key, medium, tenant_id});
}

void KvEventPublisher::PublishRemoved(const std::string& object_key,
                                      const std::string& medium,
                                      const std::string& tenant_id) {
    if (!config_.enabled) {
        return;
    }
    Enqueue(PendingEvent{EventKind::kRemoved, object_key, medium, tenant_id});
}

KvEventPublisher::Stats KvEventPublisher::GetStats() const {
    Stats stats;
    stats.published_batches = published_batches_.load();
    stats.published_events = published_events_.load();
    stats.dropped_events = dropped_events_.load();
    stats.skipped_unparsed_keys = skipped_unparsed_keys_.load();
    return stats;
}

void KvEventPublisher::Enqueue(PendingEvent event) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push_back(std::move(event));
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
        std::optional<uint64_t> seq_hash;
        uint64_t event_id{0};
    };
    std::vector<EncodedEvent> encoded;
    encoded.reserve(batch.size());
    for (const auto& pending : batch) {
        const auto seq_hash = ParseSeqHashFromObjectKey(pending.object_key);
        if (!seq_hash.has_value()) {
            if (!config_.emit_object_key || pending.object_key.empty()) {
                skipped_unparsed_keys_.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            skipped_unparsed_keys_.fetch_add(1, std::memory_order_relaxed);
        }
        encoded.push_back(EncodedEvent{
            pending, seq_hash,
            next_event_id_.fetch_add(1, std::memory_order_relaxed)});
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
        const char* rfc_type = is_stored ? "stored" : "removed";
        const char* legacy_type = is_stored ? "BlockStored" : "BlockRemoved";
        const std::string& tenant_id =
            item.pending.tenant_id.empty() ? "default" : item.pending.tenant_id;

        const size_t map_size =
            ComputeEventMapSize(is_stored, config_.emit_legacy_compat_fields,
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
        // Per-block envelope fields unknown to the storage pool are omitted
        // (nil). Indexer registration supplies model/block_size/dp_rank.
        packer.pack("model_name");
        packer.pack_nil();
        packer.pack("block_size");
        packer.pack_nil();
        packer.pack("additional_salt");
        packer.pack_nil();
        packer.pack("lora_name");
        packer.pack_nil();
        packer.pack("tenant_id");
        packer.pack(tenant_id);
        packer.pack("backend_id");
        packer.pack(config_.backend_id);
        packer.pack("medium");
        PackOptionalString(packer, item.pending.medium);
        packer.pack("dp_rank");
        packer.pack_nil();

        if (config_.emit_object_key) {
            packer.pack("object_key");
            packer.pack(item.pending.object_key);
        }

        packer.pack("seq_hashes");
        if (item.seq_hash.has_value()) {
            packer.pack_array(1);
            packer.pack(item.seq_hash.value());
        } else {
            packer.pack_array(0);
        }

        if (config_.emit_legacy_compat_fields && item.seq_hash.has_value()) {
            packer.pack("block_hashes");
            packer.pack_array(1);
            packer.pack(static_cast<int64_t>(item.seq_hash.value()));
        } else if (config_.emit_legacy_compat_fields) {
            packer.pack("block_hashes");
            packer.pack_array(0);
        }

        if (is_stored) {
            // Master keys are standalone pool blocks; depth 0 satisfies RFC
            // #1527 requirement that base_block_idx or parent_hash be present.
            packer.pack("base_block_idx");
            packer.pack(static_cast<uint32_t>(0));
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

        published_events_.fetch_add(1, std::memory_order_relaxed);
    }

    // Batch-level dp_rank; storage pool has no DP context (0).
    packer.pack(static_cast<uint32_t>(0));

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

    const int rc = zmq_sendmsg(zmq_socket_, &topic_msg, ZMQ_SNDMORE);
    if (rc >= 0) {
        if (zmq_sendmsg(zmq_socket_, &seq_msg, ZMQ_SNDMORE) >= 0) {
            if (zmq_sendmsg(zmq_socket_, &payload_msg, 0) < 0) {
                zmq_msg_close(&payload_msg);
            }
        } else {
            zmq_msg_close(&seq_msg);
            zmq_msg_close(&payload_msg);
        }
    } else {
        zmq_msg_close(&seq_msg);
        zmq_msg_close(&payload_msg);
    }
    zmq_msg_close(&topic_msg);

    published_batches_.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace mooncake

#endif  // MOONCAKE_ENABLE_KV_EVENTS
