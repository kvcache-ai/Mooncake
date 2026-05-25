#include "kv_event/kv_event_publisher.h"

#include <glog/logging.h>
#include <msgpack.hpp>
#include <zmq.h>

#include <chrono>
#include <cstring>
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

void PackOptionalU32(msgpack::packer<msgpack::sbuffer>& packer,
                     uint32_t value, bool has_value) {
    if (!has_value) {
        packer.pack_nil();
    } else {
        packer.pack(value);
    }
}

}  // namespace

std::optional<uint64_t> KvEventPublisher::ParseSeqHashFromObjectKey(
    const std::string& object_key) {
    if (object_key.empty()) {
        return std::nullopt;
    }
    try {
        size_t idx = 0;
        if (object_key.size() >= 2 &&
            (object_key[0] == '0' &&
             (object_key[1] == 'x' || object_key[1] == 'X'))) {
            uint64_t value = std::stoull(object_key, &idx, 16);
            if (idx == object_key.size()) {
                return value;
            }
            return std::nullopt;
        }
        uint64_t value = std::stoull(object_key, &idx, 10);
        if (idx == object_key.size()) {
            return value;
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }
    return std::nullopt;
}

KvEventPublisher::KvEventPublisher(KvEventConfig config) : config_(std::move(config)) {
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
    if (config_.block_size == 0) {
        LOG(WARNING) << "kv_events block_size is 0; block_size field will be "
                        "omitted in events";
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
              << " backend_id=" << config_.backend_id
              << " model_name=" << config_.model_name
              << " tenant_id=" << config_.tenant_id;
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
                                     const std::string& medium) {
    if (!config_.enabled) {
        return;
    }
    Enqueue(PendingEvent{EventKind::kStored, object_key, medium});
}

void KvEventPublisher::PublishRemoved(const std::string& object_key,
                                      const std::string& medium) {
    if (!config_.enabled) {
        return;
    }
    Enqueue(PendingEvent{EventKind::kRemoved, object_key, medium});
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
        if (queue_.size() >= config_.queue_capacity) {
            dropped_events_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        queue_.push_back(std::move(event));
    }
    queue_cv_.notify_one();
}

void KvEventPublisher::WorkerLoop() {
    std::vector<PendingEvent> batch;
    batch.reserve(kMaxBatchSize);
    while (!stop_.load()) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait_for(lock, std::chrono::milliseconds(5), [this] {
                return stop_.load() || !queue_.empty();
            });
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
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        while (!queue_.empty() && batch.size() < kMaxBatchSize) {
            batch.push_back(std::move(queue_.front()));
            queue_.pop_front();
        }
    }
    if (!batch.empty()) {
        PublishBatch(batch);
    }
}

void KvEventPublisher::PublishBatch(const std::vector<PendingEvent>& batch) {
    struct EncodedEvent {
        PendingEvent pending;
        uint64_t seq_hash{0};
        uint64_t event_id{0};
    };
    std::vector<EncodedEvent> encoded;
    encoded.reserve(batch.size());
    for (const auto& pending : batch) {
        const auto seq_hash = ParseSeqHashFromObjectKey(pending.object_key);
        if (!seq_hash.has_value()) {
            skipped_unparsed_keys_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        encoded.push_back(
            EncodedEvent{pending, seq_hash.value(),
                         next_event_id_.fetch_add(1, std::memory_order_relaxed)});
    }
    if (encoded.empty()) {
        return;
    }

    msgpack::sbuffer payload_buffer;
    msgpack::packer<msgpack::sbuffer> packer(&payload_buffer);

    const int64_t timestamp_ms = CurrentUnixTimeMs();
    const bool has_block_size = config_.block_size > 0;
    const bool has_dp_rank = true;

    packer.pack_array(3);
    packer.pack(timestamp_ms);

    packer.pack_array(encoded.size());
    for (const auto& item : encoded) {
        const bool is_stored = item.pending.kind == EventKind::kStored;
        const char* rfc_type = is_stored ? "stored" : "removed";
        const char* legacy_type =
            is_stored ? "BlockStored" : "BlockRemoved";

        const size_t map_size =
            is_stored
                ? (config_.emit_legacy_compat_fields ? 17 : 14)
                : (config_.emit_legacy_compat_fields ? 15 : 12);

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
        PackOptionalU32(packer, config_.block_size, has_block_size);
        packer.pack("additional_salt");
        PackOptionalString(packer, config_.additional_salt);
        packer.pack("lora_name");
        PackOptionalString(packer, config_.lora_name);
        packer.pack("tenant_id");
        packer.pack(config_.tenant_id);
        packer.pack("backend_id");
        packer.pack(config_.backend_id);
        packer.pack("medium");
        PackOptionalString(packer, item.pending.medium);
        packer.pack("dp_rank");
        PackOptionalU32(packer, config_.dp_rank, has_dp_rank);

        packer.pack("seq_hashes");
        packer.pack_array(1);
        packer.pack(item.seq_hash);

        if (config_.emit_legacy_compat_fields) {
            packer.pack("block_hashes");
            packer.pack_array(1);
            packer.pack(static_cast<int64_t>(item.seq_hash));
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

        published_events_.fetch_add(1, std::memory_order_relaxed);
    }

    packer.pack(config_.dp_rank);

    const uint64_t seq = next_zmq_sequence_.fetch_add(1);
    uint64_t seq_be = 0;
    const auto* in = reinterpret_cast<const unsigned char*>(&seq);
    auto* out = reinterpret_cast<unsigned char*>(&seq_be);
    for (int i = 0; i < 8; ++i) {
        out[i] = in[7 - i];
    }

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
            zmq_sendmsg(zmq_socket_, &payload_msg, 0);
        } else {
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
