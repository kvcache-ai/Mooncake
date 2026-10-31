#include "engram/engram_store.h"

#include <algorithm>
#include <cstring>
#include <chrono>
#include <glog/logging.h>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <typeinfo>
#include <utility>
#include <vector>

#include "pyclient.h"
#include "real_client.h"

namespace mooncake {
namespace engram {

namespace {

// Composition keeps both existing client classes' layouts unchanged.
class RealClientStrictCreate final : public StrictCreateClient {
   public:
    explicit RealClientStrictCreate(RealClient& client) : client_(client) {}

    std::vector<int> batch_put_from_create_only(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReplicateConfig& config) override {
        return client_.batch_put_from_create_only(keys, buffers, sizes, config);
    }

   private:
    RealClient& client_;
};

struct PopulateOwnerGuard {
    explicit PopulateOwnerGuard(std::shared_ptr<void> value)
        : owner(value.use_count()
                    ? std::make_unique<std::shared_ptr<void>>(std::move(value))
                    : nullptr) {}

    ~PopulateOwnerGuard() noexcept {
        // A shared TE or pending DMA may outlive the Store. Never reclaim an
        // uncertain registration's owner, even during process teardown.
        if (!release_safe) (void)owner.release();
    }

    std::unique_ptr<std::shared_ptr<void>> owner;
    bool release_safe = false;
};

}  // namespace

struct EngramStore::QueryCacheEntry {
    mooncake::PyClient::RangedReadSnapshot snapshot;
};

EngramStore::EngramStore(const std::map<int, EngramStoreConfig>& layers,
                         std::shared_ptr<PyClient> store)
    : store_(std::move(store)) {
    strict_store_ = std::dynamic_pointer_cast<StrictCreateClient>(store_);
    // Do not silently bypass a subclass's storage implementation. Custom
    // backends (including RealClient subclasses) must opt into the capability.
    if (!strict_store_ && store_ && typeid(*store_) == typeid(RealClient)) {
        strict_store_ = std::make_shared<RealClientStrictCreate>(
            static_cast<RealClient&>(*store_));
    }
    if (layers.empty()) {
        throw std::invalid_argument("EngramStore requires at least one layer");
    }
    for (const auto& [layer_id, config] : layers) {
        if (layer_id < 0 || config.row_bytes <= 0 ||
            config.table_vocab_sizes.empty()) {
            throw std::invalid_argument(
                "EngramStore requires nonnegative layer IDs, positive "
                "row_bytes and nonempty tables");
        }
        auto& layer = layers_.try_emplace(layer_id).first->second;
        layer.config = config;
        for (size_t h = 0; h < config.table_vocab_sizes.size(); ++h) {
            const int64_t vocab_size = config.table_vocab_sizes[h];
            if (vocab_size <= 0 ||
                static_cast<uint64_t>(vocab_size) >
                    std::numeric_limits<size_t>::max() / config.row_bytes) {
                throw std::invalid_argument("Invalid Engram table size");
            }
            std::ostringstream key;
            key << "engram:l" << layer_id << ":h" << h;
            layer.keys.push_back(key.str());
        }
    }
}

const EngramStore::Layer& EngramStore::get_layer(int layer_id) const {
    auto it = layers_.find(layer_id);
    if (it == layers_.end()) {
        throw std::invalid_argument("Unknown Engram layer: " +
                                    std::to_string(layer_id));
    }
    return it->second;
}

std::shared_ptr<const EngramStore::QueryCacheEntry>
EngramStore::get_query_cache(int layer_id,
                             const std::vector<std::string>& keys) const {
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(query_cache_mutex_);
        auto it = query_cache_.find(layer_id);
        if (it != query_cache_.end() && it->second &&
            it->second->snapshot.reusable(now)) {
            return it->second;
        }
    }

    auto entry = std::make_shared<QueryCacheEntry>();
    // RangedReadSnapshot::reset() stamps refresh_at with the in-call clock,
    // so reusable() must not be probed with an earlier timestamp: a failed
    // refresh (query error or expired lease) would still pass it.
    if (!store_->refresh_get_into_ranges_snapshot(entry->snapshot, keys))
        return nullptr;

    std::lock_guard<std::mutex> lock(query_cache_mutex_);
    query_cache_[layer_id] = entry;
    return entry;
}

void EngramStore::invalidate_query_cache(int layer_id) const {
    std::lock_guard<std::mutex> lock(query_cache_mutex_);
    query_cache_.erase(layer_id);
}

int EngramStore::bind_local(int layer_id,
                            const std::vector<const void*>& buffers,
                            const std::vector<size_t>& sizes) {
    return bind_local(layer_id, buffers, sizes, nullptr);
}

int EngramStore::bind_local(int layer_id,
                            const std::vector<const void*>& buffers,
                            const std::vector<size_t>& sizes,
                            std::shared_ptr<void> owner) {
    const auto& layer = get_layer(layer_id);
    // store_ is immutable after construction; reject before taking the layer
    // lock so this doomed call cannot block behind an in-flight lookup.
    if (store_) {
        return -1;
    }
    std::unique_lock<std::shared_mutex> lock(layer.mutex);
    if (!layer.local_tables.empty() || buffers.size() != layer.keys.size() ||
        sizes.size() != buffers.size()) {
        return -1;
    }
    for (size_t h = 0; h < buffers.size(); ++h) {
        if (!buffers[h] ||
            sizes[h] != static_cast<size_t>(layer.config.table_vocab_sizes[h]) *
                            layer.config.row_bytes)
            return -1;
    }
    layers_.at(layer_id).local_tables = buffers;
    layers_.at(layer_id).local_owner = std::move(owner);
    return 0;
}

int EngramStore::lookup_into(int layer_id, const int64_t* row_ids, int B, int L,
                             void* output_buffer, size_t output_size) const {
    const auto& layer = get_layer(layer_id);
    std::shared_lock<std::shared_mutex> lock(layer.mutex);
    const auto& table_vocab_sizes = layer.config.table_vocab_sizes;
    const auto& embed_keys = layer.keys;
    if ((!store_ && layer.local_tables.empty()) || row_ids == nullptr ||
        output_buffer == nullptr || B <= 0 || L <= 0) {
        return -1;
    }

    const int num_heads = static_cast<int>(table_vocab_sizes.size());
    const size_t row_bytes = layer.config.row_bytes;
    const size_t max_size = std::numeric_limits<size_t>::max();
    if (static_cast<size_t>(B) > max_size / static_cast<size_t>(L)) {
        return -1;
    }
    const size_t token_count = static_cast<size_t>(B) * static_cast<size_t>(L);
    if (token_count > max_size / static_cast<size_t>(num_heads) ||
        token_count * static_cast<size_t>(num_heads) > max_size / row_bytes) {
        return -1;
    }
    const size_t expected_size =
        token_count * static_cast<size_t>(num_heads) * row_bytes;
    if (output_size < expected_size) {
        return -1;
    }

    auto fail_lookup = [&]() {
        std::memset(output_buffer, 0, expected_size);
        return -1;
    };

    if (!layer.local_tables.empty()) {
        // Validate all IDs before touching output. No metadata or transfer
        // work.
        for (size_t t = 0; t < token_count; ++t) {
            for (int h = 0; h < num_heads; ++h) {
                const auto id = row_ids[t * num_heads + h];
                if (id < 0 || id >= table_vocab_sizes[h]) return fail_lookup();
            }
        }
        auto* dst = static_cast<char*>(output_buffer);
        for (size_t t = 0; t < token_count; ++t) {
            for (int h = 0; h < num_heads; ++h) {
                const size_t index = t * num_heads + h;
                const auto* src =
                    static_cast<const char*>(layer.local_tables[h]);
                std::memcpy(
                    dst + index * row_bytes,
                    src + static_cast<size_t>(row_ids[index]) * row_bytes,
                    row_bytes);
            }
        }
        return 0;
    }

    std::vector<void*> buffers{output_buffer};
    std::vector<std::vector<std::string>> all_keys(1);
    std::vector<std::vector<std::vector<size_t>>> all_dst_offsets(1);
    std::vector<std::vector<std::vector<size_t>>> all_src_offsets(1);
    std::vector<std::vector<std::vector<size_t>>> all_sizes(1);

    all_keys[0].reserve(static_cast<size_t>(num_heads));
    all_dst_offsets[0].reserve(static_cast<size_t>(num_heads));
    all_src_offsets[0].reserve(static_cast<size_t>(num_heads));
    all_sizes[0].reserve(static_cast<size_t>(num_heads));

    for (int h = 0; h < num_heads; ++h) {
        all_keys[0].push_back(embed_keys[h]);
        all_dst_offsets[0].emplace_back();
        all_src_offsets[0].emplace_back();
        all_sizes[0].emplace_back();
        all_dst_offsets[0].back().reserve(static_cast<size_t>(B) * L);
        all_src_offsets[0].back().reserve(static_cast<size_t>(B) * L);
        all_sizes[0].back().reserve(static_cast<size_t>(B) * L);
    }

    for (int b = 0; b < B; ++b) {
        for (int l = 0; l < L; ++l) {
            const size_t token_index = static_cast<size_t>(b) * L + l;
            const size_t row_offset =
                token_index * static_cast<size_t>(num_heads);
            for (int h = 0; h < num_heads; ++h) {
                const int64_t idx =
                    row_ids[row_offset + static_cast<size_t>(h)];
                if (idx < 0 || idx >= table_vocab_sizes[h]) {
                    return fail_lookup();
                }
                all_dst_offsets[0][h].push_back(
                    (row_offset + static_cast<size_t>(h)) * row_bytes);
                all_src_offsets[0][h].push_back(static_cast<size_t>(idx) *
                                                row_bytes);
                all_sizes[0][h].push_back(row_bytes);
            }
        }
    }

    auto query_cache = get_query_cache(layer_id, embed_keys);
    if (!query_cache) return fail_lookup();

    auto results = store_->get_into_ranges_from_snapshot(
        buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes,
        query_cache->snapshot);
    if (results.size() != 1 ||
        results[0].size() != static_cast<size_t>(num_heads)) {
        return fail_lookup();
    }
    for (int h = 0; h < num_heads; ++h) {
        if (results[0][h].size() != all_sizes[0][h].size()) {
            return fail_lookup();
        }
        for (int64_t bytes_read : results[0][h]) {
            if (bytes_read != static_cast<int64_t>(row_bytes)) {
                return fail_lookup();
            }
        }
    }

    return 0;
}

std::vector<int> EngramStore::get_layer_ids() const {
    std::vector<int> ids;
    for (const auto& [id, layer] : layers_) ids.push_back(id);
    return ids;
}

std::vector<int64_t> EngramStore::get_table_vocab_sizes(int layer_id) const {
    return get_layer(layer_id).config.table_vocab_sizes;
}

std::vector<std::string> EngramStore::get_store_keys(int layer_id) const {
    return get_layer(layer_id).keys;
}

int EngramStore::get_num_heads(int layer_id) const {
    return static_cast<int>(get_layer(layer_id).keys.size());
}

int EngramStore::get_row_bytes(int layer_id) const {
    return get_layer(layer_id).config.row_bytes;
}

int EngramStore::remove_from_store(int layer_id, bool force) {
    const auto& layer = get_layer(layer_id);
    std::unique_lock<std::shared_mutex> lock(layer.mutex);
    const auto& embed_keys = layer.keys;
    if (store_ == nullptr) {
        return static_cast<int>(ErrorCode::INVALID_PARAMS);
    }

    // Invalidate before any deletion, including partial failure or exceptions.
    // The layer lock excludes in-flight reads and old snapshot refills.
    invalidate_query_cache(layer_id);

    constexpr int kObjectNotFound =
        static_cast<int>(ErrorCode::OBJECT_NOT_FOUND);
    int removed = 0;
    int first_error = 0;

    for (const auto& key : embed_keys) {
        int rc = store_->remove(key, force);
        if (rc == 0) {
            ++removed;
            continue;
        }
        if (rc == kObjectNotFound) {
            continue;
        }
        if (first_error == 0) {
            first_error = rc;
        }
    }

    return first_error != 0 ? first_error : removed;
}

int EngramStore::populate(int layer_id,
                          const std::vector<void*>& embedding_buffers,
                          const std::vector<size_t>& buffer_sizes,
                          const ReplicateConfig& config) {
    return populate(layer_id, embedding_buffers, buffer_sizes, config, nullptr);
}

int EngramStore::populate(int layer_id,
                          const std::vector<void*>& embedding_buffers,
                          const std::vector<size_t>& buffer_sizes,
                          const ReplicateConfig& config,
                          std::shared_ptr<void> owner) {
    const auto& layer = get_layer(layer_id);
    std::unique_lock<std::shared_mutex> lock(layer.mutex);
    const auto& table_vocab_sizes = layer.config.table_vocab_sizes;
    const auto& embed_keys = layer.keys;
    if (strict_store_ == nullptr) {
        return -1;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        LOG(ERROR) << "EngramStore populate does not support flexible dual "
                      "replicas: write success cannot prove all source "
                      "transfers have ended";
        return -1;
    }
    if (embedding_buffers.size() != embed_keys.size() ||
        buffer_sizes.size() != embed_keys.size()) {
        return -1;
    }

    for (size_t i = 0; i < buffer_sizes.size(); ++i) {
        const size_t expected =
            static_cast<size_t>(table_vocab_sizes[i]) * layer.config.row_bytes;
        if (embedding_buffers[i] == nullptr || buffer_sizes[i] != expected) {
            return -1;
        }
        const auto begin = reinterpret_cast<uintptr_t>(embedding_buffers[i]);
        if (buffer_sizes[i] > std::numeric_limits<uintptr_t>::max() - begin)
            return -1;
        for (size_t j = 0; j < i; ++j) {
            const auto other =
                reinterpret_cast<uintptr_t>(embedding_buffers[j]);
            if (begin >= other ? begin - other < buffer_sizes[j]
                               : other - begin < buffer_sizes[i])
                return -1;
        }
    }

    std::vector<int> exists_results = store_->batchIsExist(embed_keys);
    if (exists_results.size() != embed_keys.size()) {
        LOG(ERROR) << "Failed to preflight EngramStore populate key existence";
        return -1;
    }
    for (size_t i = 0; i < exists_results.size(); ++i) {
        const int exists = exists_results[i];
        if (exists < 0) {
            LOG(ERROR) << "Failed to query EngramStore key '" << embed_keys[i]
                       << "' before populate, rc=" << exists;
            return -1;
        }
        if (exists != 0) {
            LOG(ERROR)
                << "EngramStore populate requires empty destination key '"
                << embed_keys[i] << "'. Remove the existing layer first.";
            return -1;
        }
    }

    PopulateOwnerGuard owner_guard(std::move(owner));
    size_t registered_count = 0;
    auto cleanup_registered_buffers = [&]() {
        bool cleanup_failed = false;
        for (size_t i = 0; i < registered_count; ++i) {
            int rc = -1;
            try {
                rc = store_->unregister_buffer(embedding_buffers[i]);
            } catch (...) {
                // Continue cleaning the other buffers after an exception.
            }
            if (rc != 0) {
                cleanup_failed = true;
                LOG(ERROR) << "Failed to unregister embedding buffer at index "
                           << i << ", rc=" << rc;
            }
        }
        return cleanup_failed;
    };

    std::vector<int> put_results;
    try {
        for (; registered_count < embedding_buffers.size();
             ++registered_count) {
            int ret =
                store_->register_buffer(embedding_buffers[registered_count],
                                        buffer_sizes[registered_count]);
            if (ret != 0) {
                LOG(ERROR) << "Failed to register embedding buffer at index "
                           << registered_count << ", rc=" << ret;
                break;
            }
        }
        if (registered_count == embedding_buffers.size()) {
            put_results = strict_store_->batch_put_from_create_only(
                embed_keys, embedding_buffers, buffer_sizes, config);
        }
    } catch (...) {
        LOG(ERROR) << "Exception during EngramStore populate; buffer ownership "
                      "must be retained";
    }
    const bool put_succeeded =
        put_results.size() == embed_keys.size() &&
        std::all_of(put_results.begin(), put_results.end(),
                    [](int result) { return result == 0; });

    const bool unregister_failed = cleanup_registered_buffers();

    if (!put_succeeded) {
        // No object-version token proves that a key still belongs to this
        // attempt. Even a successful creation may have been replaced remotely.
        // Never force-delete another writer's objects during failed populate.
        const bool terminal_results =
            put_results.size() == embed_keys.size() &&
            std::all_of(put_results.begin(), put_results.end(), [](int result) {
                return result == 0 ||
                       result ==
                           static_cast<int>(ErrorCode::OBJECT_ALREADY_EXISTS);
            });
        if (terminal_results && !unregister_failed) {
            // Strict creation conflicts never start a source transfer. Success
            // entries completed all required transfers (flexible mode is
            // rejected).
            owner_guard.release_safe = true;
            return -1;
        }
        // Failed registration can leave partial MRs; a failed upload can time
        // out before DMA completes. Unregister alone cannot prove release safe.
        return -3;
    }

    try {
        invalidate_query_cache(layer_id);
    } catch (...) {
        LOG(ERROR) << "EngramStore published layer " << layer_id
                   << " but cache invalidation failed; retain buffers until "
                      "process exit";
        return -2;
    }

    if (unregister_failed) {
        // Rolling back published tables would not release registered buffers.
        LOG(ERROR) << "EngramStore populate published layer " << layer_id
                   << " but failed to unregister some embedding buffers; "
                      "keep them alive until process exit";
        return -2;
    }
    owner_guard.release_safe = true;
    return 0;
}

}  // namespace engram
}  // namespace mooncake
