#include "engram/engram_store.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "pyclient.h"

namespace mooncake {
namespace engram {

struct EngramStore::QueryCacheEntry {
    std::vector<int> layer_ids;
    mooncake::PyClient::RangedReadSnapshot snapshot;
};

EngramStore::EngramStore(const std::map<int, EngramStoreConfig>& layers,
                         std::shared_ptr<PyClient> store)
    : store_(std::move(store)) {
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
        Layer layer{config, {}, {}};
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
        layers_.emplace(layer_id, std::move(layer));
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
EngramStore::get_query_cache(const std::vector<int>& layer_ids,
                             const std::vector<std::string>& keys) const {
    if (layer_ids.empty()) return nullptr;

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(query_cache_mutex_);
    std::shared_ptr<QueryCacheEntry>* cache_entry;
    if (layer_ids.size() == 1) {
        cache_entry = &query_cache_[layer_ids.front()];
    } else {
        cache_entry = &multi_layer_query_cache_;
    }
    if (*cache_entry && (*cache_entry)->layer_ids == layer_ids &&
        (*cache_entry)->snapshot.reusable(now)) {
        return *cache_entry;
    }

    auto entry = std::make_shared<QueryCacheEntry>();
    entry->layer_ids = layer_ids;
    entry->snapshot = store_->prepare_get_into_ranges_snapshot(keys);
    if (!entry->snapshot.reusable(now)) return nullptr;

    *cache_entry = entry;
    return entry;
}

void EngramStore::invalidate_query_cache(int layer_id) const {
    std::lock_guard<std::mutex> lock(query_cache_mutex_);
    query_cache_.erase(layer_id);
    if (multi_layer_query_cache_ &&
        std::find(multi_layer_query_cache_->layer_ids.begin(),
                  multi_layer_query_cache_->layer_ids.end(),
                  layer_id) != multi_layer_query_cache_->layer_ids.end()) {
        multi_layer_query_cache_.reset();
    }
}

int EngramStore::bind_local(int layer_id,
                            const std::vector<const void*>& buffers,
                            const std::vector<size_t>& sizes) {
    const auto& layer = get_layer(layer_id);
    if (store_ || !layer.local_tables.empty() ||
        buffers.size() != layer.keys.size() || sizes.size() != buffers.size()) {
        return -1;
    }
    for (size_t h = 0; h < buffers.size(); ++h) {
        if (!buffers[h] ||
            sizes[h] != static_cast<size_t>(layer.config.table_vocab_sizes[h]) *
                            layer.config.row_bytes)
            return -1;
    }
    layers_.at(layer_id).local_tables = buffers;
    return 0;
}

int EngramStore::lookup_into(int layer_id, const int64_t* row_ids, int B, int L,
                             void* output_buffer, size_t output_size) const {
    return lookup_many_into(
        {LookupRequest{layer_id, row_ids, B, L, output_buffer, output_size}});
}

int EngramStore::lookup_many_into(
    const std::vector<LookupRequest>& requests) const {
    if (requests.empty()) return 0;

    struct LookupPlan {
        const LookupRequest* request;
        const Layer* layer;
        size_t token_count;
        size_t expected_size;
        uintptr_t output_begin;
        uintptr_t output_end;
    };

    std::vector<LookupPlan> plans;
    plans.reserve(requests.size());
    size_t key_count = 0;
    const size_t max_size = std::numeric_limits<size_t>::max();
    for (const auto& request : requests) {
        const auto& layer = get_layer(request.layer_id);
        if ((!store_ && layer.local_tables.empty()) ||
            request.row_ids == nullptr || request.output == nullptr ||
            request.batch_size <= 0 || request.sequence_length <= 0) {
            return -1;
        }

        const size_t batch_size = static_cast<size_t>(request.batch_size);
        const size_t sequence_length =
            static_cast<size_t>(request.sequence_length);
        const size_t num_heads = layer.config.table_vocab_sizes.size();
        const size_t row_bytes = layer.config.row_bytes;
        if (batch_size > max_size / sequence_length) return -1;
        const size_t token_count = batch_size * sequence_length;
        if (token_count > max_size / num_heads ||
            token_count * num_heads > max_size / row_bytes) {
            return -1;
        }
        const size_t expected_size = token_count * num_heads * row_bytes;
        if (request.output_size < expected_size) return -1;

        const uintptr_t output_begin =
            reinterpret_cast<uintptr_t>(request.output);
        if (expected_size >
            std::numeric_limits<uintptr_t>::max() - output_begin) {
            return -1;
        }
        const uintptr_t output_end = output_begin + expected_size;
        for (const auto& plan : plans) {
            if (output_begin < plan.output_end &&
                plan.output_begin < output_end) {
                return -1;
            }
        }

        plans.push_back(LookupPlan{&request, &layer, token_count, expected_size,
                                   output_begin, output_end});
        key_count += layer.keys.size();
    }

    auto fail_lookups = [&]() {
        for (const auto& plan : plans)
            std::memset(plan.request->output, 0, plan.expected_size);
        return -1;
    };

    // Validate every layer before touching any output or starting a transfer.
    for (const auto& plan : plans) {
        const auto& vocab_sizes = plan.layer->config.table_vocab_sizes;
        const size_t num_heads = vocab_sizes.size();
        for (size_t token = 0; token < plan.token_count; ++token) {
            for (size_t head = 0; head < num_heads; ++head) {
                const int64_t id =
                    plan.request->row_ids[token * num_heads + head];
                if (id < 0 || id >= vocab_sizes[head]) return fail_lookups();
            }
        }
    }

    if (!store_) {
        for (const auto& plan : plans) {
            const size_t num_heads =
                plan.layer->config.table_vocab_sizes.size();
            const size_t row_bytes = plan.layer->config.row_bytes;
            auto* output = static_cast<char*>(plan.request->output);
            for (size_t token = 0; token < plan.token_count; ++token) {
                for (size_t head = 0; head < num_heads; ++head) {
                    const size_t index = token * num_heads + head;
                    const auto* table = static_cast<const char*>(
                        plan.layer->local_tables[head]);
                    std::memcpy(output + index * row_bytes,
                                table + static_cast<size_t>(
                                            plan.request->row_ids[index]) *
                                            row_bytes,
                                row_bytes);
                }
            }
        }
        return 0;
    }

    std::vector<void*> buffers;
    std::vector<std::vector<std::string>> all_keys;
    std::vector<std::vector<std::vector<size_t>>> all_dst_offsets;
    std::vector<std::vector<std::vector<size_t>>> all_src_offsets;
    std::vector<std::vector<std::vector<size_t>>> all_sizes;
    std::vector<std::string> query_keys;
    std::vector<int> query_layer_ids;
    buffers.reserve(plans.size());
    all_keys.reserve(plans.size());
    all_dst_offsets.reserve(plans.size());
    all_src_offsets.reserve(plans.size());
    all_sizes.reserve(plans.size());
    query_keys.reserve(key_count);
    query_layer_ids.reserve(plans.size());

    for (const auto& plan : plans) {
        const auto& layer = *plan.layer;
        const size_t num_heads = layer.config.table_vocab_sizes.size();
        const size_t row_bytes = layer.config.row_bytes;
        buffers.push_back(plan.request->output);
        query_layer_ids.push_back(plan.request->layer_id);
        all_keys.push_back(layer.keys);
        query_keys.insert(query_keys.end(), layer.keys.begin(),
                          layer.keys.end());
        all_dst_offsets.emplace_back(num_heads);
        all_src_offsets.emplace_back(num_heads);
        all_sizes.emplace_back(num_heads);

        auto& dst_offsets = all_dst_offsets.back();
        auto& src_offsets = all_src_offsets.back();
        auto& sizes = all_sizes.back();
        for (size_t head = 0; head < num_heads; ++head) {
            dst_offsets[head].reserve(plan.token_count);
            src_offsets[head].reserve(plan.token_count);
            sizes[head].reserve(plan.token_count);
        }
        for (size_t token = 0; token < plan.token_count; ++token) {
            const size_t row_offset = token * num_heads;
            for (size_t head = 0; head < num_heads; ++head) {
                const size_t index = row_offset + head;
                dst_offsets[head].push_back(index * row_bytes);
                src_offsets[head].push_back(
                    static_cast<size_t>(plan.request->row_ids[index]) *
                    row_bytes);
                sizes[head].push_back(row_bytes);
            }
        }
    }

    auto query_cache = get_query_cache(query_layer_ids, query_keys);
    if (!query_cache) return fail_lookups();

    auto results = store_->get_into_ranges_from_snapshot(
        buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes,
        query_cache->snapshot);
    if (results.size() != plans.size()) return fail_lookups();
    for (size_t i = 0; i < plans.size(); ++i) {
        const size_t num_heads = plans[i].layer->keys.size();
        const size_t row_bytes = plans[i].layer->config.row_bytes;
        if (results[i].size() != num_heads) return fail_lookups();
        for (size_t head = 0; head < num_heads; ++head) {
            if (results[i][head].size() != all_sizes[i][head].size())
                return fail_lookups();
            for (int64_t bytes_read : results[i][head]) {
                if (bytes_read != static_cast<int64_t>(row_bytes))
                    return fail_lookups();
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
    const auto& embed_keys = get_layer(layer_id).keys;
    if (store_ == nullptr) {
        return static_cast<int>(ErrorCode::INVALID_PARAMS);
    }

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

    if (first_error == 0 && removed > 0) invalidate_query_cache(layer_id);
    return first_error != 0 ? first_error : removed;
}

int EngramStore::populate(int layer_id,
                          const std::vector<void*>& embedding_buffers,
                          const std::vector<size_t>& buffer_sizes,
                          const ReplicateConfig& config) {
    const auto& layer = get_layer(layer_id);
    const auto& table_vocab_sizes = layer.config.table_vocab_sizes;
    const auto& embed_keys = layer.keys;
    if (store_ == nullptr) {
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

    auto cleanup_registered_buffers = [&](size_t count) {
        bool cleanup_failed = false;
        for (size_t i = 0; i < count; ++i) {
            int rc = store_->unregister_buffer(embedding_buffers[i]);
            if (rc != 0) {
                cleanup_failed = true;
                LOG(ERROR) << "Failed to unregister embedding buffer at index "
                           << i << ", rc=" << rc;
            }
        }
        return cleanup_failed;
    };

    for (size_t i = 0; i < embedding_buffers.size(); ++i) {
        int ret =
            store_->register_buffer(embedding_buffers[i], buffer_sizes[i]);
        if (ret != 0) {
            if (cleanup_registered_buffers(i)) {
                LOG(ERROR) << "Failed to clean up registered embedding buffers "
                              "after register_buffer error";
            }
            return -1;
        }
    }

    std::vector<int> put_results = store_->batch_put_from(
        embed_keys, embedding_buffers, buffer_sizes, config);
    const bool put_succeeded =
        put_results.size() == embed_keys.size() &&
        std::all_of(put_results.begin(), put_results.end(),
                    [](int result) { return result == 0; });

    const bool unregister_failed =
        cleanup_registered_buffers(embedding_buffers.size());

    if (!put_succeeded || unregister_failed) {
        const bool put_results_complete =
            put_results.size() == embed_keys.size();
        for (size_t i = 0; i < embed_keys.size(); ++i) {
            if (put_results_complete && put_results[i] != 0) {
                continue;
            }
            int rc = store_->remove(embed_keys[i], true);
            if (rc != 0 &&
                rc != static_cast<int>(ErrorCode::OBJECT_NOT_FOUND)) {
                LOG(ERROR)
                    << "Failed to roll back partially populated EngramStore "
                    << "key '" << embed_keys[i] << "', rc=" << rc;
            }
        }
        if (unregister_failed) {
            LOG(ERROR)
                << "Rolling back EngramStore populate because buffer cleanup "
                   "failed after publish";
        }
        return -1;
    }

    invalidate_query_cache(layer_id);
    return 0;
}

}  // namespace engram
}  // namespace mooncake
