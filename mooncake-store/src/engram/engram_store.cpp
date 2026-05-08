#include "engram/engram_store.h"

#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "pyclient.h"

namespace mooncake {
namespace engram {

EngramStore::EngramStore(int layer_id, const EngramStoreConfig& config,
                         std::shared_ptr<PyClient> store)
    : store_(std::move(store)),
      table_vocab_sizes_(config.table_vocab_sizes),
      embedding_dim_(config.embedding_dim) {
    if (table_vocab_sizes_.empty()) {
        throw std::invalid_argument(
            "EngramStoreConfig.table_vocab_sizes must not be empty");
    }
    if (embedding_dim_ <= 0) {
        throw std::invalid_argument(
            "EngramStoreConfig.embedding_dim must be positive");
    }
    for (int64_t vocab_size : table_vocab_sizes_) {
        if (vocab_size <= 0) {
            throw std::invalid_argument(
                "EngramStoreConfig.table_vocab_sizes must contain only "
                "positive values");
        }
    }

    embed_keys_.reserve(table_vocab_sizes_.size());
    for (size_t h = 0; h < table_vocab_sizes_.size(); ++h) {
        std::ostringstream oss;
        oss << "engram:l" << layer_id << ":h" << h;
        embed_keys_.push_back(oss.str());
    }
}

int EngramStore::lookup_rows_flat(const int64_t* row_ids, int B, int L,
                                  void* output_buffer,
                                  size_t output_size) const {
    if (store_ == nullptr || row_ids == nullptr || output_buffer == nullptr ||
        B <= 0 || L <= 0) {
        return -1;
    }

    const int num_heads = static_cast<int>(table_vocab_sizes_.size());
    const size_t row_bytes =
        static_cast<size_t>(embedding_dim_) * sizeof(float);
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
        all_keys[0].push_back(embed_keys_[h]);
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
                if (idx < 0 || idx >= table_vocab_sizes_[h]) {
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

    const int register_ret =
        store_->register_buffer(output_buffer, expected_size);
    if (register_ret != 0) {
        return fail_lookup();
    }

    auto results = store_->get_into_ranges(buffers, all_keys, all_dst_offsets,
                                           all_src_offsets, all_sizes);
    const int unregister_ret = store_->unregister_buffer(output_buffer);
    if (unregister_ret != 0) {
        return fail_lookup();
    }
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

int EngramStore::lookup_rows_contiguous(const int64_t* row_ids, int B, int L,
                                        void* output_buffer,
                                        size_t output_size) const {
    return lookup_rows_flat(row_ids, B, L, output_buffer, output_size);
}

int EngramStore::lookup_rows(
    const std::vector<std::vector<std::vector<int64_t>>>& row_ids,
    void* output_buffer, size_t output_size) const {
    if (store_ == nullptr || output_buffer == nullptr || row_ids.empty() ||
        row_ids[0].empty()) {
        return -1;
    }

    const int B = static_cast<int>(row_ids.size());
    const int L = static_cast<int>(row_ids[0].size());
    const int num_heads = static_cast<int>(table_vocab_sizes_.size());
    std::vector<int64_t> flat_row_ids;
    flat_row_ids.reserve(static_cast<size_t>(B) * L * num_heads);
    for (int b = 0; b < B; ++b) {
        if (static_cast<int>(row_ids[b].size()) != L) {
            return -1;
        }
        for (int l = 0; l < L; ++l) {
            if (static_cast<int>(row_ids[b][l].size()) != num_heads) {
                return -1;
            }
            flat_row_ids.insert(flat_row_ids.end(), row_ids[b][l].begin(),
                                row_ids[b][l].end());
        }
    }

    return lookup_rows_flat(flat_row_ids.data(), B, L, output_buffer,
                            output_size);
}

std::vector<int64_t> EngramStore::get_table_vocab_sizes() const {
    return table_vocab_sizes_;
}

std::vector<std::string> EngramStore::get_store_keys() const {
    return embed_keys_;
}

int EngramStore::get_num_heads() const {
    return static_cast<int>(table_vocab_sizes_.size());
}

int EngramStore::get_embedding_dim() const { return embedding_dim_; }

int EngramStore::remove_from_store(bool force) {
    if (store_ == nullptr) {
        return static_cast<int>(ErrorCode::INVALID_PARAMS);
    }

    constexpr int kObjectNotFound =
        static_cast<int>(ErrorCode::OBJECT_NOT_FOUND);
    int removed = 0;
    int first_error = 0;

    for (const auto& key : embed_keys_) {
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

int EngramStore::populate(const std::vector<void*>& embedding_buffers,
                          const std::vector<size_t>& buffer_sizes) {
    if (store_ == nullptr) {
        return -1;
    }
    if (embedding_buffers.size() != embed_keys_.size() ||
        buffer_sizes.size() != embed_keys_.size()) {
        return -1;
    }

    for (size_t i = 0; i < buffer_sizes.size(); ++i) {
        const size_t expected = static_cast<size_t>(table_vocab_sizes_[i]) *
                                embedding_dim_ * sizeof(float);
        if (embedding_buffers[i] == nullptr || buffer_sizes[i] != expected) {
            return -1;
        }
    }

    std::vector<int> exists_results = store_->batchIsExist(embed_keys_);
    if (exists_results.size() != embed_keys_.size()) {
        LOG(ERROR) << "Failed to preflight EngramStore populate key existence";
        return -1;
    }
    for (size_t i = 0; i < exists_results.size(); ++i) {
        const int exists = exists_results[i];
        if (exists < 0) {
            LOG(ERROR) << "Failed to query EngramStore key '" << embed_keys_[i]
                       << "' before populate, rc=" << exists;
            return -1;
        }
        if (exists != 0) {
            LOG(ERROR)
                << "EngramStore populate requires empty destination key '"
                << embed_keys_[i] << "'. Remove the existing layer first.";
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

    std::vector<int> put_results =
        store_->batch_put_from(embed_keys_, embedding_buffers, buffer_sizes);
    const bool put_succeeded =
        put_results.size() == embed_keys_.size() &&
        std::all_of(put_results.begin(), put_results.end(),
                    [](int result) { return result == 0; });

    const bool unregister_failed =
        cleanup_registered_buffers(embedding_buffers.size());

    if (!put_succeeded || unregister_failed) {
        const bool put_results_complete =
            put_results.size() == embed_keys_.size();
        for (size_t i = 0; i < embed_keys_.size(); ++i) {
            if (put_results_complete && put_results[i] != 0) {
                continue;
            }
            int rc = store_->remove(embed_keys_[i], true);
            if (rc != 0 &&
                rc != static_cast<int>(ErrorCode::OBJECT_NOT_FOUND)) {
                LOG(ERROR)
                    << "Failed to roll back partially populated EngramStore "
                    << "key '" << embed_keys_[i] << "', rc=" << rc;
            }
        }
        if (unregister_failed) {
            LOG(ERROR)
                << "Rolling back EngramStore populate because buffer cleanup "
                   "failed after publish";
        }
        return -1;
    }

    return 0;
}

}  // namespace engram
}  // namespace mooncake
