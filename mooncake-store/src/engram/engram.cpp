#include "engram/engram.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace mooncake {
namespace engram {

namespace {

bool query_results_are_complete(
    const std::vector<tl::expected<QueryResult, ErrorCode>>& query_results,
    size_t expected_size) {
    if (query_results.size() != expected_size) {
        return false;
    }
    for (const auto& result : query_results) {
        if (!result) {
            return false;
        }
    }
    return true;
}

bool cached_query_results_are_live(const std::vector<QueryResult>& query_results,
                                   size_t expected_size,
                                   std::chrono::steady_clock::time_point now) {
    if (query_results.size() != expected_size) {
        return false;
    }
    for (const auto& result : query_results) {
        if (result.lease_timeout <= now) {
            return false;
        }
    }
    return true;
}

}  // namespace

Engram::Engram(int layer_id, const EngramConfig& config,
               std::shared_ptr<PyClient> store)
    : store_(std::move(store)),
      table_vocab_sizes_(config.table_vocab_sizes),
      embedding_dim_(config.embedding_dim) {
    if (table_vocab_sizes_.empty()) {
        throw std::invalid_argument(
            "EngramConfig.table_vocab_sizes must not be empty");
    }
    if (embedding_dim_ <= 0) {
        throw std::invalid_argument(
            "EngramConfig.embedding_dim must be positive");
    }
    for (int64_t vocab_size : table_vocab_sizes_) {
        if (vocab_size <= 0) {
            throw std::invalid_argument(
                "EngramConfig.table_vocab_sizes must contain only positive values");
        }
    }

    embed_keys_.reserve(table_vocab_sizes_.size());
    for (size_t h = 0; h < table_vocab_sizes_.size(); ++h) {
        std::ostringstream oss;
        oss << "engram:l" << layer_id << ":h" << h;
        embed_keys_.push_back(oss.str());
    }
}

std::vector<tl::expected<QueryResult, ErrorCode>> Engram::get_query_results()
    const {
    if (store_ == nullptr) {
        return {};
    }

    std::vector<tl::expected<QueryResult, ErrorCode>> query_results;
    if (copy_cached_query_results(query_results)) {
        return query_results;
    }

    query_results = store_->batch_query(embed_keys_);
    update_query_result_cache(query_results);
    return query_results;
}

bool Engram::copy_cached_query_results(
    std::vector<tl::expected<QueryResult, ErrorCode>>& query_results) const {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(query_result_cache_mu_);
    if (!query_result_cache_valid_ ||
        !cached_query_results_are_live(cached_query_results_, embed_keys_.size(),
                                       now)) {
        return false;
    }

    query_results.reserve(cached_query_results_.size());
    for (const auto& result : cached_query_results_) {
        query_results.emplace_back(result);
    }
    return true;
}

void Engram::update_query_result_cache(
    const std::vector<tl::expected<QueryResult, ErrorCode>>& query_results)
    const {
    std::lock_guard<std::mutex> lock(query_result_cache_mu_);
    cached_query_results_.clear();
    query_result_cache_valid_ = false;
    if (!query_results_are_complete(query_results, embed_keys_.size())) {
        return;
    }

    cached_query_results_.reserve(query_results.size());
    for (const auto& result : query_results) {
        cached_query_results_.push_back(result.value());
    }
    query_result_cache_valid_ = true;
}

void Engram::invalidate_query_result_cache() const {
    std::lock_guard<std::mutex> lock(query_result_cache_mu_);
    cached_query_results_.clear();
    query_result_cache_valid_ = false;
}

int Engram::prepare_lookup_rows(
    const std::vector<std::vector<std::vector<int64_t>>>& row_ids, int& B,
    int& L,
    std::vector<std::vector<LookupRowRef>>& head_refs) const {
    B = static_cast<int>(row_ids.size());
    if (B <= 0) {
        return -1;
    }

    L = static_cast<int>(row_ids[0].size());
    if (L <= 0) {
        return -1;
    }

    const int num_heads = static_cast<int>(table_vocab_sizes_.size());
    const size_t token_stride = static_cast<size_t>(num_heads);
    const size_t output_stride = token_stride * embedding_dim_;

    head_refs.assign(num_heads, {});
    const size_t refs_per_head = static_cast<size_t>(B) * L;
    for (auto& refs : head_refs) {
        refs.reserve(refs_per_head);
    }

    for (int b = 0; b < B; ++b) {
        if (static_cast<int>(row_ids[b].size()) != L) {
            return -1;
        }
        for (int l = 0; l < L; ++l) {
            if (static_cast<int>(row_ids[b][l].size()) != num_heads) {
                return -1;
            }
            const size_t token_index = static_cast<size_t>(b) * L + l;
            const size_t token_output_base = token_index * output_stride;
            for (int h = 0; h < num_heads; ++h) {
                const int64_t idx = row_ids[b][l][h];
                if (idx < 0 || idx >= table_vocab_sizes_[h]) {
                    return -1;
                }
                head_refs[h].push_back(LookupRowRef{
                    idx,
                    token_output_base + static_cast<size_t>(h) * embedding_dim_,
                });
            }
        }
    }
    return 0;
}

void Engram::resolve_local_tables(
    const std::vector<tl::expected<QueryResult, ErrorCode>>& query_results,
    std::vector<const float*>& tables, size_t row_bytes) const {
    auto client = store_ != nullptr ? store_->get_client_shared() : nullptr;
    if (client == nullptr ||
        !query_results_are_complete(query_results, embed_keys_.size())) {
        return;
    }

    for (size_t h = 0; h < tables.size(); ++h) {
        const size_t table_bytes =
            static_cast<size_t>(table_vocab_sizes_[h]) * row_bytes;
        auto local_ptr = client->ResolveLocalMemoryAddress(
            query_results[h].value(), table_bytes);
        if (local_ptr) {
            tables[h] = static_cast<const float*>(local_ptr.value());
        }
    }
}

bool Engram::fetch_missing_tables(std::vector<const float*>& tables,
                                  size_t row_bytes) const {
    std::vector<int> missing_heads;
    std::vector<std::string> missing_keys;
    missing_heads.reserve(tables.size());
    missing_keys.reserve(tables.size());
    for (size_t h = 0; h < tables.size(); ++h) {
        if (tables[h] == nullptr) {
            missing_heads.push_back(static_cast<int>(h));
            missing_keys.push_back(embed_keys_[h]);
        }
    }

    if (missing_keys.empty()) {
        return true;
    }

    auto buffers = store_->batch_get_buffer(missing_keys);
    if (buffers.size() != missing_keys.size()) {
        return false;
    }

    for (size_t i = 0; i < missing_heads.size(); ++i) {
        const int head_idx = missing_heads[i];
        const size_t expected_table_bytes =
            static_cast<size_t>(table_vocab_sizes_[head_idx]) * row_bytes;
        if (!buffers[i] || buffers[i]->size() < expected_table_bytes) {
            return false;
        }
        tables[head_idx] = static_cast<const float*>(buffers[i]->ptr());
    }

    return true;
}

void Engram::materialize_output(
    const std::vector<std::vector<LookupRowRef>>& head_refs,
    const std::vector<const float*>& tables, float* output,
    size_t row_bytes) const {
    for (size_t h = 0; h < tables.size(); ++h) {
        const float* table = tables[h];
        for (const auto& ref : head_refs[h]) {
            const float* src =
                table + static_cast<size_t>(ref.idx) * embedding_dim_;
            float* dst = output + ref.output_offset;
            std::memcpy(dst, src, row_bytes);
        }
    }
}

int Engram::lookup_rows_flat(
    const std::vector<std::vector<LookupRowRef>>& head_refs, int B, int L,
    void* output_buffer, size_t output_size) const {
    if (store_ == nullptr || output_buffer == nullptr) {
        return -1;
    }

    const int num_heads = static_cast<int>(table_vocab_sizes_.size());
    const size_t row_bytes = static_cast<size_t>(embedding_dim_) * sizeof(float);
    const size_t expected_size =
        static_cast<size_t>(B) * L * num_heads * embedding_dim_ * sizeof(float);
    if (output_size < expected_size) {
        return -1;
    }
    auto fail_lookup = [&]() {
        std::memset(output_buffer, 0, expected_size);
        return -1;
    };

    float* output = static_cast<float*>(output_buffer);
    std::vector<const float*> tables(num_heads, nullptr);

    resolve_local_tables(get_query_results(), tables, row_bytes);
    if (!fetch_missing_tables(tables, row_bytes)) {
        return fail_lookup();
    }

    for (int h = 0; h < num_heads; ++h) {
        if (tables[h] == nullptr) {
            return fail_lookup();
        }
    }

    materialize_output(head_refs, tables, output, row_bytes);
    return 0;
}

int Engram::lookup_rows(
    const std::vector<std::vector<std::vector<int64_t>>>& row_ids, void* output,
    size_t output_size) const {
    int B = 0;
    int L = 0;
    std::vector<std::vector<LookupRowRef>> head_refs;
    if (prepare_lookup_rows(row_ids, B, L, head_refs) != 0) {
        return -1;
    }
    return lookup_rows_flat(head_refs, B, L, output, output_size);
}

std::vector<int64_t> Engram::get_table_vocab_sizes() const {
    return table_vocab_sizes_;
}

std::vector<std::string> Engram::get_store_keys() const { return embed_keys_; }

int Engram::get_num_heads() const {
    return static_cast<int>(table_vocab_sizes_.size());
}

int Engram::get_embedding_dim() const { return embedding_dim_; }

int Engram::remove_from_store(bool force) {
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

    invalidate_query_result_cache();
    return first_error != 0 ? first_error : removed;
}

int Engram::populate(const std::vector<void*>& embedding_buffers,
                     const std::vector<size_t>& buffer_sizes) {
    if (store_ == nullptr) {
        return -1;
    }
    if (embedding_buffers.size() != embed_keys_.size() ||
        buffer_sizes.size() != embed_keys_.size()) {
        return -1;
    }

    for (size_t i = 0; i < buffer_sizes.size(); ++i) {
        const size_t expected =
            static_cast<size_t>(table_vocab_sizes_[i]) * embedding_dim_ *
            sizeof(float);
        if (buffer_sizes[i] != expected) {
            return -1;
        }
    }

    for (size_t i = 0; i < embedding_buffers.size(); ++i) {
        int ret = store_->register_buffer(embedding_buffers[i], buffer_sizes[i]);
        if (ret != 0) {
            for (size_t j = 0; j < i; ++j) {
                store_->unregister_buffer(embedding_buffers[j]);
            }
            return -1;
        }
    }

    std::vector<int> put_results =
        store_->batch_put_from(embed_keys_, embedding_buffers, buffer_sizes);
    const bool ok =
        put_results.size() == embed_keys_.size() &&
        std::all_of(put_results.begin(), put_results.end(),
                    [](int result) { return result == 0; });

    for (void* buf : embedding_buffers) {
        store_->unregister_buffer(buf);
    }
    if (ok) {
        invalidate_query_result_cache();
    }
    return ok ? 0 : -1;
}

}  // namespace engram
}  // namespace mooncake
