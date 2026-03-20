#include "engram/engram.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <future>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <set>
#include <vector>

#include "engram/engram_config.h"

namespace mooncake {
namespace engram {

// -----------------------------------------------------------------------------
// Hash mapping (inlined from NgramHashMapping)
// -----------------------------------------------------------------------------

bool Engram::is_prime(int64_t n) {
    if (n < 2) return false;
    if (n == 2) return true;
    if (n % 2 == 0) return false;
    for (int64_t i = 3; i * i <= n; i += 2) {
        if (n % i == 0) return false;
    }
    return true;
}

int64_t Engram::find_next_prime(int64_t start,
                                std::unordered_set<int64_t>& seen) {
    int64_t candidate = start + 1;
    while (true) {
        if (is_prime(candidate) && seen.find(candidate) == seen.end()) {
            return candidate;
        }
        candidate++;
    }
}

std::vector<std::vector<std::vector<int64_t>>> Engram::hash_(
    const std::vector<std::vector<int64_t>>& input_ids, int layer_id) const {
    int B = static_cast<int>(input_ids.size());
    if (B == 0) return {};
    int T = static_cast<int>(input_ids[0].size());

    const auto& multipliers = layer_multipliers_.at(layer_id);

    auto shift_k = [&](int k) -> std::vector<std::vector<int64_t>> {
        if (k == 0) return input_ids;
        std::vector<std::vector<int64_t>> shifted(
            B, std::vector<int64_t>(T, pad_id_));
        for (int b = 0; b < B; ++b) {
            for (int t = 0; t < T; ++t) {
                if (t >= k) shifted[b][t] = input_ids[b][t - k];
            }
        }
        return shifted;
    };

    std::vector<std::vector<std::vector<int64_t>>> base_shifts;
    for (int k = 0; k < max_ngram_size_; ++k) {
        base_shifts.push_back(shift_k(k));
    }

    std::vector<std::vector<std::vector<int64_t>>> all_hashes;

    for (int n = 2; n <= max_ngram_size_; ++n) {
        int n_gram_index = n - 2;
        std::vector<std::vector<std::vector<int64_t>>> tokens;
        for (int k = 0; k < n; ++k) tokens.push_back(base_shifts[k]);

        std::vector<std::vector<int64_t>> mix(B, std::vector<int64_t>(T));
        for (int b = 0; b < B; ++b) {
            for (int t = 0; t < T; ++t) {
                mix[b][t] = tokens[0][b][t] * multipliers[0];
                for (int k = 1; k < n; ++k) {
                    mix[b][t] ^= (tokens[k][b][t] * multipliers[k]);
                }
            }
        }

        const auto& head_vocab_sizes =
            vocab_size_across_layers_.at(layer_id)[n_gram_index];
        for (int j = 0; j < n_head_per_ngram_; ++j) {
            int64_t mod = head_vocab_sizes[j];
            std::vector<std::vector<int64_t>> head_hash(
                B, std::vector<int64_t>(T));
            for (int b = 0; b < B; ++b) {
                for (int t = 0; t < T; ++t) {
                    int64_t val = mix[b][t];
                    head_hash[b][t] = ((val % mod) + mod) % mod;
                }
            }
            all_hashes.push_back(head_hash);
        }
    }

    int num_heads = static_cast<int>(all_hashes.size());
    std::vector<std::vector<std::vector<int64_t>>> result(
        B,
        std::vector<std::vector<int64_t>>(T, std::vector<int64_t>(num_heads)));
    for (int h = 0; h < num_heads; ++h) {
        for (int b = 0; b < B; ++b) {
            for (int t = 0; t < T; ++t) {
                result[b][t][h] = all_hashes[h][b][t];
            }
        }
    }
    return result;
}

// -----------------------------------------------------------------------------
// Embedding lookup / populate (inlined from MooncakeEngramEmbedding)
// -----------------------------------------------------------------------------

int Engram::embedding_lookup(
    const std::vector<std::vector<std::vector<int64_t>>>& hash_ids,
    void* output_buffer, size_t output_size,
    const std::vector<void*>* table_buffers,
    const std::vector<size_t>* table_sizes,
    bool buffers_are_pre_registered,
    QueryTiming* emb_timing) const {
    auto t_embed_start = std::chrono::high_resolution_clock::now();
    auto ns = [](auto t0, auto t1) {
      return 1e-6 * std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    };
    if (store_ == nullptr) return -1;
    if (hash_ids.empty() || hash_ids[0].empty() || hash_ids[0][0].empty())
        return -1;

    int B = static_cast<int>(hash_ids.size());
    int L = static_cast<int>(hash_ids[0].size());
    int num_heads = static_cast<int>(hash_ids[0][0].size());
    if (num_heads != static_cast<int>(list_of_N_.size())) return -1;
    const size_t row_bytes = static_cast<size_t>(embed_D_) * sizeof(float);

    size_t expected_size =
        static_cast<size_t>(B) * L * num_heads * embed_D_ * sizeof(float);
    if (output_size < expected_size) return -1;

    float* output = static_cast<float*>(output_buffer);
    bool use_zero_copy =
        (table_buffers != nullptr && table_sizes != nullptr &&
         static_cast<int>(table_buffers->size()) == num_heads &&
         static_cast<int>(table_sizes->size()) == num_heads);

    if (use_zero_copy) {
        const int64_t max_rows_per_head =
            std::max<int64_t>(1, static_cast<int64_t>(B) * L);
        for (int h = 0; h < num_heads; ++h) {
            size_t required =
                static_cast<size_t>(
                    std::min<int64_t>(list_of_N_[h], max_rows_per_head)) *
                row_bytes;
            if ((*table_sizes)[h] < required) {
                use_zero_copy = false;
                break;
            }
        }
    }

    std::vector<const float*> tables(num_heads, nullptr);
    std::vector<size_t> registered_indices;
    std::vector<tl::expected<QueryResult, ErrorCode>> query_results;
    bool have_query_results = false;

    auto t_register_start = std::chrono::high_resolution_clock::now();
    if (emb_timing)
        emb_timing->emb_setup_ms = ns(t_embed_start, t_register_start);
    if (use_zero_copy && !buffers_are_pre_registered) {
        for (int h = 0; h < num_heads; ++h) {
            int ret = store_->register_buffer((*table_buffers)[h], (*table_sizes)[h]);
            if (ret != 0) {
                for (int j : registered_indices)
                    store_->unregister_buffer((*table_buffers)[j]);
                use_zero_copy = false;
                break;
            }
            registered_indices.push_back(h);
        }
    }
    auto t_register_end = std::chrono::high_resolution_clock::now();

    auto t_query_start = std::chrono::high_resolution_clock::now();
    if (store_ != nullptr && store_->client_ != nullptr) {
        query_results = get_query_results();
        have_query_results =
            (query_results.size() == static_cast<size_t>(num_heads));
        if (have_query_results) {
            for (int h = 0; h < num_heads; ++h) {
                if (!query_results[h]) {
                    continue;
                }
                size_t table_bytes =
                    static_cast<size_t>(list_of_N_[h]) * row_bytes;
                auto local_ptr = store_->client_->ResolveLocalMemoryAddress(
                    query_results[h].value(), table_bytes);
                if (local_ptr) {
                    tables[h] = static_cast<const float*>(local_ptr.value());
                }
            }
        }
    }
    auto t_query_end = std::chrono::high_resolution_clock::now();
    if (emb_timing) {
        emb_timing->emb_batch_query_ms =
            ns(t_query_start, t_query_end);
    }

    bool all_tables_local = true;
    for (int h = 0; h < num_heads; ++h) {
        if (tables[h] == nullptr) {
            all_tables_local = false;
            break;
        }
    }
    if (all_tables_local) {
        use_zero_copy = false;
    }

    double acc_get_ms = 0, acc_scatter_ms = 0;
    if (use_zero_copy) {
        // Range-read path: one batch_query up front, then per-head offset reads
        // into reusable scratch buffers.
        if (!have_query_results) {
            for (int h = 0; h < num_heads; ++h) {
                for (int b = 0; b < B; ++b) {
                    for (int l = 0; l < L; ++l) {
                        std::memset(
                            output +
                                (b * L * num_heads + l * num_heads + h) *
                                    embed_D_,
                            0, embed_D_ * sizeof(float));
                    }
                }
            }
            if (!buffers_are_pre_registered) {
                for (int h : registered_indices)
                    store_->unregister_buffer((*table_buffers)[h]);
            }
            return -1;
        }

        int total_ranges = 0;
        size_t total_requested_bytes = 0;
        const size_t total_table_bytes = get_embedding_tables_workspace_size();
        std::vector<std::vector<std::pair<int64_t, int64_t>>> head_ranges(
            num_heads);
        auto t_prep_start = std::chrono::high_resolution_clock::now();
        for (int h = 0; h < num_heads; ++h) {
            if (!query_results[h]) continue;
            int64_t vocab_size_h = list_of_N_[h];
            std::set<int64_t> unique_idx;
            for (int b = 0; b < B; ++b) {
                for (int l = 0; l < L; ++l) {
                    int64_t idx = hash_ids[b][l][h];
                    if (idx < 0) idx = 0;
                    if (idx >= vocab_size_h) idx = vocab_size_h - 1;
                    unique_idx.insert(idx);
                }
            }
            if (unique_idx.empty()) continue;
            std::vector<std::pair<int64_t, int64_t>> ranges;
            int64_t range_start = *unique_idx.begin();
            int64_t range_end = range_start + 1;
            for (auto it = std::next(unique_idx.begin()); it != unique_idx.end();
                 ++it) {
                if (*it == range_end) {
                    range_end++;
                } else {
                    ranges.emplace_back(range_start, range_end);
                    range_start = *it;
                    range_end = range_start + 1;
                }
            }
            ranges.emplace_back(range_start, range_end);
            head_ranges[h] = std::move(ranges);
            total_ranges += static_cast<int>(head_ranges[h].size());
            for (const auto& r : head_ranges[h]) {
                total_requested_bytes +=
                    static_cast<size_t>(r.second - r.first) * row_bytes;
            }
        }
        auto t_prep_end = std::chrono::high_resolution_clock::now();

        constexpr int kRangeThreshold = 256;
        const bool prefer_range =
            total_ranges > 0 && total_ranges <= kRangeThreshold &&
            total_requested_bytes * 2 < total_table_bytes;
        if (!prefer_range) {
            auto t_unreg_start = std::chrono::high_resolution_clock::now();
            if (!buffers_are_pre_registered) {
                for (int h : registered_indices)
                    store_->unregister_buffer((*table_buffers)[h]);
            }
            auto t_unreg_end = std::chrono::high_resolution_clock::now();
            use_zero_copy = false;
            auto t_batch_start = std::chrono::high_resolution_clock::now();
            auto buffers = store_->batch_get_buffer(embed_keys_);
            if (buffers.size() == static_cast<size_t>(num_heads)) {
                for (int h = 0; h < num_heads; ++h) {
                    if (buffers[h] &&
                        buffers[h]->size() >=
                            static_cast<size_t>(list_of_N_[h]) * embed_D_ *
                                sizeof(float)) {
                        tables[h] =
                            static_cast<const float*>(buffers[h]->ptr());
                    }
                }
            }
            auto t_batch_end = std::chrono::high_resolution_clock::now();
            if (emb_timing) {
                emb_timing->emb_register_ms = ns(t_register_start, t_register_end);
                emb_timing->emb_prep_ms = ns(t_prep_start, t_prep_end);
                emb_timing->emb_unregister_ms =
                    buffers_are_pre_registered ? 0
                                               : ns(t_unreg_start, t_unreg_end);
                emb_timing->emb_batch_get_buffer_ms =
                    ns(t_batch_start, t_batch_end);
            }
        }
        if (use_zero_copy) {
        auto process_head = [&](int h) {
            double head_get_ms = 0, head_scatter_ms = 0;
            if (!query_results[h]) {
                for (int b = 0; b < B; ++b) {
                    for (int l = 0; l < L; ++l) {
                        std::memset(
                            output +
                                (b * L * num_heads + l * num_heads + h) *
                                    embed_D_,
                            0, embed_D_ * sizeof(float));
                    }
                }
                return std::make_pair(head_get_ms, head_scatter_ms);
            }
            int64_t vocab_size_h = list_of_N_[h];
            const auto& ranges = head_ranges[h];
            if (ranges.empty()) {
                return std::make_pair(head_get_ms, head_scatter_ms);
            }

            float* buf = static_cast<float*>((*table_buffers)[h]);
            size_t buf_bytes = (*table_sizes)[h];
            const QueryResult& qr = query_results[h].value();
            bool ok = true;
            for (const auto& r : ranges) {
                int64_t start = r.first;
                int64_t end = r.second;
                size_t src_offset = static_cast<size_t>(start) * row_bytes;
                size_t read_size =
                    static_cast<size_t>(end - start) * row_bytes;
                if (read_size > buf_bytes) {
                    ok = false;
                    break;
                }
                auto t_get_start = std::chrono::high_resolution_clock::now();
                int64_t ret = store_->get_into_range_with_query_result(
                    embed_keys_[h], qr, buf, 0, src_offset, read_size);
                auto t_get_end = std::chrono::high_resolution_clock::now();
                head_get_ms += ns(t_get_start, t_get_end);
                if (ret < 0 || static_cast<size_t>(ret) < read_size) {
                    ok = false;
                    break;
                }
                auto t_scatter_start =
                    std::chrono::high_resolution_clock::now();
                for (int b = 0; b < B; ++b) {
                    for (int l = 0; l < L; ++l) {
                        int64_t idx = hash_ids[b][l][h];
                        if (idx < 0) idx = 0;
                        if (idx >= vocab_size_h) idx = vocab_size_h - 1;
                        if (idx >= start && idx < end) {
                            size_t off =
                                static_cast<size_t>(idx - start) * embed_D_;
                            float* dst =
                                output +
                                (b * L * num_heads + l * num_heads + h) *
                                    embed_D_;
                            std::memcpy(dst, buf + off,
                                        embed_D_ * sizeof(float));
                        }
                    }
                }
                auto t_scatter_end =
                    std::chrono::high_resolution_clock::now();
                head_scatter_ms += ns(t_scatter_start, t_scatter_end);
            }
            if (!ok) {
                for (int b = 0; b < B; ++b) {
                    for (int l = 0; l < L; ++l) {
                        std::memset(
                            output +
                                (b * L * num_heads + l * num_heads + h) *
                                    embed_D_,
                            0, embed_D_ * sizeof(float));
                    }
                }
            }
            return std::make_pair(head_get_ms, head_scatter_ms);
        };

        const bool parallelize_heads = total_ranges > num_heads * 2;
        if (parallelize_heads) {
            std::vector<std::future<std::pair<double, double>>> futures;
            futures.reserve(num_heads);
            for (int h = 0; h < num_heads; ++h) {
                futures.push_back(
                    std::async(std::launch::async, process_head, h));
            }
            for (int h = 0; h < num_heads; ++h) {
                auto [head_get, head_scatter] = futures[h].get();
                acc_get_ms = std::max(acc_get_ms, head_get);
                acc_scatter_ms = std::max(acc_scatter_ms, head_scatter);
            }
        } else {
            for (int h = 0; h < num_heads; ++h) {
                auto [head_get, head_scatter] = process_head(h);
                acc_get_ms += head_get;
                acc_scatter_ms += head_scatter;
            }
        }
        auto t_unreg_start = std::chrono::high_resolution_clock::now();
        if (!buffers_are_pre_registered) {
            for (int h : registered_indices)
                store_->unregister_buffer((*table_buffers)[h]);
        }
        auto t_unreg_end = std::chrono::high_resolution_clock::now();
        if (emb_timing) {
            emb_timing->emb_register_ms = ns(t_register_start, t_register_end);
            emb_timing->emb_get_into_range_ms = acc_get_ms;
            emb_timing->emb_scatter_ms = acc_scatter_ms;
            emb_timing->emb_prep_ms = ns(t_prep_start, t_prep_end);
            emb_timing->emb_unregister_ms =
                buffers_are_pre_registered ? 0
                                           : ns(t_unreg_start, t_unreg_end);
        }
        }
    }
    if (!use_zero_copy) {
        bool need_fetch = false;
        for (int h = 0; h < num_heads; ++h) {
            if (tables[h] == nullptr) {
                need_fetch = true;
                break;
            }
        }
        if (need_fetch) {
            auto t_batch_start = std::chrono::high_resolution_clock::now();
            std::vector<std::shared_ptr<BufferHandle>> buffers =
                store_->batch_get_buffer(embed_keys_);
            if (buffers.size() != embed_keys_.size()) {
                auto t_batch_end = std::chrono::high_resolution_clock::now();
                if (emb_timing)
                    emb_timing->emb_batch_get_buffer_ms =
                        ns(t_batch_start, t_batch_end);
                return -1;
            }
            for (int h = 0; h < num_heads; ++h) {
                if (buffers[h] &&
                    buffers[h]->size() >=
                        static_cast<size_t>(list_of_N_[h]) * embed_D_ *
                            sizeof(float)) {
                    tables[h] =
                        static_cast<const float*>(buffers[h]->ptr());
                }
            }
            auto t_batch_end = std::chrono::high_resolution_clock::now();
            if (emb_timing)
                emb_timing->emb_batch_get_buffer_ms =
                    ns(t_batch_start, t_batch_end);
        }
    }

    for (int h = 0; h < num_heads; ++h) {
        if (tables[h] == nullptr) {
            std::memset(output_buffer, 0, expected_size);
            return -1;
        }
    }

    auto t_lookup_start = std::chrono::high_resolution_clock::now();
    for (int h = 0; h < num_heads; ++h) {
        if (tables[h] == nullptr) continue;
        const float* table = tables[h];
        int64_t vocab_size_h = list_of_N_[h];
        for (int b = 0; b < B; ++b) {
            for (int l = 0; l < L; ++l) {
                int64_t idx = hash_ids[b][l][h];
                if (idx < 0) idx = 0;
                if (idx >= vocab_size_h) idx = vocab_size_h - 1;
                float* dst =
                    output + (b * L * num_heads + l * num_heads + h) * embed_D_;
                const float* src = table + idx * embed_D_;
                std::memcpy(dst, src, embed_D_ * sizeof(float));
            }
        }
    }
    auto t_lookup_end = std::chrono::high_resolution_clock::now();
    auto t_embed_end = std::chrono::high_resolution_clock::now();
    if (emb_timing) {
        emb_timing->emb_lookup_ms = ns(t_lookup_start, t_lookup_end);
        emb_timing->emb_total_internal_ms = ns(t_embed_start, t_embed_end);
        double phase_sum = emb_timing->emb_setup_ms + emb_timing->emb_register_ms +
                          emb_timing->emb_batch_query_ms +
                          emb_timing->emb_get_into_range_ms +
                          emb_timing->emb_scatter_ms + emb_timing->emb_unregister_ms +
                          emb_timing->emb_prep_ms +
                          emb_timing->emb_batch_get_buffer_ms +
                          emb_timing->emb_lookup_ms;
        emb_timing->emb_gap_ms = std::max(
            0.0, emb_timing->emb_total_internal_ms - phase_sum);
    }
    return 0;
}

int Engram::embedding_populate_from_tensors(
    const std::vector<void*>& embedding_data,
    const std::vector<size_t>& sizes) {
    if (embedding_data.size() != embed_keys_.size() ||
        sizes.size() != embed_keys_.size()) {
        return -1;
    }

    for (size_t i = 0; i < sizes.size(); ++i) {
        size_t expected =
            static_cast<size_t>(list_of_N_[i]) * embed_D_ * sizeof(float);
        if (sizes[i] < expected) {
            return -1;
        }
    }

    for (size_t i = 0; i < embedding_data.size(); ++i) {
        int ret = store_->register_buffer(embedding_data[i], sizes[i]);
        if (ret != 0) {
            for (size_t j = 0; j < i; ++j)
                store_->unregister_buffer(embedding_data[j]);
            return -1;
        }
    }

    std::vector<int> put_results =
        store_->batch_put_from(embed_keys_, embedding_data, sizes);
    bool ok = std::all_of(put_results.begin(), put_results.end(),
                          [](int r) { return r == 0; });

    for (void* buf : embedding_data) store_->unregister_buffer(buf);
    return ok ? 0 : -1;
}

// -----------------------------------------------------------------------------
// Engram: constructor, query, populate
// -----------------------------------------------------------------------------

Engram::Engram(int layer_id, const EngramConfig& config,
               const BackboneConfig& backbone_cfg,
               std::shared_ptr<PyClient> store)
    : layer_id_(layer_id),
      config_(config),
      backbone_cfg_(backbone_cfg),
      store_(store),
      vocab_size_per_ngram_(config.engram_vocab_size),
      max_ngram_size_(config.max_ngram_size),
      n_embed_per_ngram_(config.n_embed_per_ngram),
      n_head_per_ngram_(config.n_head_per_ngram),
      pad_id_(config.pad_id),
      layer_ids_(config.layer_ids),
      tokenizer_vocab_size_(static_cast<int>(
          std::max(*std::max_element(config.engram_vocab_size.begin(),
                                     config.engram_vocab_size.end()) *
                       2,
                   static_cast<int64_t>(1)))) {
    const int64_t PRIME_1 = 10007;
    const int64_t max_long = std::numeric_limits<int64_t>::max();
    const int64_t M_max = max_long / std::max(1, tokenizer_vocab_size_);
    const int64_t half_bound = std::max(static_cast<int64_t>(1), M_max / 2);

    for (int lid : layer_ids_) {
        std::mt19937_64 gen(static_cast<uint64_t>(config.seed + PRIME_1 * lid));
        std::uniform_int_distribution<int64_t> dis(0, half_bound - 1);
        std::vector<int64_t> multipliers;
        for (int i = 0; i < max_ngram_size_; ++i) {
            multipliers.push_back(dis(gen) * 2 + 1);
        }
        layer_multipliers_[lid] = multipliers;
    }

    std::unordered_set<int64_t> seen_primes;
    for (int lid : layer_ids_) {
        std::vector<std::vector<int64_t>> all_ngram;
        for (int n = 2; n <= max_ngram_size_; ++n) {
            std::vector<int64_t> heads;
            int64_t vocab = vocab_size_per_ngram_[n - 2];
            int64_t start = std::max(static_cast<int64_t>(0), vocab - 1);
            for (int h = 0; h < n_head_per_ngram_; ++h) {
                int64_t p = find_next_prime(start, seen_primes);
                seen_primes.insert(p);
                heads.push_back(p);
                start = p;
            }
            all_ngram.push_back(heads);
        }
        vocab_size_across_layers_[lid] = all_ngram;
    }

    const auto& vs = vocab_size_across_layers_[layer_id];
    for (const auto& ngram_sizes : vs) {
        for (int64_t s : ngram_sizes) {
            list_of_N_.push_back(s);
        }
    }
    if (list_of_N_.empty()) {
        for (int n = 2; n <= config.max_ngram_size; ++n) {
            for (int h = 0; h < config.n_head_per_ngram; ++h) {
                list_of_N_.push_back(config.engram_vocab_size[n - 2]);
            }
        }
    }

    embed_D_ = config.n_embed_per_ngram / config.n_head_per_ngram;
    for (size_t h = 0; h < list_of_N_.size(); ++h) {
        std::ostringstream oss;
        oss << "engram:l" << layer_id_ << ":h" << h;
        embed_keys_.push_back(oss.str());
    }
}

Engram::~Engram() { release_internal_workspace(); }

size_t Engram::get_embedding_tables_workspace_size() const {
    size_t total = 0;
    for (int64_t N : list_of_N_) {
        total += static_cast<size_t>(N) * embed_D_ * sizeof(float);
    }
    return total;
}

std::vector<size_t> Engram::get_query_table_buffer_sizes(int B, int L) const {
    const int64_t max_rows_per_head =
        std::max<int64_t>(1, static_cast<int64_t>(B) * L);
    const size_t row_bytes = static_cast<size_t>(embed_D_) * sizeof(float);

    std::vector<size_t> sizes;
    sizes.reserve(list_of_N_.size());
    for (int64_t N_h : list_of_N_) {
        const int64_t rows = std::min<int64_t>(N_h, max_rows_per_head);
        sizes.push_back(static_cast<size_t>(rows) * row_bytes);
    }
    return sizes;
}

size_t Engram::get_query_workspace_size(int B, int L) const {
    size_t total = 0;
    for (size_t sz : get_query_table_buffer_sizes(B, L)) {
        total += sz;
    }
    return total;
}

int Engram::ensure_internal_workspace(int B, int L) const {
    if (store_ == nullptr) return -1;

    const std::vector<size_t> table_sizes = get_query_table_buffer_sizes(B, L);
    const size_t required_bytes = std::accumulate(
        table_sizes.begin(), table_sizes.end(), static_cast<size_t>(0));

    bool reuse_existing =
        internal_workspace_registered_ &&
        internal_workspace_.size() >= required_bytes &&
        internal_table_sizes_.size() == table_sizes.size();
    if (reuse_existing) {
        for (size_t i = 0; i < table_sizes.size(); ++i) {
            if (internal_table_sizes_[i] < table_sizes[i]) {
                reuse_existing = false;
                break;
            }
        }
    }
    if (reuse_existing) return 0;

    release_internal_workspace();

    internal_workspace_.assign(required_bytes, 0);
    internal_table_buffers_.clear();
    internal_table_sizes_ = table_sizes;
    internal_table_buffers_.reserve(table_sizes.size());

    char* base = internal_workspace_.data();
    for (size_t sz : table_sizes) {
        internal_table_buffers_.push_back(base);
        base += sz;
    }

    for (size_t i = 0; i < internal_table_buffers_.size(); ++i) {
        int ret =
            store_->register_buffer(internal_table_buffers_[i], table_sizes[i]);
        if (ret != 0) {
            for (size_t j = 0; j < i; ++j) {
                store_->unregister_buffer(internal_table_buffers_[j]);
            }
            internal_workspace_.clear();
            internal_table_buffers_.clear();
            internal_table_sizes_.clear();
            internal_workspace_registered_ = false;
            return -1;
        }
    }

    internal_workspace_registered_ = true;
    return 0;
}

void Engram::release_internal_workspace() const {
    if (store_ != nullptr && internal_workspace_registered_) {
        for (void* buf : internal_table_buffers_) {
            store_->unregister_buffer(buf);
        }
    }
    internal_workspace_registered_ = false;
    internal_workspace_.clear();
    internal_table_buffers_.clear();
    internal_table_sizes_.clear();
}

std::vector<tl::expected<QueryResult, ErrorCode>> Engram::get_query_results()
    const {
    if (store_ == nullptr) return {};

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(query_result_cache_mu_);
        if (query_result_cache_valid_ &&
            cached_query_results_.size() == embed_keys_.size()) {
            bool cache_valid = true;
            for (const auto& result : cached_query_results_) {
                if (result.lease_timeout <= now) {
                    cache_valid = false;
                    break;
                }
            }
            if (cache_valid) {
                std::vector<tl::expected<QueryResult, ErrorCode>> query_results;
                query_results.reserve(cached_query_results_.size());
                for (const auto& result : cached_query_results_) {
                    query_results.emplace_back(result);
                }
                return query_results;
            }
        }
    }

    auto query_results = store_->batch_query(embed_keys_);
    {
        std::lock_guard<std::mutex> lock(query_result_cache_mu_);
        cached_query_results_.clear();
        query_result_cache_valid_ = false;
        bool can_cache = (query_results.size() == embed_keys_.size());
        if (can_cache) {
            cached_query_results_.reserve(query_results.size());
            for (const auto& result : query_results) {
                if (!result) {
                    can_cache = false;
                    break;
                }
                cached_query_results_.push_back(result.value());
            }
        }
        if (can_cache) {
            query_result_cache_valid_ = true;
        } else {
            cached_query_results_.clear();
        }
    }
    return query_results;
}

void Engram::invalidate_query_result_cache() const {
    std::lock_guard<std::mutex> lock(query_result_cache_mu_);
    cached_query_results_.clear();
    query_result_cache_valid_ = false;
}

std::vector<std::vector<std::vector<int64_t>>> Engram::hash_input_ids(
    const std::vector<std::vector<int64_t>>& input_ids) const {
    return hash_(input_ids, layer_id_);
}

std::vector<int64_t> Engram::get_table_vocab_sizes() const {
    return list_of_N_;
}

std::vector<std::string> Engram::get_store_keys() const {
    return embed_keys_;
}

int Engram::get_num_heads() const {
    return static_cast<int>(list_of_N_.size());
}

int Engram::get_embedding_dim() const {
    return embed_D_;
}

int Engram::query_embeddings(
    const std::vector<std::vector<int64_t>>& input_ids, void* output,
    size_t output_size, void* workspace, size_t workspace_size,
    QueryTiming* out_timing) const {
    if (output == nullptr) return -1;
    if (store_ == nullptr) return -1;

    int B = static_cast<int>(input_ids.size());
    if (B == 0) return -1;
    int L = static_cast<int>(input_ids[0].size());

    size_t expected =
        static_cast<size_t>(B) * L * list_of_N_.size() * embed_D_ * sizeof(float);
    if (output_size < expected) return -1;

    std::unique_lock<std::mutex> internal_workspace_lock;
    std::vector<void*> table_buffers;
    std::vector<size_t> table_sizes;
    bool buffers_are_pre_registered = false;
    const size_t query_workspace_bytes = get_query_workspace_size(B, L);
    const bool caller_workspace_ready =
        (workspace != nullptr && workspace_size >= query_workspace_bytes);
    if (store_ != nullptr) {
        if (caller_workspace_ready) {
            internal_workspace_lock = std::unique_lock<std::mutex>(
                internal_workspace_mu_, std::try_to_lock);
        } else {
            internal_workspace_lock =
                std::unique_lock<std::mutex>(internal_workspace_mu_);
        }
        if (internal_workspace_lock.owns_lock() &&
            ensure_internal_workspace(B, L) == 0) {
            table_buffers = internal_table_buffers_;
            table_sizes = internal_table_sizes_;
            buffers_are_pre_registered = true;
        }
    }

    if (table_buffers.empty() && caller_workspace_ready) {
        char* tables_base = static_cast<char*>(workspace);
        table_sizes = get_query_table_buffer_sizes(B, L);
        table_buffers.reserve(table_sizes.size());
        for (size_t sz : table_sizes) {
            table_buffers.push_back(tables_base);
            tables_base += sz;
        }
    }

    auto t_hash_start = std::chrono::high_resolution_clock::now();
    auto hash_ids = hash_(input_ids, layer_id_);
    auto t_hash_end = std::chrono::high_resolution_clock::now();

    const std::vector<void*>* tbl_bufs =
        table_buffers.empty() ? nullptr : &table_buffers;
    const std::vector<size_t>* tbl_szs =
        table_sizes.empty() ? nullptr : &table_sizes;

    auto t_lookup_start = std::chrono::high_resolution_clock::now();
    if (embedding_lookup(hash_ids, output, output_size, tbl_bufs, tbl_szs,
                         buffers_are_pre_registered, out_timing) != 0) {
        return -1;
    }
    auto t_lookup_end = std::chrono::high_resolution_clock::now();

    if (out_timing != nullptr) {
        using namespace std::chrono;
        out_timing->hash_ms =
            1e-6 * duration_cast<nanoseconds>(t_hash_end - t_hash_start).count();
        out_timing->store_read_ms =
            1e-6 * duration_cast<nanoseconds>(t_lookup_end - t_lookup_start)
                       .count();
    }

    return 0;
}

int Engram::populate_store_from_buffers(
    const std::vector<void*>& embedding_buffers,
    const std::vector<size_t>& buffer_sizes) {
    if (store_ == nullptr) return -1;
    int ret = embedding_populate_from_tensors(embedding_buffers, buffer_sizes);
    if (ret == 0) invalidate_query_result_cache();
    return ret;
}

}  // namespace engram
}  // namespace mooncake
