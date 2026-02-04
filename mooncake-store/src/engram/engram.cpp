#include "engram/engram.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <sstream>

#include "engram/engram_config.h"

namespace mooncake {
namespace engram {

// -----------------------------------------------------------------------------
// Helpers: RMSNorm, Linear, SiLU, DepthwiseConv1d
// -----------------------------------------------------------------------------

static void rms_norm(const float* input, int hidden_size, float eps,
                     float* output, int numel) {
    for (int i = 0; i < numel; i += hidden_size) {
        float sum_sq = 0.0f;
        for (int j = 0; j < hidden_size; ++j) {
            float v = input[i + j];
            sum_sq += v * v;
        }
        float rms = std::sqrt(sum_sq / hidden_size + eps);
        for (int j = 0; j < hidden_size; ++j) {
            output[i + j] = input[i + j] / rms;
        }
    }
}

// Strided version: each row has `hidden_size` elements; consecutive rows are
// `input_stride` / `output_stride` floats apart. Avoids temporary copies when
// normalizing non-contiguous groups (e.g. per hc_mult in [B, L, hc_mult, D]).
static void rms_norm_strided(const float* input, int hidden_size,
                             int input_stride, int num_rows, float eps,
                             float* output, int output_stride) {
    for (int r = 0; r < num_rows; ++r) {
        const float* in_row = input + r * input_stride;
        float* out_row = output + r * output_stride;
        float sum_sq = 0.0f;
        for (int j = 0; j < hidden_size; ++j) {
            float v = in_row[j];
            sum_sq += v * v;
        }
        float rms = std::sqrt(sum_sq / hidden_size + eps);
        for (int j = 0; j < hidden_size; ++j) {
            out_row[j] = in_row[j] / rms;
        }
    }
}

static void linear_proj(const float* input, const float* weight, int in_dim,
                        int out_dim, int numel_in, float* output) {
    int num_out = (numel_in / in_dim) * out_dim;
    std::memset(output, 0, num_out * sizeof(float));
    for (int i = 0; i < numel_in; i += in_dim) {
        int out_base = (i / in_dim) * out_dim;
        for (int j = 0; j < in_dim; ++j) {
            float in_val = input[i + j];
            for (int k = 0; k < out_dim; ++k) {
                output[out_base + k] += in_val * weight[j * out_dim + k];
            }
        }
    }
}

static void silu(float* data, int numel) {
    for (int i = 0; i < numel; ++i) {
        float x = data[i];
        data[i] = x / (1.0f + std::exp(-x));
    }
}

static void depthwise_conv1d(const float* input, int B, int L, int C,
                             int kernel_size, int dilation, float* output) {
    int pad = (kernel_size - 1) * dilation;
    for (int b = 0; b < B; ++b) {
        for (int c = 0; c < C; ++c) {
            for (int l = 0; l < L; ++l) {
                float sum = 0.0f;
                for (int k = 0; k < kernel_size; ++k) {
                    int pos = l - pad + k * dilation;
                    if (pos >= 0 && pos < L) {
                        sum += input[(b * L + pos) * C + c];
                    }
                }
                output[(b * L + l) * C + c] = sum;
            }
        }
    }
}

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
    void* output_buffer, size_t output_size) const {
    if (store_ == nullptr) return -1;
    if (hash_ids.empty() || hash_ids[0].empty() || hash_ids[0][0].empty())
        return -1;

    int B = static_cast<int>(hash_ids.size());
    int L = static_cast<int>(hash_ids[0].size());
    int num_heads = static_cast<int>(hash_ids[0][0].size());
    if (num_heads != static_cast<int>(list_of_N_.size())) return -1;

    size_t expected_size =
        static_cast<size_t>(B) * L * num_heads * embed_D_ * sizeof(float);
    if (output_size < expected_size) return -1;

    std::vector<std::shared_ptr<BufferHandle>> buffers =
        store_->batch_get_buffer(embed_keys_);
    if (buffers.size() != embed_keys_.size()) return -1;

    float* output = static_cast<float*>(output_buffer);

    for (int h = 0; h < num_heads; ++h) {
        if (!buffers[h] ||
            buffers[h]->size() <
                static_cast<size_t>(list_of_N_[h]) * embed_D_ * sizeof(float)) {
            for (int b = 0; b < B; ++b) {
                for (int l = 0; l < L; ++l) {
                    std::memset(
                        output +
                            (b * L * num_heads + l * num_heads + h) * embed_D_,
                        0, embed_D_ * sizeof(float));
                }
            }
            continue;
        }

        const float* table = static_cast<const float*>(buffers[h]->ptr());
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
    return 0;
}

int Engram::embedding_populate_from_tensors(
    const std::vector<void*>& embedding_data,
    const std::vector<size_t>& sizes) {
    if (embedding_data.size() != embed_keys_.size() ||
        sizes.size() != embed_keys_.size()) {
        return -1;
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
// Engram: constructor, gating, ShortConv, forward, populate
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
        for (int i = 0; i < max_ngram_size_; ++i)
            multipliers.push_back(dis(gen) * 2 + 1);
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
        for (int64_t s : ngram_sizes) list_of_N_.push_back(s);
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

    int engram_hidden = (config.max_ngram_size - 1) * config.n_embed_per_ngram;
    key_proj_weights_.resize(backbone_cfg.hc_mult);
    for (int i = 0; i < backbone_cfg.hc_mult; ++i) {
        key_proj_weights_[i].resize(engram_hidden * backbone_cfg.hidden_size);
        std::mt19937 gen(42 + i);
        std::normal_distribution<float> d(
            0.0f, std::sqrt(2.0f / (engram_hidden + backbone_cfg.hidden_size)));
        for (float& w : key_proj_weights_[i]) w = d(gen);
    }
    value_proj_weights_.resize(engram_hidden * backbone_cfg.hidden_size);
    std::mt19937 gen(43);
    std::normal_distribution<float> d(
        0.0f, std::sqrt(2.0f / (engram_hidden + backbone_cfg.hidden_size)));
    for (float& w : value_proj_weights_) w = d(gen);
}

void Engram::compute_gates_and_fuse(const float* embeddings,
                                    const float* hidden_states, int B, int L,
                                    float* output) const {
    int hc_mult = backbone_cfg_.hc_mult;
    int D = backbone_cfg_.hidden_size;
    int engram_hidden =
        (config_.max_ngram_size - 1) * config_.n_embed_per_ngram;

    std::vector<float> normed_embeddings(B * L * engram_hidden);
    std::vector<float> normed_hidden(B * L * hc_mult * D);
    std::vector<float> keys(B * L * hc_mult * D);
    std::vector<float> gates(B * L * hc_mult);
    std::vector<float> values(B * L * D);

    rms_norm(embeddings, engram_hidden, 1e-5f, normed_embeddings.data(),
             B * L * engram_hidden);

    // Normalize hidden states for each hc_mult group (strided: no temp copies)
    // hidden_states layout: [B, L, hc_mult, D] -> group hc at (i*hc_mult+hc)*D
    const int hidden_stride = hc_mult * D;
    for (int hc = 0; hc < hc_mult; ++hc) {
        const float* src = hidden_states + hc * D;
        float* dst = normed_hidden.data() + hc * D;
        rms_norm_strided(src, D, hidden_stride, B * L, 1e-5f, dst,
                        hidden_stride);
    }

    // Compute keys for each hc_mult group
    for (int hc = 0; hc < hc_mult; ++hc) {
        // Use temporary buffer for linear projection output
        std::vector<float> key_hc(B * L * D);
        std::memset(key_hc.data(), 0, B * L * D * sizeof(float));
        linear_proj(normed_embeddings.data(), key_proj_weights_[hc].data(),
                    engram_hidden, D, B * L * engram_hidden, key_hc.data());
        rms_norm(key_hc.data(), D, 1e-5f, key_hc.data(), B * L * D);

        // Write keys to correct positions in keys array
        for (int i = 0; i < B * L; ++i) {
            float* dst = keys.data() + (i * hc_mult + hc) * D;
            const float* src = key_hc.data() + i * D;
            std::memcpy(dst, src, D * sizeof(float));
        }

        // Compute gates
        for (int i = 0; i < B * L; ++i) {
            float dot = 0.0f;
            const float* k = keys.data() + (i * hc_mult + hc) * D;
            const float* q = normed_hidden.data() + (i * hc_mult + hc) * D;
            for (int j = 0; j < D; ++j) dot += k[j] * q[j];
            float g = dot / std::sqrt(static_cast<float>(D));
            float sign = (g >= 0) ? 1.0f : -1.0f;
            g = std::sqrt(std::max(std::abs(g), 1e-6f)) * sign;
            gates[i * hc_mult + hc] = 1.0f / (1.0f + std::exp(-g));
        }
    }

    std::memset(values.data(), 0, B * L * D * sizeof(float));
    linear_proj(normed_embeddings.data(), value_proj_weights_.data(),
                engram_hidden, D, B * L * engram_hidden, values.data());

    for (int b = 0; b < B; ++b) {
        for (int l = 0; l < L; ++l) {
            for (int hc = 0; hc < hc_mult; ++hc) {
                float g = gates[(b * L + l) * hc_mult + hc];
                const float* v = values.data() + (b * L + l) * D;
                float* out = output + ((b * L + l) * hc_mult + hc) * D;
                for (int d = 0; d < D; ++d) out[d] = g * v[d];
            }
        }
    }
}

void Engram::apply_short_conv(const float* input, int B, int L,
                              float* output) const {
    int hc_mult = backbone_cfg_.hc_mult;
    int D = backbone_cfg_.hidden_size;
    int total_C = hc_mult * D;

    std::vector<float> normed(B * L * total_C);
    // Normalize each hc_mult group separately (strided: no temp copies)
    // input layout: [B, L, hc_mult, D] -> group hc at (i*hc_mult+hc)*D
    const int hidden_stride = hc_mult * D;
    for (int hc = 0; hc < hc_mult; ++hc) {
        const float* src = input + hc * D;
        float* dst = normed.data() + hc * D;
        rms_norm_strided(src, D, hidden_stride, B * L, 1e-5f, dst,
                        hidden_stride);
    }

    std::vector<float> conv_out(B * L * total_C);
    depthwise_conv1d(normed.data(), B, L, total_C, config_.kernel_size,
                     config_.max_ngram_size, conv_out.data());
    silu(conv_out.data(), B * L * total_C);

    for (int i = 0; i < B * L * total_C; ++i) {
        output[i] = input[i] + conv_out[i];
    }
}

size_t Engram::get_forward_workspace_size(int B, int L) const {
    int engram_hidden =
        (config_.max_ngram_size - 1) * config_.n_embed_per_ngram;
    int hc_mult = backbone_cfg_.hc_mult;
    int D = backbone_cfg_.hidden_size;
    size_t emb_floats = static_cast<size_t>(B) * L * engram_hidden;
    size_t gated_floats = static_cast<size_t>(B) * L * hc_mult * D;
    return (emb_floats + gated_floats) * sizeof(float);
}

int Engram::forward(const void* hidden_states, size_t hidden_states_size,
                    const std::vector<std::vector<int64_t>>& input_ids,
                    void* output, size_t output_size, void* workspace,
                    size_t workspace_size) const {
    if (hidden_states == nullptr || output == nullptr) return -1;
    if (store_ == nullptr) return -1;

    int B = static_cast<int>(input_ids.size());
    if (B == 0) return -1;
    int L = static_cast<int>(input_ids[0].size());
    int hc_mult = backbone_cfg_.hc_mult;
    int D = backbone_cfg_.hidden_size;

    size_t expected = static_cast<size_t>(B) * L * hc_mult * D * sizeof(float);
    if (hidden_states_size < expected || output_size < expected) return -1;

    int engram_hidden =
        (config_.max_ngram_size - 1) * config_.n_embed_per_ngram;
    size_t emb_sz = static_cast<size_t>(B) * L * engram_hidden * sizeof(float);
    size_t required_ws = get_forward_workspace_size(B, L);

    float* embeddings_buffer = nullptr;
    float* gated_output = nullptr;
    std::vector<float> embeddings_vec;
    std::vector<float> gated_vec;

    if (workspace != nullptr && workspace_size >= required_ws) {
        embeddings_buffer = static_cast<float*>(workspace);
        gated_output = static_cast<float*>(workspace) + (emb_sz / sizeof(float));
    } else {
        embeddings_vec.resize(B * L * engram_hidden);
        gated_vec.resize(B * L * hc_mult * D);
        embeddings_buffer = embeddings_vec.data();
        gated_output = gated_vec.data();
    }

    auto hash_ids = hash_(input_ids, layer_id_);

    if (embedding_lookup(hash_ids, embeddings_buffer, emb_sz) != 0) return -1;

    compute_gates_and_fuse(embeddings_buffer,
                           static_cast<const float*>(hidden_states), B, L,
                           gated_output);

    apply_short_conv(gated_output, B, L, static_cast<float*>(output));

    const float* h = static_cast<const float*>(hidden_states);
    float* out = static_cast<float*>(output);
    for (int i = 0; i < B * L * hc_mult * D; ++i) out[i] += h[i];

    return 0;
}

int Engram::populate_store_from_buffers(
    const std::vector<void*>& embedding_buffers,
    const std::vector<size_t>& buffer_sizes) {
    if (store_ == nullptr) return -1;
    return embedding_populate_from_tensors(embedding_buffers, buffer_sizes);
}

}  // namespace engram
}  // namespace mooncake
