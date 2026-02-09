// ============================================================================
// C2C: Cross-model KV Cache converter for Mooncake
// ============================================================================
// Based on thu-nics/C2C method for cross-model KV Cache conversion
// Formula: output = gate * sigmoid(scalar) * projector(source_kv)
//
// Transparent to upper-layer frameworks, Mooncake handles internally:
// 1. Detect model name in key during batch_put_from
// 2. Match conversion rule and trigger background conversion
// 3. Auto put converted KV after completion
// ============================================================================
#pragma once

#include <atomic>
#include <algorithm>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

namespace mooncake {

// Default RDMA buffer size for C2C converter output (added to
// local_buffer_size)
constexpr size_t kC2cDefaultBufferSize = 256 << 20;  // 256MB

// ============================================================================
// Configuration
// ============================================================================
struct KVModelInfo {
    std::string model_id;  // e.g. "qwen3-4b"
    std::string
        hf_model;  // e.g. "Qwen/Qwen3-4B" (auto-fetch params from HuggingFace)
    int32_t num_layers;
    int32_t num_kv_heads;
    int32_t head_dim;
};

struct KVConversionRule {
    std::string source_model;       // source model ID
    std::string target_model;       // target model ID
    std::string source_model_name;  // source model name for key matching (e.g.
                                    // "Qwen/Qwen3-4B")
    std::string
        target_model_name;       // target model name (e.g. "Qwen/Qwen3-0.6B")
    std::string projector_file;  // C2C projector weight file path (local)
    std::string projector_url;   // C2C projector weight URL (HTTP download,
                                 // priority over file)

    // =========================================================================
    // C2C projector parameters (per-layer projector)
    // =========================================================================
    int32_t num_layers{0};
    int32_t src_dim{0};  // source KV dim (num_heads * head_dim)
    int32_t tgt_dim{0};  // target KV dim
    int32_t hidden_dim{0};

    struct LayerWeights {
        float key_gate_logit{0.0f};
        float value_gate_logit{0.0f};
        float key_gate{0.0f};    // sigmoid(key_gate_logit), precomputed
        float value_gate{0.0f};  // sigmoid(value_gate_logit), precomputed
        std::vector<float> key_in_weight,
            key_in_bias;  // [hidden, src] (transposed)
        std::vector<float> key_mlp1_weight,
            key_mlp1_bias;  // [hidden, hidden] (transposed)
        std::vector<float> key_proj_weight,
            key_proj_bias;  // [tgt, hidden] (transposed)
        std::vector<float> value_in_weight, value_in_bias;
        std::vector<float> value_mlp1_weight, value_mlp1_bias;
        std::vector<float> value_proj_weight, value_proj_bias;
    };
    std::vector<LayerWeights> layers;

    bool is_valid() const {
        return num_layers > 0 && !layers.empty() && src_dim > 0 &&
               tgt_dim > 0 && hidden_dim > 0;
    }
};

// ============================================================================
// Auto converter
// ============================================================================
class KVAutoConverter {
   public:
    using PutFunc = std::function<int(const std::string&, void*, size_t)>;
    using BatchPutFunc = std::function<int(const std::vector<std::string>&,
                                           const std::vector<void*>&,
                                           const std::vector<size_t>&)>;

    static KVAutoConverter& instance();

    void init(int workers = 2);
    void shutdown();

    bool load_config(const std::string& config_file);

    // Parse buffer_size from config JSON (returns kC2cDefaultBufferSize if
    // absent)
    static size_t parse_buffer_size(const std::string& config_json);

    void add_model(const KVModelInfo& info);
    void add_rule(const KVConversionRule& rule);
    void set_put_func(PutFunc fn) { put_fn_ = fn; }
    void set_batch_put_func(BatchPutFunc fn) { batch_put_fn_ = fn; }

    // Single key entry point (called from put_from)
    void on_put(const std::string& key, const void* data, size_t size);

    // Batch entry point: groups by (rule, is_key), merges into large matrix for
    // sgemm
    void on_batch_put(const std::vector<std::string>& keys,
                      const std::vector<void*>& buffers,
                      const std::vector<size_t>& sizes);

    bool is_enabled() const { return running_; }

   private:
    KVAutoConverter() = default;

    struct Task {
        std::string src_key, tgt_key;
        std::vector<uint8_t> data;
        KVModelInfo src_info, tgt_info;
        const KVConversionRule* rule;
    };

    void worker();
    void convert(Task& t);
    void convert_batch(std::vector<Task>& tasks);

    // Match key against source_model_name substring
    const KVConversionRule* match_rule(const std::string& key);

    bool load_projector_file(const std::string& path, KVConversionRule& rule);

    // URL-based projector download (libcurl)
    bool download_file(const std::string& url, const std::string& local_path);

    // Auto-fetch model params from HuggingFace config.json
    bool fetch_hf_model_info(const std::string& hf_model, KVModelInfo& info);

    // Get cache directory (~/.cache/mooncake/c2c/)
    static std::string get_cache_dir();

    // SHA256-prefix cache filename for URL
    static std::string url_to_cache_path(const std::string& url);

    void c2c_project_layer(const float* src, float* out,
                           const KVConversionRule::LayerWeights& weights,
                           bool is_key, int num_tokens, int src_dim,
                           int tgt_dim, int hidden_dim, float* h1, float* h2);

    std::unordered_map<std::string, KVModelInfo> models_;
    std::unordered_map<std::string, KVConversionRule> rules_;

    std::queue<std::vector<Task>> queue_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};
    PutFunc put_fn_;
    BatchPutFunc batch_put_fn_;

    // Progress tracking
    std::atomic<uint64_t> completed_keys_{0};
    std::atomic<uint64_t> completed_pages_{0};
    std::atomic<uint64_t> total_ms_{0};
    std::mutex log_mtx_;
    std::chrono::steady_clock::time_point last_log_time_;
};

}  // namespace mooncake
