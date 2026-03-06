#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {
namespace engram {

/**
 * Configuration for Engram module.
 */
struct EngramConfig {
    std::string tokenizer_name_or_path = "deepseek-ai/DeepSeek-V3";
    std::vector<int64_t> engram_vocab_size = {129280 * 5, 129280 * 5};
    int max_ngram_size = 3;
    int n_embed_per_ngram = 512;
    int n_head_per_ngram = 8;
    std::vector<int> layer_ids = {1, 15};
    int64_t pad_id = 2;
    int seed = 0;
    int kernel_size = 4;
};

/**
 * Configuration for backbone Transformer.
 */
struct BackboneConfig {
    int hidden_size = 1024;
    int hc_mult = 4;
    int vocab_size = 129280;
    int num_layers = 30;
};

}  // namespace engram
}  // namespace mooncake
