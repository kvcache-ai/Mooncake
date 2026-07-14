#pragma once

#include <compare>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace conductor {
namespace prefixindex {

struct ContextKey {
    std::string tenant_id;
    std::string model_name;
    std::string lora_name;
    int64_t block_size = 0;

    bool operator==(const ContextKey&) const = default;
};

struct HashProfile {
    std::string strategy;
    std::string algorithm;
    std::string root_digest;
    std::string index_projection;

    bool operator==(const HashProfile&) const = default;
};

struct ProjectedPrefix {
    uint64_t value = 0;

    auto operator<=>(const ProjectedPrefix&) const = default;
};

enum class StorageTier { kGpu, kCpu, kDisk };

struct EngineOwner {
    std::string source_stream;
    std::string instance_id;
    int64_t dp_rank = 0;

    auto operator<=>(const EngineOwner&) const = default;
};

struct SharedObjectOwner {
    std::string source_stream;
    std::string backend_id;
    std::string object_id;

    auto operator<=>(const SharedObjectOwner&) const = default;
};

struct EngineRegistration {
    ContextKey context;
    HashProfile profile;
    std::string instance_id;
    int64_t dp_rank = 0;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

struct GpuMutation {
    ContextKey context;
    std::vector<ProjectedPrefix> prefixes;
    EngineOwner owner;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

struct SharedMutation {
    ContextKey context;
    std::vector<ProjectedPrefix> prefixes;
    StorageTier tier = StorageTier::kCpu;
    SharedObjectOwner owner;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

struct GpuClear {
    ContextKey context;
    EngineOwner owner;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

struct SharedClear {
    ContextKey context;
    SharedObjectOwner owner;
    std::optional<StorageTier> tier;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

}  // namespace prefixindex
}  // namespace conductor

template <>
struct std::hash<conductor::prefixindex::ContextKey> {
    size_t operator()(
        const conductor::prefixindex::ContextKey& context) const noexcept {
        size_t seed = 0;
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<std::string>{}(context.tenant_id));
        combine(std::hash<std::string>{}(context.model_name));
        combine(std::hash<std::string>{}(context.lora_name));
        combine(std::hash<int64_t>{}(context.block_size));
        return seed;
    }
};

template <>
struct std::hash<conductor::prefixindex::ProjectedPrefix> {
    size_t operator()(
        conductor::prefixindex::ProjectedPrefix prefix) const noexcept {
        return std::hash<uint64_t>{}(prefix.value);
    }
};
