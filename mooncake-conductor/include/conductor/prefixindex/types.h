#pragma once

#include <compare>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>
#include <variant>

#include "conductor/common/types.h"

namespace mooncake::conductor::prefixindex {

struct ContextKey {
    std::string tenant_id;
    std::string model_name;
    std::string lora_name;
    int64_t block_size = 0;

    bool operator==(const ContextKey&) const = default;
};

using HashProfile = common::ResolvedHashProfile;

struct ProjectedPrefix {
    uint64_t value = 0;

    auto operator<=>(const ProjectedPrefix&) const = default;
};

enum class StorageTier { kNpu, kCpuLocal, kCpuShare, kDisk };

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

// A storage medium does not imply that another engine can access it.
using TierOwner = std::variant<EngineOwner, SharedObjectOwner>;

struct EngineRegistration {
    ContextKey context;
    HashProfile profile;
    std::string instance_id;
    int64_t dp_rank = 0;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

struct EngineMutation {
    ContextKey context;
    std::vector<ProjectedPrefix> prefixes;
    EngineOwner owner;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
    StorageTier tier = StorageTier::kNpu;
};

struct SharedMutation {
    ContextKey context;
    std::vector<ProjectedPrefix> prefixes;
    StorageTier tier = StorageTier::kCpuShare;
    SharedObjectOwner owner;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

struct EngineClear {
    ContextKey context;
    EngineOwner owner;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
    // Explicit nullopt clears all local tiers; the default clears only NPU.
    // Shared Store ownership is never affected.
    std::optional<StorageTier> tier = StorageTier::kNpu;
};

struct SharedClear {
    ContextKey context;
    SharedObjectOwner owner;
    std::optional<StorageTier> tier;
    int64_t effective_block_size = 0;
    std::optional<int64_t> cache_group;
};

}  // namespace mooncake::conductor::prefixindex

template <>
struct std::hash<mooncake::conductor::prefixindex::ContextKey> {
    size_t operator()(const mooncake::conductor::prefixindex::ContextKey&
                          context) const noexcept {
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
struct std::hash<mooncake::conductor::prefixindex::ProjectedPrefix> {
    size_t operator()(mooncake::conductor::prefixindex::ProjectedPrefix prefix)
        const noexcept {
        return std::hash<uint64_t>{}(prefix.value);
    }
};
