#include "conductor/prefixindex/prefix_indexer.h"

#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

#include "conductor/prefixindex/hash_strategy.h"

namespace conductor {
namespace prefixindex {

namespace {

std::string ValidateContext(const ContextKey& context) {
    if (context.tenant_id.empty()) {
        return "tenant_id is required";
    }
    if (context.model_name.empty()) {
        return "model_name is required";
    }
    if (context.block_size <= 0) {
        return "block_size must be positive";
    }
    return "";
}

std::string ValidateLayout(const ContextKey& context,
                           int64_t effective_block_size,
                           std::optional<int64_t> cache_group) {
    if (auto error = ValidateContext(context); !error.empty()) {
        return error;
    }
    if (effective_block_size <= 0) {
        return "effective_block_size must be positive";
    }
    if (effective_block_size != context.block_size) {
        return "effective_block_size must equal ContextKey block_size";
    }
    if (cache_group.has_value() && *cache_group != 0) {
        return "only cache group 0 is supported";
    }
    return "";
}

std::string ValidateEngineOwner(const EngineOwner& owner) {
    if (owner.source_stream.empty()) {
        return "engine owner source_stream is required";
    }
    if (owner.instance_id.empty()) {
        return "engine owner instance_id is required";
    }
    if (owner.dp_rank < 0) {
        return "engine owner dp_rank must be non-negative";
    }
    return "";
}

std::string ValidateSharedOwner(const SharedObjectOwner& owner) {
    if (owner.source_stream.empty()) {
        return "shared owner source_stream is required";
    }
    if (owner.backend_id.empty()) {
        return "shared owner backend_id is required";
    }
    if (owner.object_id.empty()) {
        return "shared owner object_id is required";
    }
    return "";
}

std::string ValidateGpuMutation(const GpuMutation& mutation) {
    if (auto error =
            ValidateLayout(mutation.context, mutation.effective_block_size,
                           mutation.cache_group);
        !error.empty()) {
        return error;
    }
    return ValidateEngineOwner(mutation.owner);
}

std::string ValidateGpuClear(const GpuClear& clear) {
    if (auto error = ValidateLayout(clear.context, clear.effective_block_size,
                                    clear.cache_group);
        !error.empty()) {
        return error;
    }
    return ValidateEngineOwner(clear.owner);
}

bool IsSharedTier(StorageTier tier) {
    return tier == StorageTier::kCpu || tier == StorageTier::kDisk;
}

std::string ValidateSharedMutation(const SharedMutation& mutation) {
    if (auto error =
            ValidateLayout(mutation.context, mutation.effective_block_size,
                           mutation.cache_group);
        !error.empty()) {
        return error;
    }
    if (!IsSharedTier(mutation.tier)) {
        return "shared mutation tier must be CPU or DISK";
    }
    return ValidateSharedOwner(mutation.owner);
}

std::string ValidateSharedClear(const SharedClear& clear) {
    if (auto error = ValidateLayout(clear.context, clear.effective_block_size,
                                    clear.cache_group);
        !error.empty()) {
        return error;
    }
    if (clear.tier.has_value() && !IsSharedTier(*clear.tier)) {
        return "shared clear tier must be CPU, DISK, or omitted";
    }
    return ValidateSharedOwner(clear.owner);
}

std::set<SharedObjectOwner>& SharedOwners(BlockPresence& presence,
                                          StorageTier tier) {
    return tier == StorageTier::kCpu ? presence.cpu_owners
                                     : presence.disk_owners;
}

void EraseEmptyBlocks(ContextState& state) {
    std::erase_if(state.blocks,
                  [](const auto& item) { return item.second.Empty(); });
}

int64_t TokensForBlocks(size_t block_count, int64_t block_size) {
    const uint64_t max_blocks =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max() / block_size);
    if (block_count > max_blocks) {
        return std::numeric_limits<int64_t>::max();
    }
    return static_cast<int64_t>(block_count) * block_size;
}

}  // namespace

RegistrationResult PrefixCacheTable::ValidateRegistration(
    const EngineRegistration& registration) {
    if (auto error = ValidateLayout(registration.context,
                                    registration.effective_block_size,
                                    registration.cache_group);
        !error.empty()) {
        return {.error = std::move(error)};
    }
    if (registration.instance_id.empty()) {
        return {.error = "instance_id is required"};
    }
    if (registration.dp_rank < 0) {
        return {.error = "dp_rank must be non-negative"};
    }
    if (auto error = ValidateHashProfile(registration.profile);
        !error.empty()) {
        return {.error = std::move(error)};
    }
    return {};
}

RegistrationResult PrefixCacheTable::Register(
    const EngineRegistration& registration) {
    if (auto validation = ValidateRegistration(registration);
        !validation.error.empty()) {
        return validation;
    }

    auto candidate = std::make_shared<ContextState>(registration.profile);
    candidate->instance_ranks[registration.instance_id].insert(
        registration.dp_rank);

    std::shared_ptr<ContextState> state;
    {
        std::unique_lock map_lock(context_map_mutex_);
        auto [it, inserted] =
            contexts_.try_emplace(registration.context, std::move(candidate));
        if (inserted) {
            return {.inserted = true, .error = ""};
        }
        state = it->second;
    }

    std::unique_lock state_lock(state->mutex);
    if (state->profile != registration.profile) {
        return {.error =
                    "registration conflicts with the ContextKey hash profile"};
    }
    const bool inserted = state->instance_ranks[registration.instance_id]
                              .insert(registration.dp_rank)
                              .second;
    return {.inserted = inserted, .error = ""};
}

std::shared_ptr<ContextState> PrefixCacheTable::LoadContextState(
    const ContextKey& context) const {
    std::shared_lock map_lock(context_map_mutex_);
    auto it = contexts_.find(context);
    return it == contexts_.end() ? nullptr : it->second;
}

std::string PrefixCacheTable::ValidateProfileBinding(
    const ContextKey& context, const HashProfile& profile) const {
    if (auto error = ValidateContext(context); !error.empty()) {
        return error;
    }
    if (auto error = ValidateHashProfile(profile); !error.empty()) {
        return error;
    }
    auto state = LoadContextState(context);
    if (!state) {
        return "ContextKey is not registered";
    }

    std::shared_lock state_lock(state->mutex);
    if (state->profile != profile) {
        return "hash profile conflicts with the registered ContextKey profile";
    }
    return "";
}

std::string PrefixCacheTable::Unregister(const ContextKey& context,
                                         const std::string& instance_id,
                                         int64_t dp_rank) {
    if (auto error = ValidateContext(context); !error.empty()) {
        return error;
    }
    if (instance_id.empty()) {
        return "instance_id is required";
    }
    if (dp_rank < 0) {
        return "dp_rank must be non-negative";
    }

    auto state = LoadContextState(context);
    if (!state) {
        return "";
    }

    std::unique_lock state_lock(state->mutex);
    auto instance = state->instance_ranks.find(instance_id);
    if (instance != state->instance_ranks.end()) {
        instance->second.erase(dp_rank);
        if (instance->second.empty()) {
            state->instance_ranks.erase(instance);
        }
    }

    for (auto& [unused_prefix, presence] : state->blocks) {
        (void)unused_prefix;
        std::erase_if(presence.gpu_owners, [&](const EngineOwner& owner) {
            return owner.instance_id == instance_id && owner.dp_rank == dp_rank;
        });
    }
    EraseEmptyBlocks(*state);
    return "";
}

std::string PrefixCacheTable::StoreGpu(const GpuMutation& mutation) {
    if (auto error = ValidateGpuMutation(mutation); !error.empty()) {
        return error;
    }
    auto state = LoadContextState(mutation.context);
    if (!state) {
        return "ContextKey is not registered";
    }

    std::unique_lock state_lock(state->mutex);
    auto instance = state->instance_ranks.find(mutation.owner.instance_id);
    if (instance == state->instance_ranks.end() ||
        !instance->second.contains(mutation.owner.dp_rank)) {
        return "engine owner instance/rank is not registered";
    }
    for (ProjectedPrefix prefix : mutation.prefixes) {
        state->blocks[prefix].gpu_owners.insert(mutation.owner);
    }
    return "";
}

std::string PrefixCacheTable::RemoveGpu(const GpuMutation& mutation) {
    if (auto error = ValidateGpuMutation(mutation); !error.empty()) {
        return error;
    }
    auto state = LoadContextState(mutation.context);
    if (!state) {
        return "";
    }

    std::unique_lock state_lock(state->mutex);
    for (ProjectedPrefix prefix : mutation.prefixes) {
        auto block = state->blocks.find(prefix);
        if (block != state->blocks.end()) {
            block->second.gpu_owners.erase(mutation.owner);
        }
    }
    EraseEmptyBlocks(*state);
    return "";
}

std::string PrefixCacheTable::ClearGpu(const GpuClear& clear) {
    if (auto error = ValidateGpuClear(clear); !error.empty()) {
        return error;
    }
    auto state = LoadContextState(clear.context);
    if (!state) {
        return "";
    }

    std::unique_lock state_lock(state->mutex);
    for (auto& [unused_prefix, presence] : state->blocks) {
        (void)unused_prefix;
        presence.gpu_owners.erase(clear.owner);
    }
    EraseEmptyBlocks(*state);
    return "";
}

std::string PrefixCacheTable::StoreShared(const SharedMutation& mutation) {
    if (auto error = ValidateSharedMutation(mutation); !error.empty()) {
        return error;
    }
    auto state = LoadContextState(mutation.context);
    if (!state) {
        return "ContextKey is not registered";
    }

    std::unique_lock state_lock(state->mutex);
    for (ProjectedPrefix prefix : mutation.prefixes) {
        SharedOwners(state->blocks[prefix], mutation.tier)
            .insert(mutation.owner);
    }
    return "";
}

std::string PrefixCacheTable::RemoveShared(const SharedMutation& mutation) {
    if (auto error = ValidateSharedMutation(mutation); !error.empty()) {
        return error;
    }
    auto state = LoadContextState(mutation.context);
    if (!state) {
        return "";
    }

    std::unique_lock state_lock(state->mutex);
    for (ProjectedPrefix prefix : mutation.prefixes) {
        auto block = state->blocks.find(prefix);
        if (block != state->blocks.end()) {
            SharedOwners(block->second, mutation.tier).erase(mutation.owner);
        }
    }
    EraseEmptyBlocks(*state);
    return "";
}

std::string PrefixCacheTable::ClearShared(const SharedClear& clear) {
    if (auto error = ValidateSharedClear(clear); !error.empty()) {
        return error;
    }
    auto state = LoadContextState(clear.context);
    if (!state) {
        return "";
    }

    std::unique_lock state_lock(state->mutex);
    for (auto& [unused_prefix, presence] : state->blocks) {
        (void)unused_prefix;
        if (!clear.tier.has_value() || *clear.tier == StorageTier::kCpu) {
            presence.cpu_owners.erase(clear.owner);
        }
        if (!clear.tier.has_value() || *clear.tier == StorageTier::kDisk) {
            presence.disk_owners.erase(clear.owner);
        }
    }
    EraseEmptyBlocks(*state);
    return "";
}

std::map<std::string, CacheHitResult> PrefixCacheTable::Query(
    const ContextKey& context, std::span<const int32_t> token_ids,
    std::optional<std::string> cache_salt,
    std::optional<std::string> instance_filter) const {
    std::map<std::string, CacheHitResult> results;
    auto state = LoadContextState(context);
    if (!state) {
        return results;
    }

    std::shared_lock state_lock(state->mutex);
    std::map<std::string, const std::set<int64_t>*> selected_instances;
    if (instance_filter.has_value()) {
        auto instance = state->instance_ranks.find(*instance_filter);
        if (instance == state->instance_ranks.end()) {
            return results;
        }
        selected_instances.emplace(instance->first, &instance->second);
    } else {
        for (const auto& [instance_id, ranks] : state->instance_ranks) {
            selected_instances.emplace(instance_id, &ranks);
        }
    }

    std::string strategy_error;
    auto strategy = CreateHashStrategy(state->profile, &strategy_error);
    if (!strategy) {
        LOG(ERROR) << "Registered hash profile became invalid: "
                   << strategy_error;
        return results;
    }

    std::vector<HashBlock> hashes;
    if (auto error = strategy->Compute(context, token_ids,
                                       std::move(cache_salt), &hashes);
        !error.empty()) {
        LOG(ERROR) << "Query hash computation failed: " << error;
        return results;
    }

    auto advance_cursor = [&](size_t& cursor, const auto& present) {
        while (cursor < hashes.size()) {
            auto block = state->blocks.find(hashes[cursor].projected);
            if (block == state->blocks.end() || !present(block->second)) {
                break;
            }
            ++cursor;
        }
    };

    for (const auto& [instance_id, ranks] : selected_instances) {
        CacheHitResult result;

        for (int64_t rank : *ranks) {
            auto gpu_present = [&](const BlockPresence& block) {
                return std::any_of(
                    block.gpu_owners.begin(), block.gpu_owners.end(),
                    [&](const EngineOwner& owner) {
                        return owner.instance_id == instance_id &&
                               owner.dp_rank == rank;
                    });
            };

            size_t cursor = 0;
            advance_cursor(cursor, gpu_present);

            RankCacheHitResult rank_match;
            rank_match.gpu = TokensForBlocks(cursor, context.block_size);

            advance_cursor(cursor, [](const BlockPresence& block) {
                return !block.cpu_owners.empty();
            });
            rank_match.cpu = TokensForBlocks(cursor, context.block_size);

            advance_cursor(cursor, [](const BlockPresence& block) {
                return !block.disk_owners.empty();
            });
            rank_match.disk = TokensForBlocks(cursor, context.block_size);

            result.dp.emplace(rank, rank_match.gpu);
            result.rank_matches.emplace(rank, rank_match);
            result.gpu = std::max(result.gpu, rank_match.gpu);
            result.cpu = std::max(result.cpu, rank_match.cpu);
            result.disk = std::max(result.disk, rank_match.disk);
        }
        result.longest_match_tokens = result.disk;
        results.emplace(instance_id, std::move(result));
    }
    return results;
}

GlobalView PrefixCacheTable::GetGlobalView() const {
    GlobalView view;
    std::vector<std::pair<ContextKey, std::shared_ptr<ContextState>>> contexts;
    {
        std::shared_lock map_lock(context_map_mutex_);
        contexts.reserve(contexts_.size());
        for (const auto& item : contexts_) {
            contexts.push_back(item);
        }
    }

    view.context_count = static_cast<int32_t>(contexts.size());
    view.contexts.reserve(contexts.size());
    for (const auto& [context, state] : contexts) {
        std::shared_lock state_lock(state->mutex);
        ContextView context_view;
        context_view.context = context;
        context_view.profile = state->profile;
        context_view.instance_ranks = state->instance_ranks;
        context_view.prefix_count = state->blocks.size();
        view.contexts.push_back(std::move(context_view));
    }
    return view;
}

}  // namespace prefixindex
}  // namespace conductor
