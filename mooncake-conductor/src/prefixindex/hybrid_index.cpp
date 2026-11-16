#include "conductor/prefixindex/hybrid_index.h"

#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <limits>
#include <shared_mutex>
#include <utility>

#include "conductor/prefixindex/hash_strategy.h"
#include "conductor/prefixindex/prefix_indexer.h"

namespace mooncake::conductor::prefixindex {

std::string HybridIndex::ActivateDeployment(const SourceLayout& binding,
                                            const std::string& instance,
                                            int64_t rank) {
    const auto reader = readers_.find({instance, rank});
    if (reader == readers_.end()) return "deployment owner is not registered";
    if (binding.layout->id != layout_->id ||
        binding.layout->hash_unit_tokens != layout_->hash_unit_tokens)
        return "deployment conflicts with the registered model/hash unit";
    if (deployment_) {
        if (*binding.layout != *layout_ ||
            binding.groups != deployment_->groups ||
            binding.objects != deployment_->objects ||
            binding.storage_domain != deployment_->storage_domain)
            return "conflicting deployment declaration";
    } else {
        if (layout_->unavailable_reason.empty() || records_ != 0)
            return "only an unavailable context may activate its deployment";
        layout_ = binding.layout;
        deployment_ = binding;
    }
    reader->second = binding.readable_domains;
    return "";
}

std::string HybridIndex::Register(const std::string& instance, int64_t rank,
                                  const std::set<std::string>& domains) {
    const auto key = std::make_pair(instance, rank);
    auto [it, inserted] = readers_.emplace(key, domains);
    if (!inserted && it->second != domains)
        return "conflicting shared-cache reader domains";
    return "";
}

HybridIndex::ReplicaKey HybridIndex::KeyFor(const GroupMutation& mutation) {
    auto owner = mutation.owner;
    if (auto* shared = std::get_if<SharedObjectOwner>(&owner))
        shared->object_id.clear();
    return {mutation.tier, std::move(owner), mutation.storage_domain,
            mutation.replica};
}

std::string HybridIndex::Validate(const GroupMutation& mutation) const {
    if (std::find(layout_->hash_domains.begin(), layout_->hash_domains.end(),
                  mutation.hash_domain) == layout_->hash_domains.end())
        return "undeclared hash domain";
    if (mutation.group >= layout_->groups.size()) return "unknown group slot";
    if (mutation.part &&
        *mutation.part >= layout_->groups[mutation.group].parts.size())
        return "unknown part slot";
    if (mutation.replica.empty()) return "replica must not be empty";
    if (const auto* engine = std::get_if<EngineOwner>(&mutation.owner)) {
        if (engine->source_stream.empty() || engine->instance_id.empty() ||
            engine->dp_rank < 0)
            return "invalid engine owner";
        if (!readers_.contains({engine->instance_id, engine->dp_rank}))
            return "engine owner is not registered";
        if (mutation.tier != StorageTier::kDevice &&
            mutation.tier != StorageTier::kCpuLocal &&
            mutation.tier != StorageTier::kDisk)
            return "invalid engine tier";
    } else {
        const auto& shared = std::get<SharedObjectOwner>(mutation.owner);
        if (shared.source_stream.empty() || shared.backend_id.empty() ||
            shared.object_id.empty() || mutation.storage_domain.empty())
            return "invalid shared object owner/domain";
        if (mutation.tier != StorageTier::kCpuShare &&
            mutation.tier != StorageTier::kDisk)
            return "invalid shared tier";
        if (!mutation.part)
            return "shared objects must identify their physical part";
    }
    return "";
}

void HybridIndex::Erase(DomainPrefix prefix) {
    auto it = blocks_.find(prefix);
    if (it == blocks_.end()) return;
    records_ -= it->second.records;
    write_order_.erase(it->second.order);
    blocks_.erase(it);
}

void HybridIndex::Store(const GroupMutation& mutation) {
    const auto key = KeyFor(mutation);
    const auto* shared = std::get_if<SharedObjectOwner>(&mutation.owner);
    const std::string object = shared ? shared->object_id : "engine";
    for (auto hash : mutation.prefixes) {
        const DomainPrefix domain_hash{mutation.hash_domain, hash};
        auto [entry, inserted] = blocks_.try_emplace(domain_hash);
        auto& prefix = entry->second;
        if (inserted) {
            prefix.groups.resize(layout_->groups.size());
            write_order_.push_front(domain_hash);
            prefix.order = write_order_.begin();
        } else {
            write_order_.splice(write_order_.begin(), write_order_,
                                prefix.order);
        }
        auto& replica = prefix.groups[mutation.group][key];
        if (replica.parts.empty())
            replica.parts.resize(layout_->groups[mutation.group].parts.size());
        const size_t first = mutation.part.value_or(0);
        const size_t end = mutation.part ? first + 1 : replica.parts.size();
        for (size_t part = first; part < end; ++part) {
            auto& objects = replica.parts[part];
            const bool was_empty = objects.empty();
            if (objects.insert(object).second) {
                ++records_;
                ++prefix.records;
                replica.present_parts += was_empty;
            }
        }
    }
    while (records_ > layout_->max_records && !write_order_.empty()) {
        Erase(write_order_.back());
    }
}

void HybridIndex::Remove(const GroupMutation& mutation) {
    const auto key = KeyFor(mutation);
    const auto* shared = std::get_if<SharedObjectOwner>(&mutation.owner);
    const std::string object = shared ? shared->object_id : "engine";
    for (auto hash : mutation.prefixes) {
        const DomainPrefix domain_hash{mutation.hash_domain, hash};
        auto entry = blocks_.find(domain_hash);
        if (entry == blocks_.end()) continue;
        auto& prefix = entry->second;
        auto& group = prefix.groups[mutation.group];
        auto copy = group.find(key);
        if (copy == group.end()) continue;
        auto& replica = copy->second;
        const size_t first = mutation.part.value_or(0);
        const size_t end = mutation.part ? first + 1 : replica.parts.size();
        for (size_t part = first; part < end; ++part) {
            auto& objects = replica.parts[part];
            if (objects.erase(object)) {
                --records_;
                --prefix.records;
                replica.present_parts -= objects.empty();
            }
        }
        if (replica.present_parts == 0) group.erase(copy);
        if (prefix.records == 0) Erase(domain_hash);
    }
}

template <typename Predicate>
void HybridIndex::EraseIf(Predicate matches) {
    for (auto entry = blocks_.begin(); entry != blocks_.end();) {
        auto& prefix = entry->second;
        for (auto& group : prefix.groups) {
            for (auto copy = group.begin(); copy != group.end();) {
                if (!matches(copy->first)) {
                    ++copy;
                    continue;
                }
                size_t removed = 0;
                for (const auto& part : copy->second.parts)
                    removed += part.size();
                records_ -= removed;
                prefix.records -= removed;
                copy = group.erase(copy);
            }
        }
        if (prefix.records == 0) {
            write_order_.erase(prefix.order);
            entry = blocks_.erase(entry);
        } else
            ++entry;
    }
}

void HybridIndex::Clear(const std::string& source,
                        const std::optional<std::string>& backend,
                        std::optional<StorageTier> tier) {
    EraseIf([&](const ReplicaKey& key) {
        const bool source_matches = std::visit(
            [&](const auto& owner) { return owner.source_stream == source; },
            key.owner);
        const auto* shared = std::get_if<SharedObjectOwner>(&key.owner);
        return source_matches && (!tier || key.tier == *tier) &&
               (!backend || (shared && shared->backend_id == *backend));
    });
}

void HybridIndex::Unregister(const std::string& instance, int64_t rank) {
    readers_.erase({instance, rank});
    // Shared objects outlive engine registrations. Remove only this rank's
    // engine contributions, including all of its source streams and groups.
    EraseIf([&](const ReplicaKey& key) {
        const auto* owner = std::get_if<EngineOwner>(&key.owner);
        return owner && owner->instance_id == instance &&
               owner->dp_rank == rank;
    });
}

uint8_t HybridIndex::Presence(ProjectedPrefix hash, HashDomain domain,
                              size_t group, const std::string& instance,
                              int64_t rank,
                              const std::set<std::string>& domains) const {
    const auto block = blocks_.find({domain, hash});
    if (block == blocks_.end()) return 0;
    uint8_t mask = 0;
    for (const auto& [key, replica] : block->second.groups[group]) {
        if (replica.present_parts != replica.parts.size()) continue;
        if (const auto* owner = std::get_if<EngineOwner>(&key.owner)) {
            if (owner->instance_id != instance || owner->dp_rank != rank)
                continue;
        } else if (!domains.contains(key.storage_domain))
            continue;
        // The four bits represent cumulative permitted-medium sets.
        const auto level = static_cast<unsigned>(key.tier);
        mask |= static_cast<uint8_t>((0x0fU << level) & 0x0fU);
    }
    return mask;
}

std::map<std::string, CacheHitResult> HybridIndex::Query(
    const ContextState& state, const ContextKey& context,
    std::span<const int32_t> token_ids, const std::optional<std::string>& salt,
    const std::optional<std::string>& filter) const {
    std::map<std::string, CacheHitResult> results;
    std::shared_lock layout_lock(state.mutex);
    const auto layout = layout_;
    // Requires the state lock. A complete required group cannot be present
    // in an empty index, so cold queries need no token hashing at all.
    const auto zero_results = [&]() {
        for (const auto& [instance, ranks] : state.instance_ranks) {
            if (filter && *filter != instance) continue;
            auto& result = results[instance];
            for (auto rank : ranks) {
                result.rank_matches.emplace(rank, RankCacheHitResult{});
                result.dp.emplace(rank, 0);
            }
        }
        return results;
    };
    if (!layout->unavailable_reason.empty() || blocks_.empty())
        return zero_results();
    layout_lock.unlock();
    ContextKey hash_context = context;
    hash_context.block_size = layout->hash_unit_tokens;
    const bool bigram = state.profile.strategy == "sglang_bigram";
    const size_t logical_tokens =
        bigram && !token_ids.empty() ? token_ids.size() - 1 : token_ids.size();
    // Only complete hash units are accepted for hybrid recovery. The ordinary
    // SGLang path keeps its existing partial-page semantics.
    const size_t count =
        logical_tokens / static_cast<size_t>(layout->hash_unit_tokens);
    std::unique_ptr<HashChain> raw_chain;
    if (layout->hash_domains == std::vector<HashDomain>{HashDomain::kRaw}) {
        std::string error;
        auto strategy = CreateHashStrategy(state.profile, &error);
        if (strategy)
            raw_chain =
                strategy->CreateChain(hash_context, token_ids, salt, &error);
        if (!raw_chain || raw_chain->BlockCount() < count) return {};
        const auto dense = std::find_if(
            layout->groups.begin(), layout->groups.end(),
            [](const auto& group) {
                return group.required && group.match == MatchPolicy::kDense;
            });
        if (dense != layout->groups.end()) {
            const size_t first =
                dense->block_tokens / layout->hash_unit_tokens - 1;
            // Dense recovery always needs its first physical block, even if
            // checkpoint/window groups have later entries. Hash outside the
            // lock; the complete scan rechecks presence under its shared lock.
            const auto* hash =
                first < count ? raw_chain->At(first, &error) : nullptr;
            std::shared_lock probe_lock(state.mutex);
            if (first >= count) return zero_results();
            if (!hash) return {};
            const size_t slot =
                static_cast<size_t>(dense - layout->groups.begin());
            bool any = false;
            for (const auto& [instance, ranks] : state.instance_ranks) {
                if (filter && *filter != instance) continue;
                for (auto rank : ranks) {
                    const auto reader = readers_.find({instance, rank});
                    if (reader != readers_.end() &&
                        Presence(hash->projected, HashDomain::kRaw, slot,
                                 instance, rank, reader->second) != 0) {
                        any = true;
                        break;
                    }
                }
                if (any) break;
            }
            if (!any) return zero_results();
        }
    }
    std::map<HashDomain, std::vector<HashBlock>> hashes;
    for (auto domain : layout->hash_domains) {
        if (domain == HashDomain::kRaw && raw_chain) {
            auto& chain = hashes[domain];
            chain.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                std::string error;
                const auto* hash = raw_chain->At(i, &error);
                if (!hash) return {};
                chain.push_back(*hash);
            }
        } else {
            std::string error;
            auto strategy = CreateHashStrategy(state.profile, &error, domain,
                                               layout->sglang_extra_key);
            if (strategy)
                error = strategy->Compute(hash_context, token_ids, salt,
                                          &hashes[domain]);
            if (!strategy || !error.empty()) {
                LOG(ERROR) << "Special-KV query hash failed: " << error;
                return {};
            }
        }
        if (hashes[domain].size() < count) return {};
    }
    std::shared_lock lock(state.mutex);
    for (const auto& [instance, ranks] : state.instance_ranks) {
        if (filter && *filter != instance) continue;
        auto& result = results[instance];
        for (auto rank : ranks) {
            const auto reader = readers_.find({instance, rank});
            if (reader == readers_.end()) continue;
            std::vector<uint8_t> joint(count, 0x0f);
            for (size_t slot = 0; slot < layout->groups.size(); ++slot) {
                const auto& group = layout->groups[slot];
                if (!group.required) continue;
                const size_t stride = static_cast<size_t>(
                    group.block_tokens / layout->hash_unit_tokens);
                uint8_t dense = 0x0f;
                std::array<size_t, 4> runs{};
                const size_t window_blocks =
                    group.trailing_blocks > 0
                        ? static_cast<size_t>(group.trailing_blocks)
                    : group.match == MatchPolicy::kWindow
                        ? static_cast<size_t>(
                              (group.window_tokens - 1) / group.block_tokens +
                              ((group.window_tokens - 1) % group.block_tokens !=
                               0))
                        : 0;
                for (size_t i = 0; i < count; ++i) {
                    const int64_t boundary =
                        static_cast<int64_t>(i + 1) * layout->hash_unit_tokens;
                    uint8_t valid = 0;
                    uint8_t available = 0;
                    if (group.match == MatchPolicy::kCheckpoint ||
                        (i + 1) % stride == 0) {
                        for (const auto& [domain, chain] : hashes) {
                            available |=
                                Presence(chain[i].projected, domain, slot,
                                         instance, rank, reader->second);
                        }
                    }
                    if (group.match == MatchPolicy::kCheckpoint) {
                        if (boundary % group.alignment_tokens == 0)
                            valid = available;
                    } else if ((i + 1) % stride == 0) {
                        if (group.match == MatchPolicy::kDense) {
                            dense &= available;
                            valid = dense;
                        } else {
                            const size_t required =
                                std::min(std::max<size_t>(1, window_blocks),
                                         (i + 1) / stride);
                            for (size_t level = 0; level < runs.size();
                                 ++level) {
                                runs[level] = (available & (1U << level))
                                                  ? runs[level] + 1
                                                  : 0;
                                if (runs[level] >= required)
                                    valid |= static_cast<uint8_t>(1U << level);
                            }
                        }
                        if (boundary % group.alignment_tokens != 0) valid = 0;
                    }
                    joint[i] &= valid;
                }
            }
            std::array<int64_t, 4> longest{};
            for (size_t i = 0; i < count; ++i) {
                const int64_t boundary =
                    static_cast<int64_t>(i + 1) * layout->hash_unit_tokens;
                if (boundary % layout->alignment_tokens != 0) continue;
                for (size_t tier = 0; tier < longest.size(); ++tier) {
                    if (joint[i] & (1U << tier)) longest[tier] = boundary;
                }
            }
            RankCacheHitResult matched{longest[0], longest[1], longest[2],
                                       longest[3]};
            result.rank_matches.emplace(rank, matched);
            result.dp.emplace(rank, matched.device);
            result.device = std::max(result.device, matched.device);
            result.cpu_local = std::max(result.cpu_local, matched.cpu_local);
            result.cpu_share = std::max(result.cpu_share, matched.cpu_share);
            result.disk = std::max(result.disk, matched.disk);
        }
        result.longest_match_tokens = result.disk;
    }
    return results;
}

}  // namespace mooncake::conductor::prefixindex
