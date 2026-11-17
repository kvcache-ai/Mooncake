#include "conductor/kvevent/event_manager.h"

#include <string_view>

#include "ascii_string.h"
#include "conductor/kvevent/object_key_parser.h"
#include "conductor/prefixindex/hybrid_index.h"
#include "conductor/prefixindex/layout_parser.h"

namespace mooncake::conductor::kvevent {
namespace {

using prefixindex::GroupMutation;
using prefixindex::LayoutSource;
using prefixindex::MatchPolicy;
using prefixindex::StorageTier;

prefixindex::ContextKey Context(const common::ServiceConfig& service) {
    return {service.tenant_id, service.model_name, service.lora_name,
            service.block_size};
}

prefixindex::EngineOwner Owner(const common::ServiceConfig& service) {
    return {service.endpoint, service.instance_id, service.dp_rank};
}

std::optional<StorageTier> EngineTier(
    const std::optional<std::string>& medium,
    const prefixindex::SourceLayout& binding) {
    const auto name = AsciiToLower(medium.value_or(""));
    if (name == "gpu" ||
        (name.empty() && binding.source == LayoutSource::kSglang))
        return StorageTier::kDevice;
    if (binding.source == LayoutSource::kSglang) {
        if (name == "cpu" || name == "cpu_pinned")
            return StorageTier::kCpuLocal;
        if (name == "disk") return StorageTier::kDisk;
    }
    return std::nullopt;
}

std::optional<StorageTier> ObjectTier(
    const std::optional<std::string>& medium) {
    const auto name = AsciiToLower(medium.value_or(""));
    if (name == "cpu" || name == "cpu_pinned") return StorageTier::kCpuShare;
    if (name == "disk") return StorageTier::kDisk;
    return std::nullopt;
}

std::string ResolveGroup(const prefixindex::SourceLayout& binding,
                         std::optional<int64_t> id, size_t* slot) {
    const auto group = binding.groups.find(id.value_or(0));
    if (group == binding.groups.end())
        return "event group is not present in the deployment mapping";
    *slot = group->second;
    return "";
}

std::string ObjectMutation(const common::ServiceConfig& service,
                           const prefixindex::SourceLayout& binding,
                           const zmq::MooncakeEventFields& fields,
                           const zmq::MooncakeObjectFields& object,
                           GroupMutation* mutation) {
    if (fields.backend_id.empty() || !object.object_key ||
        object.object_key->empty())
        return "backend_id and object_key are required";
    if (fields.tenant_id != service.tenant_id ||
        (fields.model_name && *fields.model_name != service.model_name) ||
        (fields.lora_name && *fields.lora_name != service.lora_name))
        return "object context conflicts with registered layout";
    const std::string_view key = *object.object_key;
    const prefixindex::ObjectPartRule* selected = nullptr;
    for (const auto& rule : binding.objects) {
        if (key.size() <= rule.key_prefix.size() + rule.key_suffix.size() ||
            !key.starts_with(rule.key_prefix) ||
            !key.ends_with(rule.key_suffix))
            continue;
        if (selected) return "object key matches multiple layout rules";
        selected = &rule;
    }
    if (!selected) return "object key does not match the deployment layout";
    const auto hash = key.substr(
        selected->key_prefix.size(),
        key.size() - selected->key_prefix.size() - selected->key_suffix.size());
    // Rules isolate the exact hash field. Rank/group/schema strings remain in
    // the fixed prefix/suffix, so they cannot be mistaken for a cache hash.
    if (hash.size() != 64 && hash.size() != 16)
        return "object hash must be a 64- or 16-character hex digest";
    uint64_t projected = 0;
    const size_t start =
        service.hash_profile.strategy == "vllm_v1" ? hash.size() - 16 : 0;
    for (size_t i = 0; i < hash.size(); ++i) {
        const char ch = hash[i];
        const int digit = ch >= '0' && ch <= '9'   ? ch - '0'
                          : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                          : ch >= 'A' && ch <= 'F' ? ch - 'A' + 10
                                                   : -1;
        if (digit < 0) return "object hash contains non-hex characters";
        if (i >= start && i < start + 16)
            projected = (projected << 4) | static_cast<uint64_t>(digit);
    }
    if (!object.seq_hashes.empty() && (object.seq_hashes.size() != 1 ||
                                       object.seq_hashes.front() != projected))
        return "object hash conflicts with seq_hashes";
    mutation->context = Context(service);
    mutation->prefixes = {{projected}};
    mutation->group = selected->group;
    mutation->part = selected->part;
    mutation->owner = prefixindex::SharedObjectOwner{
        service.endpoint, fields.backend_id, *object.object_key};
    mutation->storage_domain = binding.storage_domain;
    mutation->replica = selected->replica;
    mutation->hash_domain = binding.hash_domain;
    return "";
}

}  // namespace

std::string KVEventHandler::HandleGroupedVllmStored(
    const zmq::VllmStoredEvent& event) {
    const auto tier = EngineTier(event.medium, layout_);
    // Connector CPU announcements do not prove Store residency. Raw Mooncake
    // objects supply the authoritative Store/Remove lifecycle instead.
    if (!tier) return "";
    size_t slot = 0;
    if (auto error = ResolveGroup(layout_, event.group_idx, &slot);
        !error.empty())
        return error;
    const auto& group = layout_.layout->groups[slot];
    if (auto error = prefixindex::ValidateVllmGroupedStoredAssertions(
            event, service_, *layout_.layout, group, *tier);
        !error.empty())
        return error;
    GroupMutation mutation{.context = Context(service_),
                           .group = slot,
                           .tier = *tier,
                           .owner = Owner(service_),
                           .hash_domain = layout_.hash_domain};
    if (auto error =
            NormalizeExternalHashes(event.block_hashes, &mutation.prefixes);
        !error.empty())
        return error;
    return manager_->GetIndexer()->StoreGrouped(mutation);
}

std::string KVEventHandler::HandleGroupedVllmRemoved(
    const zmq::VllmRemovedEvent& event) {
    const auto tier = EngineTier(event.medium, layout_);
    if (!tier) return "";
    size_t slot = 0;
    if (auto error = ResolveGroup(layout_, event.group_idx, &slot);
        !error.empty())
        return error;
    GroupMutation mutation{.context = Context(service_),
                           .group = slot,
                           .tier = *tier,
                           .owner = Owner(service_),
                           .hash_domain = layout_.hash_domain};
    if (auto error =
            NormalizeExternalHashes(event.block_hashes, &mutation.prefixes);
        !error.empty())
        return error;
    return manager_->GetIndexer()->RemoveGrouped(mutation);
}

std::string KVEventHandler::HandleGroupedSglangStored(
    const zmq::SglangStoredEvent& event) {
    const auto tier = EngineTier(event.medium, layout_);
    if (!tier) return "unsupported SGLang medium";
    size_t slot = 0;
    if (auto error = ResolveGroup(layout_, 0, &slot); !error.empty())
        return error;
    const auto& group = layout_.layout->groups[slot];
    if (group.match != MatchPolicy::kDense)
        return "native SGLang pages do not certify independent "
               "checkpoint/window components";
    if (event.block_size <= 0 || event.block_size > group.block_tokens)
        return "unsupported SGLang page geometry";
    if (event.lora_name.value_or("") != service_.lora_name ||
        (event.lora_id && (*event.lora_id <= 0 || !event.lora_name ||
                           !layout_.layout->sglang_extra_key)))
        return "unsupported SGLang LoRA identity";
    GroupMutation mutation{.context = Context(service_),
                           .group = slot,
                           .tier = *tier,
                           .owner = Owner(service_),
                           .hash_domain = layout_.hash_domain};
    for (auto hash : event.block_hashes) mutation.prefixes.push_back({hash});
    return manager_->GetIndexer()->StoreGrouped(mutation);
}

std::string KVEventHandler::HandleGroupedSglangRemoved(
    const zmq::SglangRemovedEvent& event) {
    const auto tier = EngineTier(event.medium, layout_);
    if (!tier) return "unsupported SGLang medium";
    size_t slot = 0;
    if (auto error = ResolveGroup(layout_, 0, &slot); !error.empty())
        return error;
    if (layout_.layout->groups[slot].match != MatchPolicy::kDense)
        return "unobservable SGLang component";
    GroupMutation mutation{.context = Context(service_),
                           .group = slot,
                           .tier = *tier,
                           .owner = Owner(service_),
                           .hash_domain = layout_.hash_domain};
    for (auto hash : event.block_hashes) mutation.prefixes.push_back({hash});
    return manager_->GetIndexer()->RemoveGrouped(mutation);
}

std::string KVEventHandler::HandleGroupedMooncakeStored(
    const zmq::MooncakeStoredEvent& event) {
    const auto tier = ObjectTier(event.fields.medium);
    if (!tier) return "unsupported shared storage medium";
    GroupMutation mutation;
    if (auto error = ObjectMutation(service_, layout_, event.fields,
                                    event.object, &mutation);
        !error.empty())
        return error;
    mutation.tier = *tier;
    return manager_->GetIndexer()->StoreGrouped(mutation);
}

std::string KVEventHandler::HandleGroupedMooncakeRemoved(
    const zmq::MooncakeRemovedEvent& event) {
    GroupMutation mutation;
    if (auto error = ObjectMutation(service_, layout_, event.fields,
                                    event.object, &mutation);
        !error.empty())
        return error;
    if (event.fields.medium) {
        const auto tier = ObjectTier(event.fields.medium);
        if (!tier) return "unsupported shared storage medium";
        mutation.tier = *tier;
        return manager_->GetIndexer()->RemoveGrouped(mutation);
    }
    for (auto tier : {StorageTier::kCpuShare, StorageTier::kDisk}) {
        mutation.tier = tier;
        if (auto error = manager_->GetIndexer()->RemoveGrouped(mutation);
            !error.empty())
            return error;
    }
    return "";
}

}  // namespace mooncake::conductor::kvevent
