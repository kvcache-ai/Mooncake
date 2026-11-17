#include "conductor/prefixindex/layout_parser.h"

#include <json/json.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>

namespace mooncake::conductor::prefixindex {
namespace {

bool IsLayerwiseNamespace(std::string_view prefix, const std::string& model,
                          const std::string& group, int64_t block_tokens) {
    const auto head = model + "@mooncake_hybrid_v1:";
    const auto tail =
        "@group:" + group + "@block:" + std::to_string(block_tokens) + "@";
    if (prefix.size() != head.size() + 64 + tail.size() ||
        !prefix.starts_with(head) || !prefix.ends_with(tail))
        return false;
    const auto digest = prefix.substr(head.size(), 64);
    return std::all_of(digest.begin(), digest.end(), [](char ch) {
        return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') ||
               (ch >= 'A' && ch <= 'F');
    });
}

}  // namespace

std::string ResolveVllmDeployment(const common::ServiceConfig& service,
                                  std::string_view declaration,
                                  SourceLayout* result) {
    const auto* model = FindModelCacheSpec(service.model_name);
    if (!result || !model || !model->checkpoints || model->sliding_window ||
        service.publisher_kind != common::PublisherKind::kVllm ||
        service.hash_profile.strategy != "vllm_v1")
        return "unsupported vLLM deployment/model";
    if (declaration.empty() || declaration.size() > 65536)
        return "cache layout declaration must contain at most 65536 bytes";
    Json::CharReaderBuilder builder;
    builder["rejectDupKeys"] = true;
    builder["failIfExtra"] = true;
    Json::Value root;
    std::string error;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(declaration.data(),
                       declaration.data() + declaration.size(), &root,
                       &error) ||
        !root.isObject())
        return "invalid cache layout JSON";
    auto positive = [](const Json::Value& value) {
        return value.isInt64() && value.asInt64() > 0;
    };
    if (!root["format"].isString() ||
        root["format"].asString() != "ascend-store-v1" ||
        !root["model_name"].isString() ||
        FindModelCacheSpec(root["model_name"].asString()) != model ||
        !positive(root["hash_unit_tokens"]) ||
        root["hash_unit_tokens"].asInt64() != service.block_size ||
        !positive(root["alignment_tokens"]) ||
        !root["storage_domain"].isString() ||
        root["storage_domain"].asString().empty() ||
        !root["groups"].isArray() || root["groups"].empty() ||
        root["groups"].size() > 128)
        return "invalid deployment identity or geometry";
    const bool layerwise = root.isMember("object_key_format");
    if (layerwise && root["object_key_format"] != "mooncake-layerwise-v1")
        return "unsupported deployment object key format";
    auto layout = std::make_shared<CacheLayout>();
    layout->id = std::string(model->name);
    layout->hash_unit_tokens = service.block_size;
    layout->alignment_tokens = root["alignment_tokens"].asInt64();
    // GLM-Next's private Indexer K-pool tail is regenerated at pool-aligned
    // boundaries, not a persisted checkpoint. Never infer this for other rings.
    if (model->token_compression && (root["transient_alignment_tokens"] != 4 ||
                                     layout->alignment_tokens % 4 != 0))
        return "GLM Flash requires verified pool-aligned tail recovery";
    SourceLayout binding;
    binding.source = LayoutSource::kVllmAnnouncement;
    binding.layout = layout;
    binding.storage_domain = root["storage_domain"].asString();
    binding.readable_domains.insert(binding.storage_domain);
    bool checkpoint = false, dense = false;
    std::set<std::pair<std::string, std::string>> namespaces;
    for (const auto& item : root["groups"]) {
        if (!item.isObject() || !item["group_idx"].isInt64() ||
            item["group_idx"].asInt64() < 0 || !item["kind"].isString() ||
            !positive(item["block_tokens"]) ||
            !item["key_prefixes"].isArray() || item["key_prefixes"].empty() ||
            item["key_prefixes"].size() > 256)
            return "invalid cache group declaration";
        const auto id = item["group_idx"].asInt64();
        const auto slot = layout->groups.size();
        if (!binding.groups.emplace(id, slot).second)
            return "duplicate deployment group";
        CacheGroupLayout group;
        group.id = std::to_string(id);
        group.event_kind = item["kind"].asString();
        group.block_tokens = item["block_tokens"].asInt64();
        group.alignment_tokens = group.block_tokens;
        if (group.event_kind == "mamba") {
            if (item["mamba_cache_mode"] != "align")
                return "only aligned KDA checkpoints are supported";
            group.match = MatchPolicy::kCheckpoint;
            checkpoint = true;
        } else if (group.event_kind == "full_attention" ||
                   group.event_kind == "mla_attention") {
            group.match = MatchPolicy::kDense;
            dense = true;
        } else {
            return "unsupported group recovery policy";
        }
        group.parts.clear();
        const auto& suffixes = item["key_suffixes"];
        if (layerwise ? (!suffixes.isArray() ||
                         suffixes.size() != item["key_prefixes"].size())
                      : item.isMember("key_suffixes"))
            return "object suffixes conflict with deployment key format";
        for (Json::ArrayIndex part_id = 0;
             part_id < item["key_prefixes"].size(); ++part_id) {
            const auto& part = item["key_prefixes"][part_id];
            if (!part.isString()) return "invalid object namespace";
            const auto prefix = part.asString();
            if (layerwise && !suffixes[part_id].isString())
                return "invalid layerwise object suffix";
            const auto suffix = layerwise ? suffixes[part_id].asString() : "";
            const bool valid_namespace =
                layerwise
                    ? IsLayerwiseNamespace(prefix,
                                           root["model_name"].asString(),
                                           group.id, group.block_tokens) &&
                          suffix == "@" + std::to_string(part_id)
                    : prefix.find("@group:" + group.id + "@") !=
                              std::string::npos &&
                          prefix.find("@cache_role:kv@") != std::string::npos;
            if (prefix.size() > 1024 || !prefix.ends_with('@') ||
                !prefix.starts_with(root["model_name"].asString() + "@") ||
                !valid_namespace || !namespaces.emplace(prefix, suffix).second)
                return "invalid or duplicate object namespace";
            binding.objects.push_back({.key_prefix = prefix,
                                       .key_suffix = suffix,
                                       .group = slot,
                                       .part = group.parts.size()});
            group.parts.push_back(prefix + suffix);
        }
        layout->groups.push_back(std::move(group));
    }
    if (!checkpoint || !dense)
        return "hybrid deployment must declare dense and checkpoint groups";
    if (auto validation = ValidateCacheLayout(*layout); !validation.empty())
        return validation;
    *result = std::move(binding);
    return "";
}

std::string ValidateVllmGroup(std::optional<int64_t> group) {
    if (group.has_value() && *group != 0) {
        return "only cache group zero is supported";
    }
    return "";
}

std::string ValidateVllmStoredAssertions(const zmq::VllmStoredEvent& event,
                                         const common::ServiceConfig& service) {
    if (event.block_size != service.block_size) {
        return "block_size mismatch: expected " +
               std::to_string(service.block_size) + ", got " +
               std::to_string(event.block_size);
    }
    if (event.lora_id.has_value()) {
        // Modern vLLM hashes the adapter name, but still publishes the legacy
        // numeric ID. It need not match an ID on another engine. ID-only
        // events cannot establish the name used by our query hash strategy.
        if (*event.lora_id <= 0) {
            return "lora_id must be positive when provided";
        }
        if (event.lora_name.value_or("").empty()) {
            return "lora_name is required when lora_id is provided";
        }
    }
    if (event.lora_name.value_or("") != service.lora_name) {
        return "lora_name conflicts with trusted registration";
    }
    if (std::string error = ValidateVllmGroup(event.group_idx);
        !error.empty()) {
        return error;
    }
    // MLA shares the full-attention prefix lifecycle. Static sink blocks are
    // separate from request blocks and do not participate in their events.
    if (event.kv_cache_spec_kind.has_value() &&
        *event.kv_cache_spec_kind != "full_attention" &&
        *event.kv_cache_spec_kind != "mla_attention" &&
        *event.kv_cache_spec_kind != "sink_full_attention") {
        return "unsupported kv_cache_spec_kind assertion: " +
               *event.kv_cache_spec_kind;
    }
    if (event.kv_cache_spec_sliding_window.has_value()) {
        return "sliding-window cache specs are not supported";
    }
    return "";
}

std::string ValidateVllmGroupedStoredAssertions(
    const zmq::VllmStoredEvent& event, const common::ServiceConfig& service,
    const CacheLayout& layout, const CacheGroupLayout& group,
    StorageTier tier) {
    if (group.trailing_blocks > 0) {
        const int64_t expected =
            group.window_tokens > 0
                ? (group.window_tokens - 1) / group.block_tokens +
                      ((group.window_tokens - 1) % group.block_tokens != 0)
                : -1;
        if (expected < 0 ||
            group.trailing_blocks != std::max<int64_t>(1, expected))
            return "retained-page policy is not a native vLLM sliding window";
    }
    if (event.block_size <= 0)
        return "placeholder event cannot prove cache residency";
    // Native vLLM also advertises fine-grained aliases for an existing dense
    // page. Keep their endpoint hashes; Query still accepts only complete
    // physical-group/alignment boundaries, never the partial alias itself.
    const bool dense_alias = tier == StorageTier::kDevice &&
                             group.match == MatchPolicy::kDense &&
                             event.block_size <= group.block_tokens &&
                             event.block_size % layout.hash_unit_tokens == 0;
    if (group.match != MatchPolicy::kCheckpoint &&
        event.block_size != group.block_tokens && !dense_alias)
        return "event block_size conflicts with group logical span";
    if (event.lora_name.value_or("") != service.lora_name ||
        (event.lora_id && (*event.lora_id <= 0 || !event.lora_name)))
        return "event LoRA conflicts with registration";
    if (event.kv_cache_spec_kind) {
        const auto& kind = *event.kv_cache_spec_kind;
        const bool accepted =
            !group.event_kind.empty()                 ? kind == group.event_kind
            : group.match == MatchPolicy::kCheckpoint ? kind == "mamba"
            : group.match == MatchPolicy::kWindow
                ? kind == "sliding_window"
                : (kind == "full_attention" || kind == "mla_attention" ||
                   kind == "sink_full_attention");
        if (!accepted) return "event kind conflicts with group match policy";
    }
    if (event.kv_cache_spec_sliding_window &&
        (group.match != MatchPolicy::kWindow ||
         *event.kv_cache_spec_sliding_window != group.window_tokens))
        return "event window conflicts with layout";
    return "";
}

}  // namespace mooncake::conductor::prefixindex
