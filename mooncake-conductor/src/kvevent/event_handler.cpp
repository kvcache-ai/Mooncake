#include "conductor/kvevent/event_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace conductor {
namespace kvevent {

namespace {

std::string LowerAscii(std::string_view value) {
    std::string normalized(value);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char character) {
                       return static_cast<char>(std::tolower(character));
                   });
    return normalized;
}

prefixindex::ContextKey ContextFromService(
    const common::ServiceConfig& service) {
    return {.tenant_id = service.tenant_id,
            .model_name = service.model_name,
            .lora_name = service.lora_name,
            .block_size = service.block_size};
}

prefixindex::HashProfile ProfileFromService(
    const common::ServiceConfig& service) {
    return {.strategy = service.hash_profile.strategy,
            .algorithm = service.hash_profile.algorithm,
            .root_digest = service.hash_profile.root_digest,
            .index_projection = service.hash_profile.index_projection};
}

prefixindex::EngineOwner EngineOwnerFromService(
    const common::ServiceConfig& service) {
    return {.source_stream = service.endpoint,
            .instance_id = service.instance_id,
            .dp_rank = service.dp_rank};
}

std::optional<prefixindex::StorageTier> SharedTier(
    const std::optional<std::string>& medium) {
    const std::string normalized = LowerAscii(medium.value_or(""));
    if (normalized == "cpu") {
        return prefixindex::StorageTier::kCpu;
    }
    if (normalized == "disk") {
        return prefixindex::StorageTier::kDisk;
    }
    return std::nullopt;
}

std::string OriginalMedium(const std::optional<std::string>& medium) {
    return medium.value_or("");
}

std::string ValidateVllmGroup(std::optional<int64_t> group) {
    if (group.has_value() && *group != 0) {
        return "only cache group zero is supported";
    }
    return "";
}

std::string ValidateMooncakeGroup(
    const std::optional<std::string>& encoded_group) {
    if (!encoded_group.has_value() || encoded_group->empty()) {
        return "";
    }
    int64_t group = -1;
    const auto [end, error] =
        std::from_chars(encoded_group->data(),
                        encoded_group->data() + encoded_group->size(), group);
    if (error != std::errc{} ||
        end != encoded_group->data() + encoded_group->size()) {
        return "group_id must be a decimal integer or nil";
    }
    if (group != 0) {
        return "only cache group zero is supported";
    }
    return "";
}

std::string NormalizeExternalHash(const zmq::ExternalHash& external_hash,
                                  prefixindex::ProjectedPrefix* output) {
    if (const auto* integer = std::get_if<uint64_t>(&external_hash)) {
        output->value = *integer;
        return "";
    }
    const auto& bytes = std::get<std::vector<uint8_t>>(external_hash);
    if (bytes.size() < 8) {
        return "binary hash must contain at least eight bytes";
    }
    uint64_t projected = 0;
    for (size_t index = bytes.size() - 8; index < bytes.size(); ++index) {
        projected = (projected << 8) | bytes[index];
    }
    output->value = projected;
    return "";
}

std::string NormalizeExternalHashes(
    const std::vector<zmq::ExternalHash>& external_hashes,
    std::vector<prefixindex::ProjectedPrefix>* output) {
    std::vector<prefixindex::ProjectedPrefix> normalized;
    normalized.reserve(external_hashes.size());
    for (size_t index = 0; index < external_hashes.size(); ++index) {
        prefixindex::ProjectedPrefix prefix;
        if (std::string error =
                NormalizeExternalHash(external_hashes[index], &prefix);
            !error.empty()) {
            return "block_hashes[" + std::to_string(index) + "]: " + error;
        }
        normalized.push_back(prefix);
    }
    *output = std::move(normalized);
    return "";
}

int HexNibble(char character) {
    if (character >= '0' && character <= '9') {
        return character - '0';
    }
    if (character >= 'a' && character <= 'f') {
        return character - 'a' + 10;
    }
    if (character >= 'A' && character <= 'F') {
        return character - 'A' + 10;
    }
    return -1;
}

struct ConnectorHash {
    prefixindex::ProjectedPrefix prefix;
    std::string normalized_hex;
};

std::string DecodeConnectorHash(std::string_view encoded,
                                ConnectorHash* output) {
    if (encoded.starts_with("0x") || encoded.starts_with("0X")) {
        encoded.remove_prefix(2);
    }
    if (encoded.size() < 16 || encoded.size() % 2 != 0) {
        return "connector_block_hash must be even-length hex with at least "
               "eight bytes";
    }
    std::string normalized;
    normalized.reserve(encoded.size());
    for (char character : encoded) {
        const int nibble = HexNibble(character);
        if (nibble < 0) {
            return "connector_block_hash contains a non-hex character";
        }
        normalized.push_back(
            static_cast<char>(nibble < 10 ? '0' + nibble : 'a' + nibble - 10));
    }
    uint64_t projected = 0;
    for (size_t index = normalized.size() - 16; index < normalized.size();
         ++index) {
        projected = (projected << 4) |
                    static_cast<uint64_t>(HexNibble(normalized[index]));
    }
    output->prefix.value = projected;
    output->normalized_hex = std::move(normalized);
    return "";
}

std::string ValidateSeqHashes(const std::vector<uint64_t>& seq_hashes,
                              prefixindex::ProjectedPrefix prefix) {
    if (seq_hashes.empty()) {
        return "";
    }
    if (seq_hashes.size() != 1 || seq_hashes.front() != prefix.value) {
        return "seq_hashes conflicts with connector_block_hash low64";
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
        return "lora_id cannot be validated against trusted registration";
    }
    if (event.lora_name.value_or("") != service.lora_name) {
        return "lora_name conflicts with trusted registration";
    }
    if (std::string error = ValidateVllmGroup(event.group_idx);
        !error.empty()) {
        return error;
    }
    if (event.kv_cache_spec_kind.has_value() &&
        *event.kv_cache_spec_kind != "full_attention") {
        return "unsupported kv_cache_spec_kind assertion: " +
               *event.kv_cache_spec_kind;
    }
    if (event.kv_cache_spec_sliding_window.has_value()) {
        return "sliding-window cache specs are not supported";
    }
    return "";
}

prefixindex::ContextKey MooncakeContext(
    const zmq::MooncakeEventFields& fields) {
    return {.tenant_id = fields.tenant_id,
            .model_name = fields.model_name.value_or(""),
            .lora_name = fields.lora_name.value_or(""),
            .block_size = fields.block_size.value_or(0)};
}

std::string ValidateMooncakeContext(const zmq::MooncakeEventFields& fields,
                                    prefixindex::ContextKey* context) {
    *context = MooncakeContext(fields);
    if (context->tenant_id.empty()) {
        return "tenant_id is required";
    }
    if (context->model_name.empty()) {
        return "model_name is required";
    }
    if (context->block_size <= 0) {
        return "block_size must be positive";
    }
    return "";
}

template <typename Event>
size_t HashCount(const Event& event) {
    return event.block_hashes.size();
}

}  // namespace

KVEventHandler::KVEventHandler(EventManager* manager, common::ServiceConfig svc)
    : manager_(manager), service_(std::move(svc)) {}

bool KVEventHandler::BeginDispatch() {
    std::lock_guard lock(lifecycle_mu_);
    if (!available_) {
        return false;
    }
    ++in_flight_;
    return true;
}

void KVEventHandler::EndDispatch() {
    {
        std::lock_guard lock(lifecycle_mu_);
        --in_flight_;
    }
    lifecycle_cv_.notify_all();
}

void KVEventHandler::MarkUnavailable() {
    std::lock_guard lock(lifecycle_mu_);
    available_ = false;
}

void KVEventHandler::WaitForIdle() {
    std::unique_lock lock(lifecycle_mu_);
    lifecycle_cv_.wait(lock, [this] { return in_flight_ == 0; });
}

std::string KVEventHandler::HandleBatch(const zmq::DecodedBatch& batch,
                                        const zmq::MessageMetadata& metadata) {
    if (!BeginDispatch()) {
        return "subscription unavailable";
    }
    DispatchLease lease(this);
    if (manager_->IsStopped()) {
        return "manager stopped";
    }
    if (metadata.publisher_kind != service_.publisher_kind) {
        return "message publisher kind conflicts with trusted registration";
    }
    if (metadata.endpoint != service_.endpoint) {
        return "message endpoint conflicts with trusted registration";
    }
    if (service_.publisher_kind == common::PublisherKind::kVllm) {
        const auto* vllm_batch = std::get_if<zmq::VllmEventBatch>(&batch);
        if (vllm_batch == nullptr) {
            return "registered vLLM source received a Mooncake batch";
        }
        return HandleVllmBatch(*vllm_batch, metadata);
    }
    const auto* mooncake_batch = std::get_if<zmq::MooncakeEventBatch>(&batch);
    if (mooncake_batch == nullptr) {
        return "registered Mooncake source received a vLLM batch";
    }
    return HandleMooncakeBatch(*mooncake_batch, metadata);
}

std::string KVEventHandler::HandleVllmBatch(
    const zmq::VllmEventBatch& batch, const zmq::MessageMetadata& metadata) {
    if (batch.data_parallel_rank.has_value() &&
        *batch.data_parallel_rank != service_.dp_rank) {
        LOG(WARNING) << "Rejected vLLM batch DP assertion endpoint="
                     << service_.endpoint << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " instance=" << service_.instance_id
                     << " expected_dp=" << service_.dp_rank
                     << " actual_dp=" << *batch.data_parallel_rank
                     << " topic=" << metadata.topic
                     << " seq=" << metadata.sequence;
        return "batch data_parallel_rank conflicts with trusted registration";
    }

    for (size_t index = 0; index < batch.events.size(); ++index) {
        const auto& decoded = batch.events[index];
        if (!decoded.ok()) {
            LOG(WARNING) << "Rejected vLLM event endpoint=" << service_.endpoint
                         << " publisher_kind="
                         << common::PublisherKindName(metadata.publisher_kind)
                         << " instance=" << service_.instance_id
                         << " dp_rank=" << service_.dp_rank
                         << " topic=" << metadata.topic
                         << " seq=" << metadata.sequence
                         << " event_index=" << index
                         << " error=" << decoded.error;
            continue;
        }
        std::string error = std::visit(
            [&](const auto& event) -> std::string {
                using Event = std::decay_t<decltype(event)>;
                if constexpr (std::is_same_v<Event, zmq::VllmStoredEvent>) {
                    return HandleVllmStored(event, metadata);
                } else if constexpr (std::is_same_v<Event,
                                                    zmq::VllmRemovedEvent>) {
                    return HandleVllmRemoved(event, metadata);
                } else {
                    return HandleVllmCleared(metadata);
                }
            },
            *decoded.event);
        if (!error.empty()) {
            LOG(WARNING) << "Rejected vLLM event endpoint=" << service_.endpoint
                         << " publisher_kind="
                         << common::PublisherKindName(metadata.publisher_kind)
                         << " instance=" << service_.instance_id
                         << " dp_rank=" << service_.dp_rank
                         << " topic=" << metadata.topic
                         << " seq=" << metadata.sequence
                         << " event_index=" << index << " error=" << error;
        }
    }
    return "";
}

std::string KVEventHandler::HandleMooncakeBatch(
    const zmq::MooncakeEventBatch& batch,
    const zmq::MessageMetadata& metadata) {
    for (size_t index = 0; index < batch.events.size(); ++index) {
        const auto& decoded = batch.events[index];
        if (!decoded.ok()) {
            LOG(WARNING) << "Rejected Mooncake event endpoint="
                         << service_.endpoint << " publisher_kind="
                         << common::PublisherKindName(metadata.publisher_kind)
                         << " instance=" << service_.instance_id << " batch_dp="
                         << (batch.data_parallel_rank.has_value()
                                 ? std::to_string(*batch.data_parallel_rank)
                                 : "nil")
                         << " topic=" << metadata.topic
                         << " seq=" << metadata.sequence
                         << " event_index=" << index
                         << " error=" << decoded.error;
            continue;
        }
        std::string error = std::visit(
            [&](const auto& event) -> std::string {
                using Event = std::decay_t<decltype(event)>;
                if constexpr (std::is_same_v<Event, zmq::MooncakeStoredEvent>) {
                    return HandleMooncakeStored(event, metadata);
                } else if constexpr (std::is_same_v<
                                         Event, zmq::MooncakeRemovedEvent>) {
                    return HandleMooncakeRemoved(event, metadata);
                } else {
                    return HandleMooncakeCleared(event, metadata);
                }
            },
            *decoded.event);
        if (!error.empty()) {
            const int64_t event_dp = std::visit(
                [](const auto& event) {
                    return event.fields.data_parallel_rank;
                },
                *decoded.event);
            LOG(WARNING) << "Rejected Mooncake event endpoint="
                         << service_.endpoint << " publisher_kind="
                         << common::PublisherKindName(metadata.publisher_kind)
                         << " instance=" << service_.instance_id
                         << " topic=" << metadata.topic
                         << " seq=" << metadata.sequence
                         << " event_dp=" << event_dp << " event_index=" << index
                         << " error=" << error;
        }
    }
    return "";
}

std::string KVEventHandler::HandleVllmStored(
    const zmq::VllmStoredEvent& event, const zmq::MessageMetadata& metadata) {
    if (std::string error = ValidateVllmStoredAssertions(event, service_);
        !error.empty()) {
        return error;
    }
    const std::string medium = LowerAscii(event.medium.value_or(""));
    if (medium != "gpu") {
        LOG(WARNING) << "Ignoring vLLM non-GPU event endpoint="
                     << service_.endpoint << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " instance=" << service_.instance_id
                     << " dp_rank=" << service_.dp_rank
                     << " event_type=BlockStored medium="
                     << OriginalMedium(event.medium)
                     << " hash_count=" << HashCount(event)
                     << " topic=" << metadata.topic
                     << " seq=" << metadata.sequence;
        return "";
    }
    std::vector<prefixindex::ProjectedPrefix> prefixes;
    if (std::string error =
            NormalizeExternalHashes(event.block_hashes, &prefixes);
        !error.empty()) {
        return error;
    }
    return manager_->GetIndexer()->StoreGpu({
        .context = ContextFromService(service_),
        .prefixes = std::move(prefixes),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
    });
}

std::string KVEventHandler::HandleVllmRemoved(
    const zmq::VllmRemovedEvent& event, const zmq::MessageMetadata& metadata) {
    if (std::string error = ValidateVllmGroup(event.group_idx);
        !error.empty()) {
        return error;
    }
    const std::string medium = LowerAscii(event.medium.value_or(""));
    if (medium != "gpu") {
        LOG(WARNING) << "Ignoring vLLM non-GPU event endpoint="
                     << service_.endpoint << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " instance=" << service_.instance_id
                     << " dp_rank=" << service_.dp_rank
                     << " event_type=BlockRemoved medium="
                     << OriginalMedium(event.medium)
                     << " hash_count=" << HashCount(event)
                     << " topic=" << metadata.topic
                     << " seq=" << metadata.sequence;
        return "";
    }
    std::vector<prefixindex::ProjectedPrefix> prefixes;
    if (std::string error =
            NormalizeExternalHashes(event.block_hashes, &prefixes);
        !error.empty()) {
        return error;
    }
    return manager_->GetIndexer()->RemoveGpu({
        .context = ContextFromService(service_),
        .prefixes = std::move(prefixes),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
    });
}

std::string KVEventHandler::HandleVllmCleared(
    const zmq::MessageMetadata& metadata) {
    VLOG(1) << "Clearing vLLM engine owner endpoint=" << service_.endpoint
            << " publisher_kind="
            << common::PublisherKindName(metadata.publisher_kind)
            << " instance=" << service_.instance_id
            << " dp_rank=" << service_.dp_rank << " topic=" << metadata.topic
            << " seq=" << metadata.sequence;
    return manager_->GetIndexer()->ClearGpu({
        .context = ContextFromService(service_),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
    });
}

std::string KVEventHandler::HandleMooncakeStored(
    const zmq::MooncakeStoredEvent& event,
    const zmq::MessageMetadata& metadata) {
    const auto tier = SharedTier(event.fields.medium);
    if (!tier.has_value()) {
        LOG(WARNING) << "Ignoring Mooncake unsupported-medium event endpoint="
                     << service_.endpoint << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " instance=" << service_.instance_id
                     << " event_type=stored medium="
                     << OriginalMedium(event.fields.medium)
                     << " hash_count=" << event.object.seq_hashes.size()
                     << " backend=" << event.fields.backend_id
                     << " event_dp=" << event.fields.data_parallel_rank
                     << " topic=" << metadata.topic
                     << " seq=" << metadata.sequence;
        return "";
    }
    if (std::string error = ValidateMooncakeGroup(event.object.group_id);
        !error.empty()) {
        return error;
    }
    if (event.fields.backend_id.empty()) {
        return "backend_id is required";
    }
    if (!event.object.object_key.has_value() ||
        event.object.object_key->empty()) {
        return "object_key is required";
    }
    if (!event.object.connector_block_hash.has_value()) {
        return "connector_block_hash is required";
    }

    prefixindex::ContextKey context;
    if (std::string error = ValidateMooncakeContext(event.fields, &context);
        !error.empty()) {
        return error;
    }
    if (std::string error = manager_->GetIndexer()->ValidateProfileBinding(
            context, ProfileFromService(service_));
        !error.empty()) {
        return "Mooncake profile binding rejected: " + error;
    }

    ConnectorHash hash;
    if (std::string error =
            DecodeConnectorHash(*event.object.connector_block_hash, &hash);
        !error.empty()) {
        return error;
    }
    if (std::string error =
            ValidateSeqHashes(event.object.seq_hashes, hash.prefix);
        !error.empty()) {
        return error;
    }

    const prefixindex::SharedObjectOwner owner{
        .source_stream = service_.endpoint,
        .backend_id = event.fields.backend_id,
        .object_id = *event.object.object_key,
    };
    const PoolObjectKey key{
        .source_endpoint = service_.endpoint,
        .backend_id = event.fields.backend_id,
        .tenant_id = event.fields.tenant_id,
        .object_key = *event.object.object_key,
        .tier = *tier,
    };
    const PoolObjectBinding binding{
        .context = context,
        .prefix = hash.prefix,
        .connector_block_hash = hash.normalized_hex,
        .owner = owner,
        .tenant_id = event.fields.tenant_id,
    };

    std::lock_guard lock(bindings_mu_);
    auto existing = pool_bindings_.find(key);
    if (existing != pool_bindings_.end() &&
        (existing->second.context != binding.context ||
         existing->second.prefix != binding.prefix ||
         existing->second.connector_block_hash !=
             binding.connector_block_hash ||
         existing->second.owner != binding.owner ||
         existing->second.tenant_id != binding.tenant_id)) {
        return "conflicting active binding for Mooncake object/tier";
    }
    prefixindex::SharedMutation mutation{
        .context = context,
        .prefixes = {hash.prefix},
        .tier = *tier,
        .owner = owner,
        .effective_block_size = context.block_size,
        .cache_group = std::nullopt,
    };
    if (std::string error = manager_->GetIndexer()->StoreShared(mutation);
        !error.empty()) {
        return error;
    }
    if (existing == pool_bindings_.end()) {
        pool_bindings_.emplace(key, binding);
    }
    return "";
}

std::string KVEventHandler::HandleMooncakeRemoved(
    const zmq::MooncakeRemovedEvent& event,
    const zmq::MessageMetadata& metadata) {
    const auto tier = SharedTier(event.fields.medium);
    if (!tier.has_value()) {
        LOG(WARNING) << "Ignoring Mooncake unsupported-medium event endpoint="
                     << service_.endpoint << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " instance=" << service_.instance_id
                     << " event_type=removed medium="
                     << OriginalMedium(event.fields.medium)
                     << " hash_count=" << event.object.seq_hashes.size()
                     << " backend=" << event.fields.backend_id
                     << " event_dp=" << event.fields.data_parallel_rank
                     << " topic=" << metadata.topic
                     << " seq=" << metadata.sequence;
        return "";
    }
    if (std::string error = ValidateMooncakeGroup(event.object.group_id);
        !error.empty()) {
        return error;
    }
    if (event.fields.backend_id.empty()) {
        return "backend_id is required";
    }
    if (!event.object.object_key.has_value() ||
        event.object.object_key->empty()) {
        return "object_key is required";
    }
    const PoolObjectKey key{
        .source_endpoint = service_.endpoint,
        .backend_id = event.fields.backend_id,
        .tenant_id = event.fields.tenant_id,
        .object_key = *event.object.object_key,
        .tier = *tier,
    };

    std::lock_guard lock(bindings_mu_);
    auto binding = pool_bindings_.find(key);
    if (binding == pool_bindings_.end()) {
        return "";
    }
    if (event.fields.tenant_id != binding->second.tenant_id) {
        return "tenant_id conflicts with stored object binding";
    }
    if (event.fields.model_name.has_value() &&
        *event.fields.model_name != binding->second.context.model_name) {
        return "model_name conflicts with stored object binding";
    }
    if (event.fields.lora_name.value_or("") !=
        binding->second.context.lora_name) {
        return "lora_name conflicts with stored object binding";
    }
    if (event.fields.block_size.has_value() &&
        *event.fields.block_size != binding->second.context.block_size) {
        return "block_size conflicts with stored object binding";
    }
    if (event.object.connector_block_hash.has_value()) {
        ConnectorHash asserted_hash;
        if (std::string error = DecodeConnectorHash(
                *event.object.connector_block_hash, &asserted_hash);
            !error.empty()) {
            return error;
        }
        if (asserted_hash.normalized_hex !=
                binding->second.connector_block_hash ||
            asserted_hash.prefix != binding->second.prefix) {
            return "connector_block_hash conflicts with stored object binding";
        }
    }
    if (!event.object.seq_hashes.empty() &&
        (event.object.seq_hashes.size() != 1 ||
         event.object.seq_hashes.front() != binding->second.prefix.value)) {
        return "seq_hashes conflicts with stored object binding";
    }

    prefixindex::SharedMutation mutation{
        .context = binding->second.context,
        .prefixes = {binding->second.prefix},
        .tier = *tier,
        .owner = binding->second.owner,
        .effective_block_size = binding->second.context.block_size,
        .cache_group = std::nullopt,
    };
    if (std::string error = manager_->GetIndexer()->RemoveShared(mutation);
        !error.empty()) {
        return error;
    }
    pool_bindings_.erase(binding);
    return "";
}

std::string KVEventHandler::HandleMooncakeCleared(
    const zmq::MooncakeClearedEvent& event,
    const zmq::MessageMetadata& metadata) {
    if (event.fields.backend_id.empty()) {
        return "backend_id is required";
    }
    if (event.fields.tenant_id.empty()) {
        return "tenant_id is required";
    }
    VLOG(1) << "Clearing Mooncake bindings endpoint=" << service_.endpoint
            << " publisher_kind="
            << common::PublisherKindName(metadata.publisher_kind)
            << " backend=" << event.fields.backend_id
            << " tenant=" << event.fields.tenant_id
            << " topic=" << metadata.topic << " seq=" << metadata.sequence;
    return ClearMooncakeBindings(event.fields.backend_id,
                                 event.fields.tenant_id);
}

std::string KVEventHandler::ClearMooncakeBindings(
    const std::optional<std::string>& backend_id,
    const std::optional<std::string>& tenant_id) {
    std::lock_guard lock(bindings_mu_);
    for (auto binding = pool_bindings_.begin();
         binding != pool_bindings_.end();) {
        if ((backend_id.has_value() &&
             binding->first.backend_id != *backend_id) ||
            (tenant_id.has_value() &&
             binding->second.tenant_id != *tenant_id)) {
            ++binding;
            continue;
        }
        prefixindex::SharedMutation mutation{
            .context = binding->second.context,
            .prefixes = {binding->second.prefix},
            .tier = binding->first.tier,
            .owner = binding->second.owner,
            .effective_block_size = binding->second.context.block_size,
            .cache_group = std::nullopt,
        };
        if (std::string error = manager_->GetIndexer()->RemoveShared(mutation);
            !error.empty()) {
            return error;
        }
        binding = pool_bindings_.erase(binding);
    }
    return "";
}

std::string KVEventHandler::InvalidateEndpoint() {
    MarkUnavailable();
    if (service_.publisher_kind == common::PublisherKind::kVllm) {
        return manager_->GetIndexer()->ClearGpu({
            .context = ContextFromService(service_),
            .owner = EngineOwnerFromService(service_),
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group,
        });
    }
    return ClearMooncakeBindings(std::nullopt, std::nullopt);
}

}  // namespace kvevent
}  // namespace conductor
