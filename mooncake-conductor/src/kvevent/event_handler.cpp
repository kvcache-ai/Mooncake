#include "conductor/kvevent/event_manager.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "ascii_string.h"
#include "conductor/kvevent/object_key_parser.h"
#include "conductor/prefixindex/layout_parser.h"
#include "integer_parser.h"

namespace mooncake::conductor::kvevent {

namespace {

prefixindex::ContextKey ContextFromService(
    const common::ServiceConfig& service) {
    return {.tenant_id = service.tenant_id,
            .model_name = service.model_name,
            .lora_name = service.lora_name,
            .block_size = service.block_size};
}

prefixindex::HashProfile ProfileFromService(
    const common::ServiceConfig& service) {
    return service.hash_profile;
}

prefixindex::EngineOwner EngineOwnerFromService(
    const common::ServiceConfig& service) {
    return {.source_stream = service.endpoint,
            .instance_id = service.instance_id,
            .dp_rank = service.dp_rank};
}

std::optional<prefixindex::StorageTier> SharedTier(
    const std::optional<std::string>& medium) {
    const std::string normalized = AsciiToLower(medium.value_or(""));
    if (normalized == "cpu" || normalized == "cpu_pinned") {
        return prefixindex::StorageTier::kCpuShare;
    }
    if (normalized == "disk") {
        return prefixindex::StorageTier::kDisk;
    }
    return std::nullopt;
}

std::optional<prefixindex::StorageTier> VllmTier(
    const std::optional<std::string>& medium) {
    const std::string normalized = AsciiToLower(medium.value_or(""));
    if (normalized == "gpu") {
        return prefixindex::StorageTier::kDevice;
    }
    if (normalized == "cpu") {
        return prefixindex::StorageTier::kCpuLocal;
    }
    return std::nullopt;
}

bool IsSglangDeviceMedium(const std::optional<std::string>& medium) {
    // SGLang's base radix caches omit medium for their engine-local cache.
    // The upstream wire name remains GPU on both GPU and NPU platforms.
    // Explicit CPU_PINNED and DISK values identify HiCache tiers instead.
    return !medium.has_value() || AsciiToLower(*medium) == "gpu";
}

std::optional<prefixindex::StorageTier> SglangLocalTier(
    const std::optional<std::string>& medium) {
    const std::string normalized = AsciiToLower(medium.value_or(""));
    if (normalized == "cpu" || normalized == "cpu_pinned") {
        return prefixindex::StorageTier::kCpuLocal;
    }
    if (normalized == "disk") {
        return prefixindex::StorageTier::kDisk;
    }
    return std::nullopt;
}

std::string OriginalMedium(const std::optional<std::string>& medium) {
    return medium.value_or("");
}

std::string ValidateMooncakeGroup(
    const std::optional<std::string>& encoded_group) {
    if (!encoded_group.has_value() || encoded_group->empty()) {
        return "";
    }
    // Default options reject surrounding whitespace and a leading plus, and
    // require the whole value to be consumed.
    const auto group = TryParseInteger<int64_t>(*encoded_group);
    if (!group.has_value()) {
        return "group_id must be a decimal integer or nil";
    }
    if (*group != 0) {
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

}  // namespace

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

namespace {

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

constexpr size_t kMaxPendingBatches = 256;
constexpr size_t kMaxPendingEvents = 4096;
constexpr size_t kMaxPendingBytes = 64 * 1024 * 1024;

struct PendingBatchUsage {
    size_t event_count;
    size_t bytes;
};

size_t HashPayloadBytes(const std::vector<uint64_t>& hashes) {
    return hashes.size() * sizeof(uint64_t);
}

size_t HashPayloadBytes(const std::vector<zmq::ExternalHash>& hashes) {
    size_t bytes = hashes.size() * sizeof(zmq::ExternalHash);
    for (const auto& hash : hashes) {
        if (const auto* binary = std::get_if<std::vector<uint8_t>>(&hash))
            bytes += binary->size();
    }
    return bytes;
}

// Approximate retained payload size for the startup queue's memory budget.
template <typename Event>
size_t EstimatePendingEventBytes(const Event& event) {
    size_t bytes = sizeof(event);
    if constexpr (requires { event.block_hashes; })
        bytes += HashPayloadBytes(event.block_hashes);
    if constexpr (requires { event.token_ids; })
        if (event.token_ids) bytes += event.token_ids->size() * sizeof(int32_t);
    if constexpr (requires { event.kv_cache_layout; })
        if (event.kv_cache_layout) bytes += event.kv_cache_layout->size();
    if constexpr (requires { event.fields; }) {
        bytes += event.fields.backend_id.size() + event.fields.tenant_id.size();
        for (const auto* text :
             {&event.fields.model_name, &event.fields.lora_name,
              &event.fields.additional_salt, &event.fields.medium})
            if (*text) bytes += text->value().size();
    }
    if constexpr (requires { event.object; }) {
        for (const auto* text :
             {&event.object.object_key, &event.object.group_id,
              &event.object.connector_block_hash, &event.object.cache_prefix})
            if (*text) bytes += text->value().size();
        bytes += event.object.seq_hashes.size() * sizeof(uint64_t);
        if (event.object.legacy_block_hashes)
            bytes +=
                event.object.legacy_block_hashes->size() * sizeof(uint64_t);
    }
    return bytes;
}

template <typename Batch>
PendingBatchUsage EstimatePendingBatchUsage(const Batch& batch) {
    PendingBatchUsage usage{batch.events.size(), sizeof(batch)};
    for (const auto& decoded : batch.events) {
        usage.bytes += sizeof(decoded) + decoded.error.size();
        if (decoded.ok()) {
            usage.bytes += std::visit(
                [](const auto& event) {
                    return EstimatePendingEventBytes(event);
                },
                *decoded.event);
        }
    }
    return usage;
}

PendingBatchUsage EstimatePendingUsage(const zmq::DecodedBatch& batch,
                                       const zmq::MessageMetadata& metadata) {
    auto usage = std::visit(
        [](const auto& item) { return EstimatePendingBatchUsage(item); },
        batch);
    usage.bytes += metadata.endpoint.size() + metadata.topic.size();
    return usage;
}

}  // namespace

KVEventHandler::KVEventHandler(EventManager* manager, common::ServiceConfig svc,
                               prefixindex::SourceLayout layout)
    : manager_(manager),
      service_(std::move(svc)),
      layout_(std::move(layout)) {}

bool KVEventHandler::AttachDeployment(
    const prefixindex::SourceLayout& binding) {
    // 注意：即使本实例已有 layout（可能与宣告者共享同一份），也必须走一遍
    // ActivateDeployment 以登记 (instance, rank) 的 reader；否则该实例看不到池副本。
    if (!binding.layout) {
        LOG(WARNING) << "Skip attaching deployment without layout instance="
                     << service_.instance_id;
        return false;
    }
    if (!binding.layout->unavailable_reason.empty()) {
        LOG(WARNING) << "Skip attaching unavailable deployment instance="
                     << service_.instance_id
                     << " reason=" << binding.layout->unavailable_reason;
        return false;
    }
    if (auto error = manager_->GetIndexer()->ActivateDeployment(
            ContextFromService(service_), service_.hash_profile, binding,
            service_.instance_id, service_.dp_rank);
        !error.empty()) {
        LOG(WARNING) << "Failed to attach propagated deployment instance="
                     << service_.instance_id << " error=" << error;
        return false;
    }
    layout_ = binding;
    LOG(INFO) << "Attached propagated deployment instance="
              << service_.instance_id << " groups=" << layout_.layout->groups.size()
              << " readable_domains=" << layout_.readable_domains.size();
    return true;
}

bool KVEventHandler::EnsureDeployment() {
    if (layout_.layout) return true;
    // 同 ContextKey 的其他实例可能已经宣告过布局（例如同一个 P 组内只有一个 rank 发声明）。
    prefixindex::SourceLayout binding;
    if (!manager_->GetIndexer()->GetDeployment(ContextFromService(service_),
                                               &binding)) {
        return false;
    }
    if (!binding.layout || !binding.layout->unavailable_reason.empty())
        return false;
    if (auto error = manager_->GetIndexer()->ActivateDeployment(
            ContextFromService(service_), service_.hash_profile, binding,
            service_.instance_id, service_.dp_rank);
        !error.empty()) {
        LOG(WARNING) << "Failed to attach shared deployment instance="
                     << service_.instance_id << " error=" << error;
        return false;
    }
    layout_ = std::move(binding);
    LOG(INFO) << "Attached shared cache deployment instance="
              << service_.instance_id
              << " groups=" << layout_.layout->groups.size();
    return true;
}

std::string KVEventHandler::QueuePending(const zmq::DecodedBatch& batch,
                                         const zmq::MessageMetadata& metadata) {
    const auto usage = EstimatePendingUsage(batch, metadata);
    // Bounded startup buffering, never silent loss: overflow quarantines the
    // source until it is explicitly resynchronized/re-registered.
    if (usage.event_count > kMaxPendingEvents - pending_event_count_ ||
        pending_.size() >= kMaxPendingBatches ||
        usage.bytes > kMaxPendingBytes - pending_bytes_) {
        pending_.clear();
        pending_event_count_ = 0;
        pending_bytes_ = 0;
        MarkUnavailable();
        return "deployment startup buffer exceeded; source must resynchronize";
    }
    pending_.emplace_back(batch, metadata);
    pending_event_count_ += usage.event_count;
    pending_bytes_ += usage.bytes;
    return "";
}

void KVEventHandler::DrainPending() {
    auto pending = std::move(pending_);
    pending_.clear();
    pending_event_count_ = 0;
    pending_bytes_ = 0;
    for (const auto& [batch, metadata] : pending) {
        if (const auto* vllm = std::get_if<zmq::VllmEventBatch>(&batch))
            HandleVllmBatch(*vllm, metadata);
        else if (const auto* store =
                     std::get_if<zmq::MooncakeEventBatch>(&batch))
            HandleMooncakeBatch(*store, metadata);
    }
}

void KVEventHandler::RefreshDeployment() {
    if (!grouped_context() || !BeginDispatch()) return;
    DispatchLease lease(this);
    std::lock_guard lock(grouped_dispatch_mu_);
    if (manager_->GetIndexer()->GetDeployment(ContextFromService(service_),
                                              &layout_)) {
        layout_.source = prefixindex::LayoutSource::kMooncake;
        DrainPending();
    }
}

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
    std::unique_lock<std::mutex> grouped_lock;
    if (grouped_context()) grouped_lock = std::unique_lock(grouped_dispatch_mu_);
    if (service_.publisher_kind == common::PublisherKind::kVllm) {
        const auto* vllm_batch = std::get_if<zmq::VllmEventBatch>(&batch);
        if (vllm_batch == nullptr) {
            return "registered vLLM source received a Mooncake batch";
        }
        return HandleVllmBatch(*vllm_batch, metadata);
    }
    if (service_.publisher_kind == common::PublisherKind::kSglang) {
        if (const auto* sglang_batch =
                std::get_if<zmq::SglangEventBatch>(&batch);
            sglang_batch != nullptr) {
            return HandleSglangBatch(*sglang_batch, metadata);
        }
        return "registered SGLang source received an unsupported batch";
    }
    const auto* mooncake_batch = std::get_if<zmq::MooncakeEventBatch>(&batch);
    if (mooncake_batch == nullptr) {
        return "registered Mooncake source received a vLLM batch";
    }
    return HandleMooncakeBatch(*mooncake_batch, metadata);
}

void KVEventHandler::OnSourceStale(const std::string& cache_pool_key,
                                   const zmq::MessageMetadata& metadata,
                                   const std::string& reason) {
    // ZMQClient::MarkStale() already reported why the source went stale, so
    // only the retraction this handler performs is logged here.
    // InvalidateEndpoint() stops admitting callbacks before it retracts the
    // index entries, and it runs on the source's own event loop between
    // dispatches, so no batch of ours is in flight while the index is mutated.
    if (std::string error = InvalidateEndpoint(); !error.empty()) {
        LOG(ERROR) << "Failed to retract stale source service_key="
                   << cache_pool_key << " endpoint=" << service_.endpoint
                   << " error=" << error;
    } else {
        LOG(WARNING) << "Retracted stale source service_key=" << cache_pool_key
                     << " endpoint=" << service_.endpoint
                     << " last_sequence=" << metadata.sequence
                     << " reason=" << reason;
    }
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
    // kv_cache_layout 可能先于/后于数据事件到达，且它正是“是否启用分组路径”的输入，
    // 所以这里必须无条件扫描；不能用 grouped_context() 作为门控（会形成循环依赖）。
    {
        for (const auto& decoded : batch.events) {
            if (!decoded.ok()) continue;
            const auto* stored =
                std::get_if<zmq::VllmStoredEvent>(&*decoded.event);
            if (!stored || !stored->kv_cache_layout ||
                *stored->kv_cache_layout == deployment_declaration_)
                continue;
            prefixindex::SourceLayout binding;
            if (auto error = prefixindex::ResolveVllmDeployment(
                    service_, *stored->kv_cache_layout, &binding);
                !error.empty())
                return error;
            if (auto error = manager_->GetIndexer()->ActivateDeployment(
                    ContextFromService(service_), service_.hash_profile,
                    binding, service_.instance_id, service_.dp_rank);
                !error.empty())
                return error;
            layout_ = std::move(binding);
            deployment_declaration_ = *stored->kv_cache_layout;
            LOG(INFO) << "Activated complete cache deployment model="
                      << service_.model_name
                      << " instance=" << service_.instance_id
                      << " groups=" << layout_.layout->groups.size();
            manager_->RefreshMooncakeDeployments(ContextFromService(service_));
            // 同一 ContextKey 的其他实例（还没收到过 layout 的 P、D、mc）也挂上这份部署，
            // 否则它们既没有 hybrid 索引，也看不到池里的共享副本（cpu_share）。
            manager_->PropagateDeployment(ContextFromService(service_), layout_);
        }
    }
    if (layout_.layout && !layout_.layout->unavailable_reason.empty())
        return QueuePending(batch, metadata);
    if (!pending_.empty()) DrainPending();

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

std::string KVEventHandler::HandleSglangBatch(
    const zmq::SglangEventBatch& batch, const zmq::MessageMetadata& metadata) {
    if (batch.data_parallel_rank.has_value() &&
        *batch.data_parallel_rank != service_.dp_rank) {
        return "batch data_parallel_rank conflicts with trusted registration";
    }
    if (layout_.layout && !layout_.layout->unavailable_reason.empty())
        return "";
    for (size_t index = 0; index < batch.events.size(); ++index) {
        const auto& decoded = batch.events[index];
        if (!decoded.ok()) {
            LOG(WARNING) << "Rejected SGLang event endpoint="
                         << service_.endpoint << " event_index=" << index
                         << " error=" << decoded.error;
            continue;
        }
        std::string error = std::visit(
            [&](const auto& event) -> std::string {
                using Event = std::decay_t<decltype(event)>;
                if constexpr (std::is_same_v<Event, zmq::SglangStoredEvent>) {
                    return HandleSglangStored(event, metadata);
                } else if constexpr (std::is_same_v<Event,
                                                    zmq::SglangRemovedEvent>) {
                    return HandleSglangRemoved(event, metadata);
                } else {
                    return HandleSglangCleared(metadata);
                }
            },
            *decoded.event);
        if (!error.empty()) {
            LOG(WARNING) << "Rejected SGLang event endpoint="
                         << service_.endpoint << " event_index=" << index
                         << " error=" << error;
        }
    }
    return "";
}

std::string KVEventHandler::HandleSglangStored(
    const zmq::SglangStoredEvent& event, const zmq::MessageMetadata& metadata) {
    if (layout_.layout) return HandleGroupedSglangStored(event);
    if (event.block_size <= 0 || event.block_size > service_.block_size) {
        return "block_size must be in the range [1, " +
               std::to_string(service_.block_size) + "], got " +
               std::to_string(event.block_size);
    }
    if (event.lora_id.has_value()) {
        return "lora_id cannot be validated against trusted registration";
    }
    std::vector<prefixindex::ProjectedPrefix> prefixes;
    prefixes.reserve(event.block_hashes.size());
    for (uint64_t hash : event.block_hashes) {
        prefixes.push_back({.value = hash});
    }
    if (prefixes.empty()) {
        return "SGLang stored event has no block hashes";
    }

    if (IsSglangDeviceMedium(event.medium)) {
        return manager_->GetIndexer()->StoreEngine({
            .context = ContextFromService(service_),
            .prefixes = std::move(prefixes),
            .owner = EngineOwnerFromService(service_),
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group,
        });
    }
    const auto tier = SglangLocalTier(event.medium);
    if (!tier.has_value()) {
        return "unsupported SGLang storage medium: " +
               OriginalMedium(event.medium);
    }

    return manager_->GetIndexer()->StoreEngine({
        .context = ContextFromService(service_),
        .prefixes = std::move(prefixes),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
        .tier = *tier,
    });
}

std::string KVEventHandler::HandleSglangRemoved(
    const zmq::SglangRemovedEvent& event,
    const zmq::MessageMetadata& metadata) {
    if (layout_.layout) return HandleGroupedSglangRemoved(event);
    std::vector<prefixindex::ProjectedPrefix> prefixes;
    prefixes.reserve(event.block_hashes.size());
    for (uint64_t hash : event.block_hashes) {
        prefixes.push_back({.value = hash});
    }
    if (prefixes.empty()) {
        return "";
    }
    if (IsSglangDeviceMedium(event.medium)) {
        return manager_->GetIndexer()->RemoveEngine({
            .context = ContextFromService(service_),
            .prefixes = std::move(prefixes),
            .owner = EngineOwnerFromService(service_),
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group,
        });
    }
    const auto tier = SglangLocalTier(event.medium);
    if (!tier.has_value()) {
        return "unsupported SGLang storage medium: " +
               OriginalMedium(event.medium);
    }
    return manager_->GetIndexer()->RemoveEngine({
        .context = ContextFromService(service_),
        .prefixes = std::move(prefixes),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
        .tier = *tier,
    });
}

std::string KVEventHandler::HandleSglangCleared(
    const zmq::MessageMetadata& metadata) {
    return manager_->GetIndexer()->ClearEngine(
        {.context = ContextFromService(service_),
         .owner = EngineOwnerFromService(service_),
         .effective_block_size = service_.block_size,
         .cache_group = service_.cache_group,
         .tier = std::nullopt});
}

std::string KVEventHandler::HandleSglangMooncakeStored(
    const zmq::MooncakeStoredEvent& event,
    const zmq::MessageMetadata& metadata) {
    const auto tier = SharedTier(event.fields.medium);
    if (!tier.has_value()) {
        return "";
    }
    if (event.fields.backend_id.empty() ||
        !event.object.object_key.has_value() ||
        event.object.object_key->empty()) {
        return "backend_id and object_key are required";
    }
    prefixindex::ContextKey context;
    if (std::string error = ValidateMooncakeContext(event.fields, &context);
        !error.empty()) {
        return error;
    }
    if (context.model_name != service_.model_name ||
        context.lora_name != service_.lora_name ||
        context.block_size != service_.block_size ||
        context.tenant_id != service_.tenant_id) {
        return "SGLang Mooncake event context conflicts with registration";
    }
    if (std::string error = manager_->GetIndexer()->ValidateProfileBinding(
            context, ProfileFromService(service_));
        !error.empty()) {
        return "SGLang Mooncake profile binding rejected: " + error;
    }

    ParsedSglangObjectKey parsed;
    if (std::string error =
            ParseSglangObjectKey(*event.object.object_key, &parsed);
        !error.empty()) {
        return error;
    }
    if (event.object.connector_block_hash.has_value()) {
        ConnectorHash asserted_hash;
        if (std::string error = DecodeConnectorHash(
                *event.object.connector_block_hash, &asserted_hash);
            !error.empty()) {
            return error;
        }
        if (asserted_hash.normalized_hex != parsed.full_hash) {
            return "connector_block_hash conflicts with SGLang object_key";
        }
    }
    if (!event.object.seq_hashes.empty() &&
        (event.object.seq_hashes.size() != 1 ||
         event.object.seq_hashes.front() != parsed.prefix.value)) {
        return "seq_hashes conflicts with SGLang object_key";
    }

    const prefixindex::SharedObjectOwner owner{
        .source_stream = service_.endpoint,
        .backend_id = event.fields.backend_id,
        .object_id = parsed.logical_key,
    };
    const PoolObjectKey key{service_.endpoint, event.fields.backend_id,
                            event.fields.tenant_id, parsed.logical_key, *tier};
    PoolObjectBinding binding{context,
                              parsed.prefix,
                              parsed.full_hash,
                              owner,
                              event.fields.tenant_id,
                              event.object.group_id.value_or(""),
                              {parsed.component_suffix}};
    std::lock_guard lock(bindings_mu_);
    auto existing = pool_bindings_.find(key);
    if (existing != pool_bindings_.end() &&
        (existing->second.context != binding.context ||
         existing->second.prefix != binding.prefix ||
         existing->second.connector_block_hash !=
             binding.connector_block_hash ||
         existing->second.owner != binding.owner ||
         existing->second.group_id != binding.group_id)) {
        return "conflicting active SGLang logical-hash binding";
    }
    // Capacity eviction drops index entries but retains bindings. Refresh the
    // idempotent logical owner on every announcement to restore lost presence.
    if (std::string error = manager_->GetIndexer()->StoreShared(
            {.context = context,
             .prefixes = {parsed.prefix},
             .tier = *tier,
             .owner = owner,
             .effective_block_size = context.block_size,
             .cache_group = std::nullopt});
        !error.empty()) {
        return error;
    }
    if (existing == pool_bindings_.end()) {
        pool_bindings_.emplace(key, std::move(binding));
    } else {
        existing->second.physical_components.insert(parsed.component_suffix);
    }
    return "";
}

std::string KVEventHandler::HandleSglangMooncakeRemoved(
    const zmq::MooncakeRemovedEvent& event,
    const zmq::MessageMetadata& metadata) {
    if (event.fields.backend_id.empty() ||
        !event.object.object_key.has_value() ||
        event.object.object_key->empty()) {
        return "backend_id and object_key are required";
    }
    const auto tier = SharedTier(event.fields.medium);
    if (!tier.has_value()) {
        return "";
    }
    ParsedSglangObjectKey parsed;
    if (std::string error =
            ParseSglangObjectKey(*event.object.object_key, &parsed);
        !error.empty()) {
        return error;
    }
    const PoolObjectKey key{service_.endpoint, event.fields.backend_id,
                            event.fields.tenant_id, parsed.logical_key, *tier};
    std::lock_guard lock(bindings_mu_);
    auto binding = pool_bindings_.find(key);
    if (binding == pool_bindings_.end()) {
        return "";
    }
    if (event.fields.model_name.has_value() &&
        *event.fields.model_name != binding->second.context.model_name) {
        return "model_name conflicts with stored SGLang object binding";
    }
    if (event.fields.lora_name.value_or("") !=
            binding->second.context.lora_name ||
        (event.fields.block_size.has_value() &&
         *event.fields.block_size != binding->second.context.block_size)) {
        return "event context conflicts with stored SGLang object binding";
    }
    if (event.object.group_id.has_value() &&
        *event.object.group_id != binding->second.group_id) {
        return "group_id conflicts with stored SGLang object binding";
    }
    if (!event.object.seq_hashes.empty() &&
        (event.object.seq_hashes.size() != 1 ||
         event.object.seq_hashes.front() != binding->second.prefix.value)) {
        return "seq_hashes conflicts with stored SGLang object binding";
    }
    if (event.object.connector_block_hash.has_value()) {
        ConnectorHash asserted_hash;
        if (std::string error = DecodeConnectorHash(
                *event.object.connector_block_hash, &asserted_hash);
            !error.empty()) {
            return error;
        }
        if (asserted_hash.normalized_hex !=
            binding->second.connector_block_hash) {
            return "connector_block_hash conflicts with stored SGLang object "
                   "binding";
        }
    }
    auto component =
        binding->second.physical_components.find(parsed.component_suffix);
    if (component == binding->second.physical_components.end()) {
        return "";
    }
    if (binding->second.physical_components.size() > 1) {
        binding->second.physical_components.erase(component);
        return "";
    }
    if (std::string error = manager_->GetIndexer()->RemoveShared(
            {.context = binding->second.context,
             .prefixes = {binding->second.prefix},
             .tier = *tier,
             .owner = binding->second.owner,
             .effective_block_size = binding->second.context.block_size,
             .cache_group = std::nullopt});
        !error.empty()) {
        return error;
    }
    pool_bindings_.erase(binding);
    return "";
}

std::string KVEventHandler::HandleMooncakeBatch(
    const zmq::MooncakeEventBatch& batch,
    const zmq::MessageMetadata& metadata) {
    if (layout_.layout && !layout_.layout->unavailable_reason.empty()) {
        if (!manager_->GetIndexer()->GetDeployment(ContextFromService(service_),
                                                   &layout_))
            return QueuePending(batch, metadata);
        layout_.source = prefixindex::LayoutSource::kMooncake;
    }
    if (!pending_.empty()) DrainPending();
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
    if (EnsureDeployment()) return HandleGroupedVllmStored(event);
    if (std::string error =
            prefixindex::ValidateVllmStoredAssertions(event, service_);
        !error.empty()) {
        return error;
    }
    const auto tier = VllmTier(event.medium);
    if (!tier.has_value()) {
        VLOG(1) << "Ignoring vLLM unsupported-medium event endpoint="
                << service_.endpoint << " publisher_kind="
                << common::PublisherKindName(metadata.publisher_kind)
                << " instance=" << service_.instance_id
                << " dp_rank=" << service_.dp_rank
                << " event_type=BlockStored medium="
                << OriginalMedium(event.medium)
                << " hash_count=" << HashCount(event)
                << " topic=" << metadata.topic << " seq=" << metadata.sequence;
        return "";
    }
    std::vector<prefixindex::ProjectedPrefix> prefixes;
    if (std::string error =
            NormalizeExternalHashes(event.block_hashes, &prefixes);
        !error.empty()) {
        return error;
    }
    return manager_->GetIndexer()->StoreEngine({
        .context = ContextFromService(service_),
        .prefixes = std::move(prefixes),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
        .tier = *tier,
    });
}

std::string KVEventHandler::HandleVllmRemoved(
    const zmq::VllmRemovedEvent& event, const zmq::MessageMetadata& metadata) {
    if (EnsureDeployment()) return HandleGroupedVllmRemoved(event);
    if (std::string error = prefixindex::ValidateVllmGroup(event.group_idx);
        !error.empty()) {
        return error;
    }
    const auto tier = VllmTier(event.medium);
    if (!tier.has_value()) {
        VLOG(1) << "Ignoring vLLM unsupported-medium event endpoint="
                << service_.endpoint << " publisher_kind="
                << common::PublisherKindName(metadata.publisher_kind)
                << " instance=" << service_.instance_id
                << " dp_rank=" << service_.dp_rank
                << " event_type=BlockRemoved medium="
                << OriginalMedium(event.medium)
                << " hash_count=" << HashCount(event)
                << " topic=" << metadata.topic << " seq=" << metadata.sequence;
        return "";
    }
    std::vector<prefixindex::ProjectedPrefix> prefixes;
    if (std::string error =
            NormalizeExternalHashes(event.block_hashes, &prefixes);
        !error.empty()) {
        return error;
    }
    return manager_->GetIndexer()->RemoveEngine({
        .context = ContextFromService(service_),
        .prefixes = std::move(prefixes),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
        .tier = *tier,
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
    return manager_->GetIndexer()->ClearEngine({
        .context = ContextFromService(service_),
        .owner = EngineOwnerFromService(service_),
        .effective_block_size = service_.block_size,
        .cache_group = service_.cache_group,
        .tier = std::nullopt,
    });
}

std::string KVEventHandler::HandleMooncakeStored(
    const zmq::MooncakeStoredEvent& event,
    const zmq::MessageMetadata& metadata) {
    if (EnsureDeployment()) {
        return HandleGroupedMooncakeStored(event);
    }
    if (service_.hash_profile.strategy == "sglang" ||
        service_.hash_profile.strategy == "sglang_bigram") {
        return HandleSglangMooncakeStored(event, metadata);
    }
    if (service_.hash_profile.strategy != "vllm_v1") {
        return "unsupported Mooncake object-key strategy";
    }
    const auto tier = SharedTier(event.fields.medium);
    if (!tier.has_value()) {
        LOG(WARNING) << "Ignoring Mooncake unsupported-medium event endpoint="
                     << service_.endpoint << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " instance=" << service_.instance_id
                     << " event_type=stored medium="
                     << OriginalMedium(event.fields.medium)
                     << " seq_hash_count=" << event.object.seq_hashes.size()
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
    if (event.object.connector_block_hash.has_value()) {
        if (std::string error =
                DecodeConnectorHash(*event.object.connector_block_hash, &hash);
            !error.empty()) {
            return error;
        }
    } else {
        ParsedSglangObjectKey parsed;
        if (std::string error =
                ParseVllmObjectKey(*event.object.object_key, &parsed);
            !error.empty()) {
            return "connector_block_hash is absent and object_key parsing " +
                   error;
        }
        hash.prefix = parsed.prefix;
        hash.normalized_hex = parsed.full_hash;
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
        .group_id = event.object.group_id.value_or(""),
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
    if (EnsureDeployment()) return HandleGroupedMooncakeRemoved(event);
    if (service_.hash_profile.strategy == "sglang" ||
        service_.hash_profile.strategy == "sglang_bigram") {
        return HandleSglangMooncakeRemoved(event, metadata);
    }
    if (service_.hash_profile.strategy != "vllm_v1") {
        return "unsupported Mooncake object-key strategy";
    }
    const auto tier = SharedTier(event.fields.medium);
    if (!tier.has_value()) {
        LOG(WARNING) << "Ignoring Mooncake unsupported-medium event endpoint="
                     << service_.endpoint << " publisher_kind="
                     << common::PublisherKindName(metadata.publisher_kind)
                     << " instance=" << service_.instance_id
                     << " event_type=removed medium="
                     << OriginalMedium(event.fields.medium)
                     << " seq_hash_count=" << event.object.seq_hashes.size()
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
    if (layout_.layout) {
        if (tenant_id && *tenant_id != service_.tenant_id)
            return "clear tenant conflicts with registration";
        return manager_->GetIndexer()->ClearGroupedSource(
            ContextFromService(service_), service_.endpoint, backend_id);
    }
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
    std::unique_lock<std::mutex> grouped_lock;
    if (grouped_context()) grouped_lock = std::unique_lock(grouped_dispatch_mu_);
    pending_.clear();
    pending_event_count_ = pending_bytes_ = 0;
    if (service_.publisher_kind == common::PublisherKind::kVllm ||
        service_.publisher_kind == common::PublisherKind::kSglang) {
        return manager_->GetIndexer()->ClearEngine({
            .context = ContextFromService(service_),
            .owner = EngineOwnerFromService(service_),
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group,
            .tier = std::nullopt,
        });
    }
    return ClearMooncakeBindings(std::nullopt, std::nullopt);
}

}  // namespace mooncake::conductor::kvevent
