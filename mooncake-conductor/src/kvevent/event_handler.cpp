#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <utility>
#include <variant>

#include "conductor/kvevent/event_manager.h"

namespace conductor {
namespace kvevent {

namespace {

std::string LowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char character) {
                       return static_cast<char>(std::tolower(character));
                   });
    return value;
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

std::vector<prefixindex::ProjectedPrefix> ProjectedPrefixes(
    const std::vector<uint64_t>& hashes) {
    std::vector<prefixindex::ProjectedPrefix> prefixes;
    prefixes.reserve(hashes.size());
    for (const uint64_t hash : hashes) {
        prefixes.push_back(prefixindex::ProjectedPrefix{hash});
    }
    return prefixes;
}

std::optional<prefixindex::StorageTier> SharedTier(const std::string& medium) {
    const std::string normalized = LowerAscii(medium);
    if (normalized == "cpu") {
        return prefixindex::StorageTier::kCpu;
    }
    if (normalized == "disk") {
        return prefixindex::StorageTier::kDisk;
    }
    return std::nullopt;
}

std::string ProjectedObjectId(const std::vector<uint64_t>& hashes) {
    std::string object_id;
    for (const uint64_t hash : hashes) {
        if (!object_id.empty()) {
            object_id.push_back(',');
        }
        object_id += std::to_string(hash);
    }
    return object_id;
}

prefixindex::SharedObjectOwner SharedOwner(
    const common::ServiceConfig& service,
    const std::vector<uint64_t>& projected_hashes) {
    return {.source_stream = service.endpoint,
            .backend_id = service.instance_id.empty() ? service.endpoint
                                                      : service.instance_id,
            // The current removal wire format has no Mooncake object key.
            // A projected-hash identity keeps Store/Remove symmetric until
            // the source-specific ingest adapter supplies the complete key.
            .object_id = ProjectedObjectId(projected_hashes)};
}

}  // namespace

KVEventHandler::KVEventHandler(EventManager* manager, common::ServiceConfig svc)
    : manager_(manager), service_(std::move(svc)) {}

std::string KVEventHandler::HandleEvent(const zmq::KVEvent& event,
                                        int64_t dp_rank) {
    // Reads manager state under the manager read lock — this is the
    // reason unsubscribeFromService must call client->Stop() OUTSIDE the
    // manager lock (see event_manager.cpp).
    if (manager_->IsStopped()) {
        return "manager stopped";
    }
    LOG(INFO) << "Handling KV event instance_id=" << service_.instance_id
              << " dpRank=" << dp_rank;

    // No per-event timeout is enforced; processing is synchronous.

    if (const auto* e = std::get_if<zmq::BlockStoredEvent>(&event)) {
        VLOG(1) << "BlockStored instance_id=" << service_.instance_id
                << " dpRank=" << dp_rank
                << " blocks=" << e->block_hashes.size();
        LOG(INFO) << "Received BlockStoredEvent medium=" << e->medium;
        return HandleBlockStored(*e, dp_rank);
    }
    if (const auto* e = std::get_if<zmq::BlockRemovedEvent>(&event)) {
        VLOG(1) << "BlockRemoved instance_id=" << service_.instance_id
                << " dpRank=" << dp_rank
                << " blocks=" << e->block_hashes.size();
        LOG(INFO) << "Received BlockRemovedEvent medium=" << e->medium;
        return HandleBlockRemoved(*e, dp_rank);
    }

    LOG(WARNING) << "Unknown event type";
    return "";
}

std::string KVEventHandler::HandleBlockStored(
    const zmq::BlockStoredEvent& event, int64_t dp_rank) {
    if (event.block_size != service_.block_size) {
        LOG(WARNING) << "handleBlockStored: Block size mismatch, the event "
                        "will be discarded. expected="
                     << service_.block_size << " actual=" << event.block_size;
        return "block_size mismatch: expected " +
               std::to_string(service_.block_size) + ", got " +
               std::to_string(event.block_size);
    }

    const prefixindex::ContextKey context = ContextFromService(service_);
    auto* const indexer = manager_->GetIndexer();
    if (service_.type == common::kServiceTypeVLLM) {
        if (dp_rank != service_.dp_rank) {
            LOG(WARNING) << "Ignoring vLLM event from an unregistered DP rank. "
                         << "expected=" << service_.dp_rank
                         << " actual=" << dp_rank;
            return "dp_rank mismatch: expected " +
                   std::to_string(service_.dp_rank) + ", got " +
                   std::to_string(dp_rank);
        }
        if (LowerAscii(event.medium) != "gpu") {
            LOG(WARNING) << "Ignoring non-GPU vLLM cache event medium="
                         << event.medium;
            return "";
        }
        prefixindex::GpuMutation mutation{
            .context = context,
            .prefixes = ProjectedPrefixes(event.block_hashes),
            .owner = {.source_stream = service_.endpoint,
                      .instance_id = service_.instance_id,
                      .dp_rank = service_.dp_rank},
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group};
        return indexer->StoreGpu(mutation);
    }

    if (service_.type == common::kServiceTypeMooncake) {
        if (std::string error = indexer->ValidateProfileBinding(
                context, ProfileFromService(service_));
            !error.empty()) {
            return "Mooncake profile binding rejected: " + error;
        }
        const auto tier = SharedTier(event.medium);
        if (!tier.has_value()) {
            LOG(WARNING) << "Ignoring Mooncake event without a supported "
                            "CPU/DISK medium medium="
                         << event.medium;
            return "";
        }
        prefixindex::SharedMutation mutation{
            .context = context,
            .prefixes = ProjectedPrefixes(event.block_hashes),
            .tier = *tier,
            .owner = SharedOwner(service_, event.block_hashes),
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group};
        return indexer->StoreShared(mutation);
    }

    return "unsupported service type: " + service_.type;
}

std::string KVEventHandler::HandleBlockRemoved(
    const zmq::BlockRemovedEvent& event, int64_t dp_rank) {
    const prefixindex::ContextKey context = ContextFromService(service_);
    auto* const indexer = manager_->GetIndexer();
    if (service_.type == common::kServiceTypeVLLM) {
        if (dp_rank != service_.dp_rank) {
            LOG(WARNING) << "Rejecting vLLM removal from an unregistered DP "
                            "rank expected="
                         << service_.dp_rank << " actual=" << dp_rank;
            return "dp_rank mismatch: expected " +
                   std::to_string(service_.dp_rank) + ", got " +
                   std::to_string(dp_rank);
        }
        if (LowerAscii(event.medium) != "gpu") {
            LOG(WARNING) << "Ignoring vLLM removal outside its registered GPU "
                            "rank medium="
                         << event.medium << " dp_rank=" << dp_rank;
            return "";
        }
        prefixindex::GpuMutation mutation{
            .context = context,
            .prefixes = ProjectedPrefixes(event.block_hashes),
            .owner = {.source_stream = service_.endpoint,
                      .instance_id = service_.instance_id,
                      .dp_rank = service_.dp_rank},
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group};
        return indexer->RemoveGpu(mutation);
    }

    if (service_.type == common::kServiceTypeMooncake) {
        if (std::string error = indexer->ValidateProfileBinding(
                context, ProfileFromService(service_));
            !error.empty()) {
            return "Mooncake profile binding rejected: " + error;
        }
        const auto tier = SharedTier(event.medium);
        if (!tier.has_value()) {
            LOG(WARNING) << "Ignoring Mooncake removal without a supported "
                            "CPU/DISK medium medium="
                         << event.medium;
            return "";
        }
        prefixindex::SharedMutation mutation{
            .context = context,
            .prefixes = ProjectedPrefixes(event.block_hashes),
            .tier = *tier,
            .owner = SharedOwner(service_, event.block_hashes),
            .effective_block_size = service_.block_size,
            .cache_group = service_.cache_group};
        return indexer->RemoveShared(mutation);
    }

    return "unsupported service type: " + service_.type;
}

// TODO: support mooncake BlockUpdateEvent / RemoveAllEvent

}  // namespace kvevent
}  // namespace conductor
