#include <glog/logging.h>

#include <utility>
#include <variant>

#include "conductor/kvevent/event_manager.h"

namespace conductor {
namespace kvevent {

KVEventHandler::KVEventHandler(EventManager* manager, common::ServiceConfig svc)
    : manager_(manager),
      tenant_id_(svc.tenant_id),
      model_name_(svc.model_name),
      lora_name_(svc.lora_name),
      instance_id_(svc.instance_id),
      block_size_(svc.block_size),
      additional_salt_(svc.additional_salt) {}

std::string KVEventHandler::HandleEvent(const zmq::KVEvent& event,
                                        int64_t dp_rank) {
    // Reads manager state under the manager read lock — this is the
    // reason unsubscribeFromService must call client->Stop() OUTSIDE the
    // manager lock (see event_manager.cpp).
    if (manager_->IsStopped()) {
        return "manager stopped";
    }
    LOG(INFO) << "Handling KV event instance_id=" << instance_id_
              << " dpRank=" << dp_rank;

    // No per-event timeout is enforced; processing is synchronous.

    if (const auto* e = std::get_if<zmq::BlockStoredEvent>(&event)) {
        VLOG(1) << "BlockStored instance_id=" << instance_id_
                << " dpRank=" << dp_rank
                << " blocks=" << e->block_hashes.size();
        LOG(INFO) << "Received BlockStoredEvent medium=" << e->medium;
        return HandleBlockStored(*e, dp_rank);
    }
    if (const auto* e = std::get_if<zmq::BlockRemovedEvent>(&event)) {
        VLOG(1) << "BlockRemoved instance_id=" << instance_id_
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
    if (event.block_size != block_size_) {
        LOG(WARNING) << "handleBlockStored: Block size mismatch, the event "
                        "will be discarded. expected="
                     << block_size_ << " actual=" << event.block_size;
        return "";
    }
    // Convert to kvindexer event
    common::StoredEvent conductor_event;
    conductor_event.block_hashes = event.block_hashes;
    conductor_event.block_size = event.block_size;
    conductor_event.model_name = model_name_;
    conductor_event.lora_name = lora_name_;
    conductor_event.instance_id = instance_id_;
    conductor_event.parent_block_hash = event.parent_block_hash;
    conductor_event.token_ids = event.token_ids;
    conductor_event.medium = event.medium;

    auto* indexer = manager_->GetIndexer();
    const auto err = indexer->ProcessStoreEvent(conductor_event, dp_rank);
    // TODO: support mooncake_key map
    if (!err.empty()) {
        LOG(ERROR) << "process store event failed. error=" << err;
    }

    VLOG(1) << "in handleBlockStored instance_id=" << instance_id_;

    return "";
}

std::string KVEventHandler::HandleBlockRemoved(
    const zmq::BlockRemovedEvent& event, int64_t dp_rank) {
    // Convert to conductor event
    common::RemovedEvent conductor_event;
    conductor_event.block_hashes = event.block_hashes;
    conductor_event.model_name = model_name_;
    conductor_event.lora_name = lora_name_;
    conductor_event.instance_id = instance_id_;
    conductor_event.block_size = block_size_;
    // BUG: Medium not propagated to RemovedEvent — medium_set cannot shrink
    // correctly.

    auto* indexer = manager_->GetIndexer();
    const auto err =
        indexer->ProcessRemoveEvent(conductor_event, dp_rank, instance_id_);
    if (!err.empty()) {
        LOG(ERROR) << "process store event failed.";
    }
    VLOG(1) << "in handleBlockRemoved instance_id=" << instance_id_;

    return "";
}

// TODO: support mooncake BlockUpdateEvent / RemoveAllEvent

}  // namespace kvevent
}  // namespace conductor
