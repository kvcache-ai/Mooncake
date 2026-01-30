#pragma once

#ifndef MOONCAKE_KV_EVENT_H
#define MOONCAKE_KV_EVENT_H

#include "replica.h"

#include <msgpack.hpp>

#include <atomic>
#include <vector>
#include <memory>
#include <string>
#include <chrono>

namespace mooncake {

/**
 * @brief Base class for KV cache events
 * Defines the interface that all KV cache events must implement for msgpack
 * serialization Uses the Abstract Base Class pattern to provide a uniform event
 * handling interface
 */
class KVCacheEvent {
   public:
    virtual ~KVCacheEvent() = default;

    /**
     * @brief Serialize the event to msgpack format
     * @param pk Reference to msgpack packer
     * Derived classes must implement this method to provide type-specific
     * serialization logic
     */
    virtual void pack(msgpack::packer<msgpack::sbuffer>& pk) const = 0;

    /**
     * @brief Get the event type identifier
     * @return String view of the event type
     * Used for event type identification during deserialization
     */
    virtual std::string_view type_tag() const = 0;
};

/**
 * @brief Additional Information when a KV Cache Store Event happens
 * Contains metadata required for KVCache-aware algorithm
 */
struct StoreEventInfo {
    std::string model_name{""};
    uint32_t block_size{0};
    std::string block_hash{""};
    std::string parent_block_hash{""};
    std::vector<uint32_t> token_ids{};

    YLT_REFL(StoreEventInfo, model_name, block_size, block_hash,
             parent_block_hash, token_ids);
};

/**
 * @brief Block storage event
 * Event triggered when KV cache is stored for the first time
 * Contains StoreEventInfo
 */
class BlockStoreEvent : public KVCacheEvent {
   public:
    // data stored in Mooncake
    std::string mooncake_key;
    std::vector<Replica::Descriptor> replicas;
    StoreEventInfo store_event_info;

    static constexpr size_t kFieldCount{8};

    BlockStoreEvent(std::string key,
                    std::vector<Replica::Descriptor> replica_list,
                    const StoreEventInfo& info)
        : mooncake_key(std::move(key)),
          replicas(std::move(replica_list)),
          store_event_info(info) {}

    void pack(msgpack::packer<msgpack::sbuffer>& pk) const override {
        pk.pack_array(kFieldCount);

        pk.pack(type_tag());
        pk.pack(mooncake_key);
        pk.pack_array(replicas.size());
        for (const auto& replica : replicas) {
            if (replica.is_memory_replica()) {
                pk.pack_array(2);
                pk.pack("memory");
                pk.pack(replica.get_memory_descriptor()
                            .buffer_descriptor.transport_endpoint_);
            } else if (replica.is_disk_replica()) {
                pk.pack_array(2);
                pk.pack("disk");
                pk.pack(replica.get_disk_descriptor().file_path);
            } else if (replica.is_local_disk_replica()) {
                pk.pack_array(2);
                pk.pack("local_disk");
                pk.pack(replica.get_local_disk_descriptor().transport_endpoint);
            } else {
                throw std::runtime_error(
                    "Unknown replica type in BlockStoreEvent");
            }
        }

        pk.pack(store_event_info.model_name);
        pk.pack(store_event_info.block_size);
        pk.pack(store_event_info.block_hash);
        pk.pack(store_event_info.parent_block_hash);
        pk.pack(store_event_info.token_ids);
    }

    std::string_view type_tag() const override { return "BlockStoreEvent"; }
};

/**
 * @brief Block update event
 * Used for replica management operations within Mooncake-Store:
 * 1. Replica copy
 * 2. Replica removal
 * 3. Replica migration
 */
class BlockUpdateEvent : public KVCacheEvent {
   public:
    std::string mooncake_key;
    std::vector<Replica::Descriptor> replicas;
    static constexpr size_t kFieldCount{3};

    BlockUpdateEvent(std::string key,
                     std::vector<Replica::Descriptor> replica_list)
        : mooncake_key(std::move(key)), replicas(std::move(replica_list)) {}

    void pack(msgpack::packer<msgpack::sbuffer>& pk) const override {
        pk.pack_array(kFieldCount);

        pk.pack(type_tag());
        pk.pack(mooncake_key);
        pk.pack_array(replicas.size());
        for (const auto& replica : replicas) {
            if (replica.is_memory_replica()) {
                pk.pack_array(2);
                pk.pack("memory");
                pk.pack(replica.get_memory_descriptor()
                            .buffer_descriptor.transport_endpoint_);
            } else if (replica.is_disk_replica()) {
                pk.pack_array(2);
                pk.pack("disk");
                pk.pack(replica.get_disk_descriptor().file_path);
            } else if (replica.is_local_disk_replica()) {
                pk.pack_array(2);
                pk.pack("local_disk");
                pk.pack(replica.get_local_disk_descriptor().transport_endpoint);
            } else {
                throw std::runtime_error(
                    "Unknown replica type in BlockUpdateEvent");
            }
        }
    }

    std::string_view type_tag() const override { return "BlockUpdateEvent"; }
};

/**
 * @brief Remove all event
 * Special event that instructs receivers to clear all cached data
 */
class RemoveAllEvent : public KVCacheEvent {
   public:
    static constexpr size_t kFieldCount{1};

    RemoveAllEvent() = default;

    void pack(msgpack::packer<msgpack::sbuffer>& pk) const override {
        pk.pack_array(kFieldCount);
        pk.pack(type_tag());
    }

    std::string_view type_tag() const override { return "RemoveAllEvent"; }
};

/**
 * @brief Event batch
 * Packages multiple events into a batch for transmission to improve network
 * efficiency Includes timestamp for receivers to process events in
 * chronological order
 */
class EventBatch {
   public:
    double ts;
    std::vector<std::shared_ptr<KVCacheEvent>> events;
    static constexpr size_t kFieldCount{2};

    EventBatch(std::vector<std::shared_ptr<KVCacheEvent>> evts)
        : ts(get_current_time()), events(std::move(evts)) {}

    // [ts, events]
    msgpack::sbuffer serialize() const {
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> pk(buffer);

        pk.pack_array(kFieldCount);

        pk.pack(ts);

        pk.pack_array(events.size());
        for (const auto& event : events) {
            event->pack(pk);
        }

        return buffer;
    }

   private:
    static double get_current_time() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration<double>(now.time_since_epoch()).count();
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_KV_EVENT_H