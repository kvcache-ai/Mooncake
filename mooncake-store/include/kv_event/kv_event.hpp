#pragma once

#ifndef KVEVENT_H
#define KVEVENT_H

#include "replica.h"

#include <msgpack.hpp>

#include <atomic>
#include <vector>
#include <memory>
#include <string>
#include <chrono>

namespace mooncake {

class KVCacheEvent {
public:

    virtual ~KVCacheEvent() = default;
    virtual void pack(msgpack::packer<msgpack::sbuffer>& pk) const = 0;
    virtual std::string_view type_tag() const = 0;
};

template<typename Event>
    concept DerivedFromKVCacheEvent = std::is_base_of_v<KVCacheEvent, Event>;

struct StoreEventInfo {
    std::string model_name;
    uint32_t block_size;
    std::string block_hash;
    std::string parent_block_hash;
    std::vector<uint32_t> token_ids;

    YLT_REFL(StoreEventInfo, model_name, block_size, block_hash, parent_block_hash, token_ids);
};

// KVEvent to be reported only for the first-time storage of KVCache
class BlockStoreEvent : public KVCacheEvent {
public:
    // data stored in Mooncake
    std::string mooncake_key;
    std::vector<Replica::Descriptor> replicas;

    StoreEventInfo store_event_info;

    BlockStoreEvent(std::string key, 
                   std::vector<Replica::Descriptor> replica_list,
                   const StoreEventInfo& info)
        : mooncake_key(std::move(key)), 
          replicas(std::move(replica_list)),
          store_event_info(info) {}

    void pack(msgpack::packer<msgpack::sbuffer>& pk) const override {
        pk.pack_array(8);

        pk.pack(type_tag());
        pk.pack(mooncake_key);
        pk.pack_array(replicas.size());
        for (const auto& replica : replicas) {
            if (replica.is_memory_replica()) {
                pk.pack_array(2); 
                pk.pack("memory");
                pk.pack(replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_);
            } else if (replica.is_disk_replica()) {
                pk.pack_array(2);
                pk.pack("disk");
                pk.pack(replica.get_disk_descriptor().file_path);
            } else {
                throw std::runtime_error("Unknown replica type in BlockStoreEvent");
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

// KVEvent used within Mooncake-Store includes: 
//   replica replication (addition), replica removal, and replica migration.
class BlockUpdateEvent : public KVCacheEvent {
public:
    std::string mooncake_key;
    std::vector<Replica::Descriptor> replicas;

    BlockUpdateEvent(std::string key, std::vector<Replica::Descriptor> replica_list)
        : mooncake_key(std::move(key)), replicas(std::move(replica_list)) {}

    void pack(msgpack::packer<msgpack::sbuffer>& pk) const override {
        pk.pack_array(3);
        
        pk.pack(type_tag());
        pk.pack(mooncake_key);
        pk.pack_array(replicas.size());
        for (const auto& replica : replicas) {
            if (replica.is_memory_replica()) {
                pk.pack_array(2); 
                pk.pack("memory");
                pk.pack(replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_);
            } else if (replica.is_disk_replica()) {
                pk.pack_array(2);
                pk.pack("disk");
                pk.pack(replica.get_disk_descriptor().file_path);
            } else {
                throw std::runtime_error("Unknown replica type in BlockUpdateEvent");
            }
        }
    }

    std::string_view type_tag() const override { return "BlockUpdateEvent"; }
};

class RemoveAllEvent : public KVCacheEvent {
public:
    RemoveAllEvent() = default;

    void pack(msgpack::packer<msgpack::sbuffer>& pk) const override {
        pk.pack_array(1);
        pk.pack(type_tag());
    }

    std::string_view type_tag() const override { return "RemoveAllEvent"; }
};

class EventBatch {
public:
    double ts;
    std::vector<std::shared_ptr<KVCacheEvent>> events;

    EventBatch(std::vector<std::shared_ptr<KVCacheEvent>> evts)
        : ts(get_current_time()), 
          events(std::move(evts)) {}

    // [ts, events]
    msgpack::sbuffer serialize() const {
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> pk(buffer);
        
        pk.pack_array(2);

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


}

#endif // KVEVENT_H