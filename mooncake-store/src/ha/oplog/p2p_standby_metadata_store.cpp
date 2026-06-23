#include "ha/oplog/p2p_standby_metadata_store.h"

#include <glog/logging.h>
#include <xxhash.h>

namespace mooncake {

// ============================================================================
// MetadataStore interface
// ============================================================================

bool P2PStandbyMetadataStore::PutMetadata(
    const std::string& key, const StandbyObjectMetadata& metadata) {
    objects_[key] = metadata;
    return true;
}

bool P2PStandbyMetadataStore::Put(const std::string& key,
                                  const std::string& /*payload*/) {
    // Legacy Put — create empty metadata. Called by OpLogApplier base class
    // for PUT_END without payload (legacy compatibility).
    objects_[key] = StandbyObjectMetadata{};
    return true;
}

std::optional<StandbyObjectMetadata> P2PStandbyMetadataStore::GetMetadata(
    const std::string& key) const {
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool P2PStandbyMetadataStore::Remove(const std::string& key) {
    return objects_.erase(key) > 0;
}

bool P2PStandbyMetadataStore::Exists(const std::string& key) const {
    return objects_.find(key) != objects_.end();
}

size_t P2PStandbyMetadataStore::GetKeyCount() const { return objects_.size(); }

// ============================================================================
// P2P-specific operations
// ============================================================================

void P2PStandbyMetadataStore::AddReplica(const std::string& object_key,
                                         const UUID& client_id,
                                         const UUID& segment_id, size_t size,
                                         int priority,
                                         const std::vector<std::string>& tags,
                                         MemoryType memory_type) {
    auto& metadata = objects_[object_key];
    metadata.size = size;
    metadata.last_sequence_id = 0;  // Will be set by Applier

    // Build a Replica::Descriptor for this replica.
    // P2PProxyDescriptor is the P2P-specific descriptor type.
    Replica::Descriptor desc;
    ReplicaID replica_id = static_cast<ReplicaID>(XXH64(
        object_key.data(), object_key.size(),
        XXH64(&client_id, sizeof(UUID), XXH64(&segment_id, sizeof(UUID), 0))));
    desc.id = replica_id;

    // Look up client info for IP/port
    auto client_it = clients_.find(client_id);
    if (client_it != clients_.end()) {
        P2PProxyDescriptor p2p_desc;
        p2p_desc.client_id = client_id;
        p2p_desc.segment_id = segment_id;
        p2p_desc.ip_address = client_it->second.ip_address;
        p2p_desc.rpc_port = client_it->second.rpc_port;
        p2p_desc.object_size = size;
        desc.descriptor_variant = p2p_desc;
    } else {
        // Client not registered yet — create a P2PProxyDescriptor with
        // empty IP/port. This can happen if ADD_REPLICA arrives before
        // REGISTER_CLIENT. The entry will be updated when REGISTER_CLIENT
        // arrives later.
        P2PProxyDescriptor p2p_desc;
        p2p_desc.client_id = client_id;
        p2p_desc.segment_id = segment_id;
        p2p_desc.object_size = size;
        desc.descriptor_variant = p2p_desc;
    }
    desc.status = ReplicaStatus::COMPLETE;

    metadata.replicas.push_back(std::move(desc));
    VLOG(1) << "P2PStandbyMetadataStore::AddReplica key=" << object_key
            << " client=" << client_id.first << ":" << client_id.second
            << " seg=" << segment_id.first << ":" << segment_id.second
            << " total_replicas=" << metadata.replicas.size();
}

void P2PStandbyMetadataStore::RemoveReplica(const std::string& object_key,
                                            const UUID& client_id,
                                            const UUID& segment_id) {
    auto it = objects_.find(object_key);
    if (it == objects_.end()) {
        VLOG(1) << "P2PStandbyMetadataStore::RemoveReplica key=" << object_key
                << " not found, ignoring";
        return;
    }

    auto& replicas = it->second.replicas;
    replicas.erase(
        std::remove_if(replicas.begin(), replicas.end(),
                       [&](const Replica::Descriptor& desc) {
                           if (!std::holds_alternative<P2PProxyDescriptor>(
                                   desc.descriptor_variant)) {
                               return false;
                           }
                           const auto& p2p = std::get<P2PProxyDescriptor>(
                               desc.descriptor_variant);
                           return p2p.client_id == client_id &&
                                  p2p.segment_id == segment_id;
                       }),
        replicas.end());

    // Remove object if no replicas left
    if (replicas.empty()) {
        objects_.erase(it);
        VLOG(1) << "P2PStandbyMetadataStore::RemoveReplica key=" << object_key
                << " removed (no replicas left)";
    } else {
        VLOG(1) << "P2PStandbyMetadataStore::RemoveReplica key=" << object_key
                << " remaining_replicas=" << replicas.size();
    }
}

void P2PStandbyMetadataStore::RegisterClient(
    const UUID& client_id, const std::string& ip_address, uint16_t rpc_port,
    const std::vector<Segment>& segments) {
    auto& info = clients_[client_id];
    info.client_id = client_id;
    info.ip_address = ip_address;
    info.rpc_port = rpc_port;
    // Merge segments instead of overwriting to preserve segments added by
    // out-of-order AddSegment (MOUNT_SEGMENT) calls that arrived before
    // REGISTER_CLIENT.
    for (const auto& seg : segments) {
        if (std::find_if(info.segments.begin(), info.segments.end(),
                         [&](const Segment& existing) {
                             return existing.id == seg.id;
                         }) == info.segments.end()) {
            info.segments.push_back(seg);
        }
    }

    VLOG(1) << "P2PStandbyMetadataStore::RegisterClient "
            << "client=" << client_id.first << ":" << client_id.second
            << " ip=" << ip_address << ":" << rpc_port
            << " segments=" << segments.size();

    // IP/port backfilling is deferred to ExportMetadata() to avoid O(N*M)
    // overhead on the OpLog replication thread. IP/port is only needed after
    // promotion, so a one-time scan during ExportMetadata() is acceptable.
}

void P2PStandbyMetadataStore::AddSegment(const UUID& client_id,
                                         const Segment& segment) {
    auto& info = clients_[client_id];
    info.client_id = client_id;  // In case client wasn't registered yet

    // Check if segment already exists
    for (const auto& seg : info.segments) {
        if (seg.id == segment.id) {
            VLOG(1) << "P2PStandbyMetadataStore::AddSegment "
                    << "segment already exists, ignoring";
            return;
        }
    }

    info.segments.push_back(segment);
    VLOG(1) << "P2PStandbyMetadataStore::AddSegment "
            << "client=" << client_id.first << ":" << client_id.second
            << " segment=" << segment.id.first << ":" << segment.id.second
            << " total_segments=" << info.segments.size();
}

void P2PStandbyMetadataStore::RemoveSegment(const UUID& segment_id,
                                            const UUID& client_id) {
    // Remove segment from client
    auto client_it = clients_.find(client_id);
    if (client_it != clients_.end()) {
        auto& segments = client_it->second.segments;
        segments.erase(std::remove_if(segments.begin(), segments.end(),
                                      [&](const Segment& seg) {
                                          return seg.id == segment_id;
                                      }),
                       segments.end());
    }

    // Cascade delete: remove all replicas on this segment
    RemoveReplicasBySegmentInternal(segment_id);

    VLOG(1) << "P2PStandbyMetadataStore::RemoveSegment "
            << "segment=" << segment_id.first << ":" << segment_id.second
            << " client=" << client_id.first << ":" << client_id.second;
}

void P2PStandbyMetadataStore::RemoveReplicasBySegment(const UUID& segment_id) {
    RemoveReplicasBySegmentInternal(segment_id);
}

void P2PStandbyMetadataStore::RemoveAllMetadata() {
    objects_.clear();
    clients_.clear();
    VLOG(1) << "P2PStandbyMetadataStore::RemoveAllMetadata";
}

// ============================================================================
// Export for Promotion
// ============================================================================

P2PStandbyMetadataStore::ExportedMetadata
P2PStandbyMetadataStore::ExportMetadata() const {
    ExportedMetadata result;
    result.objects = objects_;
    result.clients = clients_;

    // Backfill IP/port for replicas whose clients were registered out-of-order.
    // This is deferred from RegisterClient to avoid O(N*M) overhead on the
    // replication thread. ExportMetadata() is a one-time call during promotion,
    // where this cost is acceptable.
    for (auto& [key, metadata] : result.objects) {
        for (auto& desc : metadata.replicas) {
            if (!std::holds_alternative<P2PProxyDescriptor>(
                    desc.descriptor_variant)) {
                continue;
            }
            auto& p2p = std::get<P2PProxyDescriptor>(desc.descriptor_variant);
            if (p2p.ip_address.empty()) {
                auto client_it = result.clients.find(p2p.client_id);
                if (client_it != result.clients.end()) {
                    p2p.ip_address = client_it->second.ip_address;
                    p2p.rpc_port = client_it->second.rpc_port;
                }
            }
        }
    }

    return result;
}

// ============================================================================
// Query helpers
// ============================================================================

const P2PStandbyClientInfo* P2PStandbyMetadataStore::GetClient(
    const UUID& client_id) const {
    auto it = clients_.find(client_id);
    return it != clients_.end() ? &it->second : nullptr;
}

// ============================================================================
// Internal helpers
// ============================================================================

void P2PStandbyMetadataStore::RemoveReplicasBySegmentInternal(
    const UUID& segment_id) {
    // Iterate all objects, remove replicas referencing this segment
    for (auto it = objects_.begin(); it != objects_.end();) {
        auto& replicas = it->second.replicas;
        replicas.erase(
            std::remove_if(replicas.begin(), replicas.end(),
                           [&](const Replica::Descriptor& desc) {
                               if (!std::holds_alternative<P2PProxyDescriptor>(
                                       desc.descriptor_variant)) {
                                   return false;
                               }
                               const auto& p2p = std::get<P2PProxyDescriptor>(
                                   desc.descriptor_variant);
                               return p2p.segment_id == segment_id;
                           }),
            replicas.end());

        if (replicas.empty()) {
            it = objects_.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace mooncake