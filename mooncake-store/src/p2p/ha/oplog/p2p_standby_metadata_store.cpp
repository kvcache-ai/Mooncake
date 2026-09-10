#include "p2p/ha/oplog/p2p_standby_metadata_store.h"

#include <algorithm>

#include <glog/logging.h>

namespace mooncake {

// ============================================================================
// Route metadata
// ============================================================================

std::optional<P2PStandbyRouteEntry> P2PStandbyMetadataStore::GetRoute(
    const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = routes_.find(key);
    if (it == routes_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool P2PStandbyMetadataStore::RouteExists(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return routes_.find(key) != routes_.end();
}

size_t P2PStandbyMetadataStore::GetRouteKeyCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return routes_.size();
}

void P2PStandbyMetadataStore::RestoreRoute(const std::string& key,
                                           const P2PStandbyRouteEntry& route) {
    std::lock_guard<std::mutex> lock(mutex_);
    routes_[key] = route;
}

// ============================================================================
// P2P-specific operations
// ============================================================================

bool P2PStandbyMetadataStore::PublishRoute(const std::string& key,
                                           const P2PRouteLocation& location,
                                           uint64_t object_size,
                                           uint64_t sequence_id) {
    if (object_size == 0) {
        LOG(ERROR) << "Standby rejected zero-sized route"
                   << ", key=" << key;
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto [it, inserted] = routes_.try_emplace(
        key, P2PStandbyRouteEntry{.object_size = object_size,
                                  .locations = {},
                                  .last_sequence_id = sequence_id});
    auto& route = it->second;
    if (!inserted && route.object_size != object_size) {
        LOG(ERROR) << "Standby rejected route size mismatch"
                   << ", key=" << key << ", existing_size=" << route.object_size
                   << ", requested_size=" << object_size;
        return false;
    }
    route.last_sequence_id = sequence_id;
    if (std::find(route.locations.begin(), route.locations.end(), location) !=
        route.locations.end()) {
        VLOG(1) << "P2PStandbyMetadataStore::PublishRoute key=" << key
                << " client=" << location.client_id.first << ":"
                << location.client_id.second
                << " seg=" << location.segment_id.first << ":"
                << location.segment_id.second
                << " already exists, ignoring duplicate";
        return true;
    }

    route.locations.push_back(location);
    VLOG(1) << "P2PStandbyMetadataStore::PublishRoute key=" << key
            << " client=" << location.client_id.first << ":"
            << location.client_id.second << " seg=" << location.segment_id.first
            << ":" << location.segment_id.second
            << " total_replicas=" << route.locations.size();
    return true;
}

void P2PStandbyMetadataStore::WithdrawRoute(const std::string& key,
                                            const P2PRouteLocation& location) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = routes_.find(key);
    if (it == routes_.end()) {
        VLOG(1) << "P2PStandbyMetadataStore::WithdrawRoute key=" << key
                << " not found, ignoring";
        return;
    }
    std::erase(it->second.locations, location);
    if (it->second.locations.empty()) {
        routes_.erase(it);
        VLOG(1) << "P2PStandbyMetadataStore::WithdrawRoute key=" << key
                << " removed (no replicas left)";
    } else {
        VLOG(1) << "P2PStandbyMetadataStore::WithdrawRoute key=" << key
                << " remaining_replicas=" << it->second.locations.size();
    }
}

void P2PStandbyMetadataStore::RegisterClient(
    const UUID& client_id, const std::string& ip_address, uint16_t rpc_port,
    const std::vector<P2PSegment>& segments) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& info = clients_[client_id];
    info.client_id = client_id;
    info.ip_address = ip_address;
    info.rpc_port = rpc_port;
    // Merge segments instead of overwriting to preserve segments added by
    // out-of-order MountSegment (MOUNT_SEGMENT) calls that arrived before
    // REGISTER_CLIENT.
    for (const auto& seg : segments) {
        if (std::find_if(info.segments.begin(), info.segments.end(),
                         [&](const P2PSegment& existing) {
                             return existing.id == seg.id;
                         }) == info.segments.end()) {
            info.segments.push_back(seg);
        }
    }

    VLOG(1) << "P2PStandbyMetadataStore::RegisterClient "
            << "client=" << client_id.first << ":" << client_id.second
            << " ip=" << ip_address << ":" << rpc_port
            << " segments=" << segments.size();
}

void P2PStandbyMetadataStore::UnregisterClient(const UUID& client_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    clients_.erase(client_id);

    // Cascade delete: remove all routes owned by this client.
    for (auto it = routes_.begin(); it != routes_.end();) {
        std::erase_if(it->second.locations,
                      [&](const P2PRouteLocation& location) {
                          return location.client_id == client_id;
                      });
        if (it->second.locations.empty()) {
            it = routes_.erase(it);
        } else {
            ++it;
        }
    }

    VLOG(1) << "P2PStandbyMetadataStore::UnregisterClient "
            << "client=" << client_id.first << ":" << client_id.second;
}

void P2PStandbyMetadataStore::MountSegment(const UUID& client_id,
                                           const P2PSegment& segment) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& info = clients_[client_id];
    info.client_id = client_id;  // In case client wasn't registered yet

    // Check if segment already exists
    for (const auto& seg : info.segments) {
        if (seg.id == segment.id) {
            VLOG(1) << "P2PStandbyMetadataStore::MountSegment "
                    << "segment already exists, ignoring";
            return;
        }
    }

    info.segments.push_back(segment);
    VLOG(1) << "P2PStandbyMetadataStore::MountSegment "
            << "client=" << client_id.first << ":" << client_id.second
            << " segment=" << segment.id.first << ":" << segment.id.second
            << " total_segments=" << info.segments.size();
}

void P2PStandbyMetadataStore::UnmountSegment(const P2PRouteLocation& location) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client = clients_.find(location.client_id);
    if (client != clients_.end()) {
        std::erase_if(client->second.segments, [&](const P2PSegment& segment) {
            return segment.id == location.segment_id;
        });
    }
    RemoveLocationLocked(location);

    VLOG(1) << "P2PStandbyMetadataStore::UnmountSegment "
            << "segment=" << location.segment_id.first << ":"
            << location.segment_id.second
            << " client=" << location.client_id.first << ":"
            << location.client_id.second;
}

void P2PStandbyMetadataStore::RemoveAllMetadata() {
    std::lock_guard<std::mutex> lock(mutex_);
    routes_.clear();
    clients_.clear();
    VLOG(1) << "P2PStandbyMetadataStore::RemoveAllMetadata";
}

// ============================================================================
// Export for Promotion
// ============================================================================

P2PStandbyMetadataStore::ExportedMetadata
P2PStandbyMetadataStore::ExportMetadata() const {
    std::lock_guard<std::mutex> lock(mutex_);
    ExportedMetadata result;
    result.routes = routes_;
    result.clients = clients_;
    return result;
}

std::vector<std::string> P2PStandbyMetadataStore::ListRouteKeys() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> keys;
    keys.reserve(routes_.size());
    for (const auto& [key, route] : routes_) {
        keys.push_back(key);
    }
    return keys;
}

std::vector<UUID> P2PStandbyMetadataStore::ListClientIds() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<UUID> ids;
    ids.reserve(clients_.size());
    for (const auto& [id, client] : clients_) {
        ids.push_back(id);
    }
    return ids;
}

std::optional<P2PStandbyClientInfo> P2PStandbyMetadataStore::GetClientInfo(
    const UUID& client_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
        return std::nullopt;
    }
    return it->second;
}

// ============================================================================
// Query helpers
// ============================================================================

std::unordered_map<std::string, P2PStandbyRouteEntry>
P2PStandbyMetadataStore::GetRoutes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return routes_;
}

std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>>
P2PStandbyMetadataStore::GetClients() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return clients_;
}

// ============================================================================
// Internal helpers
// ============================================================================

void P2PStandbyMetadataStore::RemoveLocationLocked(
    const P2PRouteLocation& location) {
    for (auto it = routes_.begin(); it != routes_.end();) {
        std::erase(it->second.locations, location);
        if (it->second.locations.empty()) {
            it = routes_.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace mooncake
