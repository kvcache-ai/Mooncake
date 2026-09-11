#include "p2p/master/p2p_route_table.h"

#include <algorithm>
#include <utility>

#include <boost/functional/hash.hpp>
#include <glog/logging.h>

namespace mooncake {

size_t P2PRouteTable::CountOwnerClients(const P2PRouteEntry& entry) {
    std::unordered_set<UUID, boost::hash<UUID>> clients;
    for (const auto& location : entry.locations) {
        clients.insert(location.client_id);
    }
    return clients.size();
}

auto P2PRouteTable::Publish(std::string_view key, uint64_t object_size,
                            const P2PRouteLocation& location,
                            uint64_t max_client_per_key) -> Mutation {
    if (object_size == 0) {
        LOG(ERROR) << "Publish route rejected: object_size must be positive"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto it = routes_.find(key);
    if (it != routes_.end()) {
        auto& entry = it->second;
        if (entry.object_size != object_size) {
            LOG(ERROR) << "Publish route rejected: object size mismatch"
                       << ", key=" << key
                       << ", existing_size=" << entry.object_size
                       << ", requested_size=" << object_size;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (std::find(entry.locations.begin(), entry.locations.end(),
                      location) != entry.locations.end()) {
            LOG(WARNING) << "Publish route rejected: location already exists"
                         << ", key=" << key
                         << ", client_id=" << location.client_id
                         << ", segment_id=" << location.segment_id;
            return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
        }
        const bool new_owner =
            std::none_of(entry.locations.begin(), entry.locations.end(),
                         [&](const P2PRouteLocation& existing) {
                             return existing.client_id == location.client_id;
                         });
        // A client may publish the same key from multiple segments during
        // tier migration. The configured limit applies to owner clients, not
        // physical route locations.
        if (new_owner && max_client_per_key > 0 &&
            CountOwnerClients(entry) >= max_client_per_key) {
            LOG(WARNING) << "Publish route rejected: owner client limit "
                            "exceeded"
                         << ", key=" << key
                         << ", client_id=" << location.client_id
                         << ", segment_id=" << location.segment_id
                         << ", max_clients_per_key=" << max_client_per_key;
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }

        entry.locations.push_back(location);
        keys_by_location_[location].insert(std::string_view(it->first));
        return MutationResult{};
    }

    P2PRouteEntry entry;
    entry.object_size = object_size;
    entry.locations.push_back(location);
    auto [inserted_it, inserted] =
        routes_.emplace(std::string(key), std::move(entry));
    if (!inserted) {
        LOG(ERROR) << "Publish route failed to insert a new key"
                   << ", key=" << key;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    keys_by_location_[location].insert(std::string_view(inserted_it->first));
    return MutationResult{.created_key = true};
}

void P2PRouteTable::RemoveReverseIndex(std::string_view key,
                                       const P2PRouteLocation& location) {
    auto location_it = keys_by_location_.find(location);
    if (location_it == keys_by_location_.end()) {
        LOG(ERROR) << "Route reverse index is missing a location"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id;
        return;
    }
    if (location_it->second.erase(key) == 0) {
        LOG(ERROR) << "Route reverse index is missing a key"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id;
    }
    if (location_it->second.empty()) {
        keys_by_location_.erase(location_it);
    }
}

void P2PRouteTable::RemoveAllReverseIndexes(std::string_view key,
                                            const P2PRouteEntry& entry) {
    for (const auto& location : entry.locations) {
        RemoveReverseIndex(key, location);
    }
}

auto P2PRouteTable::Withdraw(std::string_view key,
                             const P2PRouteLocation& location) -> Mutation {
    return Withdraw(key, location, PreWithdraw{});
}

auto P2PRouteTable::Withdraw(std::string_view key,
                             const P2PRouteLocation& location,
                             const PreWithdraw& pre_withdraw) -> Mutation {
    auto route_it = routes_.find(key);
    if (route_it == routes_.end()) {
        LOG(WARNING) << "Withdraw route rejected: key not found"
                     << ", key=" << key << ", client_id=" << location.client_id
                     << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& locations = route_it->second.locations;
    auto location_it = std::find(locations.begin(), locations.end(), location);
    if (location_it == locations.end()) {
        LOG(WARNING) << "Withdraw route rejected: location not found"
                     << ", key=" << key << ", client_id=" << location.client_id
                     << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    if (pre_withdraw) {
        const auto error = pre_withdraw();
        if (error != ErrorCode::OK) {
            LOG(ERROR) << "Withdraw route rejected by pre-mutation hook"
                       << ", key=" << key
                       << ", client_id=" << location.client_id
                       << ", segment_id=" << location.segment_id
                       << ", error=" << toString(error);
            return tl::make_unexpected(error);
        }
    }

    RemoveReverseIndex(route_it->first, location);
    locations.erase(location_it);
    if (locations.empty()) {
        routes_.erase(route_it);
        return MutationResult{.removed_key = true};
    }
    return MutationResult{};
}

bool P2PRouteTable::RouteExists(std::string_view key) const {
    auto it = routes_.find(key);
    return it != routes_.end() && !it->second.locations.empty();
}

std::optional<P2PRouteEntry> P2PRouteTable::GetRoute(
    std::string_view key) const {
    auto it = routes_.find(key);
    if (it == routes_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<std::string> P2PRouteTable::ListRouteKeys() const {
    std::vector<std::string> keys;
    keys.reserve(routes_.size());
    for (const auto& route : routes_) {
        keys.push_back(route.first);
    }
    return keys;
}

size_t P2PRouteTable::GetRouteKeyCount() const { return routes_.size(); }

P2PRouteTable::CleanupResult P2PRouteTable::RemoveLocation(
    const P2PRouteLocation& location) {
    CleanupResult result;
    auto index_it = keys_by_location_.find(location);
    if (index_it == keys_by_location_.end()) {
        return result;
    }

    std::vector<std::string> affected_keys;
    affected_keys.reserve(index_it->second.size());
    for (std::string_view key : index_it->second) {
        affected_keys.emplace_back(key);
    }
    keys_by_location_.erase(index_it);

    for (const auto& key : affected_keys) {
        auto route_it = routes_.find(key);
        if (route_it == routes_.end()) {
            LOG(ERROR) << "Route reverse index references a missing key"
                       << ", key=" << key;
            continue;
        }
        auto& locations = route_it->second.locations;
        const size_t old_size = locations.size();
        std::erase(locations, location);
        result.removed_routes += old_size - locations.size();
        if (locations.empty()) {
            ++result.removed_key_count;
            routes_.erase(route_it);
        }
    }
    return result;
}

bool P2PRouteTable::RemoveKey(std::string_view key) {
    auto it = routes_.find(key);
    if (it == routes_.end()) {
        return false;
    }
    RemoveAllReverseIndexes(it->first, it->second);
    routes_.erase(it);
    return true;
}

size_t P2PRouteTable::Clear() {
    const size_t removed_keys = routes_.size();
    keys_by_location_.clear();
    routes_.clear();
    return removed_keys;
}

}  // namespace mooncake
