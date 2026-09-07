#include "p2p/client/v2/movement.h"

#include <string>

namespace mooncake::v2 {

const char* ToString(MovementKind kind) {
    switch (kind) {
        case MovementKind::kReplicate:
            return "replicate";
        case MovementKind::kMigrate:
            return "migrate";
    }
    return "unknown";
}

const char* ToString(MovementPriority priority) {
    switch (priority) {
        case MovementPriority::kForeground:
            return "foreground";
        case MovementPriority::kBackground:
            return "background";
    }
    return "unknown";
}

size_t MovementRouteHash::operator()(
    const MovementRoute& route) const noexcept {
    size_t seed = static_cast<size_t>(route.kind);
    boost::hash_combine(seed, boost::hash<UUID>{}(route.source_tiler));
    boost::hash_combine(seed, boost::hash<UUID>{}(route.destination_tiler));
    boost::hash_combine(seed, static_cast<size_t>(route.source_domain));
    boost::hash_combine(seed, static_cast<size_t>(route.destination_domain));
    return seed;
}

std::string ToLabel(const MovementRoute& route) {
    // Tiler ids rather than keys: a route label is a metric dimension, and its
    // cardinality has to stay bounded by the topology.
    std::string label = ToString(route.kind);
    label += ':';
    label += std::to_string(route.source_tiler.first);
    label += '_';
    label += std::to_string(route.source_tiler.second);
    label += "->";
    label += std::to_string(route.destination_tiler.first);
    label += '_';
    label += std::to_string(route.destination_tiler.second);
    label += ':';
    label += ToString(route.source_domain);
    label += "->";
    label += ToString(route.destination_domain);
    return label;
}

}  // namespace mooncake::v2
