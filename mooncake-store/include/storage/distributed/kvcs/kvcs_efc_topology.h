#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

enum class KvcsEfcRouteKind {
    kLocal,
    kKvCacheStore,
};

const char* ToString(KvcsEfcRouteKind kind);

struct KvcsEfcMountpoint {
    std::string id;
    uint32_t index = 0;
    bool is_default = false;
};

struct KvcsEfcDeployment {
    std::string backend;
    std::vector<std::string> extra_backends;
    std::vector<KvcsEfcMountpoint> mountpoints;
    bool require_single_default = false;
};

struct KvcsEfcRoute {
    std::string id;
    uint32_t mountpoint_index = 0;
    KvcsEfcRouteKind kind = KvcsEfcRouteKind::kLocal;
};

// Resolves the routes that Mooncake can select independently. The main EFC
// backend is index 0; KVCacheStore mountpoints retain their configured indices.
tl::expected<std::vector<KvcsEfcRoute>, ErrorCode> ResolveKvcsEfcTopology(
    const KvcsEfcDeployment& deployment);

// Loads the official EFC deployment variables when path is empty, otherwise
// loads the equivalent YAML file.
tl::expected<std::vector<KvcsEfcRoute>, ErrorCode> LoadKvcsEfcTopology(
    const std::string& path);

}  // namespace mooncake
