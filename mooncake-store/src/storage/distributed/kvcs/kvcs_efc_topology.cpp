#include "storage/distributed/kvcs/kvcs_efc_topology.h"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <set>
#include <string_view>

#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include "ascii_string.h"

namespace mooncake {
namespace {

bool Contains(std::span<const std::string> values, std::string_view expected) {
    return std::find(values.begin(), values.end(), expected) != values.end();
}

std::vector<std::string> ParseCsv(const char* csv) {
    std::vector<std::string> values;
    if (csv == nullptr) return values;
    std::string_view remaining(csv);
    while (!remaining.empty()) {
        const size_t comma = remaining.find(',');
        const auto token = TrimAsciiWhitespace(remaining.substr(0, comma));
        if (!token.empty()) values.emplace_back(token);
        if (comma == std::string_view::npos) break;
        remaining.remove_prefix(comma + 1);
    }
    return values;
}

std::vector<std::string> ParseYamlStrings(const YAML::Node& values) {
    std::vector<std::string> result;
    if (!values) return result;
    if (!values.IsSequence())
        throw YAML::RepresentationException(values.Mark(),
                                            "expected a sequence");
    result.reserve(values.size());
    for (const auto& value : values) result.push_back(value.as<std::string>());
    return result;
}

tl::expected<KvcsEfcDeployment, ErrorCode> LoadFromEnvironment() {
    const char* backend = std::getenv("KVCS_BACKEND");
    if (backend == nullptr || *backend == '\0') {
        LOG(ERROR) << "KVCS_BACKEND is required for Low Level topology "
                      "discovery";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    KvcsEfcDeployment deployment;
    deployment.backend = backend;
    deployment.extra_backends = ParseCsv(std::getenv("KVCS_EXTRA_BACKENDS"));

    const bool needs_mountpoints =
        deployment.backend == "kvcachestore" ||
        Contains(deployment.extra_backends, "kvcachestore");
    if (!needs_mountpoints) return deployment;

    const char* json = std::getenv("KVCS_MOUNTPOINTS_JSON");
    if (json == nullptr || *json == '\0') {
        LOG(ERROR) << "KVCS_MOUNTPOINTS_JSON is required for direct "
                      "KVCacheStore access";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto root = YAML::Load(json);
    const auto mountpoints = root["mountPoints"];
    if (!mountpoints || !mountpoints.IsSequence())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    deployment.mountpoints.reserve(mountpoints.size());
    for (size_t i = 0; i < mountpoints.size(); ++i) {
        if (i + 1 > std::numeric_limits<uint32_t>::max())
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        const auto mountpoint_id =
            mountpoints[i]["mountPointID"].as<std::string>();
        const auto subpath = mountpoints[i]["subpath"]
                                 ? mountpoints[i]["subpath"].as<std::string>()
                                 : std::string{};
        deployment.mountpoints.push_back(
            {.id = mountpoint_id + subpath,
             .index = static_cast<uint32_t>(i + 1)});
    }
    return deployment;
}

tl::expected<KvcsEfcDeployment, ErrorCode> LoadFromYaml(
    const std::string& path) {
    const auto root = YAML::LoadFile(path);
    if (!root["backend"]) {
        LOG(ERROR) << "KVCS EFC config must declare backend: " << path;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    KvcsEfcDeployment deployment;
    deployment.backend = root["backend"].as<std::string>();
    deployment.extra_backends = ParseYamlStrings(root["extra_backends"]);
    deployment.require_single_default = true;

    const auto mountpoints = root["mountpoints"];
    if (mountpoints) {
        if (!mountpoints.IsSequence())
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        deployment.mountpoints.reserve(mountpoints.size());
        for (const auto& mountpoint : mountpoints) {
            deployment.mountpoints.push_back(KvcsEfcMountpoint{
                .id = mountpoint["mountpoint_id"].as<std::string>(),
                .index = mountpoint["mountpoint_index"].as<uint32_t>(),
                .is_default =
                    mountpoint["default"] && mountpoint["default"].as<bool>(),
            });
        }
    }
    return deployment;
}

}  // namespace

const char* ToString(KvcsEfcRouteKind kind) {
    switch (kind) {
        case KvcsEfcRouteKind::kLocal:
            return "local";
        case KvcsEfcRouteKind::kKvCacheStore:
            return "kvcachestore";
    }
    return "unknown";
}

tl::expected<std::vector<KvcsEfcRoute>, ErrorCode> ResolveKvcsEfcTopology(
    const KvcsEfcDeployment& deployment) {
    if (deployment.backend.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    const bool has_kvcachestore =
        Contains(deployment.extra_backends, "kvcachestore");
    if (!has_kvcachestore && deployment.backend != "kvcachestore") {
        if (deployment.backend == "diskless")
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        return std::vector<KvcsEfcRoute>{
            {.id = "local",
             .mountpoint_index = 0,
             .kind = KvcsEfcRouteKind::kLocal}};
    }

    if (deployment.mountpoints.empty())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    std::set<std::string> ids;
    std::set<uint32_t> indices;
    size_t defaults = 0;
    std::vector<KvcsEfcRoute> routes;
    const bool has_local_backend = deployment.backend != "kvcachestore" &&
                                   deployment.backend != "diskless";
    routes.reserve(deployment.mountpoints.size() +
                   static_cast<size_t>(has_local_backend));
    if (has_local_backend) {
        routes.push_back({.id = "local",
                          .mountpoint_index = 0,
                          .kind = KvcsEfcRouteKind::kLocal});
    }
    for (const auto& mountpoint : deployment.mountpoints) {
        defaults += mountpoint.is_default ? 1 : 0;
        if (mountpoint.id.empty() || mountpoint.index == 0 ||
            !ids.insert(mountpoint.id).second ||
            !indices.insert(mountpoint.index).second) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        routes.push_back({.id = mountpoint.id,
                          .mountpoint_index = mountpoint.index,
                          .kind = KvcsEfcRouteKind::kKvCacheStore});
    }
    if (deployment.require_single_default && defaults != 1)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return routes;
}

tl::expected<std::vector<KvcsEfcRoute>, ErrorCode> LoadKvcsEfcTopology(
    const std::string& path) {
    try {
        auto deployment =
            path.empty() ? LoadFromEnvironment() : LoadFromYaml(path);
        if (!deployment) return tl::make_unexpected(deployment.error());
        return ResolveKvcsEfcTopology(*deployment);
    } catch (const std::exception& error) {
        LOG(ERROR) << "Failed to parse KVCS EFC topology"
                   << (path.empty() ? " from environment" : " from " + path)
                   << ": " << error.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

}  // namespace mooncake
