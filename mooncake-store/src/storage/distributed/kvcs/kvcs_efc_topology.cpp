#include "storage/distributed/kvcs/kvcs_efc_topology.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <set>
#include <string_view>
#include <sys/stat.h>

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

constexpr std::string_view kDefaultKvcsBackend = "kvcachestore";
constexpr std::string_view kDefaultKvcsMountpointId = "kvcachestore-default";
constexpr uint32_t kDefaultKvcsMountpointIndex = 1;

tl::expected<KvcsEfcDeployment, ErrorCode> LoadFromEnvironment() {
    // The EFC deployment owns the real backend configuration.  Mooncake only
    // needs a stable low-level target index, so missing deployment variables
    // fall back to the single default KVCacheStore mountpoint used by the
    // official EFC chart.  Explicit variables remain supported for mixed or
    // multi-mountpoint deployments.
    const char* backend = std::getenv("KVCS_BACKEND");
    const auto backend_name =
        backend == nullptr ? std::string_view{}
                           : TrimAsciiWhitespace(std::string_view(backend));
    KvcsEfcDeployment deployment;
    deployment.backend = backend_name.empty()
                              ? std::string(kDefaultKvcsBackend)
                              : std::string(backend_name);
    deployment.extra_backends = ParseCsv(std::getenv("KVCS_EXTRA_BACKENDS"));

    const bool needs_mountpoints =
        deployment.backend == kDefaultKvcsBackend ||
        Contains(deployment.extra_backends, kDefaultKvcsBackend);
    if (!needs_mountpoints) return deployment;

    const char* json = std::getenv("KVCS_MOUNTPOINTS_JSON");
    if (json && *json) {
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
    } else {
        deployment.mountpoints.push_back(
            {.id = std::string(kDefaultKvcsMountpointId),
             .index = kDefaultKvcsMountpointIndex,
             .is_default = true});
        LOG(INFO) << "KVCS topology variables are absent; using built-in "
                     "KVCacheStore mountpoint index "
                  << kDefaultKvcsMountpointIndex;
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
        tl::expected<KvcsEfcDeployment, ErrorCode> deployment =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        if (path.empty()) {
            deployment = LoadFromEnvironment();
        } else {
            // MOONCAKE_KVCS_EFC_CONFIG is a legacy YAML hook.  Older launchers
            // also put the EFC UDS path in this variable; never try to parse a
            // socket (or another non-file) as YAML.  Low-level operation can
            // proceed with the built-in topology in that case.
            std::error_code fs_error;
            if (std::filesystem::is_regular_file(path, fs_error)) {
                deployment = LoadFromYaml(path);
            } else {
                struct stat path_stat {};
                const bool is_socket =
                    ::stat(path.c_str(), &path_stat) == 0 &&
                    S_ISSOCK(path_stat.st_mode);
                if (!is_socket) {
                    LOG(ERROR) << "KVCS EFC topology config must be a regular "
                                   "YAML file or the legacy EFC socket path: "
                                << path;
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                LOG(INFO) << "Ignoring legacy socket path " << path
                          << " as KVCS topology config; using built-in "
                             "topology";
                deployment = LoadFromEnvironment();
            }
        }
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
