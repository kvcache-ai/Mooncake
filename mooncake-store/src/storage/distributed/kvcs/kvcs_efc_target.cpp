#include "storage/distributed/kvcs/kvcs_efc_target.h"

#include <cstdlib>
#include <string_view>

#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

namespace mooncake {
namespace {

tl::expected<KvcsEfcTarget, ErrorCode> FromEnvironment() {
    const char* json = std::getenv("KVCS_MOUNTPOINTS_JSON");
    if (!json || !*json)
        return KvcsEfcTarget{.mountpoint_index = 1,
                             .mountpoint_id = "kvcachestore-default"};
    const auto root = YAML::Load(json);
    const auto mounts = root["mountPoints"];
    if (!mounts || !mounts.IsSequence() || mounts.size() != 1)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const std::string id = mounts[0]["mountPointID"].as<std::string>();
    if (id.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return KvcsEfcTarget{.mountpoint_index = 1, .mountpoint_id = id};
}

tl::expected<KvcsEfcTarget, ErrorCode> FromYaml(const std::string& path) {
    const auto root = YAML::LoadFile(path);
    const auto mounts = root["mountpoints"];
    if (!mounts || !mounts.IsSequence() || mounts.size() != 1)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const auto mount = mounts[0];
    const std::string id = mount["mountpoint_id"].as<std::string>();
    const uint32_t index = mount["mountpoint_index"].as<uint32_t>();
    if (id.empty() || !index)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return KvcsEfcTarget{.mountpoint_index = index, .mountpoint_id = id};
}

}  // namespace

tl::expected<KvcsEfcTarget, ErrorCode> LoadKvcsEfcTarget(
    const std::string& path) {
    try {
        if (path.empty()) return FromEnvironment();
        return FromYaml(path);
    } catch (const std::exception& error) {
        LOG(ERROR) << "Invalid KVCS EFC target configuration: " << error.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

}  // namespace mooncake
