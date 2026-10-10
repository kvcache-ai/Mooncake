#include "storage/distributed/kvcs/kvcs_efc_topology.h"

#include <cstdlib>
#include <filesystem>
#include <string_view>
#include <sys/stat.h>

#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include "ascii_string.h"

namespace mooncake {
namespace {

bool SingleSharedBackend(std::string_view backend,
                         std::string_view extra_backend) {
    return (backend == "kvcachestore" &&
            (extra_backend.empty() || extra_backend == "kvcachestore")) ||
           (backend == "diskless" && extra_backend == "kvcachestore");
}

tl::expected<KvcsEfcTarget, ErrorCode> FromEnvironment() {
    const char* backend = std::getenv("KVCS_BACKEND");
    const char* extra = std::getenv("KVCS_EXTRA_BACKENDS");
    const auto backend_name = backend && !TrimAsciiWhitespace(backend).empty()
                                  ? TrimAsciiWhitespace(backend)
                                  : std::string_view("kvcachestore");
    const auto extra_name =
        extra ? TrimAsciiWhitespace(extra) : std::string_view{};
    if (!SingleSharedBackend(backend_name, extra_name))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    const char* json = std::getenv("KVCS_MOUNTPOINTS_JSON");
    if (!json || !*json)
        return KvcsEfcTarget{.id = "kvcachestore-default",
                             .mountpoint_index = 1};
    const auto root = YAML::Load(json);
    const auto mounts = root["mountPoints"];
    if (!mounts || !mounts.IsSequence() || mounts.size() != 1)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const std::string id = mounts[0]["mountPointID"].as<std::string>();
    const std::string subpath = mounts[0]["subpath"]
                                    ? mounts[0]["subpath"].as<std::string>()
                                    : std::string{};
    if (id.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return KvcsEfcTarget{.id = id + subpath, .mountpoint_index = 1};
}

tl::expected<KvcsEfcTarget, ErrorCode> FromYaml(const std::string& path) {
    const auto root = YAML::LoadFile(path);
    if (!root["backend"]) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const auto extra = root["extra_backends"];
    if (extra && (!extra.IsSequence() || extra.size() > 1))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const std::string extra_name =
        extra && extra.size() == 1 ? extra[0].as<std::string>() : std::string{};
    if (!SingleSharedBackend(root["backend"].as<std::string>(), extra_name))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const auto mounts = root["mountpoints"];
    if (!mounts || !mounts.IsSequence() || mounts.size() != 1)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const auto mount = mounts[0];
    const std::string id = mount["mountpoint_id"].as<std::string>();
    const uint32_t index = mount["mountpoint_index"].as<uint32_t>();
    if (id.empty() || !index || !mount["default"] ||
        !mount["default"].as<bool>())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return KvcsEfcTarget{.id = id, .mountpoint_index = index};
}

}  // namespace

tl::expected<KvcsEfcTarget, ErrorCode> LoadKvcsEfcTarget(
    const std::string& path) {
    try {
        if (path.empty()) return FromEnvironment();
        std::error_code error;
        if (std::filesystem::is_regular_file(path, error))
            return FromYaml(path);

        // Older launchers pass the EFC Unix socket in this field.
        struct stat info{};
        if (::stat(path.c_str(), &info) == 0 && S_ISSOCK(info.st_mode))
            return FromEnvironment();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    } catch (const std::exception& error) {
        LOG(ERROR) << "Invalid KVCS EFC target configuration: " << error.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

}  // namespace mooncake
