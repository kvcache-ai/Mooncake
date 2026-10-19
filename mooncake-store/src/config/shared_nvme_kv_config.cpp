#include "config/shared_nvme_kv_config.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include <initializer_list>
#include <set>

namespace mooncake {
namespace {

bool HasOnlyFields(const Json::Value& value,
                   std::initializer_list<const char*> allowed) {
    if (!value.isObject()) {
        return false;
    }

    const std::set<std::string> names(allowed.begin(), allowed.end());
    for (const auto& name : value.getMemberNames()) {
        if (!names.contains(name)) {
            return false;
        }
    }
    return true;
}

bool ReadPositiveU32(const Json::Value& object, const char* name,
                     uint32_t& value) {
    if (!object.isMember(name)) {
        return true;
    }

    const auto& field = object[name];
    if (!field.isUInt() || field.asUInt() == 0) {
        return false;
    }
    value = field.asUInt();
    return true;
}

}  // namespace

tl::expected<SharedNvmeKvConfig, std::string> SharedNvmeKvConfig::Parse(
    std::istream& input) {
    Json::CharReaderBuilder builder;
    builder["rejectDupKeys"] = true;
    builder["failIfExtra"] = true;

    Json::Value root;
    std::string error;
    if (!Json::parseFromStream(builder, input, &root, &error)) {
        return tl::make_unexpected(error);
    }
    if (!HasOnlyFields(root, {"namespaces"}) || !root["namespaces"].isArray() ||
        root["namespaces"].empty()) {
        return tl::make_unexpected(
            std::string("namespaces must be a nonempty ordered array"));
    }

    SharedNvmeKvConfig config;
    std::set<std::pair<std::string, uint32_t>> seen;
    for (const auto& ns : root["namespaces"]) {
        if (!HasOnlyFields(ns, {"device_path", "transport", "nsid",
                                "queue_depth", "runtime_transfer_limit"})) {
            return tl::make_unexpected(std::string("invalid namespace fields"));
        }

        NvmeKvConnectorConfig options;
        if (!ns["device_path"].isString()) {
            return tl::make_unexpected(std::string("device_path is required"));
        }
        options.device_path = ns["device_path"].asString();
        if (options.device_path.empty() || options.device_path.front() != '/' ||
            options.device_path.find('\0') != std::string::npos) {
            return tl::make_unexpected(
                std::string("absolute device_path is required"));
        }

        if (ns.isMember("transport")) {
            if (!ns["transport"].isString()) {
                return tl::make_unexpected(std::string("invalid transport"));
            }
            auto transport = ParseNvmeKvTransport(ns["transport"].asString());
            if (!transport) {
                return tl::make_unexpected(std::string("invalid transport"));
            }
            options.transport = *transport;
        }

        if (!ReadPositiveU32(ns, "nsid", options.nsid) ||
            options.nsid == UINT32_MAX ||
            !ReadPositiveU32(ns, "queue_depth", options.queue_depth) ||
            !ReadPositiveU32(ns, "runtime_transfer_limit",
                             options.runtime_transfer_limit)) {
            return tl::make_unexpected(
                std::string("invalid namespace numeric option"));
        }
        if (!seen.emplace(options.device_path, options.nsid).second) {
            return tl::make_unexpected(std::string("duplicate namespace"));
        }

        config.namespaces.push_back(std::move(options));
    }

    return config;
}

}  // namespace mooncake
