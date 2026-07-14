
#include "conductor/kvevent/config.h"

#include <glog/logging.h>
#include <json/json.h>

#include <cstdlib>
#include <fstream>
#include <limits>

#include "conductor/common/utils.h"

namespace conductor {
namespace kvevent {

namespace {

std::string DefaultConductorConfigPath() {
    const char* home = std::getenv("HOME");
    if (home == nullptr || home[0] == '\0') {
        return "~/.mooncake/conductor_config.json";
    }
    return std::string(home) + "/.mooncake/conductor_config.json";
}

// Service type mapping: only "vLLM" and "Mooncake" are valid.
bool MapServiceType(const std::string& s, std::string* out) {
    if (s == "vLLM") {
        *out = common::kServiceTypeVLLM;
        return true;
    }
    if (s == "Mooncake") {
        *out = common::kServiceTypeMooncake;
        return true;
    }
    return false;
}

std::string JsonString(const Json::Value& obj, const char* key) {
    if (obj.isMember(key) && obj[key].isString()) {
        return obj[key].asString();
    }
    return "";
}

bool JsonInt64(const Json::Value& value, int64_t* out) {
    if (value.type() == Json::intValue) {
        *out = value.asInt64();
        return true;
    }
    if (value.type() == Json::uintValue &&
        value.asUInt64() <=
            static_cast<Json::UInt64>(std::numeric_limits<int64_t>::max())) {
        *out = static_cast<int64_t>(value.asUInt64());
        return true;
    }
    return false;
}

common::HashProfileConfig ParseHashProfile(const Json::Value& raw) {
    common::HashProfileConfig profile;
    const Json::Value& value = raw["hash_profile"];
    if (!value.isObject()) {
        return profile;
    }
    profile.strategy = JsonString(value, "strategy");
    profile.algorithm = JsonString(value, "algorithm");
    profile.root_digest = JsonString(value, "root_digest");
    profile.index_projection = JsonString(value, "index_projection");
    return profile;
}

bool ParseCacheGroup(const Json::Value& raw,
                     std::optional<int64_t>* cache_group) {
    if (!raw.isMember("cache_group") || raw["cache_group"].isNull()) {
        cache_group->reset();
        return true;
    }
    const Json::Value& value = raw["cache_group"];
    int64_t parsed = 0;
    if (value.type() == Json::intValue) {
        parsed = value.asInt64();
    } else if (value.type() == Json::uintValue &&
               value.asUInt64() <= static_cast<Json::UInt64>(
                                       std::numeric_limits<int64_t>::max())) {
        parsed = static_cast<int64_t>(value.asUInt64());
    } else {
        return false;
    }
    if (parsed != 0) {
        return false;
    }
    *cache_group = parsed;
    return true;
}

}  // namespace

std::vector<common::ServiceConfig> ParseConfig(int* http_server_port) {
    const std::string config_path =
        common::LoadEnv("CONDUCTOR_CONFIG_PATH", DefaultConductorConfigPath());

    std::ifstream in(config_path, std::ios::binary);
    if (!in) {
        // Warn and continue with empty service list (do not exit).
        LOG(WARNING) << "Unable to open config file; continuing with empty "
                        "service list. path="
                     << config_path;
        return {};
    }

    Json::Value cfg;
    {
        Json::CharReaderBuilder rb;
        std::string errs;
        if (!Json::parseFromStream(rb, in, &cfg, &errs) || !cfg.isObject()) {
            LOG(ERROR) << "Failed to parse JSON config error=" << errs;
            std::exit(1);
        }
    }

    // An absent http_server_port field leaves the port as 0.
    *http_server_port =
        cfg.isMember("http_server_port") && cfg["http_server_port"].isNumeric()
            ? cfg["http_server_port"].asInt()
            : 0;

    std::vector<common::ServiceConfig> services;
    const Json::Value& instances = cfg["kvevent_instance"];
    if (instances.isObject()) {
        for (const auto& name : instances.getMemberNames()) {
            const Json::Value& raw = instances[name];
            if (!raw.isObject()) {
                continue;
            }

            std::string service_type;
            if (!MapServiceType(JsonString(raw, "type"), &service_type)) {
                LOG(ERROR) << "Unknown service type type="
                           << JsonString(raw, "type");
                continue;
            }

            common::ServiceConfig svc;
            svc.endpoint = JsonString(raw, "endpoint");
            svc.replay_endpoint = JsonString(raw, "replay_endpoint");
            svc.type = service_type;
            svc.model_name = JsonString(raw, "modelname");
            if (raw.isMember("lora_name") && !raw["lora_name"].isString()) {
                LOG(ERROR) << "Invalid lora_name stream_key=" << name;
                continue;
            }
            svc.lora_name = JsonString(raw, "lora_name");
            if (raw.isMember("tenant_id") && !raw["tenant_id"].isString()) {
                LOG(ERROR) << "Invalid tenant_id stream_key=" << name;
                continue;
            }
            svc.tenant_id = JsonString(raw, "tenant_id");
            if (svc.tenant_id.empty()) {
                svc.tenant_id = "default";
            }
            if (raw.isMember("instance_id")) {
                if (!raw["instance_id"].isString() ||
                    raw["instance_id"].asString().empty()) {
                    LOG(ERROR) << "Invalid instance_id stream_key=" << name;
                    continue;
                }
                svc.instance_id = raw["instance_id"].asString();
            } else {
                svc.instance_id = name;
            }
            if (!raw.isMember("block_size") ||
                !JsonInt64(raw["block_size"], &svc.block_size)) {
                LOG(ERROR) << "Invalid block_size instance_id=" << name;
                continue;
            }
            int64_t dp_rank = 0;
            if (!raw.isMember("dp_rank") ||
                !JsonInt64(raw["dp_rank"], &dp_rank) || dp_rank < 0 ||
                dp_rank > std::numeric_limits<int>::max()) {
                LOG(ERROR) << "Invalid dp_rank instance_id=" << name;
                continue;
            }
            svc.dp_rank = static_cast<int>(dp_rank);
            if (!ParseCacheGroup(raw, &svc.cache_group)) {
                LOG(ERROR) << "Invalid cache_group instance_id=" << name;
                continue;
            }
            svc.hash_profile = ParseHashProfile(raw);
            services.push_back(std::move(svc));
        }
    }

    return services;
}

}  // namespace kvevent
}  // namespace conductor
