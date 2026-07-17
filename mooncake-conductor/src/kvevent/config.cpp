
#include "conductor/kvevent/config.h"

#include <glog/logging.h>
#include <json/json.h>

#include <cstdlib>
#include <fstream>
#include <limits>
#include <set>

#include "conductor/common/utils.h"
#include "conductor/prefixindex/hash_strategy.h"

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

bool ParseHashProfile(const Json::Value& raw,
                      common::ResolvedHashProfile* profile,
                      std::string* error) {
    if (!raw.isMember("hash_profile")) {
        *error = "hash_profile is required";
        return false;
    }
    const Json::Value& value = raw["hash_profile"];
    if (!value.isObject()) {
        *error = "hash_profile must be an object";
        return false;
    }

    static const std::set<std::string> kAllowedProfileFields = {
        "algorithm", "index_projection", "python_hash_seed", "strategy"};
    for (const std::string& name : value.getMemberNames()) {
        if (!kAllowedProfileFields.contains(name)) {
            *error = "unsupported hash_profile field: " + name;
            return false;
        }
    }

    common::HashProfileConfig source;
    const auto require_string = [&](const char* field, std::string* out) {
        if (!value.isMember(field)) {
            *error = std::string(field) + " is required";
            return false;
        }
        if (!value[field].isString()) {
            *error = std::string(field) + " must be a string";
            return false;
        }
        *out = value[field].asString();
        return true;
    };
    if (!require_string("strategy", &source.strategy) ||
        !require_string("algorithm", &source.algorithm) ||
        !require_string("python_hash_seed", &source.python_hash_seed) ||
        !require_string("index_projection", &source.index_projection)) {
        return false;
    }

    *error = prefixindex::ResolveHashProfile(source, profile);
    return error->empty();
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

            const auto publisher_kind =
                common::ParsePublisherKind(JsonString(raw, "type"));
            if (!publisher_kind.has_value()) {
                LOG(ERROR) << "Unknown service type type="
                           << JsonString(raw, "type");
                continue;
            }

            common::ServiceConfig svc;
            svc.endpoint = JsonString(raw, "endpoint");
            svc.replay_endpoint = JsonString(raw, "replay_endpoint");
            svc.publisher_kind = *publisher_kind;
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
            std::string profile_error;
            if (!ParseHashProfile(raw, &svc.hash_profile, &profile_error)) {
                LOG(ERROR) << "Invalid hash_profile stream_key=" << name
                           << " error=" << profile_error;
                continue;
            }
            services.push_back(std::move(svc));
        }
    }

    return services;
}

}  // namespace kvevent
}  // namespace conductor
