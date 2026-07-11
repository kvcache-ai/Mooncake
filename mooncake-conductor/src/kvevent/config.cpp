
#include "conductor/kvevent/config.h"

#include <glog/logging.h>
#include <json/json.h>

#include <cstdlib>
#include <fstream>

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
            svc.lora_name = JsonString(raw, "lora_name");
            svc.tenant_id = JsonString(raw, "tenant_id");
            svc.instance_id = name;  // map key is the instance id
            svc.block_size =
                raw.isMember("block_size") && raw["block_size"].isNumeric()
                    ? raw["block_size"].asInt64()
                    : 0;
            svc.dp_rank = raw.isMember("dp_rank") && raw["dp_rank"].isNumeric()
                              ? raw["dp_rank"].asInt()
                              : 0;
            svc.additional_salt = JsonString(raw, "additionalsalt");
            services.push_back(std::move(svc));
        }
    }

    return services;
}

}  // namespace kvevent
}  // namespace conductor
