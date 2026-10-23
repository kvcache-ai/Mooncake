#include "p2p/client/runtime_config_store.h"

#include <glog/logging.h>

namespace mooncake {

P2PWriteRouteConfig RuntimeConfigStore::getDefaultWriteConfig() const {
    std::shared_lock lock(mu_);
    return write_;
}

P2PReadRouteConfig RuntimeConfigStore::getDefaultReadConfig() const {
    std::shared_lock lock(mu_);
    return read_;
}

bool RuntimeConfigStore::updateWriteConfig(const Json::Value& json) {
    std::unique_lock lock(mu_);
    return applyPatch(write_, json);
}

void RuntimeConfigStore::updateReadConfig(const Json::Value& json) {
    std::unique_lock lock(mu_);
    applyPatch(read_, json);
}

Json::Value RuntimeConfigStore::exportConfig() const {
    std::shared_lock lock(mu_);
    Json::Value root;
    root["write"] = toJson(write_);
    root["read"] = toJson(read_);
    return root;
}

bool RuntimeConfigStore::loadFromJson(const Json::Value& root) {
    if (root.isNull() || !root.isObject()) return true;

    std::unique_lock lock(mu_);
    bool ok = true;
    if (root.isMember("write")) {
        ok = applyPatch(write_, root["write"]);
    }
    if (root.isMember("read")) {
        applyPatch(read_, root["read"]);
    }
    lock.unlock();

    LOG(INFO) << "Loaded runtime config: " << root.toStyledString();
    return ok;
}

// --- Patch helpers ---

bool RuntimeConfigStore::applyPatch(P2PWriteRouteConfig& config,
                                    const Json::Value& json) {
    if (!json.isObject()) return false;

    // Apply the patch to a copy so that a rejected patch leaves the original
    // config untouched.
    auto patched = config;

    if (json.isMember("max_candidates") && json["max_candidates"].isUInt64()) {
        patched.max_candidates = json["max_candidates"].asUInt64();
    }
    if (json.isMember("strategy") && json["strategy"].isInt()) {
        patched.strategy =
            static_cast<P2PClientSelectionStrategy>(json["strategy"].asInt());
    }
    if (json.isMember("remote_weight") && json["remote_weight"].isNumeric()) {
        double w = json["remote_weight"].asDouble();
        patched.remote_weight = w < 0.0 ? 0.0 : (w > 1.0 ? 1.0 : w);
    }
    if (json.isMember("local_write_waterline") &&
        json["local_write_waterline"].isNumeric()) {
        double wl = json["local_write_waterline"].asDouble();
        patched.local_write_waterline = wl < 0.0 ? 0.0 : (wl > 1.0 ? 1.0 : wl);
    }
    if (json.isMember("top_tier_only") && json["top_tier_only"].isBool()) {
        patched.top_tier_only = json["top_tier_only"].asBool();
    }
    if (json.isMember("early_return") && json["early_return"].isBool()) {
        patched.early_return = json["early_return"].asBool();
    }
    if (json.isMember("tag_filters") && json["tag_filters"].isArray()) {
        patched.tag_filters.clear();
        for (const auto& tag : json["tag_filters"]) {
            if (tag.isString()) {
                patched.tag_filters.push_back(tag.asString());
            }
        }
    }
    if (json.isMember("priority_limit") && json["priority_limit"].isInt()) {
        patched.priority_limit = json["priority_limit"].asInt();
    }

    if (!patched.IsValid()) {
        LOG(WARNING) << "write config patch would produce an invalid config"
                     << ", rejected: " << patched;
        return false;
    }

    config = std::move(patched);
    return true;
}

void RuntimeConfigStore::applyPatch(P2PReadRouteConfig& config,
                                    const Json::Value& json) {
    if (!json.isObject()) return;
    if (json.isMember("max_candidates") && json["max_candidates"].isUInt64()) {
        config.max_candidates = json["max_candidates"].asUInt64();
    }
    if (json.isMember("p2p_config") && json["p2p_config"].isObject()) {
        auto& p2p = config;
        const auto& p2p_json = json["p2p_config"];
        if (p2p_json.isMember("tag_filters") &&
            p2p_json["tag_filters"].isArray()) {
            p2p.tag_filters.clear();
            for (const auto& tag : p2p_json["tag_filters"]) {
                if (tag.isString()) {
                    p2p.tag_filters.push_back(tag.asString());
                }
            }
        }
        if (p2p_json.isMember("priority_limit") &&
            p2p_json["priority_limit"].isInt()) {
            p2p.priority_limit = p2p_json["priority_limit"].asInt();
        }
    }
}

// --- toJson helpers ---

Json::Value RuntimeConfigStore::toJson(const P2PWriteRouteConfig& config) {
    Json::Value json;
    json["max_candidates"] = Json::Value::UInt64(config.max_candidates);
    json["strategy"] = static_cast<int>(config.strategy);
    json["remote_weight"] = config.remote_weight;
    json["local_write_waterline"] = config.local_write_waterline;
    json["top_tier_only"] = config.top_tier_only;
    json["early_return"] = config.early_return;
    Json::Value tags(Json::arrayValue);
    for (const auto& tag : config.tag_filters) {
        tags.append(tag);
    }
    json["tag_filters"] = tags;
    json["priority_limit"] = config.priority_limit;
    return json;
}

Json::Value RuntimeConfigStore::toJson(const P2PReadRouteConfig& config) {
    Json::Value json;
    json["max_candidates"] = Json::Value::UInt64(config.max_candidates);
    Json::Value p2p;
    Json::Value tags(Json::arrayValue);
    for (const auto& tag : config.tag_filters) {
        tags.append(tag);
    }
    p2p["tag_filters"] = tags;
    p2p["priority_limit"] = config.priority_limit;
    json["p2p_config"] = p2p;
    return json;
}

}  // namespace mooncake
