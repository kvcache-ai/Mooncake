#include "runtime_config_store.h"

#include <glog/logging.h>

namespace mooncake {

RuntimeConfigStore::RuntimeConfigStore(DeploymentMode mode)
    : write_(mode == DeploymentMode::P2P
                 ? WriteConfig{WriteRouteRequestConfig{}}
                 : WriteConfig{ReplicateConfig{}}) {}

RuntimeConfigStore::WriteConfig RuntimeConfigStore::getDefaultWriteConfig()
    const {
    std::shared_lock lock(mu_);
    return write_;
}

ReadRouteConfig RuntimeConfigStore::getDefaultReadConfig() const {
    std::shared_lock lock(mu_);
    return read_;
}

void RuntimeConfigStore::updateWriteConfig(const Json::Value& json) {
    std::unique_lock lock(mu_);
    std::visit([&json](auto& cfg) { applyPatch(cfg, json); }, write_);
}

void RuntimeConfigStore::updateReadConfig(const Json::Value& json) {
    std::unique_lock lock(mu_);
    applyPatch(read_, json);
}

Json::Value RuntimeConfigStore::exportConfig() const {
    std::shared_lock lock(mu_);
    Json::Value root;
    root["write"] =
        std::visit([](const auto& cfg) { return toJson(cfg); }, write_);
    root["read"] = toJson(read_);
    return root;
}

void RuntimeConfigStore::loadFromJson(const Json::Value& root) {
    if (root.isNull()) return;

    if (root.isMember("write")) {
        updateWriteConfig(root["write"]);
    }
    if (root.isMember("read")) {
        updateReadConfig(root["read"]);
    }

    LOG(INFO) << "Loaded runtime config: " << root.toStyledString();
}

// --- Patch helpers ---

void RuntimeConfigStore::applyPatch(ReplicateConfig& config,
                                    const Json::Value& json) {
    if (json.isMember("replica_num")) {
        config.replica_num = json["replica_num"].asUInt64();
    }
    if (json.isMember("with_soft_pin")) {
        config.with_soft_pin = json["with_soft_pin"].asBool();
    }
    if (json.isMember("preferred_segments") &&
        json["preferred_segments"].isArray()) {
        config.preferred_segments.clear();
        for (const auto& seg : json["preferred_segments"]) {
            config.preferred_segments.push_back(seg.asString());
        }
    }
    if (json.isMember("preferred_segment")) {
        config.preferred_segment = json["preferred_segment"].asString();
    }
    if (json.isMember("prefer_alloc_in_same_node")) {
        config.prefer_alloc_in_same_node =
            json["prefer_alloc_in_same_node"].asBool();
    }
}

void RuntimeConfigStore::applyPatch(WriteRouteRequestConfig& config,
                                    const Json::Value& json) {
    if (json.isMember("max_candidates")) {
        config.max_candidates = json["max_candidates"].asUInt64();
    }
    if (json.isMember("strategy")) {
        int val = json["strategy"].asInt();
        config.strategy = static_cast<ObjectIterateStrategy>(val);
    }
    if (json.isMember("allow_local")) {
        config.allow_local = json["allow_local"].asBool();
    }
    if (json.isMember("prefer_local")) {
        config.prefer_local = json["prefer_local"].asBool();
    }
    if (json.isMember("early_return")) {
        config.early_return = json["early_return"].asBool();
    }
    if (json.isMember("tag_filters") && json["tag_filters"].isArray()) {
        config.tag_filters.clear();
        for (const auto& tag : json["tag_filters"]) {
            config.tag_filters.push_back(tag.asString());
        }
    }
    if (json.isMember("priority_limit")) {
        config.priority_limit = json["priority_limit"].asInt();
    }
}

void RuntimeConfigStore::applyPatch(ReadRouteConfig& config,
                                    const Json::Value& json) {
    if (json.isMember("max_candidates")) {
        config.max_candidates = json["max_candidates"].asUInt64();
    }
    if (json.isMember("p2p_config") && json["p2p_config"].isObject()) {
        if (!config.p2p_config.has_value()) {
            config.p2p_config = P2PReadRouteConfigExtra{};
        }
        auto& p2p = config.p2p_config.value();
        const auto& p2p_json = json["p2p_config"];
        if (p2p_json.isMember("tag_filters") &&
            p2p_json["tag_filters"].isArray()) {
            p2p.tag_filters.clear();
            for (const auto& tag : p2p_json["tag_filters"]) {
                p2p.tag_filters.push_back(tag.asString());
            }
        }
        if (p2p_json.isMember("priority_limit")) {
            p2p.priority_limit = p2p_json["priority_limit"].asInt();
        }
    }
}

// --- toJson helpers ---

Json::Value RuntimeConfigStore::toJson(const ReplicateConfig& config) {
    Json::Value json;
    json["replica_num"] = Json::Value::UInt64(config.replica_num);
    json["with_soft_pin"] = config.with_soft_pin;
    Json::Value segs(Json::arrayValue);
    for (const auto& seg : config.preferred_segments) {
        segs.append(seg);
    }
    json["preferred_segments"] = segs;
    json["preferred_segment"] = config.preferred_segment;
    json["prefer_alloc_in_same_node"] = config.prefer_alloc_in_same_node;
    return json;
}

Json::Value RuntimeConfigStore::toJson(const WriteRouteRequestConfig& config) {
    Json::Value json;
    json["max_candidates"] = Json::Value::UInt64(config.max_candidates);
    json["strategy"] = static_cast<int>(config.strategy);
    json["allow_local"] = config.allow_local;
    json["prefer_local"] = config.prefer_local;
    json["early_return"] = config.early_return;
    Json::Value tags(Json::arrayValue);
    for (const auto& tag : config.tag_filters) {
        tags.append(tag);
    }
    json["tag_filters"] = tags;
    json["priority_limit"] = config.priority_limit;
    return json;
}

Json::Value RuntimeConfigStore::toJson(const ReadRouteConfig& config) {
    Json::Value json;
    json["max_candidates"] = Json::Value::UInt64(config.max_candidates);
    if (config.p2p_config.has_value()) {
        Json::Value p2p;
        Json::Value tags(Json::arrayValue);
        for (const auto& tag : config.p2p_config->tag_filters) {
            tags.append(tag);
        }
        p2p["tag_filters"] = tags;
        p2p["priority_limit"] = config.p2p_config->priority_limit;
        json["p2p_config"] = p2p;
    }
    return json;
}

}  // namespace mooncake
