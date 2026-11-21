#pragma once

#include <cstddef>
#include <string>

#include <json/value.h>

namespace mooncake {
namespace ed_config {

// Helpers to read scalars from config["scheduler"][key] with a default. Shared
// by the scheduler (its own knobs) and the policy factory (policy knobs).

inline const Json::Value* Node(const Json::Value& config) {
    return config.isMember("scheduler") ? &config["scheduler"] : nullptr;
}

inline size_t ReadSize(const Json::Value& config, const char* key, size_t def) {
    const auto* s = Node(config);
    return (s && s->isMember(key)) ? static_cast<size_t>((*s)[key].asUInt64())
                                   : def;
}

inline int ReadInt(const Json::Value& config, const char* key, int def) {
    const auto* s = Node(config);
    return (s && s->isMember(key)) ? (*s)[key].asInt() : def;
}

inline double ReadDouble(const Json::Value& config, const char* key,
                         double def) {
    const auto* s = Node(config);
    return (s && s->isMember(key)) ? (*s)[key].asDouble() : def;
}

inline std::string ReadString(const Json::Value& config, const char* key,
                              const std::string& def) {
    const auto* s = Node(config);
    return (s && s->isMember(key)) ? (*s)[key].asString() : def;
}

}  // namespace ed_config
}  // namespace mooncake
