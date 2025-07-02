#include "default_config.h"

#include <stdexcept>
#include <yaml-cpp/yaml.h>
#include <yaml-cpp/node/node.h>

namespace mooncake {

void DefaultConfig::Load() {
    if (path_.empty()) {
        throw std::runtime_error("Default config path is not set");
    }
    //if the suffix is .yaml
    if (path_.ends_with(".yaml") || path_.ends_with(".yml")) {
        loadFromYAML();
    } else {
        throw std::runtime_error("Unsupported config file format");
    }
}

void DefaultConfig::loadFromYAML() {
    YAML::Node node = YAML::LoadFile(path_);
    processNode(node, "");
}

void DefaultConfig::processNode(const YAML::Node& node, std::string key) {
    if (node.IsScalar()) {
        data_[key] = node.as<std::string>();
    } else if (node.IsMap()) {
        for (const auto& iter : node) {
            std::string new_key = key.empty() ? iter.first.as<std::string>()
                                              : key + "." + iter.first.as<std::string>();
            processNode(iter.second, new_key);
        }
    } else {
        throw std::runtime_error("Unsupported YAML node type");
    }
}

void DefaultConfig::GetInt32(const std::string& key, int32_t* val, int32_t default_value) {
    std::string value;
    if (getValue(key, &value)) {
        *val = std::stoi(value);
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetUInt32(const std::string& key, uint32_t* val, uint32_t default_value) {
    std::string value;
    if (getValue(key, &value)) {
        *val = static_cast<uint32_t>(std::stoul(value));
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetInt64(const std::string& key, int64_t* val, int64_t default_value) {
    std::string value;
    if (getValue(key, &value)) {
        *val = std::stoll(value);
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetUInt64(const std::string& key, uint64_t* val, uint64_t default_value) {
    std::string value;
    if (getValue(key, &value)) {
        *val = static_cast<uint64_t>(std::stoull(value));
    } else {
        *val = default_value;       
    }   
}

void DefaultConfig::GetDouble(const std::string& key, double* val, double default_value) {
    std::string value;
    if (getValue(key, &value)) {
        *val = std::stod(value);
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetFloat(const std::string& key, float* val, float default_value) {
    std::string value;
    if (getValue(key, &value)) {
        *val = std::stof(value);
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetBool(const std::string& key, bool* val, bool default_value) {
    std::string value;
    if (getValue(key, &value)) {
        if (value == "true" || value == "1") {
            *val = true;
        } else if (value == "false" || value == "0") {
            *val = false;
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetString(const std::string& key, std::string* val, const std::string& default_value) {
    std::string value;
    if (getValue(key, &value)) {
        *val = value;
    } else {
        *val = default_value;
    }
}

}  // namespace mooncake
