#include "default_config.h"

#include <cstdint>
#include <stdexcept>
#include <yaml-cpp/yaml.h>
#include <yaml-cpp/node/node.h>

namespace mooncake {

void DefaultConfig::Load() {
    if (path_.empty()) {
        throw std::runtime_error("Default config path is not set");
    }
    std::string extension = ".yaml";
    if (path_.size() >= extension.size() && 
        path_.compare(path_.size()-extension.size(), extension.size(), extension) == 0) {
        loadFromYAML();
        type_ = ConfigType::YAML; // YAML type
    } else {
        type_ = ConfigType::UNKnown; // Unknown type
        throw std::runtime_error("Unsupported config file format");
    }
}

void DefaultConfig::loadFromYAML() {
    YAML::Node node = YAML::LoadFile(path_);
    processNode(node, "");
}

void DefaultConfig::processNode(const YAML::Node& node, std::string key) {
    if (node.IsScalar()) {
        data_[key] = Node{node };
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
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<int32_t>();
        }
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetUInt32(const std::string& key, uint32_t* val, uint32_t default_value) {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<uint32_t>();
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetInt64(const std::string& key, int64_t* val, int64_t default_value) {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<int64_t>();
        }
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetUInt64(const std::string& key, uint64_t* val, uint64_t default_value) {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<uint64_t>();
        }
    } else {
        *val = default_value;       
    }   
}

void DefaultConfig::GetDouble(const std::string& key, double* val, double default_value) {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<double>();
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetFloat(const std::string& key, float* val, float default_value) {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<float>();
        }
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetBool(const std::string& key, bool* val, bool default_value) {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<bool>();
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetString(const std::string& key, std::string* val, const std::string& default_value) {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<std::string>();
        }
    } else {
        *val = default_value;
    }
}

}  // namespace mooncake
