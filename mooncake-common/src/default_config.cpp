#include "default_config.h"

#if __has_include(<jsoncpp/json/reader.h>)
#include <jsoncpp/json/reader.h>
#include <jsoncpp/json/value.h>  // Ubuntu
#else
#include <json/reader.h>
#include <json/value.h>  // CentOS
#endif

#include <yaml-cpp/yaml.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <stdexcept>

namespace mooncake {

void DefaultConfig::Load() {
    if (path_.empty()) {
        throw std::runtime_error("Default config path is not set");
    }
    std::cout << "Loading config from: " << path_ << std::endl;
    auto check_extension = [](const std::string& path, const std::string& ext) {
        return path.size() >= ext.size() &&
               path.compare(path.size() - ext.size(), ext.size(), ext) == 0;
    };
    if (check_extension(path_, ".yaml")) {
        loadFromYAML();
        type_ = ConfigType::YAML;  // YAML type
    } else if (check_extension(path_, ".json")) {
        loadFromJSON();
        type_ = ConfigType::JSON;  // JSON type
    } else {
        type_ = ConfigType::UNKNOWN;  // UNKNOWN type
        throw std::runtime_error("Unsupported config file format");
    }
}

void DefaultConfig::loadFromYAML() {
    YAML::Node node = YAML::LoadFile(path_);
    processNode(node, "");
}

void DefaultConfig::loadFromJSON() {
    Json::Value root;
    Json::Reader reader;
    std::ifstream file;
    file.open(path_);

    if (!reader.parse(file, root, false)) {
        file.close();
        throw std::runtime_error("Failed to parse JSON file: " +
                                 reader.getFormattedErrorMessages());
    }
    try {
        processNode(root, "");
    } catch (const std::exception& e) {
        file.close();
        throw e;
    }
    file.close();
}

void DefaultConfig::processNode(const YAML::Node& node, std::string key) {
    if (node.IsScalar()) {
        data_[key] = Node{.yaml_node_ = node, .json_value_ = Json::nullValue};
    } else if (node.IsMap()) {
        for (const auto& iter : node) {
            std::string new_key =
                key.empty() ? iter.first.as<std::string>()
                            : key + "." + iter.first.as<std::string>();
            processNode(iter.second, new_key);
        }
    } else {
        throw std::runtime_error("Unsupported YAML node type");
    }
}

void DefaultConfig::processNode(const Json::Value& node, std::string key) {
    if (!node.isObject() && !node.isArray()) {
        data_[key] = Node{.yaml_node_ = YAML::Node(), .json_value_ = node};
    } else if (node.isObject()) {
        for (const auto& member : node.getMemberNames()) {
            std::string new_key = key.empty() ? member : key + "." + member;
            processNode(node[member], new_key);
        }
    } else {
        throw std::runtime_error("Unsupported JSON node type");
    }
}

void DefaultConfig::GetInt32(const std::string& key, int32_t* val,
                             int32_t default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<int32_t>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asInt();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetUInt32(const std::string& key, uint32_t* val,
                              uint32_t default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<uint32_t>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asUInt();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetInt64(const std::string& key, int64_t* val,
                             int64_t default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<int64_t>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asInt64();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetUInt64(const std::string& key, uint64_t* val,
                              uint64_t default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<uint64_t>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asUInt64();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetDouble(const std::string& key, double* val,
                              double default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<double>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asDouble();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetFloat(const std::string& key, float* val,
                             float default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<float>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asFloat();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}
void DefaultConfig::GetBool(const std::string& key, bool* val,
                            bool default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<bool>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asBool();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}

void DefaultConfig::GetString(const std::string& key, std::string* val,
                              const std::string& default_value) const {
    Node node;
    if (getValue(key, &node)) {
        if (type_ == ConfigType::YAML) {
            *val = node.yaml_node_.as<std::string>();
        } else if (type_ == ConfigType::JSON) {
            *val = node.json_value_.asString();
        } else {
            *val = default_value;
        }
    } else {
        *val = default_value;
    }
}

}  // namespace mooncake
