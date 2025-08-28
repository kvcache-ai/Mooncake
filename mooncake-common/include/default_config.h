#pragma once

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif
#include <yaml-cpp/yaml.h>

#include <cstdint>
#include <string>
#include <unordered_map>

namespace mooncake {
class DefaultConfig {
   public:
    struct Node {
        YAML::Node yaml_node_;
        Json::Value json_value_;
    };

    enum ConfigType {
        YAML = 1,
        JSON = 2,
        UNKNOWN = 3,
    };

   public:
    void Load();
    /**
     * @brief GetInt32 retrieves an integer value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetInt32(const std::string& key, int32_t* val,
                  int32_t default_value = 0) const;

    /**
     * @brief GetInt32 retrieves an unsigned integer value from the
     * configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetUInt32(const std::string& key, uint32_t* val,
                   uint32_t default_value = 0) const;

    /**
     * @brief GetInt64 retrieves a 64-bit integer value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetInt64(const std::string& key, int64_t* val,
                  int64_t default_value = 0) const;

    /**
     * @brief GetInt64 retrieves a 64-bit unsigned integer value from the
     * configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetUInt64(const std::string& key, uint64_t* val,
                   uint64_t default_value = 0) const;

    /**
     * @brief GetDouble retrieves a double value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetDouble(const std::string& key, double* val,
                   double default_value = 0.0) const;

    /**
     * @brief GetFloat retrieves a float value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetFloat(const std::string& key, float* val,
                  float default_value = 0.0f) const;

    /**
     * @brief GetBool retrieves a boolean value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetBool(const std::string& key, bool* val,
                 bool default_value = false) const;

    /**
     * @brief GetString retrieves a string value from the configuration
     * @param key The key to look up in the configuration
     * @param val Pointer to store the retrieved value
     * @param default_value Default value to return if the key is not found
     * @note If the key is not found, default_value will be assigned to val
     */
    void GetString(const std::string& key, std::string* val,
                   const std::string& default_value = "") const;

    void SetPath(const std::string& path) { path_ = path; }

   private:
    void processNode(const YAML::Node& node, std::string key);

    void processNode(const Json::Value& node, std::string key);

    void loadFromYAML();

    void loadFromJSON();

    bool getValue(const std::string& key, Node* node) const {
        auto it = data_.find(key);
        if (it != data_.end()) {
            *node = it->second;
            return true;
        }
        return false;
    }

   private:
    std::string path_;
    ConfigType type_;
    std::unordered_map<std::string, Node> data_;
};

}  // namespace mooncake
