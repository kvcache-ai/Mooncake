// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/metrics/config_loader.h"

#include <glog/logging.h>
#include <cstdlib>
#include <sstream>

namespace mooncake {
namespace tent {

MetricsConfig MetricsConfigLoader::loadFromConfig(const Config& config) {
    MetricsConfig metrics_config = getDefaultConfig();
    
    // Load basic settings
    metrics_config.enabled = config.get(config_keys::METRICS_ENABLED, metrics_config.enabled);
    metrics_config.http_port = static_cast<uint16_t>(
        config.get(config_keys::METRICS_HTTP_PORT, static_cast<int>(metrics_config.http_port)));
    metrics_config.http_host = config.get(config_keys::METRICS_HTTP_HOST, metrics_config.http_host);
    metrics_config.http_server_threads = static_cast<uint16_t>(
        config.get(config_keys::METRICS_HTTP_SERVER_THREADS, static_cast<int>(metrics_config.http_server_threads)));
    metrics_config.report_interval_seconds = config.get(config_keys::METRICS_REPORT_INTERVAL, 
                                                       metrics_config.report_interval_seconds);
    metrics_config.enable_prometheus = config.get(config_keys::METRICS_ENABLE_PROMETHEUS, 
                                                  metrics_config.enable_prometheus);
    metrics_config.enable_json = config.get(config_keys::METRICS_ENABLE_JSON, 
                                           metrics_config.enable_json);
    
    // Load bucket configurations
    auto latency_buckets_array = config.getArray<double>(config_keys::METRICS_LATENCY_BUCKETS);
    if (!latency_buckets_array.empty()) {
        metrics_config.latency_buckets = latency_buckets_array;
    }
    
    auto size_buckets_array = config.getArray<double>(config_keys::METRICS_SIZE_BUCKETS);
    if (!size_buckets_array.empty()) {
        metrics_config.size_buckets = size_buckets_array;
    }
    
    LOG(INFO) << "Loaded metrics config from Config object: enabled=" << metrics_config.enabled
              << ", port=" << metrics_config.http_port;
    
    return metrics_config;
}

MetricsConfig MetricsConfigLoader::loadFromEnvironment() {
    MetricsConfig metrics_config = getDefaultConfig();
    
    // Load from environment variables
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_ENABLED)) {
        metrics_config.enabled = parseBool(env_val, metrics_config.enabled);
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_HTTP_PORT)) {
        metrics_config.http_port = parsePort(env_val, metrics_config.http_port);
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_HTTP_HOST)) {
        metrics_config.http_host = env_val;
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_REPORT_INTERVAL)) {
        metrics_config.report_interval_seconds = parseInt(env_val, metrics_config.report_interval_seconds);
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_HTTP_SERVER_THREADS)) {
        int threads = parseInt(env_val, metrics_config.http_server_threads);
        if (threads > 0 && threads <= 65535) {
            metrics_config.http_server_threads = static_cast<uint16_t>(threads);
        }
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_ENABLE_PROMETHEUS)) {
        metrics_config.enable_prometheus = parseBool(env_val, metrics_config.enable_prometheus);
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_ENABLE_JSON)) {
        metrics_config.enable_json = parseBool(env_val, metrics_config.enable_json);
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_LATENCY_BUCKETS)) {
        auto buckets = parseDoubleArray(env_val);
        if (!buckets.empty()) {
            metrics_config.latency_buckets = buckets;
        }
    }
    
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_SIZE_BUCKETS)) {
        auto buckets = parseDoubleArray(env_val);
        if (!buckets.empty()) {
            metrics_config.size_buckets = buckets;
        }
    }
    
    LOG(INFO) << "Loaded metrics config from environment: enabled=" << metrics_config.enabled
              << ", port=" << metrics_config.http_port;
    
    return metrics_config;
}

MetricsConfig MetricsConfigLoader::loadWithDefaults(const Config* config) {
    MetricsConfig metrics_config = getDefaultConfig();
    
    // First load from environment (lower priority)
    MetricsConfig env_config = loadFromEnvironment();
    
    // Override with environment values if they differ from defaults
    if (env_config.enabled != getDefaultConfig().enabled) {
        metrics_config.enabled = env_config.enabled;
    }
    if (env_config.http_port != getDefaultConfig().http_port) {
        metrics_config.http_port = env_config.http_port;
    }
    if (env_config.http_host != getDefaultConfig().http_host) {
        metrics_config.http_host = env_config.http_host;
    }
    if (env_config.report_interval_seconds != getDefaultConfig().report_interval_seconds) {
        metrics_config.report_interval_seconds = env_config.report_interval_seconds;
    }
    if (env_config.http_server_threads != getDefaultConfig().http_server_threads) {
        metrics_config.http_server_threads = env_config.http_server_threads;
    }
    if (env_config.enable_prometheus != getDefaultConfig().enable_prometheus) {
        metrics_config.enable_prometheus = env_config.enable_prometheus;
    }
    if (env_config.enable_json != getDefaultConfig().enable_json) {
        metrics_config.enable_json = env_config.enable_json;
    }
    
    // Then override with config file values (higher priority)
    // Only override if the config file explicitly sets a value (differs from default)
    if (config) {
        MetricsConfig file_config = loadFromConfig(*config);
        MetricsConfig default_config = getDefaultConfig();
        
        // Override with file config values only if they differ from defaults
        if (file_config.enabled != default_config.enabled) {
            metrics_config.enabled = file_config.enabled;
        }
        if (file_config.http_port != default_config.http_port) {
            metrics_config.http_port = file_config.http_port;
        }
        if (file_config.http_host != default_config.http_host) {
            metrics_config.http_host = file_config.http_host;
        }
        if (file_config.http_server_threads != default_config.http_server_threads) {
            metrics_config.http_server_threads = file_config.http_server_threads;
        }
        if (file_config.report_interval_seconds != default_config.report_interval_seconds) {
            metrics_config.report_interval_seconds = file_config.report_interval_seconds;
        }
        if (file_config.enable_prometheus != default_config.enable_prometheus) {
            metrics_config.enable_prometheus = file_config.enable_prometheus;
        }
        if (file_config.enable_json != default_config.enable_json) {
            metrics_config.enable_json = file_config.enable_json;
        }
        
        if (!file_config.latency_buckets.empty()) {
            metrics_config.latency_buckets = file_config.latency_buckets;
        }
        if (!file_config.size_buckets.empty()) {
            metrics_config.size_buckets = file_config.size_buckets;
        }
    }
    
    return metrics_config;
}

bool MetricsConfigLoader::validateConfig(const MetricsConfig& config, std::string* error_msg) {
    // Validate port range
    if (config.http_port == 0 || config.http_port > 65535) {
        if (error_msg) {
            *error_msg = "Invalid HTTP port: " + std::to_string(config.http_port) + 
                        " (must be 1-65535)";
        }
        return false;
    }
    
    // Validate report interval (0 means disabled, which is valid)
    // No validation needed for report_interval_seconds since 0 is allowed to disable periodic reporting
    
    // Validate HTTP server threads
    if (config.http_server_threads == 0) {
        if (error_msg) {
            *error_msg = "Invalid HTTP server threads: must be > 0";
        }
        return false;
    }
    
    // Validate at least one output format is enabled
    if (!config.enable_prometheus && !config.enable_json) {
        if (error_msg) {
            *error_msg = "At least one output format (Prometheus or JSON) must be enabled";
        }
        return false;
    }
    
    // Validate buckets are sorted and positive
    for (size_t i = 1; i < config.latency_buckets.size(); ++i) {
        if (config.latency_buckets[i] <= config.latency_buckets[i-1]) {
            if (error_msg) {
                *error_msg = "Latency buckets must be sorted in ascending order";
            }
            return false;
        }
    }
    
    for (size_t i = 1; i < config.size_buckets.size(); ++i) {
        if (config.size_buckets[i] <= config.size_buckets[i-1]) {
            if (error_msg) {
                *error_msg = "Size buckets must be sorted in ascending order";
            }
            return false;
        }
    }
    
    return true;
}

MetricsConfig MetricsConfigLoader::getDefaultConfig() {
    MetricsConfig config;
    // Default values are already set in the struct definition
    return config;
}

std::vector<double> MetricsConfigLoader::parseDoubleArray(const std::string& str) {
    std::vector<double> result;
    std::stringstream ss(str);
    std::string item;
    
    while (std::getline(ss, item, ',')) {
        try {
            // Trim whitespace
            item.erase(0, item.find_first_not_of(" \t"));
            item.erase(item.find_last_not_of(" \t") + 1);
            
            if (!item.empty()) {
                double value = std::stod(item);
                if (value > 0) {
                    result.push_back(value);
                }
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to parse double value '" << item << "': " << e.what();
        }
    }
    
    // Sort the result
    std::sort(result.begin(), result.end());
    
    return result;
}

bool MetricsConfigLoader::parseBool(const std::string& str, bool default_value) {
    std::string lower_str = str;
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(), ::tolower);
    
    if (lower_str == "true" || lower_str == "1" || lower_str == "yes" || lower_str == "on") {
        return true;
    } else if (lower_str == "false" || lower_str == "0" || lower_str == "no" || lower_str == "off") {
        return false;
    } else {
        LOG(WARNING) << "Invalid boolean value '" << str << "', using default: " << default_value;
        return default_value;
    }
}

int MetricsConfigLoader::parseInt(const std::string& str, int default_value) {
    try {
        return std::stoi(str);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse integer '" << str << "': " << e.what() 
                     << ", using default: " << default_value;
        return default_value;
    }
}

uint16_t MetricsConfigLoader::parsePort(const std::string& str, uint16_t default_value) {
    try {
        int port = std::stoi(str);
        if (port > 0 && port <= 65535) {
            return static_cast<uint16_t>(port);
        } else {
            LOG(WARNING) << "Port " << port << " out of range (1-65535), using default: " << default_value;
            return default_value;
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse port '" << str << "': " << e.what() 
                     << ", using default: " << default_value;
        return default_value;
    }
}

}  // namespace tent
}  // namespace mooncake