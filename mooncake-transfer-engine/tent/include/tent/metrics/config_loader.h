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

#ifndef TENT_METRICS_CONFIG_LOADER_H
#define TENT_METRICS_CONFIG_LOADER_H

#include "tent/common/config.h"

namespace mooncake {
namespace tent {

/**
 * @brief Configuration structure for TENT metrics system
 */
struct MetricsConfig {
    bool enabled = true;
    std::string http_host = "0.0.0.0";
    uint16_t http_port = 9100;
    uint16_t http_server_threads = 2;       // HTTP server thread count
    uint32_t report_interval_seconds = 30;  // 0 means disabled
    bool enable_prometheus = true;
    bool enable_json = true;
    std::vector<double> latency_buckets;
    std::vector<double> size_buckets;
};

// Helper class to load metrics configuration from various sources
// Uses ConfigHelper for common parsing utilities
class MetricsConfigLoader {
   public:
    // Load configuration from TENT Config object
    static MetricsConfig loadFromConfig(const Config& config);

    // Load configuration from environment variables
    static MetricsConfig loadFromEnvironment();

    // Load configuration with defaults and overrides
    // Priority: Config file > Environment variables > Defaults
    static MetricsConfig loadWithDefaults(const Config* config = nullptr);

    // Validate configuration
    static bool validateConfig(const MetricsConfig& config,
                               std::string* error_msg = nullptr);

    // Get default configuration
    static MetricsConfig getDefaultConfig();

   private:
    // Apply environment variable overrides to config
    static void applyEnvironmentOverrides(MetricsConfig& config);
};

// Configuration keys used in config files and environment variables
namespace config_keys {
// Main metrics configuration
constexpr const char* METRICS_ENABLED = "metrics/enabled";
constexpr const char* METRICS_HTTP_PORT = "metrics/http_port";
constexpr const char* METRICS_HTTP_HOST = "metrics/http_host";
constexpr const char* METRICS_HTTP_SERVER_THREADS =
    "metrics/http_server_threads";
constexpr const char* METRICS_REPORT_INTERVAL =
    "metrics/report_interval_seconds";
constexpr const char* METRICS_ENABLE_PROMETHEUS = "metrics/enable_prometheus";
constexpr const char* METRICS_ENABLE_JSON = "metrics/enable_json";

// Bucket configurations
constexpr const char* METRICS_LATENCY_BUCKETS = "metrics/latency_buckets";
constexpr const char* METRICS_SIZE_BUCKETS = "metrics/size_buckets";

// Environment variable names (with TENT_ prefix)
constexpr const char* ENV_METRICS_ENABLED = "TENT_METRICS_ENABLED";
constexpr const char* ENV_METRICS_HTTP_PORT = "TENT_METRICS_HTTP_PORT";
constexpr const char* ENV_METRICS_HTTP_HOST = "TENT_METRICS_HTTP_HOST";
constexpr const char* ENV_METRICS_HTTP_SERVER_THREADS =
    "TENT_METRICS_HTTP_SERVER_THREADS";
constexpr const char* ENV_METRICS_REPORT_INTERVAL =
    "TENT_METRICS_REPORT_INTERVAL";
constexpr const char* ENV_METRICS_ENABLE_PROMETHEUS =
    "TENT_METRICS_ENABLE_PROMETHEUS";
constexpr const char* ENV_METRICS_ENABLE_JSON = "TENT_METRICS_ENABLE_JSON";
constexpr const char* ENV_METRICS_LATENCY_BUCKETS =
    "TENT_METRICS_LATENCY_BUCKETS";
constexpr const char* ENV_METRICS_SIZE_BUCKETS = "TENT_METRICS_SIZE_BUCKETS";
}  // namespace config_keys

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_METRICS_CONFIG_LOADER_H