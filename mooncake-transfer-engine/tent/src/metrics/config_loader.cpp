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

namespace mooncake {
namespace tent {

void MetricsConfigLoader::applyEnvironmentOverrides(MetricsConfig& config) {
    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_ENABLED)) {
        config.enabled = ConfigHelper::parseBool(env_val, config.enabled);
    }

    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_HTTP_PORT)) {
        config.http_port = ConfigHelper::parsePort(env_val, config.http_port);
    }

    if (const char* env_val = std::getenv(config_keys::ENV_METRICS_HTTP_HOST)) {
        config.http_host = env_val;
    }

    if (const char* env_val =
            std::getenv(config_keys::ENV_METRICS_REPORT_INTERVAL)) {
        config.report_interval_seconds =
            ConfigHelper::parseInt(env_val, config.report_interval_seconds);
    }

    if (const char* env_val =
            std::getenv(config_keys::ENV_METRICS_HTTP_SERVER_THREADS)) {
        int threads =
            ConfigHelper::parseInt(env_val, config.http_server_threads);
        if (threads > 0 && threads <= 65535) {
            config.http_server_threads = static_cast<uint16_t>(threads);
        }
    }

    if (const char* env_val =
            std::getenv(config_keys::ENV_METRICS_ENABLE_PROMETHEUS)) {
        config.enable_prometheus =
            ConfigHelper::parseBool(env_val, config.enable_prometheus);
    }

    if (const char* env_val =
            std::getenv(config_keys::ENV_METRICS_ENABLE_JSON)) {
        config.enable_json =
            ConfigHelper::parseBool(env_val, config.enable_json);
    }

    if (const char* env_val =
            std::getenv(config_keys::ENV_METRICS_LATENCY_BUCKETS)) {
        auto buckets = ConfigHelper::parseDoubleArray(env_val);
        if (!buckets.empty()) {
            config.latency_buckets = buckets;
        }
    }

    if (const char* env_val =
            std::getenv(config_keys::ENV_METRICS_SIZE_BUCKETS)) {
        auto buckets = ConfigHelper::parseDoubleArray(env_val);
        if (!buckets.empty()) {
            config.size_buckets = buckets;
        }
    }
}

MetricsConfig MetricsConfigLoader::loadFromConfig(const Config& config) {
    MetricsConfig metrics_config = getDefaultConfig();

    // Load basic settings from Config object
    metrics_config.enabled =
        config.get(config_keys::METRICS_ENABLED, metrics_config.enabled);
    metrics_config.http_port = static_cast<uint16_t>(
        config.get(config_keys::METRICS_HTTP_PORT,
                   static_cast<int>(metrics_config.http_port)));
    metrics_config.http_host =
        config.get(config_keys::METRICS_HTTP_HOST, metrics_config.http_host);
    metrics_config.http_server_threads = static_cast<uint16_t>(
        config.get(config_keys::METRICS_HTTP_SERVER_THREADS,
                   static_cast<int>(metrics_config.http_server_threads)));
    metrics_config.report_interval_seconds =
        config.get(config_keys::METRICS_REPORT_INTERVAL,
                   metrics_config.report_interval_seconds);
    metrics_config.enable_prometheus =
        config.get(config_keys::METRICS_ENABLE_PROMETHEUS,
                   metrics_config.enable_prometheus);
    metrics_config.enable_json = config.get(config_keys::METRICS_ENABLE_JSON,
                                            metrics_config.enable_json);

    // Load bucket configurations
    auto latency_buckets_array =
        config.getArray<double>(config_keys::METRICS_LATENCY_BUCKETS);
    if (!latency_buckets_array.empty()) {
        metrics_config.latency_buckets = latency_buckets_array;
    }

    auto size_buckets_array =
        config.getArray<double>(config_keys::METRICS_SIZE_BUCKETS);
    if (!size_buckets_array.empty()) {
        metrics_config.size_buckets = size_buckets_array;
    }

    LOG(INFO) << "Loaded metrics config from Config object: enabled="
              << metrics_config.enabled
              << ", port=" << metrics_config.http_port;

    return metrics_config;
}

MetricsConfig MetricsConfigLoader::loadFromEnvironment() {
    MetricsConfig metrics_config = getDefaultConfig();
    applyEnvironmentOverrides(metrics_config);

    LOG(INFO) << "Loaded metrics config from environment: enabled="
              << metrics_config.enabled
              << ", port=" << metrics_config.http_port;

    return metrics_config;
}

MetricsConfig MetricsConfigLoader::loadWithDefaults(const Config* config) {
    // Priority: Config file > Environment variables > Defaults

    // 1. Start with defaults
    MetricsConfig metrics_config = getDefaultConfig();

    // 2. Override with environment variables
    applyEnvironmentOverrides(metrics_config);

    // 3. Override with file config (highest priority)
    if (config) {
        metrics_config.enabled =
            config->get(config_keys::METRICS_ENABLED, metrics_config.enabled);
        metrics_config.http_port = static_cast<uint16_t>(
            config->get(config_keys::METRICS_HTTP_PORT,
                        static_cast<int>(metrics_config.http_port)));
        metrics_config.http_host = config->get(config_keys::METRICS_HTTP_HOST,
                                               metrics_config.http_host);
        metrics_config.http_server_threads = static_cast<uint16_t>(
            config->get(config_keys::METRICS_HTTP_SERVER_THREADS,
                        static_cast<int>(metrics_config.http_server_threads)));
        metrics_config.report_interval_seconds =
            config->get(config_keys::METRICS_REPORT_INTERVAL,
                        metrics_config.report_interval_seconds);
        metrics_config.enable_prometheus =
            config->get(config_keys::METRICS_ENABLE_PROMETHEUS,
                        metrics_config.enable_prometheus);
        metrics_config.enable_json = config->get(
            config_keys::METRICS_ENABLE_JSON, metrics_config.enable_json);

        auto latency_buckets_array =
            config->getArray<double>(config_keys::METRICS_LATENCY_BUCKETS);
        if (!latency_buckets_array.empty()) {
            metrics_config.latency_buckets = latency_buckets_array;
        }

        auto size_buckets_array =
            config->getArray<double>(config_keys::METRICS_SIZE_BUCKETS);
        if (!size_buckets_array.empty()) {
            metrics_config.size_buckets = size_buckets_array;
        }
    }

    return metrics_config;
}

bool MetricsConfigLoader::validateConfig(const MetricsConfig& config,
                                         std::string* error_msg) {
    // Validate port range (http_port is uint16_t, so max is 65535)
    if (config.http_port == 0) {
        if (error_msg) {
            *error_msg = "Invalid HTTP port: 0 (must be 1-65535)";
        }
        return false;
    }

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
            *error_msg =
                "At least one output format (Prometheus or JSON) must be "
                "enabled";
        }
        return false;
    }

    // Validate buckets are sorted and positive
    for (size_t i = 1; i < config.latency_buckets.size(); ++i) {
        if (config.latency_buckets[i] <= config.latency_buckets[i - 1]) {
            if (error_msg) {
                *error_msg =
                    "Latency buckets must be sorted in ascending order";
            }
            return false;
        }
    }

    for (size_t i = 1; i < config.size_buckets.size(); ++i) {
        if (config.size_buckets[i] <= config.size_buckets[i - 1]) {
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

}  // namespace tent
}  // namespace mooncake
