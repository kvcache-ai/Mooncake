// Copyright 2026 KVCache.AI
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

#include "qos_metrics.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <set>
#include <sstream>
#include <utility>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

std::vector<std::string> split(const std::string& value, char delimiter) {
    std::vector<std::string> parts;
    std::stringstream stream(value);
    std::string part;
    while (std::getline(stream, part, delimiter)) parts.push_back(part);
    return parts;
}

template <typename T>
bool parseNumber(const std::string& value, T* result) {
    if (value.empty()) return false;
    std::istringstream stream(value);
    stream >> std::noskipws >> *result;
    return stream.eof() && !stream.fail();
}

double jainIndex(const std::vector<double>& values) {
    if (values.empty()) return 0.0;
    double sum = 0.0;
    double squared_sum = 0.0;
    for (double value : values) {
        sum += value;
        squared_sum += value * value;
    }
    if (squared_sum == 0.0) return 0.0;
    return sum * sum / (values.size() * squared_sum);
}

nlohmann::json optionalJson(const std::optional<double>& value) {
    return value ? nlohmann::json(*value) : nlohmann::json(nullptr);
}

}  // namespace

bool parseQosClasses(const std::string& spec,
                     std::vector<QosClassConfig>* classes, std::string* error) {
    classes->clear();
    if (spec.empty()) {
        *error = "qos_classes must not be empty";
        return false;
    }

    std::set<std::string> names;
    for (const auto& entry : split(spec, ',')) {
        const auto fields = split(entry, ':');
        if (fields.size() != 4 && fields.size() != 5) {
            *error =
                "each qos class must be "
                "name:threads:slo_us:weight[:isolated_gbps]";
            return false;
        }

        QosClassConfig config;
        config.name = fields[0];
        if (config.name.empty() || !names.insert(config.name).second) {
            *error = "qos class names must be non-empty and unique";
            return false;
        }
        if (!parseNumber(fields[1], &config.threads) || config.threads <= 0) {
            *error = "qos class threads must be a positive integer";
            return false;
        }
        if (fields[2].empty() || fields[2].front() == '-' ||
            !parseNumber(fields[2], &config.slo_us)) {
            *error = "qos class slo_us must be a non-negative integer";
            return false;
        }
        if (!parseNumber(fields[3], &config.weight) || config.weight <= 0.0 ||
            !std::isfinite(config.weight)) {
            *error = "qos class weight must be finite and positive";
            return false;
        }
        if (fields.size() == 5) {
            double isolated_gbps = 0.0;
            if (!parseNumber(fields[4], &isolated_gbps) ||
                isolated_gbps <= 0.0 || !std::isfinite(isolated_gbps)) {
                *error = "isolated_gbps must be finite and positive";
                return false;
            }
            config.isolated_throughput_gbps = isolated_gbps;
        }
        classes->push_back(std::move(config));
    }
    return true;
}

bool validateQosClasses(const std::vector<QosClassConfig>& classes,
                        int num_threads, std::string* error) {
    int configured_threads = 0;
    for (const auto& config : classes) configured_threads += config.threads;
    if (configured_threads != num_threads) {
        std::ostringstream stream;
        stream << "qos_classes configures " << configured_threads
               << " threads, but tebench runs " << num_threads;
        *error = stream.str();
        return false;
    }
    return true;
}

size_t qosClassForThread(const std::vector<QosClassConfig>& classes,
                         int thread_id) {
    int boundary = 0;
    for (size_t i = 0; i < classes.size(); ++i) {
        boundary += classes[i].threads;
        if (thread_id < boundary) return i;
    }
    return classes.size();
}

QosMetricsReport calculateQosMetrics(size_t block_size, size_t batch_size,
                                     int num_threads,
                                     const std::vector<QosClassConfig>& classes,
                                     std::vector<XferBenchStats>* stats,
                                     double link_capacity_gbps) {
    QosMetricsReport report;
    report.block_size = block_size;
    report.batch_size = batch_size;
    report.num_threads = num_threads;

    std::vector<double> normalized_throughput;
    std::optional<double> max_leakage;
    for (size_t i = 0; i < classes.size(); ++i) {
        const auto& config = classes[i];
        auto& class_stats = (*stats)[i];
        QosClassMetrics metrics;
        metrics.name = config.name;
        metrics.threads = config.threads;
        metrics.slo_us = config.slo_us;
        metrics.weight = config.weight;
        metrics.operations = class_stats.transfer_duration.count();
        metrics.p99_us = class_stats.transfer_duration.p99();

        const double duration_s = class_stats.total_duration.avg() / 1e6;
        const double bytes =
            static_cast<double>(block_size) * batch_size * metrics.operations;
        if (duration_s > 0.0)
            metrics.throughput_gbps = bytes / 1e9 / duration_s;

        double attainment = 1.0;
        if (config.slo_us != 0) {
            attainment = class_stats.transfer_duration.fractionAtOrBelow(
                static_cast<double>(config.slo_us));
            metrics.slo_attainment = attainment;
        }
        metrics.goodput_gbps = metrics.throughput_gbps * attainment;
        metrics.weighted_goodput_gbps = metrics.goodput_gbps * config.weight;
        normalized_throughput.push_back(metrics.throughput_gbps /
                                        config.weight);

        if (config.isolated_throughput_gbps) {
            metrics.isolation_leakage =
                std::max(0.0, 1.0 - metrics.throughput_gbps /
                                        *config.isolated_throughput_gbps);
            max_leakage =
                max_leakage ? std::max(*max_leakage, *metrics.isolation_leakage)
                            : metrics.isolation_leakage;
        }

        report.aggregate_throughput_gbps += metrics.throughput_gbps;
        report.weighted_goodput_gbps += metrics.weighted_goodput_gbps;
        report.classes.push_back(std::move(metrics));
    }

    report.jain_fairness = jainIndex(normalized_throughput);
    report.max_isolation_leakage = max_leakage;
    if (link_capacity_gbps > 0.0) {
        report.total_utilization =
            report.aggregate_throughput_gbps / link_capacity_gbps;
    }
    return report;
}

void printQosMetrics(const QosMetricsReport& report) {
    std::cout << "  [qos-summary] throughput=" << std::fixed
              << std::setprecision(6) << report.aggregate_throughput_gbps
              << " GB/s weighted_goodput=" << report.weighted_goodput_gbps
              << " GB/s jain_fairness=" << report.jain_fairness
              << " max_isolation_leakage=";
    if (report.max_isolation_leakage) {
        std::cout << *report.max_isolation_leakage;
    } else {
        std::cout << "N/A";
    }
    std::cout << " total_utilization=";
    if (report.total_utilization) {
        std::cout << *report.total_utilization;
    } else {
        std::cout << "N/A";
    }
    std::cout << std::endl;

    for (const auto& metrics : report.classes) {
        std::cout << "  [qos-class] name=" << metrics.name
                  << " threads=" << metrics.threads
                  << " operations=" << metrics.operations
                  << " throughput=" << metrics.throughput_gbps
                  << " GB/s p99_us=" << std::setprecision(1) << metrics.p99_us
                  << " slo_attainment=";
        if (metrics.slo_attainment) {
            std::cout << std::setprecision(6) << *metrics.slo_attainment;
        } else {
            std::cout << "N/A";
        }
        std::cout << " isolation_leakage=";
        if (metrics.isolation_leakage) {
            std::cout << *metrics.isolation_leakage;
        } else {
            std::cout << "N/A";
        }
        std::cout << std::endl;
    }
}

bool appendQosMetricsJsonl(const std::string& path,
                           const QosMetricsReport& report, std::string* error) {
    nlohmann::json root = {
        {"schema_version", 1},
        {"block_size", report.block_size},
        {"batch_size", report.batch_size},
        {"num_threads", report.num_threads},
        {"aggregate_throughput_gbps", report.aggregate_throughput_gbps},
        {"weighted_goodput_gbps", report.weighted_goodput_gbps},
        {"jain_fairness", report.jain_fairness},
        {"max_isolation_leakage", optionalJson(report.max_isolation_leakage)},
        {"total_utilization", optionalJson(report.total_utilization)},
        {"classes", nlohmann::json::array()},
    };
    for (const auto& metrics : report.classes) {
        root["classes"].push_back({
            {"name", metrics.name},
            {"threads", metrics.threads},
            {"slo_us", metrics.slo_us},
            {"weight", metrics.weight},
            {"operations", metrics.operations},
            {"throughput_gbps", metrics.throughput_gbps},
            {"p99_us", metrics.p99_us},
            {"slo_attainment", optionalJson(metrics.slo_attainment)},
            {"goodput_gbps", metrics.goodput_gbps},
            {"weighted_goodput_gbps", metrics.weighted_goodput_gbps},
            {"isolation_leakage", optionalJson(metrics.isolation_leakage)},
        });
    }

    std::ofstream output(path, std::ios::app);
    if (!output) {
        *error = "failed to open QoS JSONL output: " + path;
        return false;
    }
    output << root.dump() << '\n';
    if (!output) {
        *error = "failed to write QoS JSONL output: " + path;
        return false;
    }
    return true;
}

}  // namespace tent
}  // namespace mooncake
