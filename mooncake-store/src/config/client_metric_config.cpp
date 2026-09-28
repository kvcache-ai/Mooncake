#include "client_metric.h"

#include <glog/logging.h>
#include <chrono>

#include "bool_parser.h"
#include "environ.h"
#include "environment_variables.h"
#include "integer_parser.h"

namespace mooncake {

ClientMetricConfig ClientMetricConfig::FromEnvironment() {
    ClientMetricConfig config;
    using Variables = ClientMetricEnvironmentVariables;

    const auto enabled = Environ::Read(Variables::MC_STORE_CLIENT_METRIC);
    if (enabled.has_value()) {
        config.enabled = TryParseBool(*enabled).value_or(false);
    }
    if (!config.enabled) {
        return config;
    }

    const auto interval =
        Environ::Read(Variables::MC_STORE_CLIENT_METRIC_INTERVAL);
    if (interval.has_value()) {
        const auto parsed = TryParseInteger<uint64_t>(
            *interval,
            {.trim_ascii_whitespace = true, .allow_leading_plus = true});
        if (!parsed.has_value()) {
            LOG(WARNING) << "Failed to parse "
                         << Variables::MC_STORE_CLIENT_METRIC_INTERVAL.name
                         << ": " << *interval
                         << ", disabling metrics reporting";
        } else {
            config.reporting_interval = std::chrono::seconds(*parsed);
            if (*parsed == 0) {
                LOG(INFO)
                    << "Client metrics reporting disabled (interval=0) via "
                    << Variables::MC_STORE_CLIENT_METRIC_INTERVAL.name;
            } else {
                LOG(INFO) << "Client metrics interval set to " << *parsed
                          << "s via "
                          << Variables::MC_STORE_CLIENT_METRIC_INTERVAL.name;
            }
        }
    }

    const auto bandwidth =
        Environ::Read(Variables::MC_STORE_CLIENT_METRIC_BANDWIDTH);
    if (bandwidth.has_value()) {
        const auto parsed = TryParseBool(*bandwidth);
        if (parsed.has_value()) {
            config.bandwidth_reporting_enabled = *parsed;
        } else {
            LOG(WARNING) << "Failed to parse "
                         << Variables::MC_STORE_CLIENT_METRIC_BANDWIDTH.name
                         << ": " << *bandwidth << ", fallback to default="
                         << config.bandwidth_reporting_enabled;
        }
    }

    return config;
}

}  // namespace mooncake
