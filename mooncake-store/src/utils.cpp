#include "utils.h"

#include <glog/logging.h>
#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <csignal>

namespace mooncake {

namespace {
const std::string kMetricClusterID = [] {
    const char* value = std::getenv("MC_STORE_CLUSTER_ID");
    return value ? std::string(value) : std::string();
}();
}  // namespace

std::map<std::string, std::string> merge_labels(
    const std::map<std::string, std::string>& labels) {
    std::map<std::string, std::string> merged_labels;
    if (!kMetricClusterID.empty()) {
        merged_labels["cluster_id"] = kMetricClusterID;
    }
    merged_labels.insert(labels.begin(), labels.end());
    return merged_labels;
}

std::string formatDeviceNames(const std::string& device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::vector<std::string> splitString(const std::string& str, char delimiter,
                                     bool trim_spaces, bool keep_empty) {
    std::vector<std::string> result;

    boost::split(
        result, str, boost::is_any_of(std::string(1, delimiter)),
        keep_empty ? boost::token_compress_off : boost::token_compress_on);

    if (trim_spaces) {
        for (auto& token : result) {
            boost::trim(token);
        }
    }

    return result;
}

std::string GetEnvStringOr(const char* name, const std::string& default_value) {
    const char* env_val = std::getenv(name);
    return env_val ? std::string(env_val) : default_value;
}

}  // namespace mooncake
