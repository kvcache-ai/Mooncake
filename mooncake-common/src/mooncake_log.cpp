// Copyright 2024 KVCache.AI
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

#include "mooncake_log.h"

#include <ylt/easylog.hpp>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <string>
#include <unistd.h>

namespace mooncake {
namespace {

std::string ToUpper(std::string value) {
    std::transform(
        value.begin(), value.end(), value.begin(),
        [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return value;
}

void ApplyLogDirFromPath(const char* log_dir_path) {
    if (!log_dir_path || !*log_dir_path) {
        return;
    }
    if (opendir(log_dir_path) == nullptr) {
        LOG(WARNING) << "Path [" << log_dir_path
                     << "] is not a valid directory path. Still logging to "
                        "stderr.";
        return;
    }
    if (access(log_dir_path, W_OK) != 0) {
        LOG(WARNING) << "Path [" << log_dir_path
                     << "] is not writable. Still logging to stderr.";
        return;
    }
    FLAGS_log_dir = log_dir_path;
    FLAGS_logtostderr = 0;
    FLAGS_stop_logging_if_full_disk = true;
}

easylog::Severity GlogLevelToEasylog(const std::string& level_upper) {
    if (level_upper == "ERROR") {
        return easylog::Severity::ERROR;
    }
    if (level_upper == "WARNING") {
        return easylog::Severity::WARN;
    }
    if (level_upper == "INFO") {
        return easylog::Severity::INFO;
    }
    if (level_upper == "TRACE") {
        return easylog::Severity::DEBUG;
    }
    return easylog::Severity::WARN;
}

}  // namespace

void SetGlogLogDir(const char* log_dir_path) {
    ApplyLogDirFromPath(log_dir_path);
}

bool ApplyGlogEnvironment(bool* enable_transfer_trace) {
    if (enable_transfer_trace) {
        *enable_transfer_trace = false;
    }

    const char* log_level = std::getenv("MC_LOG_LEVEL");
    if (log_level) {
        const std::string level_upper = ToUpper(log_level);
        if (level_upper == "TRACE") {
            FLAGS_minloglevel = google::INFO;
            if (FLAGS_v < 1) {
                FLAGS_v = 1;
            }
            if (enable_transfer_trace) {
                *enable_transfer_trace = true;
            }
        } else if (level_upper == "INFO") {
            FLAGS_minloglevel = google::INFO;
        } else if (level_upper == "WARNING") {
            FLAGS_minloglevel = google::WARNING;
        } else if (level_upper == "ERROR") {
            FLAGS_minloglevel = google::ERROR;
        } else {
            LOG(WARNING) << "Unknown MC_LOG_LEVEL=\"" << log_level
                         << "\"; expected TRACE, INFO, WARNING, or ERROR";
        }
    }

    const char* vlog_level = std::getenv("MC_VLOG_LEVEL");
    if (vlog_level && *vlog_level) {
        const int parsed = std::atoi(vlog_level);
        if (parsed >= 0) {
            FLAGS_v = parsed;
        } else {
            LOG(WARNING) << "Invalid MC_VLOG_LEVEL=\"" << vlog_level
                         << "\"; expected a non-negative integer";
        }
    }

    const char* log_dir_path = std::getenv("MC_LOG_DIR");
    if (log_dir_path && *log_dir_path) {
        ApplyLogDirFromPath(log_dir_path);
    }

    return enable_transfer_trace && *enable_transfer_trace;
}

void InitYltLogLevelFromEnv() {
    const char* env_level = std::getenv("MC_YLT_LOG_LEVEL");
    if (!env_level || !*env_level) {
        const char* glog_level = std::getenv("MC_LOG_LEVEL");
        if (glog_level && *glog_level) {
            easylog::set_min_severity(GlogLevelToEasylog(ToUpper(glog_level)));
            return;
        }
        easylog::set_min_severity(easylog::Severity::WARN);
        return;
    }

    std::string level_str(env_level);
    std::transform(
        level_str.begin(), level_str.end(), level_str.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    easylog::Severity severity = easylog::Severity::WARN;
    if (level_str == "trace") {
        severity = easylog::Severity::TRACE;
    } else if (level_str == "debug") {
        severity = easylog::Severity::DEBUG;
    } else if (level_str == "info") {
        severity = easylog::Severity::INFO;
    } else if (level_str == "warn" || level_str == "warning") {
        severity = easylog::Severity::WARN;
    } else if (level_str == "error") {
        severity = easylog::Severity::ERROR;
    } else if (level_str == "critical") {
        severity = easylog::Severity::CRITICAL;
    } else {
        LOG(WARNING) << "Unknown MC_YLT_LOG_LEVEL=\"" << env_level
                     << "\"; defaulting to WARN";
    }
    easylog::set_min_severity(severity);
}

void InitMooncakeLogging(const char* argv0) {
    const char* program = (argv0 && *argv0) ? argv0 : "mooncake";
    static bool glog_initialized = false;
    if (!glog_initialized) {
        google::InitGoogleLogging(program);
        glog_initialized = true;
    }
    ApplyGlogEnvironment(nullptr);
    InitYltLogLevelFromEnv();
}

}  // namespace mooncake
