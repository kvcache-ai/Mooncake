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

#include "master_log_journal.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <filesystem>
#include <system_error>

namespace mooncake {

namespace {

// glog uses int severities; expose readable names for clamping.
constexpr int kSeverityMin = google::INFO;        // 0
constexpr int kSeverityMax = google::GLOG_FATAL;  // 3

int ClampSeverity(int severity) {
    return std::clamp(severity, kSeverityMin, kSeverityMax);
}

}  // namespace

MasterLogJournal& MasterLogJournal::Instance() {
    static MasterLogJournal instance;
    return instance;
}

std::string MasterLogJournal::ResolveJournalDir(
    const MasterLogJournalConfig& config) const {
    if (!config.journal_dir.empty()) {
        return config.journal_dir;
    }
    // Reuse glog's --log_dir when the operator already set one.
    if (!FLAGS_log_dir.empty()) {
        return FLAGS_log_dir;
    }
    // Otherwise keep all master logs together under a predictable directory
    // relative to the working directory.
    return config.program_name + "_logs";
}

bool MasterLogJournal::Init(const MasterLogJournalConfig& config) {
    if (active_) {
        LOG(WARNING) << "MasterLogJournal already initialized at "
                     << journal_dir_ << "; ignoring re-init";
        return true;
    }
    if (!config.enable) {
        return true;
    }

    const std::string journal_dir = ResolveJournalDir(config);

    // Create the journal directory up front; if this fails the caller may fall
    // back to its previous logging behavior.
    std::error_code ec;
    std::filesystem::create_directories(journal_dir, ec);
    if (ec) {
        // glog may not be initialized yet, so report via stderr.
        fprintf(stderr,
                "MasterLogJournal: failed to create journal directory '%s': "
                "%s\n",
                journal_dir.c_str(), ec.message().c_str());
        return false;
    }

    // Reuse glog flags for as much behavior as possible. These must be set
    // before/around InitGoogleLogging so they take effect on the first file.
    FLAGS_log_dir = journal_dir;
    if (config.max_file_size_mb > 0) {
        FLAGS_max_log_size = static_cast<int32_t>(config.max_file_size_mb);
    }
    if (config.flush_interval_sec >= 0) {
        FLAGS_logbufsecs = config.flush_interval_sec;
    }
    FLAGS_minloglevel = ClampSeverity(config.min_severity);

    // Control stderr mirroring purely through glog's stderr threshold so a
    // single sink (the journal file) remains the source of truth.
    if (config.also_log_to_stderr) {
        FLAGS_alsologtostderr = true;
        FLAGS_stderrthreshold = ClampSeverity(config.stderr_threshold);
    } else {
        FLAGS_alsologtostderr = false;
        // Suppress glog's default behavior of echoing ERROR/FATAL to stderr so
        // that the journal file is the single destination.
        FLAGS_stderrthreshold = kSeverityMax + 1;
    }

    if (!google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging(config.program_name.c_str());
    }

    // Base filename for the (lowest-severity) merged sink. glog appends
    // "<date>-<time>.<pid>" to this base, producing e.g.
    // "<dir>/mooncake_master.20240101-120000.12345".
    const std::string base = journal_dir + "/" + config.program_name + ".";

    if (config.merge_severities) {
        // Keep only the INFO sink. Because glog writes every record to the
        // file of its severity and all lower severities, the INFO file already
        // contains a complete, chronologically ordered journal. Disabling the
        // higher sinks ("") merges everything into that one file.
        google::SetLogDestination(google::INFO, base.c_str());
        google::SetLogDestination(google::WARNING, "");
        google::SetLogDestination(google::ERROR, "");
        google::SetLogDestination(google::GLOG_FATAL, "");
        // Stable symlink to the latest journal file.
        google::SetLogSymlink(google::INFO, config.program_name.c_str());
        google::SetLogSymlink(google::WARNING, "");
        google::SetLogSymlink(google::ERROR, "");
        google::SetLogSymlink(google::GLOG_FATAL, "");
        journal_symlink_ = journal_dir + "/" + config.program_name + ".INFO";
    } else {
        // Keep glog's per-severity files but anchor them inside the journal
        // directory with the configured base name.
        for (int severity = kSeverityMin; severity <= kSeverityMax;
             ++severity) {
            google::SetLogDestination(severity, base.c_str());
            google::SetLogSymlink(severity, config.program_name.c_str());
        }
        journal_symlink_ = journal_dir + "/" + config.program_name + ".INFO";
    }

    // Time based retention reuses glog's own cleaner thread.
    if (config.retention_days > 0) {
        google::EnableLogCleaner(config.retention_days);
    } else {
        google::DisableLogCleaner();
    }

    journal_dir_ = journal_dir;
    active_ = true;
    return true;
}

void MasterLogJournal::Flush() {
    if (!active_) {
        return;
    }
    google::FlushLogFiles(google::INFO);
}

void MasterLogJournal::Shutdown() {
    if (!active_) {
        return;
    }
    google::FlushLogFiles(google::INFO);
    if (google::IsGoogleLoggingInitialized()) {
        google::ShutdownGoogleLogging();
    }
    active_ = false;
}

}  // namespace mooncake
