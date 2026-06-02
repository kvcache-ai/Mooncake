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

#pragma once

#include <cstdint>
#include <string>

namespace mooncake {

// Configuration for the master log journal.
//
// The journal is a thin wrapper on top of glog. Rather than re-implementing a
// logging engine, it configures glog so that all diagnostic records emitted by
// the master (via LOG()/VLOG()) are *merged* into a single, chronologically
// ordered log file that lives inside one dedicated journal directory.
//
// glog normally splits records across one file per severity
// (mooncake_master.INFO / .WARNING / .ERROR / .FATAL) and a record is written
// to the file of its own severity *and* every lower severity. The journal
// keeps only the lowest-severity sink (which therefore already contains every
// record) and disables the higher-severity sinks, yielding a single merged
// file. All of glog's battle-tested machinery is reused:
//   - file creation, formatting and the latest-file symlink
//   - size based rotation (FLAGS_max_log_size)
//   - time based retention (google::EnableLogCleaner)
//   - periodic background flushing (FLAGS_logbufsecs)
//   - optional mirroring to stderr (FLAGS_stderrthreshold)
struct MasterLogJournalConfig {
    // When false, the journal does nothing and the caller keeps glog's default
    // behavior. Used to preserve backward compatibility.
    bool enable = true;

    // Directory that holds the merged journal file(s). When empty it is
    // resolved at Init() time (see MasterLogJournal::Init).
    std::string journal_dir;

    // Program name passed to google::InitGoogleLogging and used as the base
    // name of the journal file and its symlink.
    std::string program_name = "mooncake_master";

    // When true (default), every severity is merged into one file. When false
    // glog's per-severity files are kept, but still placed inside journal_dir.
    bool merge_severities = true;

    // Minimum severity persisted to the journal (maps to FLAGS_minloglevel).
    // 0 = INFO, 1 = WARNING, 2 = ERROR, 3 = FATAL.
    int min_severity = 0;

    // Mirror records to stderr in addition to the journal file.
    bool also_log_to_stderr = false;

    // Minimum severity mirrored to stderr (maps to FLAGS_stderrthreshold).
    // Only relevant when also_log_to_stderr is true. 0 = INFO ... 3 = FATAL.
    int stderr_threshold = 2;  // ERROR

    // Per-file rotation threshold in MiB. 0 keeps glog's current default.
    uint32_t max_file_size_mb = 0;

    // Delete journal files older than this many days. 0 disables cleanup.
    uint32_t retention_days = 0;

    // Background flush interval in seconds. Negative keeps glog's default.
    int32_t flush_interval_sec = -1;
};

// Owns master-wide diagnostic logging configuration. Process-wide singleton
// because glog itself is process-global; Init() is idempotent.
class MasterLogJournal {
   public:
    static MasterLogJournal& Instance();

    MasterLogJournal(const MasterLogJournal&) = delete;
    MasterLogJournal& operator=(const MasterLogJournal&) = delete;

    // Resolves the journal directory, initializes glog (if needed) and routes
    // all log records into the merged journal. Safe to call more than once;
    // subsequent calls are ignored and return true.
    //
    // Journal directory resolution when config.journal_dir is empty:
    //   1. FLAGS_log_dir if set, otherwise
    //   2. "<program_name>_logs" relative to the current working directory.
    // The resolved directory is created if it does not exist.
    //
    // Returns false only if the journal is enabled but the directory cannot be
    // created. In that case glog is left uninitialized and the caller may fall
    // back to its previous behavior.
    bool Init(const MasterLogJournalConfig& config);

    // Flush buffered records of INFO and above to disk. No-op if not active.
    void Flush();

    // Flush and shut glog down. No-op if not active.
    void Shutdown();

    bool active() const { return active_; }

    // Resolved directory that holds the merged journal file. Empty until Init.
    const std::string& journal_dir() const { return journal_dir_; }

    // Stable path to the symlink that always points at the current journal
    // file (e.g. "<journal_dir>/mooncake_master.INFO"). Empty until Init.
    const std::string& journal_symlink() const { return journal_symlink_; }

   private:
    MasterLogJournal() = default;

    std::string ResolveJournalDir(const MasterLogJournalConfig& config) const;

    bool active_ = false;
    std::string journal_dir_;
    std::string journal_symlink_;
};

}  // namespace mooncake
