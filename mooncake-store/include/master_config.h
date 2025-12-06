#pragma once

#include <stdexcept>
#include <sstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "utils.h"
#include "types.h"

namespace mooncake {

enum class GlogLogLevel { INFO = 0, WARNING = 1, ERROR = 2, FATAL = 3 };

struct GlogConfig {
    std::string log_dir = "/log/server/mooncake-master";  // root dir for logs
    std::string log_prefix = "master_service";        // prefix for log files
    GlogLogLevel min_log_level = GlogLogLevel::INFO;  // log level for files
    GlogLogLevel stderr_output_level =
        GlogLogLevel::WARNING;             // log level for stderr output
    bool enable_alsologtostderr = true;    // whether to also log to stderr
    uint32_t max_log_size_mb = 100;        // max size in MB for each log file
    uint32_t max_retained_log_files = 10;  // max log files to keep
    bool rotate_logs_by_day = true;        // whether to rotate logs by day

    int32_t log_buffer_seconds =
        0;  // log buffer time (0=immediate output, avoid missing logs)
    bool stop_logging_on_disk_full =
        true;  // whether to stop logging when disk is full
    bool enable_log_prefix =
        true;  // whether to enable log prefix (time/process ID/thread ID, etc.)

    std::string glogLevelToString(GlogLogLevel level) const {
        switch (level) {
            case GlogLogLevel::INFO:
                return "INFO";
            case GlogLogLevel::WARNING:
                return "WARNING";
            case GlogLogLevel::ERROR:
                return "ERROR";
            case GlogLogLevel::FATAL:
                return "FATAL";
            default:
                return "UNKNOWN";
        }
    }

    void InitLogging(const std::string& program_name = "master") {
        if (log_dir.empty()) {
            return;
        }

        auto ret = mkdirall(log_dir.c_str());
        if (ret != 0 && errno != EEXIST) {
            throw std::runtime_error("Failed to create log directory: " +
                                     log_dir + ", " + strerror(errno));
        }

        FLAGS_log_dir = log_dir;
        google::SetLogFilenameExtension(".log");

        for (auto& [level, level_str] :
             std::unordered_map<google::LogSeverity, std::string>{
                 {google::INFO, "INFO"},
                 {google::WARNING, "WARNING"},
                 {google::ERROR, "ERROR"},
                 {google::FATAL, "FATAL"}}) {
            google::SetLogDestination(
                level,
                (log_dir + "/" + log_prefix + "." + level_str + ".").c_str());
        }

        FLAGS_minloglevel = static_cast<int>(min_log_level);
        FLAGS_stderrthreshold = static_cast<int>(stderr_output_level);
        FLAGS_max_log_size = max_log_size_mb;
        FLAGS_logtostderr = false;
        FLAGS_alsologtostderr = enable_alsologtostderr;
        FLAGS_logbufsecs = log_buffer_seconds;
        FLAGS_stop_logging_if_full_disk = stop_logging_on_disk_full;
        FLAGS_log_prefix = enable_log_prefix;

        google::InitGoogleLogging(program_name.c_str());

        if (rotate_logs_by_day) {
            // create symlink for today's log file
            google::SetLogSymlink(google::INFO, (log_prefix + ".INFO").c_str());
        }

        LOG(INFO) << "Glog initialized successfully!";
        LOG(INFO) << "Log config: dir=" << log_dir
                  << ", min_level=" << glogLevelToString(min_log_level)
                  << ", max_file_size=" << max_log_size_mb << "MB"
                  << ", retained_files=" << max_retained_log_files;
    }

    void ShutdownLogging() { google::ShutdownGoogleLogging(); }
};

// The configuration for the master server
struct MasterConfig : public GlogConfig {
    bool enable_metric_reporting = true;
    uint32_t metrics_port = 9003;
    uint32_t rpc_port = 50051;
    uint32_t rpc_thread_num = 4;
    std::string rpc_address = "0.0.0.0";
    int32_t rpc_conn_timeout_seconds = 0;
    bool rpc_enable_tcp_no_delay = true;

    uint64_t default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
    uint64_t default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS;
    bool allow_evict_soft_pinned_objects =
        DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS;
    double eviction_ratio = DEFAULT_EVICTION_RATIO;
    double eviction_high_watermark_ratio =
        DEFAULT_EVICTION_HIGH_WATERMARK_RATIO;
    int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;

    std::string etcd_endpoints = "";

    std::string cluster_id =
        get_env_or_default("MC_STORE_CLUSTER_ID", DEFAULT_CLUSTER_ID);
    std::string root_fs_dir = DEFAULT_ROOT_FS_DIR;
    int64_t global_file_segment_size = DEFAULT_GLOBAL_FILE_SEGMENT_SIZE;
    std::string memory_allocator = "offset";

    bool enable_http_metadata_server = false;
    uint32_t http_metadata_server_port = 8080;
    std::string http_metadata_server_host = "0.0.0.0";

    uint64_t put_start_discard_timeout_sec = DEFAULT_PUT_START_DISCARD_TIMEOUT;
    uint64_t put_start_release_timeout_sec = DEFAULT_PUT_START_RELEASE_TIMEOUT;

    bool enable_disk_eviction = true;
    uint64_t quota_bytes = 0;

    std::string stringify() const {
        std::stringstream ss;
        auto pad = [](const std::string& s, size_t width = 32) {
            if (s.size() >= width) return s;
            return s + std::string(width - s.size(), ' ');
        };

        ss << "MasterConfig {\n";
        ss << "  [Metric Reporting Config]\n"
           << "    " << pad("enable_metric_reporting:") << std::boolalpha
           << enable_metric_reporting << "\n"
           << "    " << pad("metrics_port:") << metrics_port << "\n\n";

        ss << "  [RPC Core Config]\n"
           << "    " << pad("rpc_port:") << rpc_port << "\n"
           << "    " << pad("rpc_thread_num:") << rpc_thread_num << "\n"
           << "    " << pad("rpc_address:") << rpc_address << "\n"
           << "    " << pad("rpc_conn_timeout_seconds:")
           << rpc_conn_timeout_seconds << " sec\n"
           << "    " << pad("rpc_enable_tcp_no_delay:") << std::boolalpha
           << rpc_enable_tcp_no_delay << "\n\n";

        ss << "  [KV Lease & Eviction Config]\n"
           << "    " << pad("default_kv_lease_ttl:") << default_kv_lease_ttl
           << " ms\n"
           << "    " << pad("default_kv_soft_pin_ttl:")
           << default_kv_soft_pin_ttl << " ms\n"
           << "    " << pad("allow_evict_soft_pinned_objects:")
           << std::boolalpha << allow_evict_soft_pinned_objects << "\n"
           << "    " << pad("eviction_ratio:") << eviction_ratio << "\n"
           << "    " << pad("eviction_high_watermark_ratio:")
           << eviction_high_watermark_ratio << "\n"
           << "    " << pad("client_live_ttl_sec:") << client_live_ttl_sec
           << " sec\n\n";

        ss << "  [High Availability (HA) Config]\n"
           << "    " << pad("etcd_endpoints:")
           << (etcd_endpoints.empty() ? "HA disabled" : etcd_endpoints)
           << "\n\n";

        ss << "  [Cluster & File System Config]\n"
           << "    " << pad("cluster_id:") << cluster_id << "\n"
           << "    " << pad("root_fs_dir:")
           << (root_fs_dir.empty() ? "" : root_fs_dir) << "\n"
           << "    " << pad("global_file_segment_size:")
           << (global_file_segment_size == std::numeric_limits<int64_t>::max()
                   ? "max"
                   : byte_size_to_string(global_file_segment_size))
           << "\n"
           << "    " << pad("memory_allocator:") << memory_allocator << "\n\n";

        ss << "  [HTTP Metadata Server Config]\n"
           << "    " << pad("enable_http_metadata_server:") << std::boolalpha
           << enable_http_metadata_server << "\n"
           << "    " << pad("http_metadata_server_port:")
           << http_metadata_server_port << "\n"
           << "    " << pad("http_metadata_server_host:")
           << http_metadata_server_host << "\n\n";

        ss << "  [Put Operation Timeout Config]\n"
           << "    " << pad("put_start_discard_timeout_sec:")
           << put_start_discard_timeout_sec << " sec\n"
           << "    " << pad("put_start_release_timeout_sec:")
           << put_start_release_timeout_sec << " sec\n\n";

        ss << "  [Disk Eviction Config]\n"
           << "    " << pad("enable_disk_eviction:") << std::boolalpha
           << enable_disk_eviction << "\n"
           << "    " << pad("quota_bytes:") << quota_bytes << " ("
           << byte_size_to_string(quota_bytes) << ")\n\n";

        ss << "  [Glog Logging Config]\n"
           << "      " << pad("log_dir:") << log_dir << "\n"
           << "      " << pad("log_prefix:") << log_prefix << "\n"
           << "      " << pad("min_log_level:")
           << glogLevelToString(min_log_level) << " ("
           << static_cast<int>(min_log_level) << ")\n"
           << "      " << pad("stderr_output_level:")
           << glogLevelToString(stderr_output_level) << "\n"
           << "      " << pad("max_log_size_per_file:") << max_log_size_mb
           << " MB (roll by size)\n"
           << "      " << pad("max_retained_log_files:")
           << max_retained_log_files << " (file count limit)\n"
           << "      " << pad("rotate_logs_by_day:") << std::boolalpha
           << rotate_logs_by_day << " (rotate by day)\n"
           << "      " << pad("log_buffer_seconds:") << log_buffer_seconds
           << " sec (0=immediate output)\n"
           << "      " << pad("stop_logging_on_disk_full:") << std::boolalpha
           << stop_logging_on_disk_full << "\n"
           << "      " << pad("enable_log_prefix:") << std::boolalpha
           << enable_log_prefix << "\n";

        ss << "}";

        return ss.str();
    }
};

}  // namespace mooncake
