#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "traffic_format.h"

namespace mooncake::traffic {

class RedactionRegistry;
class RouteNameRegistry;

// Settings that come from the configuration file, already validated. A key the
// file leaves out keeps the value below.
struct RecorderConfig {
    std::string output_dir;
    std::uint64_t block_bytes = 4ull * 1024 * 1024;
    std::uint64_t segment_bytes = 256ull * 1024 * 1024;
    std::uint64_t segment_seconds = 300;
    std::uint64_t ring_bytes = 1024ull * 1024 * 1024;
    std::uint64_t max_record_bytes = 4ull * 1024 * 1024;
    std::uint64_t max_total_bytes = 10ull * 1024 * 1024 * 1024;
    std::uint64_t stop_timeout_ms = 30000;
    std::uint32_t ring_shards = 1;
    std::string control_host = "127.0.0.1";
    std::uint16_t control_port = 37901;
    std::string control_token;
    // The token file named by the configuration, read while loading.
    std::string control_token_file;
    RedactionMode redaction_mode = RedactionMode::kNone;
    std::unordered_set<std::uint32_t> allowed_routes;
    // A flat vector keeps the per-request route filter free of allocations;
    // the set is small (at most one entry per master route).
    std::vector<std::string> allowed_route_names;
};

// Reads and validates the file at `path`. Returns false and sets `error` to the
// first problem found. Reading the control token file and creating the output
// directory with its token are part of loading, because the configuration is
// not usable without them.
bool LoadRecordingConfig(const std::string& path, std::size_t rpc_threads,
                         const RedactionRegistry& redaction,
                         const RouteNameRegistry& routes,
                         RecorderConfig* config, std::string* error);

}  // namespace mooncake::traffic
