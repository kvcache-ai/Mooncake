#pragma once

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <variant>

#include "common_types.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace mooncake {

// Deployment-only placeholder: the native Client has no read options.
struct CentralizedReadConfig {
    std::monostate placeholder{};
};
YLT_REFL(CentralizedReadConfig, placeholder);

// Centralized policy defaults.
static constexpr uint64_t DEFAULT_DEFAULT_KV_LEASE_TTL =
    5000;  // in milliseconds
static constexpr uint64_t DEFAULT_KV_SOFT_PIN_TTL_MS =
    30 * 60 * 1000;  // 30 minutes
static constexpr bool DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS = true;
static constexpr double DEFAULT_EVICTION_RATIO = 0.05;
static constexpr double DEFAULT_EVICTION_HIGH_WATERMARK_RATIO = 0.95;
constexpr const char* DEFAULT_ROOT_FS_DIR = "";
// default do not limit DFS usage, and use
// int64_t to make it compaitable to file metrics monitor
static const int64_t DEFAULT_GLOBAL_FILE_SEGMENT_SIZE =
    std::numeric_limits<int64_t>::max();
constexpr const char* PUT_NO_SPACE_HELPER_STR =  // A helpful string
    " due to insufficient space. Consider lowering "
    "eviction_high_watermark_ratio or mounting more segments.";
static constexpr uint64_t DEFAULT_PUT_START_DISCARD_TIMEOUT = 30;  // 30 seconds
static constexpr uint64_t DEFAULT_PUT_START_RELEASE_TIMEOUT =
    600;  // 10 minutes

// Task manager constants
static constexpr uint32_t DEFAULT_MAX_TOTAL_FINISHED_TASKS = 10000;
static constexpr uint32_t DEFAULT_MAX_TOTAL_PENDING_TASKS = 10000;
static constexpr uint32_t DEFAULT_MAX_TOTAL_PROCESSING_TASKS = 10000;
static constexpr uint64_t DEFAULT_PENDING_TASK_TIMEOUT_SEC =
    300;  // 0 to be no timeout
static constexpr uint64_t DEFAULT_PROCESSING_TASK_TIMEOUT_SEC =
    300;  // 0 to be no timeout

/**
 * @brief Represents a contiguous memory region
 */
struct Segment {
    UUID id{0, 0};
    std::string name{};  // Logical segment name used for preferred allocation
    uintptr_t base{0};
    size_t size{0};
    // TE p2p endpoint (ip:port) for transport-only addressing
    std::string te_endpoint{};
    std::string protocol;
    Segment() = default;
};
YLT_REFL(Segment, id, name, base, size, te_endpoint, protocol);

/**
 * @brief Client status from the master's perspective
 */
enum class ClientStatus {
    UNDEFINED = 0,  // Uninitialized
    OK,             // Client is alive, no need to remount for now
    NEED_REMOUNT,   // Ping ttl expired, or the first time connect to master,
                    // so need to remount
};

/**
 * @brief Stream operator for ClientStatus
 */
inline std::ostream& operator<<(std::ostream& os,
                                const ClientStatus& status) noexcept {
    static const std::unordered_map<ClientStatus, std::string_view>
        status_strings{{ClientStatus::UNDEFINED, "UNDEFINED"},
                       {ClientStatus::OK, "OK"},
                       {ClientStatus::NEED_REMOUNT, "NEED_REMOUNT"}};

    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

}  // namespace mooncake
