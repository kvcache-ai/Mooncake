#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "Slab.h"

namespace mooncake {

// Constants
static constexpr uint64_t WRONG_VERSION = 0;
static constexpr uint64_t DEFAULT_VALUE = UINT64_MAX;
static constexpr uint64_t ERRNO_BASE = DEFAULT_VALUE - 1000;

// Forward declarations
class BufferAllocator;
class BufHandle;
class ReplicaInfo;

// Type aliases for improved readability and type safety
using ObjectKey = std::string;
using Version = uint64_t;
using SegmentId = int64_t;
using TaskID = int64_t;
using BufHandleList = std::vector<std::shared_ptr<BufHandle>>;
// using ReplicaList = std::vector<ReplicaInfo>;
using ReplicaList = std::unordered_map<uint32_t, ReplicaInfo>;
using BufferResources =
    std::map<SegmentId, std::vector<std::shared_ptr<BufferAllocator>>>;

/**
 * @brief Error codes for various operations in the system
 */
enum class ErrorCode : int32_t {
    OK = 0,               ///< Operation successful.
    INTERNAL_ERROR = -1,  ///< Internal error occurred.

    // Buffer allocation errors (Range: -20 to -99)
    BUFFER_OVERFLOW = -10,  ///< Insufficient buffer space.

    // Segment selection errors (Range: -100 to -199)
    SHARD_INDEX_OUT_OF_RANGE = -100,  ///< Shard index is out of bounds.
    AVAILABLE_SEGMENT_EMPTY = -101,   ///< No available segments found.

    // Handle selection errors (Range: -200 to -299)
    NO_AVAILABLE_HANDLE = -200,  ///< No available handles.

    // Version errors (Range: -300 to -399)
    INVALID_VERSION = -300,  ///< Invalid version.

    // Key errors (Range: -400 to -499)
    INVALID_KEY = -400,  ///< Invalid key.

    // Engine errors (Range: -500 to -599)
    WRITE_FAIL = -500,  ///< Write operation failed.

    // Parameter errors (Range: -600 to -699)
    INVALID_PARAMS = -600,  ///< Invalid parameters.

    // Engine operation errors (Range: -700 to -799)
    INVALID_WRITE = -700,          ///< Invalid write operation.
    INVALID_READ = -701,           ///< Invalid read operation.
    INVALID_REPLICA = -702,        ///< Invalid replica operation.
    REPLICA_IS_NOT_READY = -703,   ///< Replica is not ready.
    OBJECT_NOT_FOUND = -704,       ///< Object not found.
    OBJECT_ALREADY_EXISTS = -705,  ///< Object already exists.

    // Transfer errors (Range: -800 to -899)
    TRANSFER_FAIL = -800,  ///< Transfer operation failed.

    // RPC errors (Range: -900 to -999)
    RPC_FAIL = -900,  ///< RPC operation failed.
};

int32_t toInt(ErrorCode errorCode) noexcept;
ErrorCode fromInt(int32_t errorCode) noexcept;

const std::string& toString(ErrorCode errorCode) noexcept;

inline std::ostream& operator<<(std::ostream& os,
                                const ErrorCode& errorCode) noexcept {
    return os << toString(errorCode);
}

/**
 * @brief Status of a buffer in the system
 */
enum class BufStatus {
    INIT,      // Initial state
    COMPLETE,  // Complete state (buffer has been used)
    FAILED,    // Failed state (allocation failed, upstream should set handle to
               // this state)
    UNREGISTERED,  // Buffer metadata has been deleted
};

/**
 * @brief Stream operator for BufStatus
 */
inline std::ostream& operator<<(std::ostream& os,
                                const BufStatus& status) noexcept {
    static const std::unordered_map<BufStatus, std::string_view> status_strings{
        {BufStatus::INIT, "INIT"},
        {BufStatus::COMPLETE, "COMPLETE"},
        {BufStatus::FAILED, "FAILED"},
        {BufStatus::UNREGISTERED, "UNREGISTERED"}};

    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

class BufferAllocator;

/**
 * @brief Metadata for a replica in the system
 */
struct MetaForReplica {
    ObjectKey object_name{};
    Version version{0};
    uint64_t replica_id{0};
    uint64_t shard_id{0};
};

/**
 * @brief Handle for managing buffer allocations
 */
class BufHandle {
   public:
    SegmentId segment_id{0};
    std::string segment_name;
    uint64_t size{0};
    BufStatus status{BufStatus::INIT};
    MetaForReplica replica_meta;
    void* buffer{nullptr};

   public:
    BufHandle(std::shared_ptr<BufferAllocator> allocator,
              std::string segment_name, uint64_t size, void* buffer);
    ~BufHandle();

    // Prevent copying to avoid double-free issues
    BufHandle(const BufHandle&) = delete;
    BufHandle& operator=(const BufHandle&) = delete;

    friend std::ostream& operator<<(std::ostream& os,
                                    const BufHandle& handle) noexcept {
        return os << "BufHandle: { "
                  << "segment_id: " << handle.segment_id << ", "
                  << "segment_name: " << handle.segment_name << ", "
                  << "size: " << handle.size << ", "
                  << "status: " << handle.status << ", "
                  << "buffer: " << handle.buffer << " }";
    }

   private:
    std::shared_ptr<BufferAllocator> allocator_;
};

/**
 * @brief Status of a replica in the system
 */
enum class ReplicaStatus {
    UNDEFINED = 0,  // Uninitialized
    INITIALIZED,    // Space allocated, waiting for write
    PROCESSING,     // Write in progress
    COMPLETE,       // Write complete, replica is available
    REMOVED,        // Replica has been removed
    FAILED,         // Failed state (can be used for reassignment)
};

/**
 * @brief Stream operator for ReplicaStatus
 */
inline std::ostream& operator<<(std::ostream& os,
                                const ReplicaStatus& status) noexcept {
    static const std::unordered_map<ReplicaStatus, std::string_view>
        status_strings{{ReplicaStatus::UNDEFINED, "UNDEFINED"},
                       {ReplicaStatus::INITIALIZED, "INITIALIZED"},
                       {ReplicaStatus::PROCESSING, "PROCESSING"},
                       {ReplicaStatus::COMPLETE, "COMPLETE"},
                       {ReplicaStatus::REMOVED, "REMOVED"},
                       {ReplicaStatus::FAILED, "FAILED"}};

    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

/**
 * @brief Configuration for replica management
 */
struct ReplicateConfig {
    size_t replica_num{0};

    friend std::ostream& operator<<(std::ostream& os,
                                    const ReplicateConfig& config) noexcept {
        return os << "ReplicateConfig: { replica_num: " << config.replica_num
                  << " }";
    }
};

/**
 * @brief Information about a replica
 */
class ReplicaInfo {
   public:
    ReplicaInfo() = default;
    std::vector<std::shared_ptr<BufHandle>> handles;
    ReplicaStatus status{ReplicaStatus::UNDEFINED};
    uint32_t replica_id{0};

    void reset() noexcept {
        handles.clear();
        status = ReplicaStatus::UNDEFINED;
    }
};

inline std::ostream& operator<<(std::ostream& os, const ReplicaInfo& info) {
    os << "ReplicaInfo: { "
       << "replica_id: " << info.replica_id << ", "
       << "status: " << info.status << ", "
       << "handles: [";
    for (size_t i = 0; i < info.handles.size(); ++i) {
        os << *info.handles[i];
        if (i < info.handles.size() - 1) {
            os << ", ";
        }
    }
    os << "] }";
    return os;
}

/**
 * @brief Represents a contiguous memory region
 */
struct Slice {
    void* ptr{nullptr};
    size_t size{0};
};

const static uint64_t kMinSliceSize = facebook::cachelib::Slab::kMinAllocSize;
const static uint64_t kMaxSliceSize =
    facebook::cachelib::Slab::kSize - 16;  // should be lower than limit

}  // namespace mooncake
