#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <limits>
#include <unordered_map>
#include <vector>

#include "Slab.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

#ifdef STORE_USE_ETCD
#include "libetcd_wrapper.h"
#endif

namespace mooncake {

// Constants
static constexpr uint64_t WRONG_VERSION = 0;
static constexpr uint64_t DEFAULT_VALUE = UINT64_MAX;
static constexpr uint64_t ERRNO_BASE = DEFAULT_VALUE - 1000;
static constexpr uint64_t DEFAULT_DEFAULT_KV_LEASE_TTL =
    5000;  // in milliseconds
static constexpr uint64_t DEFAULT_KV_SOFT_PIN_TTL_MS =
    30 * 60 * 1000;  // 30 minutes
static constexpr bool DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS = true;
static constexpr double DEFAULT_EVICTION_RATIO = 0.05;
static constexpr double DEFAULT_EVICTION_HIGH_WATERMARK_RATIO = 0.95;
static constexpr int64_t ETCD_MASTER_VIEW_LEASE_TTL = 5;    // in seconds
static constexpr int64_t DEFAULT_CLIENT_LIVE_TTL_SEC = 10;  // in seconds
static const std::string DEFAULT_CLUSTER_ID = "mooncake_cluster";
static const std::string DEFAULT_ROOT_FS_DIR = "";
// default do not limit DFS usage, and use
// int64_t to make it compaitable to file metrics monitor
static const int64_t DEFAULT_GLOBAL_FILE_SEGMENT_SIZE =
    std::numeric_limits<int64_t>::max();
static const std::string PUT_NO_SPACE_HELPER_STR =  // A helpful string
    " due to insufficient space. Consider lowering "
    "eviction_high_watermark_ratio or mounting more segments.";
static constexpr uint64_t DEFAULT_PUT_START_DISCARD_TIMEOUT = 30;  // 30 seconds
static constexpr uint64_t DEFAULT_PUT_START_RELEASE_TIMEOUT =
    600;  // 10 minutes

// Forward declarations
class BufferAllocatorBase;
class CachelibBufferAllocator;
class OffsetBufferAllocator;
class AllocatedBuffer;
class Replica;

// Type aliases for improved readability and type safety
using ObjectKey = std::string;
using Version = uint64_t;
using SegmentId = int64_t;
using TaskID = int64_t;
using BufHandleList = std::vector<std::shared_ptr<AllocatedBuffer>>;
// using ReplicaList = std::vector<ReplicaInfo>;
using ReplicaList = std::unordered_map<uint32_t, Replica>;
using BufferResources =
    std::map<SegmentId, std::vector<std::shared_ptr<BufferAllocatorBase>>>;
// Mapping between c++ and go types
#ifdef STORE_USE_ETCD
using EtcdRevisionId = GoInt64;
using ViewVersionId = EtcdRevisionId;
using EtcdLeaseId = GoInt64;
#else
using EtcdRevisionId = int64_t;
using ViewVersionId = int64_t;
using EtcdLeaseId = int64_t;
#endif

using UUID = std::pair<uint64_t, uint64_t>;

using SerializedByte = uint8_t;  // Used as basic unit of serialized data
static_assert(sizeof(SerializedByte) == 1,
              "SerializedByte must be exactly 1 byte in size");

inline std::ostream& operator<<(std::ostream& os, const UUID& uuid) noexcept {
    os << uuid.first << "-" << uuid.second;
    return os;
}

UUID generate_uuid();

/**
 * @brief Error codes for various operations in the system
 */
enum class ErrorCode : int32_t {
    OK = 0,                ///< Operation successful.
    INTERNAL_ERROR = -1,   ///< Internal error occurred.
    NOT_IMPLEMENTED = -2,  ///< Not implemented.

    // Buffer allocation errors (Range: -20 to -99)
    BUFFER_OVERFLOW = -10,  ///< Insufficient buffer space.

    // Segment selection errors (Range: -100 to -199)
    SHARD_INDEX_OUT_OF_RANGE = -100,  ///< Shard index is out of bounds.
    SEGMENT_NOT_FOUND = -101,         ///< No available segments found.
    SEGMENT_ALREADY_EXISTS = -102,    ///< Segment already exists.
    CLIENT_NOT_FOUND = -103,          ///< Client not found.

    // Handle selection errors (Range: -200 to -299)
    NO_AVAILABLE_HANDLE =
        -200,  ///< Memory allocation failed due to insufficient space.

    // Version errors (Range: -300 to -399)
    INVALID_VERSION = -300,  ///< Invalid version.

    // Key errors (Range: -400 to -499)
    INVALID_KEY = -400,  ///< Invalid key.

    // Engine errors (Range: -500 to -599)
    WRITE_FAIL = -500,  ///< Write operation failed.

    // Parameter errors (Range: -600 to -699)
    INVALID_PARAMS = -600,  ///< Invalid parameters.
    ILLEGAL_CLIENT = -601,  ///< Illegal client to do the operation.

    // Engine operation errors (Range: -700 to -799)
    INVALID_WRITE = -700,    ///< Invalid write operation.
    INVALID_READ = -701,     ///< Invalid read operation.
    INVALID_REPLICA = -702,  ///< Invalid replica operation.

    // Object errors (Range: -703 to -707)
    REPLICA_IS_NOT_READY = -703,   ///< Replica is not ready.
    OBJECT_NOT_FOUND = -704,       ///< Object not found.
    OBJECT_ALREADY_EXISTS = -705,  ///< Object already exists.
    OBJECT_HAS_LEASE = -706,       ///< Object has lease.
    LEASE_EXPIRED = -707,  ///< Lease expired before data transfer completed.

    // Transfer errors (Range: -800 to -899)
    TRANSFER_FAIL = -800,  ///< Transfer operation failed.

    // RPC errors (Range: -900 to -999)
    RPC_FAIL = -900,  ///< RPC operation failed.

    // High availability errors (Range: -1000 to -1099)
    ETCD_OPERATION_ERROR = -1000,   ///< etcd operation failed.
    ETCD_KEY_NOT_EXIST = -1001,     ///< key not found in etcd.
    ETCD_TRANSACTION_FAIL = -1002,  ///< etcd transaction failed.
    ETCD_CTX_CANCELLED = -1003,     ///< etcd context cancelled.
    UNAVAILABLE_IN_CURRENT_STATUS =
        -1010,  ///< Request cannot be done in current status.
    UNAVAILABLE_IN_CURRENT_MODE =
        -1011,  ///< Request cannot be done in current mode.

    // FILE errors (Range: -1100 to -1199)
    FILE_NOT_FOUND = -1100,       ///< File not found.
    FILE_OPEN_FAIL = -1101,       ///< Error open file or write to a exist file.
    FILE_READ_FAIL = -1102,       ///< Error reading file.
    FILE_WRITE_FAIL = -1103,      ///< Error writing file.
    FILE_INVALID_BUFFER = -1104,  ///< File buffer is wrong.
    FILE_LOCK_FAIL = -1105,       ///< File lock operation failed.
    FILE_INVALID_HANDLE = -1106,  ///< Invalid file handle.

    BUCKET_NOT_FOUND = -1200,          ///< Bucket not found.
    BUCKET_ALREADY_EXISTS = -1201,     ///< Bucket already exists.
    KEYS_EXCEED_BUCKET_LIMIT = -1202,  ///< Keys exceed bucket limit.
    KEYS_ULTRA_LIMIT = -1203,          ///< Keys ultra limit.
    UNABLE_OFFLOAD = -1300,     ///< The offload functionality is not enabled
    UNABLE_OFFLOADING = -1301,  ///< Unable offloading.

    // Tiered backend errors (Range: -1400 to -1499)
    EMPTY_REPLICAS = -1400,
    TIER_NOT_FOUND = -1401,
    DATA_COPY_FAILED = -1402,
};

int32_t toInt(ErrorCode errorCode) noexcept;
ErrorCode fromInt(int32_t errorCode) noexcept;

const std::string& toString(ErrorCode errorCode) noexcept;

inline std::ostream& operator<<(std::ostream& os,
                                const ErrorCode& errorCode) noexcept {
    return os << toString(errorCode);
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
    Segment() = default;
};
YLT_REFL(Segment, id, name, base, size, te_endpoint);

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

enum class BufferAllocatorType {
    CACHELIB = 0,  // CachelibBufferAllocator
    OFFSET = 1,    // OffsetBufferAllocator
};

/**
 * @brief Stream operator for BufferAllocatorType
 */
inline std::ostream& operator<<(std::ostream& os,
                                const BufferAllocatorType& type) noexcept {
    static const std::unordered_map<BufferAllocatorType, std::string_view>
        type_strings{{BufferAllocatorType::CACHELIB, "CACHELIB"},
                     {BufferAllocatorType::OFFSET, "OFFSET"}};

    os << (type_strings.count(type) ? type_strings.at(type) : "UNKNOWN");
    return os;
}

struct StorageObjectMetadata {
    int64_t bucket_id;
    int64_t offset;
    int64_t key_size;
    int64_t data_size;
    std::string transport_endpoint;
    YLT_REFL(StorageObjectMetadata, bucket_id, offset, key_size, data_size,
             transport_endpoint);
};

}  // namespace mooncake

namespace std {
template <>
struct hash<mooncake::UUID> {
    std::size_t operator()(const mooncake::UUID& k) const {
        std::size_t h1 = hash<uint64_t>{}(k.first);
        std::size_t h2 = hash<uint64_t>{}(k.second);
        return h1 ^ (h2 << 1);
    }
};
}  // namespace std
