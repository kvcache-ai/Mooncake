#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Slab.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

// Shared value types and allocation facilities used by both architectures.
static constexpr uint64_t WRONG_VERSION = 0;
static constexpr uint64_t DEFAULT_VALUE = UINT64_MAX;
static constexpr uint64_t ERRNO_BASE = DEFAULT_VALUE - 1000;
static constexpr int64_t DEFAULT_CLIENT_LIVE_TTL_SEC = 10;  // in seconds
constexpr const char* DEFAULT_CLUSTER_ID = "mooncake_cluster";
static const std::string DEFAULT_CXL_PATH = "/dev/dax0.0";
static const size_t DEFAULT_CXL_BASE = 0x100000000ULL;
static const size_t DEFAULT_CXL_SIZE = 8ULL * 1024 * 1024 * 1024;

class BufferAllocatorBase;
class CachelibBufferAllocator;
class OffsetBufferAllocator;
class AllocatedBuffer;

using ObjectKey = std::string;
using Version = uint64_t;
using SegmentId = int64_t;
using TaskID = int64_t;
using BufHandleList = std::vector<std::shared_ptr<AllocatedBuffer>>;
using BufferResources =
    std::map<SegmentId, std::vector<std::shared_ptr<BufferAllocatorBase>>>;

// Match GoInt64 in etcd builds without generated-header dependencies.
#ifdef STORE_USE_ETCD
using ViewVersionId = long long;
#else
using ViewVersionId = int64_t;
#endif

using UUID = std::pair<uint64_t, uint64_t>;

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
    CLIENT_ALREADY_EXISTS = -104,     ///< Client already exists.
    CLIENT_UNHEALTHY =
        -105,  ///< Client is not in a healthy state for the operation.
    NO_AVAILABLE_CANDIDATE =
        -106,  ///< No available write-route candidate found.

    // Handle selection errors (Range: -200 to -299)
    NO_AVAILABLE_HANDLE =
        -200,  ///< Memory allocation failed due to insufficient space.

    // Version errors (Range: -300 to -399)
    INVALID_VERSION = -300,  ///< Invalid version.
    CAS_FAILED = -301,       ///< Compare and Swap failed (Optimistic Locking).

    // Key errors (Range: -400 to -499)
    INVALID_KEY = -400,  ///< Invalid key.

    // Engine errors (Range: -500 to -599)
    WRITE_FAIL = -500,  ///< Write operation failed.

    // Parameter errors (Range: -600 to -699)
    INVALID_PARAMS = -600,  ///< Invalid parameters.
    ILLEGAL_CLIENT = -601,  ///< Illegal client to do the operation.

    // Engine operation errors (Range: -700 to -711)
    INVALID_WRITE = -700,    ///< Invalid write operation.
    INVALID_READ = -701,     ///< Invalid read operation.
    INVALID_REPLICA = -702,  ///< Invalid replica operation.

    // Object errors (Range: -703 to -750)
    REPLICA_IS_NOT_READY = -703,   ///< Replica is not ready.
    OBJECT_NOT_FOUND = -704,       ///< Object not found.
    OBJECT_ALREADY_EXISTS = -705,  ///< Object already exists.
    OBJECT_HAS_LEASE = -706,       ///< Object has lease.
    LEASE_EXPIRED = -707,  ///< Lease expired before data transfer completed.
    OBJECT_HAS_REPLICATION_TASK =
        -708,  ///< Object has ongoing replication task.
    OBJECT_NO_REPLICATION_TASK =
        -709,  ///< Object does not have ongoing replication task.
    REPLICA_NOT_FOUND = -710,       ///< Replica not found.
    REPLICA_ALREADY_EXISTS = -711,  ///< Replica already exists.
    REPLICA_IS_GONE = -712,         ///< Replica existed once, but is gone now.
    REPLICA_NUM_EXCEEDED = -713,    ///< Replica number exceeded.
    REPLICA_IS_PROCESSING =
        -714,  ///< Replica is processing an in-flight write.

    // Transfer errors (Range: -800 to -899)
    TRANSFER_FAIL = -800,  ///< Transfer operation failed.

    // RPC errors (Range: -900 to -999)
    RPC_FAIL = -900,  ///< RPC operation failed.
    HEARTBEAT_RPC_UNREACHABLE =
        -901,  ///< Dedicated heartbeat RPC server unreachable.
    HEARTBEAT_ROUTING_MISMATCH =
        -902,  ///< Client/master heartbeat routing mismatch (one side
               ///< dedicated, the other legacy).

    // High availability errors (Range: -1000 to -1099)
    ETCD_OPERATION_ERROR = -1000,   ///< etcd operation failed.
    ETCD_KEY_NOT_EXIST = -1001,     ///< key not found in etcd.
    ETCD_TRANSACTION_FAIL = -1002,  ///< etcd transaction failed.
    ETCD_CTX_CANCELLED = -1003,     ///< etcd context cancelled.
    OPLOG_ENTRY_NOT_FOUND = -1004,  ///< OpLog entry not found.
    OPLOG_TRIMMED = -1005,          ///< Requested OpLog range was trimmed.
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

    // Task errors (Range: -1400 to -1499)
    TASK_NOT_FOUND = -1400,  ///< Task not found.
    TASK_PENDING_LIMIT_EXCEEDED =
        -1401,  ///< Total pending tasks exceed the limit.

    // Tiered backend errors (Range: -1500 to -1599)
    EMPTY_REPLICAS = -1500,
    TIER_NOT_FOUND = -1501,
    DATA_COPY_FAILED = -1502,

    // Store errors (Range: -1600 to -1699)
    SHUTTING_DOWN = -1600,  ///< Store is shutting down, rejecting new requests.
    ASYNC_ENQUEUE_FAILED = -1601,  ///< Async metadata notifier enqueue failed
                                   ///< (queue full/stopped).
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

// Shared storage metadata; field and reflection order are part of the wire
// contract.
struct StorageObjectMetadata {
    int64_t bucket_id;
    int64_t offset;
    int64_t key_size;
    int64_t data_size;
    std::string transport_endpoint;
    YLT_REFL(StorageObjectMetadata, bucket_id, offset, key_size, data_size,
             transport_endpoint);
};

const static uint64_t kMinSliceSize = facebook::cachelib::Slab::kMinAllocSize;
const static uint64_t kMaxSliceSize =
    facebook::cachelib::Slab::kSize - 16;  // should be lower than limit

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

enum class DeploymentMode {
    UNKNOWN = -1,
    CENTRALIZATION = 0,
    P2P,
};

inline std::ostream& operator<<(std::ostream& os,
                                const DeploymentMode& mode) noexcept {
    switch (mode) {
        case DeploymentMode::CENTRALIZATION:
            os << "CENTRALIZATION";
            break;
        case DeploymentMode::P2P:
            os << "P2P";
            break;
        default:
            os << "UNKNOWN";
            break;
    }
    return os;
}

static constexpr int64_t DEFAULT_DUMMY_CLIENT_LIVE_TTL_SEC = 30;  // in seconds

/**
 * @enum DummyClientStatus
 * @brief Heartbeat status reported by RealClient back to a DummyClient over the
 *        dummy ping RPC.
 */
enum class DummyClientStatus {
    HEALTH = 0,     // Normal operation
    DISCONNECTION,  // RealClient dropped this client's shm; dummy must
                    // re-register
};

/**
 * @brief Stream operator for DummyClientStatus
 */
inline std::ostream& operator<<(std::ostream& os,
                                const DummyClientStatus& status) noexcept {
    static const std::unordered_map<DummyClientStatus, std::string_view>
        status_strings{{DummyClientStatus::HEALTH, "HEALTH"},
                       {DummyClientStatus::DISCONNECTION, "DISCONNECTION"}};

    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

/**
 * @brief Response structure for the DummyClient ping RPC (RealClient::ping).
 */
struct DummyHeartbeatResponse {
    DummyClientStatus status = DummyClientStatus::HEALTH;
    uint64_t mapped_shm_count = 0;
};
YLT_REFL(DummyHeartbeatResponse, status, mapped_shm_count);

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
