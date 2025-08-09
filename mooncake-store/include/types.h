#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "Slab.h"
#include "allocator.h"
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
static constexpr double DEFAULT_EVICTION_RATIO = 0.1;
static constexpr double DEFAULT_EVICTION_HIGH_WATERMARK_RATIO = 1.0;
static constexpr int64_t ETCD_MASTER_VIEW_LEASE_TTL = 5;    // in seconds
static constexpr int64_t DEFAULT_CLIENT_LIVE_TTL_SEC = 10;  // in seconds
static const std::string DEFAULT_CLUSTER_ID = "mooncake_cluster";
static const std::string DEFAULT_ROOT_FS_DIR = "";

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

inline std::ostream& operator<<(std::ostream& os, const UUID& uuid) noexcept {
    os << uuid.first << "-" << uuid.second;
    return os;
}

UUID generate_uuid();

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
    SEGMENT_NOT_FOUND = -101,         ///< No available segments found.
    SEGMENT_ALREADY_EXISTS = -102,    ///< Segment already exists.

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

    // Engine operation errors (Range: -700 to -799)
    INVALID_WRITE = -700,          ///< Invalid write operation.
    INVALID_READ = -701,           ///< Invalid read operation.
    INVALID_REPLICA = -702,        ///< Invalid replica operation.
    REPLICA_IS_NOT_READY = -703,   ///< Replica is not ready.
    OBJECT_NOT_FOUND = -704,       ///< Object not found.
    OBJECT_ALREADY_EXISTS = -705,  ///< Object already exists.
    OBJECT_HAS_LEASE = -706,       ///< Object has lease.

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
    INIT = 0,      // Initial state
    COMPLETE = 1,  // Complete state (buffer has been used)
    FAILED = 2,  // Failed state (allocation failed, upstream should set handle
                 // to this state)
    UNREGISTERED = 3,  // Buffer metadata has been deleted
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
 * @brief Type of buffer allocator used in the system
 */
enum class ReplicaType {
    MEMORY,  // Memory replica
    DISK,    // Disk replica
};

/**
 * @brief Stream operator for ReplicaType
 */
inline std::ostream& operator<<(std::ostream& os,
                                const ReplicaType& replicaType) noexcept {
    static const std::unordered_map<ReplicaType, std::string_view>
        replica_type_strings{{ReplicaType::MEMORY, "MEMORY"},
                             {ReplicaType::DISK, "DISK"}};

    os << (replica_type_strings.count(replicaType)
               ? replica_type_strings.at(replicaType)
               : "UNKNOWN");
    return os;
}

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
    size_t replica_num{1};
    bool with_soft_pin{false};
    std::string preferred_segment{};  // Preferred segment for allocation

    friend std::ostream& operator<<(std::ostream& os,
                                    const ReplicateConfig& config) noexcept {
        return os << "ReplicateConfig: { replica_num: " << config.replica_num
                  << ", with_soft_pin: " << config.with_soft_pin
                  << ", preferred_segment: " << config.preferred_segment
                  << " }";
    }
};

class AllocatedBuffer {
   public:
    friend class CachelibBufferAllocator;
    friend class OffsetBufferAllocator;
    // Forward declaration of the descriptor struct
    struct Descriptor;

    AllocatedBuffer(std::shared_ptr<BufferAllocatorBase> allocator,
                    std::string segment_name, void* buffer_ptr,
                    std::size_t size,
                    std::optional<offset_allocator::OffsetAllocationHandle>&&
                        offset_handle = std::nullopt)
        : allocator_(std::move(allocator)),
          segment_name_(std::move(segment_name)),
          buffer_ptr_(buffer_ptr),
          size_(size),
          offset_handle_(std::move(offset_handle)) {}

    ~AllocatedBuffer();

    AllocatedBuffer(const AllocatedBuffer&) = delete;
    AllocatedBuffer& operator=(const AllocatedBuffer&) = delete;
    AllocatedBuffer(AllocatedBuffer&&) noexcept;
    AllocatedBuffer& operator=(AllocatedBuffer&&) noexcept;

    [[nodiscard]] void* data() const noexcept { return buffer_ptr_; }

    [[nodiscard]] std::size_t size() const noexcept { return this->size_; }

    [[nodiscard]] bool isAllocatorValid() const {
        return !allocator_.expired();
    }

    // Serialize the buffer into a descriptor for transfer
    [[nodiscard]] Descriptor get_descriptor() const;

    // Friend declaration for operator<<
    friend std::ostream& operator<<(std::ostream& os,
                                    const AllocatedBuffer& buffer);

    // Represents the serializable state
    struct Descriptor {
        std::string segment_name_;
        uint64_t size_;
        uintptr_t buffer_address_;
        BufStatus status_;
        YLT_REFL(Descriptor, segment_name_, size_, buffer_address_, status_);
    };

    void mark_complete() { status = BufStatus::COMPLETE; }

   private:
    std::weak_ptr<BufferAllocatorBase> allocator_;
    std::string segment_name_;
    BufStatus status{BufStatus::INIT};
    void* buffer_ptr_{nullptr};
    std::size_t size_{0};
    // RAII handle for buffer allocated by offset allocator
    std::optional<offset_allocator::OffsetAllocationHandle> offset_handle_{
        std::nullopt};
};

// Implementation of get_descriptor
inline AllocatedBuffer::Descriptor AllocatedBuffer::get_descriptor() const {
    return {segment_name_, static_cast<uint64_t>(size()),
            reinterpret_cast<uintptr_t>(buffer_ptr_), status};
}

// Define operator<< using public accessors or get_descriptor if appropriate
inline std::ostream& operator<<(std::ostream& os,
                                const AllocatedBuffer& buffer) {
    return os << "AllocatedBuffer: { "
              << "segment_name: " << buffer.segment_name_ << ", "
              << "size: " << buffer.size() << ", "
              << "status: " << buffer.status << ", "
              << "buffer_ptr: " << static_cast<void*>(buffer.data()) << " }";
}

struct MemoryReplicaData {
    std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
};

struct DiskReplicaData {
    std::string file_path;
    uint64_t object_size = 0;
};

struct MemoryDescriptor {
    std::vector<AllocatedBuffer::Descriptor> buffer_descriptors;
    YLT_REFL(MemoryDescriptor, buffer_descriptors);
};

struct DiskDescriptor {
    std::string file_path{};
    uint64_t object_size = 0;
    YLT_REFL(DiskDescriptor, file_path, object_size);
};

class Replica {
   public:
    struct Descriptor;

    // memory replica constructor
    Replica(std::vector<std::unique_ptr<AllocatedBuffer>> buffers,
            ReplicaStatus status)
        : data_(MemoryReplicaData{std::move(buffers)}), status_(status) {}

    // disk replica constructor
    Replica(std::string file_path, uint64_t object_size, ReplicaStatus status)
        : data_(DiskReplicaData{std::move(file_path), object_size}),
          status_(status) {}

    [[nodiscard]] Descriptor get_descriptor() const;

    [[nodiscard]] ReplicaStatus status() const { return status_; }

    [[nodiscard]] ReplicaType type() const {
        return std::visit(ReplicaTypeVisitor{}, data_);
    }

    [[nodiscard]] bool is_memory_replica() const {
        return std::holds_alternative<MemoryReplicaData>(data_);
    }

    [[nodiscard]] bool is_disk_replica() const {
        return std::holds_alternative<DiskReplicaData>(data_);
    }

    [[nodiscard]] bool has_invalid_mem_handle() const {
        if (is_memory_replica()) {
            const auto& mem_data = std::get<MemoryReplicaData>(data_);
            return std::any_of(
                mem_data.buffers.begin(), mem_data.buffers.end(),
                [](const std::unique_ptr<AllocatedBuffer>& buf_ptr) {
                    return !buf_ptr->isAllocatorValid();
                });
        }
        return false;  // DiskReplicaData does not have handles
    }

    void mark_complete() {
        if (status_ == ReplicaStatus::PROCESSING) {
            status_ = ReplicaStatus::COMPLETE;
            if (is_memory_replica()) {
                auto& mem_data = std::get<MemoryReplicaData>(data_);
                for (const auto& buf_ptr : mem_data.buffers) {
                    buf_ptr->mark_complete();
                }
            }
        } else if (status_ == ReplicaStatus::COMPLETE) {
            LOG(WARNING) << "Replica already marked as complete";
        } else {
            LOG(ERROR) << "Invalid replica status: " << status_;
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const Replica& replica);

    struct ReplicaTypeVisitor {
        ReplicaType operator()(const MemoryReplicaData&) const {
            return ReplicaType::MEMORY;
        }
        ReplicaType operator()(const DiskReplicaData&) const {
            return ReplicaType::DISK;
        }
    };

    struct Descriptor {
        std::variant<MemoryDescriptor, DiskDescriptor> descriptor_variant;
        ReplicaStatus status;
        YLT_REFL(Descriptor, descriptor_variant, status);

        // Helper functions
        bool is_memory_replica() noexcept {
            return std::holds_alternative<MemoryDescriptor>(descriptor_variant);
        }

        bool is_memory_replica() const noexcept {
            return std::holds_alternative<MemoryDescriptor>(descriptor_variant);
        }

        bool is_disk_replica() noexcept {
            return std::holds_alternative<DiskDescriptor>(descriptor_variant);
        }

        bool is_disk_replica() const noexcept {
            return std::holds_alternative<DiskDescriptor>(descriptor_variant);
        }

        MemoryDescriptor& get_memory_descriptor() {
            if (auto* desc =
                    std::get_if<MemoryDescriptor>(&descriptor_variant)) {
                return *desc;
            }
            throw std::runtime_error("Expected MemoryDescriptor");
        }

        DiskDescriptor& get_disk_descriptor() {
            if (auto* desc = std::get_if<DiskDescriptor>(&descriptor_variant)) {
                return *desc;
            }
            throw std::runtime_error("Expected DiskDescriptor");
        }

        const MemoryDescriptor& get_memory_descriptor() const {
            if (auto* desc =
                    std::get_if<MemoryDescriptor>(&descriptor_variant)) {
                return *desc;
            }
            throw std::runtime_error("Expected MemoryDescriptor");
        }

        const DiskDescriptor& get_disk_descriptor() const {
            if (auto* desc = std::get_if<DiskDescriptor>(&descriptor_variant)) {
                return *desc;
            }
            throw std::runtime_error("Expected DiskDescriptor");
        }
    };

   private:
    std::variant<MemoryReplicaData, DiskReplicaData> data_;
    ReplicaStatus status_{ReplicaStatus::UNDEFINED};
};

inline Replica::Descriptor Replica::get_descriptor() const {
    Replica::Descriptor desc;
    desc.status = status_;

    if (is_memory_replica()) {
        const auto& mem_data = std::get<MemoryReplicaData>(data_);
        MemoryDescriptor mem_desc;
        mem_desc.buffer_descriptors.reserve(mem_data.buffers.size());
        for (const auto& buf_ptr : mem_data.buffers) {
            if (buf_ptr) {
                mem_desc.buffer_descriptors.push_back(
                    buf_ptr->get_descriptor());
            }
        }
        desc.descriptor_variant = std::move(mem_desc);
    } else if (is_disk_replica()) {
        const auto& disk_data = std::get<DiskReplicaData>(data_);
        DiskDescriptor disk_desc;
        disk_desc.file_path = disk_data.file_path;
        disk_desc.object_size = disk_data.object_size;
        desc.descriptor_variant = std::move(disk_desc);
    }

    return desc;
}

inline std::ostream& operator<<(std::ostream& os, const Replica& replica) {
    os << "Replica: { status: " << replica.status_ << ", ";

    if (replica.is_memory_replica()) {
        const auto& mem_data = std::get<MemoryReplicaData>(replica.data_);
        os << "type: MEMORY, buffers: [";
        for (const auto& buf_ptr : mem_data.buffers) {
            if (buf_ptr) {
                os << *buf_ptr;
            }
        }
        os << "]";
    } else if (replica.is_disk_replica()) {
        const auto& disk_data = std::get<DiskReplicaData>(replica.data_);
        os << "type: DISK, file_path: " << disk_data.file_path
           << ", object_size: " << disk_data.object_size;
    }

    os << " }";
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

/**
 * @brief Represents a contiguous memory region
 */
struct Segment {
    UUID id{0, 0};
    std::string name{};  // The name of the segment, also might be the
                         // hostname of the server that owns the segment
    uintptr_t base{0};
    size_t size{0};
    Segment() = default;
    Segment(const UUID& id, const std::string& name, uintptr_t base,
            size_t size)
        : id(id), name(name), base(base), size(size) {}
};
YLT_REFL(Segment, id, name, base, size);

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

/**
 * @brief Response structure for Ping operation
 */
struct PingResponse {
    ViewVersionId view_version_id;
    ClientStatus client_status;

    PingResponse() = default;
    PingResponse(ViewVersionId view_version, ClientStatus status)
        : view_version_id(view_version), client_status(status) {}

    friend std::ostream& operator<<(std::ostream& os,
                                    const PingResponse& response) noexcept {
        return os << "PingResponse: { view_version_id: "
                  << response.view_version_id
                  << ", client_status: " << response.client_status << " }";
    }
};
YLT_REFL(PingResponse, view_version_id, client_status);

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

}  // namespace mooncake
