#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "Slab.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace mooncake {

// Constants
static constexpr uint64_t WRONG_VERSION = 0;
static constexpr uint64_t DEFAULT_VALUE = UINT64_MAX;
static constexpr uint64_t ERRNO_BASE = DEFAULT_VALUE - 1000;

// Forward declarations
class BufferAllocator;
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

class AllocatedBuffer {
   public:
    friend class BufferAllocator;
    // Forward declaration of the descriptor struct
    struct Descriptor;

    AllocatedBuffer(std::shared_ptr<BufferAllocator> allocator,
                    std::string segment_name, void* buffer_ptr,
                    std::size_t size)
        : allocator_(std::move(allocator)),
          segment_name_(std::move(segment_name)),
          buffer_ptr_(buffer_ptr),
          size_(size) {}

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
    std::weak_ptr<BufferAllocator> allocator_;
    std::string segment_name_;
    BufStatus status{BufStatus::INIT};
    void* buffer_ptr_{nullptr};
    std::size_t size_{0};
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

class Replica {
   public:
    struct Descriptor;

    Replica() = default;
    Replica(std::vector<std::unique_ptr<AllocatedBuffer>> buffers,
            ReplicaStatus status)
        : buffers_(std::move(buffers)), status_(status) {}

    void reset() noexcept {
        buffers_.clear();
        status_ = ReplicaStatus::UNDEFINED;
    }

    [[nodiscard]] Descriptor get_descriptor() const;

    [[nodiscard]] ReplicaStatus status() const { return status_; }

    [[nodiscard]] bool has_invalid_handle() const {
        return std::any_of(buffers_.begin(), buffers_.end(),
                           [](const std::unique_ptr<AllocatedBuffer>& buf_ptr) {
                               return !buf_ptr->isAllocatorValid();
                           });
    }

    void mark_complete() {
        // prev status should be PROCESSING
        CHECK_EQ(status_, ReplicaStatus::PROCESSING);
        status_ = ReplicaStatus::COMPLETE;
        for (const auto& buf_ptr : buffers_) {
            buf_ptr->mark_complete();
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const Replica& replica);

    struct Descriptor {
        std::vector<AllocatedBuffer::Descriptor> buffer_descriptors;
        ReplicaStatus status;
        YLT_REFL(Descriptor, buffer_descriptors, status);
    };

   private:
    std::vector<std::unique_ptr<AllocatedBuffer>> buffers_;
    ReplicaStatus status_{ReplicaStatus::UNDEFINED};
};

inline Replica::Descriptor Replica::get_descriptor() const {
    Replica::Descriptor desc;
    desc.status = status_;
    desc.buffer_descriptors.reserve(buffers_.size());
    for (const auto& buf_ptr : buffers_) {
        if (buf_ptr) {
            desc.buffer_descriptors.push_back(buf_ptr->get_descriptor());
        }
    }
    return desc;
}

inline std::ostream& operator<<(std::ostream& os, const Replica& replica) {
    os << "Replica: { " << "status: " << replica.status_ << ", "
       << "buffers: [";
    for (const auto& buf_ptr : replica.buffers_) {
        if (buf_ptr) {
            os << *buf_ptr;
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
