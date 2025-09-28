#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>
#include <unordered_map>

#include "types.h"
#include "allocator.h"

namespace mooncake {

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

    [[nodiscard]] std::vector<std::optional<std::string>> get_segment_names()
        const;

    void mark_complete() {
        if (status_ == ReplicaStatus::PROCESSING) {
            status_ = ReplicaStatus::COMPLETE;
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

inline std::vector<std::optional<std::string>> Replica::get_segment_names()
    const {
    if (is_memory_replica()) {
        const auto& mem_data = std::get<MemoryReplicaData>(data_);
        std::vector<std::optional<std::string>> segment_names(
            mem_data.buffers.size());
        for (size_t i = 0; i < mem_data.buffers.size(); ++i) {
            if (mem_data.buffers[i] &&
                mem_data.buffers[i]->isAllocatorValid()) {
                segment_names[i] = mem_data.buffers[i]->getSegmentName();
            } else {
                segment_names[i] = std::nullopt;
            }
        }
        return segment_names;
    }
    return std::vector<std::optional<std::string>>();
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

}  // namespace mooncake