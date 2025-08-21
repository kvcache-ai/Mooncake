#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "types.h"
#include "allocator.h"

namespace mooncake {

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

}  // namespace mooncake