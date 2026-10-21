#include "replica.h"

namespace mooncake {

Replica::Descriptor Replica::get_descriptor() const {
    Replica::Descriptor desc;
    desc.id = id_;
    desc.status = status_;

    if (is_memory_replica()) {
        const auto& mem_data = std::get<MemoryReplicaData>(data_);
        MemoryDescriptor mem_desc;
        if (mem_data.buffer) {
            mem_desc.buffer_descriptor = mem_data.buffer->get_descriptor();
        } else {
            mem_desc.buffer_descriptor.size_ = 0;
            mem_desc.buffer_descriptor.buffer_address_ = 0;
            mem_desc.buffer_descriptor.transport_endpoint_ = "";
            LOG(ERROR) << "Trying to get invalid memory replica descriptor";
        }
        desc.descriptor_variant = std::move(mem_desc);
    } else if (is_disk_replica()) {
        const auto& disk_data = std::get<DiskReplicaData>(data_);
        DiskDescriptor disk_desc;
        disk_desc.file_path = disk_data.file_path;
        disk_desc.object_size = disk_data.object_size;
        desc.descriptor_variant = std::move(disk_desc);
    } else if (is_local_disk_replica()) {
        const auto& disk_data = std::get<LocalDiskReplicaData>(data_);
        LocalDiskDescriptor local_disk_desc;
        local_disk_desc.client_id = disk_data.client_id;
        local_disk_desc.object_size = disk_data.object_size;
        local_disk_desc.transport_endpoint = disk_data.transport_endpoint;
        desc.descriptor_variant = std::move(local_disk_desc);
    }

    return desc;
}

std::ostream& operator<<(std::ostream& os, const Replica::Descriptor& desc) {
    os << "Descriptor: { id: " << desc.id << ", status: " << desc.status
       << ", ";
    std::visit(
        [&os](const auto& d) {
            using T = std::decay_t<decltype(d)>;
            if constexpr (std::is_same_v<T, MemoryDescriptor>) {
                os << "type: MEMORY, addr: " << std::hex << "0x"
                   << d.buffer_descriptor.buffer_address_ << std::dec
                   << ", size: " << d.buffer_descriptor.size_
                   << ", endpoint: " << d.buffer_descriptor.transport_endpoint_;
            } else if constexpr (std::is_same_v<T, DiskDescriptor>) {
                os << "type: DISK, path: " << d.file_path
                   << ", size: " << d.object_size;
            } else if constexpr (std::is_same_v<T, LocalDiskDescriptor>) {
                os << "type: LOCAL_DISK, client: " << d.client_id
                   << ", size: " << d.object_size
                   << ", endpoint: " << d.transport_endpoint;
            } else if constexpr (std::is_same_v<T, P2PProxyDescriptor>) {
                os << "type: P2P_PROXY, client: " << d.client_id
                   << ", segment: " << d.segment_id
                   << ", endpoint: " << d.ip_address << ":" << d.rpc_port
                   << ", size: " << d.object_size;
            }
        },
        desc.descriptor_variant);
    os << " }";
    return os;
}

std::ostream& operator<<(std::ostream& os, const Replica& replica) {
    os << "Replica: { id: " << replica.id_ << ", status: " << replica.status_
       << ", ";

    if (replica.is_memory_replica()) {
        const auto& mem_data = std::get<MemoryReplicaData>(replica.data_);
        os << "type: MEMORY, buffers: [";
        if (mem_data.buffer) {
            os << *mem_data.buffer;
        }
        os << "]";
    } else if (replica.is_disk_replica()) {
        const auto& disk_data = std::get<DiskReplicaData>(replica.data_);
        os << "type: DISK, file_path: " << disk_data.file_path
           << ", object_size: " << disk_data.object_size;
    } else if (replica.is_local_disk_replica()) {
        const auto& disk_data = std::get<LocalDiskReplicaData>(replica.data_);
        os << "type: LOCAL_DISK, client_id: " << disk_data.client_id
           << ", object_size: " << disk_data.object_size;
    }

    os << ", refcnt: " << replica.refcnt_.load() << " }";
    return os;
}

}  // namespace mooncake
