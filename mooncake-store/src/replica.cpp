#include "replica.h"

#include "p2p_client_meta.h"

namespace mooncake {

std::optional<UUID> Replica::get_p2p_client_id() const {
    auto client = get_p2p_client();
    if (client) {
        return client->get_client_id();
    }
    return std::nullopt;
}

Replica::Descriptor Replica::get_descriptor() const {
    Replica::Descriptor desc;
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
    } else if (is_p2p_proxy_replica()) {
        const auto& proxy_data = std::get<P2PProxyReplicaData>(data_);
        P2PProxyDescriptor proxy_desc;
        if (!proxy_data.client) {
            LOG(ERROR) << "Trying to get invalid p2p replica descriptor";
        } else {
            proxy_desc.client_id = proxy_data.client->get_client_id();
            proxy_desc.ip_address = proxy_data.client->get_ip_address();
            proxy_desc.rpc_port = proxy_data.client->get_rpc_port();
        }
        if (!proxy_data.segment) {
            LOG(ERROR) << "Trying to get invalid p2p replica descriptor";
        } else {
            proxy_desc.segment_id = proxy_data.segment->id;
        }
        desc.descriptor_variant = std::move(proxy_desc);
    }

    return desc;
}

std::ostream& operator<<(std::ostream& os, const Replica& replica) {
    os << "Replica: { status: " << replica.status_ << ", ";

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
    } else if (replica.is_p2p_proxy_replica()) {
        const auto& proxy_data = std::get<P2PProxyReplicaData>(replica.data_);
        os << "type: P2P_PROXY";
        if (proxy_data.client) {
            os << ", client_id: " << proxy_data.client->get_client_id()
               << ", ip: " << proxy_data.client->get_ip_address() << ":"
               << proxy_data.client->get_rpc_port();
        }
        if (proxy_data.segment) {
            os << ", segment_id: " << proxy_data.segment->id;
            if (proxy_data.segment->IsP2PSegment()) {
                os << ", memory_type: "
                   << MemoryTypeToString(
                          proxy_data.segment->GetP2PExtra().memory_type);
            }
        }
    }

    os << " }";
    return os;
}

}  // namespace mooncake
