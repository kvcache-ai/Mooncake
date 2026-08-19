#include "replica.h"

#include "p2p/client_meta.h"
#include "p2p/p2p_client_meta.h"

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
    } else if (is_nof_replica()) {
        const auto& nof_data = std::get<NoFReplicaData>(data_);
        NoFDescriptor nof_desc;
        if (nof_data.buffer) {
            nof_desc.buffer_descriptor = nof_data.buffer->get_descriptor();
        } else {
            nof_desc.buffer_descriptor.size_ = 0;
            nof_desc.buffer_descriptor.buffer_address_ = 0;
            nof_desc.buffer_descriptor.transport_endpoint_ = "";
            LOG(ERROR) << "Trying to get invalid nof replica descriptor";
        }
        desc.descriptor_variant = std::move(nof_desc);
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
    } else if (is_dfs_replica()) {
        desc.descriptor_variant = std::get<DfsReplicaData>(data_).descriptor;
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
        proxy_desc.object_size = proxy_data.object_size;
        desc.descriptor_variant = std::move(proxy_desc);
    }

    return desc;
}

std::vector<std::optional<std::string>> Replica::get_segment_names() const {
    if (is_memory_replica()) {
        const auto& mem_data = std::get<MemoryReplicaData>(data_);
        std::vector<std::optional<std::string>> segment_names;
        if (mem_data.buffer && mem_data.buffer->isAllocatorValid()) {
            segment_names.push_back(mem_data.buffer->getSegmentName());
        } else {
            segment_names.push_back(std::nullopt);
        }
        return segment_names;
    } else if (is_nof_replica()) {
        const auto& nof_data = std::get<NoFReplicaData>(data_);
        std::vector<std::optional<std::string>> segment_names;
        if (nof_data.buffer && nof_data.buffer->isAllocatorValid()) {
            segment_names.push_back(nof_data.buffer->getSegmentName());
        } else {
            segment_names.push_back(std::nullopt);
        }
        return segment_names;
    }
    return std::vector<std::optional<std::string>>();
}

std::optional<UUID> Replica::get_segment_id() const {
    if (is_memory_replica()) {
        const auto& mem_data = std::get<MemoryReplicaData>(data_);
        if (mem_data.buffer) {
            return mem_data.buffer->getSegmentId();
        }
    } else if (is_p2p_proxy_replica()) {
        const auto& proxy_data = std::get<P2PProxyReplicaData>(data_);
        if (proxy_data.segment) {
            return proxy_data.segment->id;
        }
    }
    return std::nullopt;
}

std::optional<UUID> Replica::get_p2p_client_id() const {
    auto client = get_p2p_client();
    if (client) {
        return client->get_client_id();
    }
    return std::nullopt;
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
            } else if constexpr (std::is_same_v<T, NoFDescriptor>) {
                os << "type: NOF_SSD, addr: " << std::hex << "0x"
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
            } else if constexpr (std::is_same_v<T, DistributedFSDescriptor>) {
                os << "type: DFS, path: " << d.file_path
                   << ", offset: " << d.offset
                   << ", size: " << d.object_size
                   << ", aligned_size: " << d.aligned_size
                   << ", shard_idx: " << d.shard_idx;
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
    } else if (replica.is_nof_replica()) {
        const auto& nof_data = std::get<NoFReplicaData>(replica.data_);
        os << "type: NOF_SSD, buffers: [";
        if (nof_data.buffer) {
            os << *nof_data.buffer;
        }
        os << "]";
    } else if (replica.is_disk_replica()) {
        const auto& disk_data = std::get<DiskReplicaData>(replica.data_);
        os << "type: DISK, file_path: " << disk_data.file_path
           << ", object_size: " << disk_data.object_size;
    } else if (replica.is_local_disk_replica()) {
        const auto& disk_data = std::get<LocalDiskReplicaData>(replica.data_);
        os << "type: LOCAL_DISK, client_id: " << disk_data.client_id
           << ", object_size: " << disk_data.object_size
           << ", transport_endpoint: " << disk_data.transport_endpoint;
    } else if (replica.is_dfs_replica()) {
        const auto& dfs_data = std::get<DfsReplicaData>(replica.data_);
        os << "type: DFS, file_path: " << dfs_data.descriptor.file_path
           << ", offset: " << dfs_data.descriptor.offset
           << ", object_size: " << dfs_data.descriptor.object_size
           << ", aligned_size: " << dfs_data.descriptor.aligned_size
           << ", shard_idx: " << dfs_data.descriptor.shard_idx;
    } else if (replica.is_p2p_proxy_replica()) {
        const auto& proxy_data =
            std::get<P2PProxyReplicaData>(replica.data_);
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
        os << ", object_size: " << proxy_data.object_size;
    }

    os << ", refcnt: " << replica.refcnt_.load() << " }";
    return os;
}

}  // namespace mooncake