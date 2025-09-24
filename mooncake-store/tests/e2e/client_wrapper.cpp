#include "client_wrapper.h"

#include <cstring>
#include <stdexcept>

#include "utils.h"

namespace mooncake {
namespace testing {

ClientTestWrapper::ClientTestWrapper(std::shared_ptr<Client> client,
                                     std::shared_ptr<SimpleAllocator> allocator)
    : client_(client), allocator_(allocator) {}

ClientTestWrapper::~ClientTestWrapper() {
    for (auto& [base, segment] : segments_) {
        free(segment.base);
    }
}

std::optional<std::shared_ptr<ClientTestWrapper>>
ClientTestWrapper::CreateClientWrapper(const std::string& hostname,
                                       const std::string& metadata_connstring,
                                       const std::string& protocol,
                                       const std::string& device_name,
                                       const std::string& master_server_entry,
                                       size_t local_buffer_size) {
    auto client_opt =
        Client::Create(hostname,  // Local hostname
                       metadata_connstring, protocol, master_server_entry);

    if (!client_opt.has_value()) {
        return std::nullopt;
    }

    std::shared_ptr<SimpleAllocator> allocator =
        std::make_shared<SimpleAllocator>(local_buffer_size);
    if (!allocator) {
        LOG(ERROR) << "Failed to create allocator";
        return std::nullopt;
    }

    auto register_result = client_opt.value()->RegisterLocalMemory(
        allocator->getBase(), local_buffer_size, "cpu:0", false, false);
    ErrorCode error_code =
        register_result.has_value() ? ErrorCode::OK : register_result.error();
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "register_local_memory_failed base="
                   << allocator->getBase() << " size=" << local_buffer_size
                   << ", error=" << error_code;
        return std::nullopt;
    }
    return std::make_shared<ClientTestWrapper>(client_opt.value(), allocator);
}

ErrorCode ClientTestWrapper::Mount(const size_t size, void*& buffer) {
    buffer = allocate_buffer_allocator_memory(size);
    if (!buffer) {
        LOG(ERROR) << " Failed to allocate memory for segment";
        return ErrorCode::INTERNAL_ERROR;
    }

    auto mount_result = client_->MountSegment(buffer, size);
    ErrorCode error_code =
        mount_result.has_value() ? ErrorCode::OK : mount_result.error();
    if (error_code != ErrorCode::OK) {
        free(buffer);
        return error_code;
    } else {
        segments_.emplace(reinterpret_cast<uintptr_t>(buffer),
                          SegmentInfo{buffer, size});
        return ErrorCode::OK;
    }
}

ErrorCode ClientTestWrapper::Unmount(const void* buffer) {
    auto it = segments_.find(reinterpret_cast<uintptr_t>(buffer));
    if (it == segments_.end()) {
        return ErrorCode::INVALID_PARAMS;
    }
    SegmentInfo& segment = it->second;
    auto unmount_result = client_->UnmountSegment(segment.base, segment.size);
    ErrorCode error_code =
        unmount_result.has_value() ? ErrorCode::OK : unmount_result.error();
    if (error_code != ErrorCode::OK) {
        return error_code;
    } else {
        // Clear the memory, so any further read will get wrong data
        memset(segment.base, 0, segment.size);
        free(segment.base);
        segments_.erase(it);
        return ErrorCode::OK;
    }
}

ErrorCode ClientTestWrapper::Get(const std::string& key, std::string& value) {
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) {
        return query_result.error();
    }

    const std::vector<Replica::Descriptor>& replica_list =
        query_result.value().replicas;
    if (replica_list.empty()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    // Create slices
    const std::vector<AllocatedBuffer::Descriptor>& descriptors =
        replica_list[0].get_memory_descriptor().buffer_descriptors;
    SliceGuard slice_guard(descriptors, allocator_);

    // Perform get operation
    auto get_result =
        client_->Get(key, query_result.value(), slice_guard.slices_);
    ErrorCode error_code =
        get_result.has_value() ? ErrorCode::OK : get_result.error();
    if (error_code != ErrorCode::OK) {
        return error_code;
    }

    // Fill value
    value.clear();
    for (const auto& slice : slice_guard.slices_) {
        value.append(static_cast<const char*>(slice.ptr), slice.size);
    }
    return ErrorCode::OK;
}

ErrorCode ClientTestWrapper::Put(const std::string& key,
                                 const std::string& value) {
    // Create slices
    SliceGuard slice_guard(value.size(), allocator_);
    size_t offset = 0;
    for (const auto& slice : slice_guard.slices_) {
        memcpy(slice.ptr, value.data() + offset, slice.size);
        offset += slice.size;
    }

    // Configure replication
    ReplicateConfig config;
    config.replica_num = 1;

    // Perform put operation
    auto put_result = client_->Put(key, slice_guard.slices_, config);
    return put_result.has_value() ? ErrorCode::OK : put_result.error();
}

ErrorCode ClientTestWrapper::Delete(const std::string& key) {
    auto remove_result = client_->Remove(key);
    return remove_result.has_value() ? ErrorCode::OK : remove_result.error();
}

ClientTestWrapper::SliceGuard::SliceGuard(
    const std::vector<AllocatedBuffer::Descriptor>& descriptors,
    std::shared_ptr<SimpleAllocator> allocator)
    : allocator_(allocator) {
    slices_.resize(descriptors.size());
    for (size_t i = 0; i < descriptors.size(); i++) {
        void* buffer = allocator_->allocate(descriptors[i].size_);
        if (!buffer) {
            LOG(ERROR) << "Failed to allocate memory for slice";
            throw std::runtime_error("Failed to allocate memory for slice");
        }
        slices_[i] = Slice{buffer, descriptors[i].size_};
    }
}

ClientTestWrapper::SliceGuard::SliceGuard(
    size_t size, std::shared_ptr<SimpleAllocator> allocator)
    : allocator_(allocator) {
    while (size != 0) {
        auto chunk_size = std::min(size, kMaxSliceSize);
        auto ptr = allocator_->allocate(chunk_size);
        if (!ptr) {
            LOG(ERROR) << "Failed to allocate memory for slice";
            throw std::runtime_error("Failed to allocate memory for slice");
        }
        slices_.emplace_back(Slice{ptr, chunk_size});
        size -= chunk_size;
    }
}

ClientTestWrapper::SliceGuard::~SliceGuard() {
    for (auto& slice : slices_) {
        allocator_->deallocate(slice.ptr, slice.size);
    }
}

}  // namespace testing
}  // namespace mooncake