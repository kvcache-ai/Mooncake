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
    std::optional<std::string> device_names = std::nullopt;
    if (!device_name.empty()) {
        device_names = device_name;
    }

    auto client_opt = Client::Create(hostname, metadata_connstring, protocol,
                                     device_names, master_server_entry);

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
    const AllocatedBuffer::Descriptor& descriptor =
        replica_list[0].get_memory_descriptor().buffer_descriptor;
    SliceGuard slice_guard(descriptor.size_, allocator_);

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

ErrorCode ClientTestWrapper::BatchSmoke(const std::string& key_prefix) {
    constexpr size_t kCount = 3;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::unique_ptr<SliceGuard>> put_guards;
    std::vector<std::vector<Slice>> put_slices;
    for (size_t i = 0; i < kCount; ++i) {
        keys.push_back(key_prefix + "-" + std::to_string(i));
        values.push_back("batch-value-" + std::to_string(i));
        auto guard =
            std::make_unique<SliceGuard>(values.back().size(), allocator_);
        size_t offset = 0;
        for (const auto& slice : guard->slices_) {
            memcpy(slice.ptr, values.back().data() + offset, slice.size);
            offset += slice.size;
        }
        put_slices.push_back(guard->slices_);
        put_guards.push_back(std::move(guard));
    }

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_results = client_->BatchPut(keys, put_slices, config);
    if (put_results.size() != kCount) return ErrorCode::INTERNAL_ERROR;
    for (const auto& result : put_results) {
        if (!result) return result.error();
    }

    std::vector<std::string> mixed_keys = keys;
    mixed_keys.push_back(key_prefix + "-missing");
    auto exist_results = client_->BatchIsExist(mixed_keys);
    if (exist_results.size() != mixed_keys.size()) {
        return ErrorCode::INTERNAL_ERROR;
    }
    for (size_t i = 0; i < kCount; ++i) {
        if (!exist_results[i] || !*exist_results[i]) {
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    if (!exist_results.back() || *exist_results.back()) {
        return ErrorCode::INTERNAL_ERROR;
    }

    std::vector<std::unique_ptr<SliceGuard>> get_guards;
    std::unordered_map<std::string, std::vector<Slice>> get_slices;
    for (size_t i = 0; i < mixed_keys.size(); ++i) {
        const size_t size = i < kCount ? values[i].size() : 1;
        auto guard = std::make_unique<SliceGuard>(size, allocator_);
        get_slices.emplace(mixed_keys[i], guard->slices_);
        get_guards.push_back(std::move(guard));
    }
    auto get_results = client_->BatchGet(mixed_keys, get_slices);
    if (get_results.size() != mixed_keys.size()) {
        return ErrorCode::INTERNAL_ERROR;
    }
    for (size_t i = 0; i < kCount; ++i) {
        if (!get_results[i]) return get_results[i].error();
        std::string actual;
        for (const auto& slice : get_slices.at(keys[i])) {
            actual.append(static_cast<const char*>(slice.ptr), slice.size);
        }
        if (actual != values[i]) return ErrorCode::INTERNAL_ERROR;
    }
    if (get_results.back() ||
        get_results.back().error() != ErrorCode::OBJECT_NOT_FOUND) {
        return ErrorCode::INTERNAL_ERROR;
    }

    auto remove_results = client_->BatchRemove(mixed_keys, /*force=*/true);
    if (remove_results.size() != mixed_keys.size()) {
        return ErrorCode::INTERNAL_ERROR;
    }
    for (size_t i = 0; i < kCount; ++i) {
        if (!remove_results[i]) return remove_results[i].error();
    }
    if (remove_results.back() ||
        remove_results.back().error() != ErrorCode::OBJECT_NOT_FOUND) {
        return ErrorCode::INTERNAL_ERROR;
    }

    exist_results = client_->BatchIsExist(keys);
    if (exist_results.size() != kCount) return ErrorCode::INTERNAL_ERROR;
    for (const auto& result : exist_results) {
        if (!result || *result) return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

bool ClientTestWrapper::HasDiskReplica(const std::string& key) {
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) return false;
    for (const auto& replica : query_result.value().replicas) {
        if (replica.is_disk_replica()) return true;
    }
    return false;
}

bool ClientTestWrapper::HasLocalDiskReplica(const std::string& key) {
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) return false;
    for (const auto& replica : query_result.value().replicas) {
        if (replica.is_local_disk_replica()) return true;
    }
    return false;
}

bool ClientTestWrapper::HasMemoryReplica(const std::string& key) {
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) return false;
    for (const auto& replica : query_result.value().replicas) {
        if (replica.is_memory_replica()) return true;
    }
    return false;
}

ErrorCode ClientTestWrapper::GetWithExpectedSize(const std::string& key,
                                                 size_t expected_size,
                                                 std::string& value) {
    SliceGuard slice_guard(expected_size, allocator_);
    auto get_result = client_->Get(key, slice_guard.slices_);
    ErrorCode error_code =
        get_result.has_value() ? ErrorCode::OK : get_result.error();
    if (error_code != ErrorCode::OK) {
        return error_code;
    }
    value.clear();
    for (const auto& slice : slice_guard.slices_) {
        value.append(static_cast<const char*>(slice.ptr), slice.size);
    }
    return ErrorCode::OK;
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
