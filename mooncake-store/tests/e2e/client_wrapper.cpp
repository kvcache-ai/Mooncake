#include "client_wrapper.h"

#include <cstdlib>
#include <cstring>
#include <utility>

#include "client_service.h"
#include "replica.h"
#include "utils.h"

namespace mooncake {
namespace testing {

ClientTestWrapper::ClientTestWrapper(std::shared_ptr<Client> client,
                                     std::shared_ptr<SimpleAllocator> allocator)
    : client_(std::move(client)), allocator_(std::move(allocator)) {}

ClientTestWrapper::~ClientTestWrapper() {
    // Finish native teardown before releasing externally registered memory.
    client_.reset();
    for (auto& [base, segment] : segments_) {
        free(segment.base);
    }
}

std::optional<std::shared_ptr<ClientTestWrapper>>
ClientTestWrapper::CreateClientWrapper(
    const std::string& hostname, const std::string& metadata_connstring,
    const std::string& protocol, const std::string& device_name,
    const std::string& master_server_entry, size_t local_buffer_size,
    const std::string& redis_cluster_id, bool enable_http_server,
    const std::string& p2p_local_transfer_mode, uint16_t p2p_client_rpc_port,
    const std::string& redis_username, const std::string& redis_password) {
    std::shared_ptr<Client> client;
    if (!redis_cluster_id.empty() || !redis_username.empty() ||
        !redis_password.empty() ||
        master_server_entry.rfind("redis://", 0) == 0) {
        LOG(ERROR) << "Centralized clients do not support Redis discovery";
        return std::nullopt;
    }
    if (enable_http_server) {
        LOG(WARNING) << "Centralized clients use native metric environment "
                        "controls, not the P2P HTTP server option";
    }
    auto client_opt = Client::Create(hostname, metadata_connstring, protocol,
                                     device_name, master_server_entry);
    if (!client_opt.has_value()) {
        LOG(ERROR) << "Failed to create centralized client";
        return std::nullopt;
    }
    client = client_opt.value();
    auto err = client->InitStorage(0, local_buffer_size, false);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize centralized client storage: "
                   << err;
        return std::nullopt;
    }

    std::shared_ptr<SimpleAllocator> allocator =
        std::make_shared<SimpleAllocator>(local_buffer_size);
    if (!allocator) {
        LOG(ERROR) << "Failed to create allocator";
        return std::nullopt;
    }

    auto register_result = client->RegisterLocalMemory(
        allocator->getBase(), local_buffer_size, "cpu:0", false, false);
    ErrorCode error_code =
        register_result.has_value() ? ErrorCode::OK : register_result.error();
    if (error_code != ErrorCode::OK) {
        LOG(ERROR) << "register_local_memory_failed base="
                   << allocator->getBase() << " size=" << local_buffer_size
                   << ", error=" << error_code;
        return std::nullopt;
    }
    return std::make_shared<ClientTestWrapper>(client, allocator);
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
    uint64_t total_size;
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) {
        return query_result.error();
    }
    if (query_result->replicas.empty()) {
        return ErrorCode::INVALID_REPLICA;
    }
    total_size = calculate_total_size(query_result->replicas[0]);
    if (total_size == 0) {
        value.clear();
        return ErrorCode::OK;
    }

    SliceGuard buffer(total_size, allocator_);
    auto get_result = client_->Get(key, *query_result, buffer.slices_);
    if (!get_result.has_value()) {
        return get_result.error();
    }

    value.clear();
    value.reserve(total_size);
    for (const auto& slice : buffer.slices_) {
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

    // Perform put operation
    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;
    auto put_result = client_->Put(key, slice_guard.slices_, replicate_config);
    return put_result.has_value() ? ErrorCode::OK : put_result.error();
}

ErrorCode ClientTestWrapper::Delete(const std::string& key) {
    auto remove_result = client_->Remove(key);
    return remove_result.has_value() ? ErrorCode::OK : remove_result.error();
}

}  // namespace testing
}  // namespace mooncake
