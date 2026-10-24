#include "client_wrapper.h"

#include <cstdlib>
#include <cstring>
#include <utility>

#include "p2p/client/p2p_client_config_builder.h"
#include "p2p/client/p2p_client_service.h"

namespace mooncake {
namespace testing {

ClientTestWrapper::ClientTestWrapper(std::shared_ptr<P2PClientService> client,
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
    std::shared_ptr<P2PClientService> client;
    static constexpr const char* kP2PTierConfig =
        R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})";
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        hostname, metadata_connstring, protocol, device_name,
        master_server_entry, kP2PTierConfig, local_buffer_size, nullptr, "",
        p2p_client_rpc_port, /*rpc_thread_num=*/2,
        /*lock_shard_count=*/1024,
        /*route_cache_max_memory_bytes=*/300 * 1024 * 1024,
        /*route_cache_ttl_ms=*/60 * 1000, p2p_local_transfer_mode,
        /*local_memcpy_async_worker_num=*/32, /*http_port=*/9003,
        enable_http_server);
    if (!redis_cluster_id.empty()) {
        config.redis_cluster_id = redis_cluster_id;
    }
    config.redis_username = redis_username;
    config.redis_password = redis_password;
    client = std::make_shared<P2PClientService>(
        config.metadata_connstring, config.http_port, config.enable_http_server,
        config.labels);
    auto err = client->Init(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "failed to init P2P client, error=" << err;
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
    LOG(ERROR) << "P2P segments are managed by the configured storage tiers";
    return ErrorCode::NOT_IMPLEMENTED;
}

ErrorCode ClientTestWrapper::Unmount(const void* buffer) {
    LOG(ERROR) << "P2P segments are managed by the configured storage tiers";
    return ErrorCode::NOT_IMPLEMENTED;
}

ErrorCode ClientTestWrapper::Get(const std::string& key, std::string& value) {
    uint64_t total_size;
    auto query_result = client_->Query(key);
    if (!query_result.has_value()) {
        return query_result.error();
    }
    if (query_result->empty()) {
        return ErrorCode::INVALID_REPLICA;
    }
    total_size = query_result->front().object_size;
    if (total_size == 0) {
        value.clear();
        return ErrorCode::OK;
    }

    SliceGuard buffer(total_size, allocator_);
    std::vector<void*> pointers;
    std::vector<size_t> sizes;
    for (const auto& slice : buffer.slices_) {
        pointers.push_back(slice.ptr);
        sizes.push_back(slice.size);
    }

    auto get_result = client_->Get(key, pointers, sizes);
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
    auto put_result =
        client_->Put(key, slice_guard.slices_, P2PWriteRouteConfig{});
    return put_result.has_value() ? ErrorCode::OK : put_result.error();
}

ErrorCode ClientTestWrapper::Delete(const std::string& key) {
    auto remove_result = client_->Remove(key);
    return remove_result.has_value() ? ErrorCode::OK : remove_result.error();
}

}  // namespace testing
}  // namespace mooncake
