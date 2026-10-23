#include "real_client.h"

#include "p2p/client/p2p_client_config_builder.h"

namespace mooncake {

void RealClient::register_dummy_rpc_handlers() {
    auto &server = *dummy_rpc_server_;
    server.register_handler<&RealClient::put_dummy_helper>(this);
    server.register_handler<&RealClient::put_batch_dummy_helper>(this);
    server.register_handler<&RealClient::put_parts_dummy_helper>(this);
    server.register_handler<&RealClient::remove_internal>(this);
    server.register_handler<&RealClient::removeByRegex_internal>(this);
    server.register_handler<&RealClient::removeAll_internal>(this);
    server.register_handler<&RealClient::removeAllLocal_internal>(this);
    server.register_handler<&RealClient::removeLocal_internal>(this);
    server.register_handler<&RealClient::isExist_internal>(this);
    server.register_handler<&RealClient::batchIsExist_internal>(this);
    server.register_handler<&RealClient::getSize_internal>(this);
    server.register_handler<&RealClient::get_replica_desc>(this);
    server.register_handler<&RealClient::batch_get_replica_desc>(this);
    server.register_handler<&RealClient::get_buffer_info_dummy_helper>(
        this);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        this);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        this);
    server.register_handler<&RealClient::map_shm_internal>(this);
    server.register_handler<&RealClient::unmap_shm_internal>(this);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        this);
    server.register_handler<&RealClient::service_ready_internal>(this);
    server.register_handler<&RealClient::ping>(this);
}

tl::expected<std::shared_ptr<ClientServiceImpl>, ErrorCode>
RealClient::CreateService(const ClientConfig &config) {
    auto client = P2PClientService::Create(config);
    if (!client) {
        LOG(ERROR) << "Failed to create native client";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return std::move(*client);
}

tl::expected<void, ErrorCode> RealClient::initAll_internal(
    const std::string & /*protocol*/, const std::string & /*device_name*/,
    size_t /*mount_segment_size*/) {
    LOG(ERROR) << "initAll is not supported by the P2P build; use setup";
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

DeploymentMode RealClient::deployment_mode() const {
    return DeploymentMode::P2P;
}

std::string RealClient::get_hostname() const {
    return client_service_ ? client_service_->local_endpoint() : "";
}

tl::expected<void, ErrorCode> RealClient::ValidateWriteOperation(
    WriteOperation operation, const std::optional<WriteConfig>& /*config*/) {
    if (operation != WriteOperation::Put &&
        operation != WriteOperation::Publish) {
        LOG(ERROR) << "Unknown write operation: "
                   << static_cast<int>(operation);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::ValidateWriteEntryConfig(
    const std::optional<WriteConfig>& config) {
    (void)config;
    return {};
}

// Implementation of get_buffer_internal method
std::shared_ptr<BufferHandle> RealClient::get_buffer_internal(
    const std::string& key,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator,
    const std::optional<ReadConfig>& config) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return nullptr;
    }

    auto result = client_service_->Get(key, client_buffer_allocator, config);
    if (!result) {
        if (result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Get failed for key: " << key
                       << " with error: " << toString(result.error());
        }
        return nullptr;
    }
    return result.value();
}

// Implementation of batch_get_buffer_internal method
std::vector<std::shared_ptr<BufferHandle>>
RealClient::batch_get_buffer_internal(const std::vector<std::string>& keys,
                                      const std::optional<ReadConfig>& config) {
    std::vector<std::shared_ptr<BufferHandle>> final_results(keys.size(),
                                                             nullptr);

    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return final_results;
    }

    if (keys.empty()) {
        return final_results;
    }

    auto results = client_service_->BatchGet(
        keys, client_service_->GetBufferAllocator(), config);

    for (size_t i = 0; i < keys.size(); ++i) {
        if (results[i]) {
            final_results[i] = results[i].value();
        } else if (results[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "BatchGet failed for key '" << keys[i]
                       << "': " << toString(results[i].error());
        }
    }

    return final_results;
}

tl::expected<int64_t, ErrorCode> RealClient::get_into_internal(
    const std::string& key, void* buffer, size_t size,
    const std::optional<ReadConfig>& config) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = client_service_->Get(key, {buffer}, {size}, config);
    if (!result && result.error() != ErrorCode::OBJECT_NOT_FOUND) {
        LOG(ERROR) << "Get failed, key=" << key << ", error=" << result.error();
    }
    return result;
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_internal(const std::vector<std::string>& keys,
                                    const std::vector<void*>& buffers,
                                    const std::vector<size_t>& sizes,
                                    const std::optional<ReadConfig>& config) {
    // Validate preconditions
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << buffers.size()
                   << ", sizes=" << sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.empty()) {
        return {};
    }

    std::vector<std::vector<void*>> all_buffers(keys.size());
    std::vector<std::vector<size_t>> all_sizes(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        all_buffers[i] = {buffers[i]};
        all_sizes[i] = {sizes[i]};
    }
    auto results =
        client_service_->BatchGet(keys, all_buffers, all_sizes, config);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i] && results[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "BatchGet failed, key=" << keys[i]
                       << ", error=" << results[i].error();
        }
    }
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_multi_buffers_internal(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    bool prefer_alloc_in_same_node, const std::optional<ReadConfig>& config) {
    // Validate preconditions
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << all_buffers.size()
                   << ", sizes=" << all_sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.empty()) {
        return {};
    }

    auto results = client_service_->BatchGet(keys, all_buffers, all_sizes,
                                             config, prefer_alloc_in_same_node);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i] && results[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "BatchGet failed, key=" << keys[i]
                       << ", error=" << results[i].error();
        }
    }
    return results;
}

std::vector<ObjectDescriptor> RealClient::get_replica_desc(
    const std::string& key) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "get_replica_desc failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return {};
    }
    auto query_result = client_service_->Query(key);
    if (!query_result) {
        std::vector<ObjectDescriptor> replica_list = {};
        if (query_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Query failed for key: " << key
                       << " with error: " << toString(query_result.error());
        }
        return replica_list;
    }
    const std::vector<ObjectDescriptor>& replica_list = *query_result;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
    }
    return replica_list;
}

std::map<std::string, std::vector<ObjectDescriptor>>
RealClient::batch_get_replica_desc(const std::vector<std::string>& keys) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "batch_get_replica_desc failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return {};
    }
    auto query_results = client_service_->BatchQuery(keys);
    std::map<std::string, std::vector<ObjectDescriptor>> replica_map;
    if (query_results.size() != keys.size()) {
        LOG(ERROR) << "Batch query response size mismatch in "
                      "batch_get_allocated_buffer_desc: expected "
                   << keys.size() << ", got " << query_results.size() << ".";
        return replica_map;
    }

    for (size_t i = 0; i < query_results.size(); ++i) {
        if (query_results[i]) {
            replica_map[keys[i]] = *query_results[i];
        } else if (query_results[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "batch_get_replica failed for key: " << keys[i]
                       << " with error: " << toString(query_results[i].error());
        }
    }
    return replica_map;
}

tl::expected<int64_t, ErrorCode> RealClient::removeAllLocal_internal() {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "removeAllLocal_internal failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_service_->RemoveAllLocal();
    if (!result) {
        LOG(ERROR) << "RemoveAllLocal failed, error=" << result.error();
    }
    return result;
}

tl::expected<void, ErrorCode> RealClient::removeLocal_internal(
    const std::string& key) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "removeLocal_internal failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_service_->RemoveLocal(key);
    if (!result) {
        LOG(ERROR) << "removeLocal_internal failed, error=" << result.error();
    }
    return result;
}

tl::expected<int64_t, ErrorCode> RealClient::getSize_internal(
    const std::string& key) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "getSize_internal failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_service_->Query(key);
    if (!result) {
        if (result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Query failed, key=" << key
                       << ", error=" << result.error();
        }
        return tl::unexpected(result.error());
    }
    const auto& entries = *result;
    if (entries.empty()) {
        LOG(ERROR) << "Empty query result, key=" << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return static_cast<int64_t>(entries.front().object_size);
}

}  // namespace mooncake
