#include "real_client.h"

#include "centralized_client_config_builder.h"

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
    server.register_handler<&RealClient::create_copy_task>(this);
    server.register_handler<&RealClient::create_move_task>(this);
    server.register_handler<&RealClient::query_task>(this);
}

tl::expected<std::shared_ptr<ClientServiceImpl>, ErrorCode>
RealClient::CreateService(const ClientConfig &config) {
    if (config.master_server_entry.rfind("redis://", 0) == 0) {
        LOG(ERROR) << "Centralized setup does not support Redis discovery";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto client = Client::Create(
        config.local_hostname, config.metadata_connstring, config.protocol,
        config.rdma_devices, config.master_server_entry, config.transfer_engine,
        config.labels);
    if (!client) {
        LOG(ERROR) << "Failed to create native client";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto error = (*client)->InitStorage(
        config.global_segment_size, config.local_buffer_size,
        config.enable_offload, config.client_rpc_port);
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize centralized storage: " << error;
        return tl::unexpected(error);
    }
    return std::move(*client);
}

tl::expected<void, ErrorCode> RealClient::initAll_internal(
    const std::string &protocol, const std::string &device_name,
    size_t mount_segment_size) {
    if (client_service_) {
        LOG(ERROR) << "Client is already initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t buffer_allocator_size = 1024 * 1024 * 1024;
    auto config = CentralizedClientConfigBuilder::build_centralized_real_client(
        "localhost:12345", "127.0.0.1:2379", protocol,
        device_name.empty() ? std::nullopt
                            : std::optional<std::string>(device_name),
        "127.0.0.1:50051", mount_segment_size, buffer_allocator_size);
    return setup_internal(config);
}

DeploymentMode RealClient::deployment_mode() const {
    return DeploymentMode::CENTRALIZATION;
}

std::string RealClient::get_hostname() const {
    return client_service_ ? client_service_->GetLocalHostname() : "";
}

tl::expected<void, ErrorCode> RealClient::ValidateWriteOperation(
    WriteOperation operation, const std::optional<WriteConfig>& config) {
    if (operation == WriteOperation::Put) {
        return {};
    }
    if (operation != WriteOperation::Publish) {
        LOG(ERROR) << "Unknown write operation: "
                   << static_cast<int>(operation);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (config && !config->preferred_segments.empty() &&
        config->preferred_segments.size() != config->replica_num) {
        LOG(ERROR) << "Preferred segments size ("
                   << config->preferred_segments.size()
                   << ") must match replica_num (" << config->replica_num
                   << ")";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::ValidateWriteEntryConfig(
    const std::optional<WriteConfig>& config) {
    if (config && config->prefer_alloc_in_same_node) {
        LOG(ERROR) << "This write entry does not support same-node allocation";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

// Both an absent and an explicit placeholder config use the existing native
// read API. CentralizedReadConfig does not add routing or filtering semantics.
// Implementation of get_buffer_internal method
std::shared_ptr<BufferHandle> RealClient::get_buffer_internal(
    const std::string& key,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator,
    const std::optional<ReadConfig>& /*config*/) {
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return nullptr;
    }

    auto result = client_service_->Get(key, client_buffer_allocator);
    if (!result) {
        if (result.error() != ErrorCode::OBJECT_NOT_FOUND &&
            result.error() != ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "Get failed for key: " << key
                       << " with error: " << toString(result.error());
        }
        return nullptr;
    }
    return result.value();
}

// Implementation of batch_get_buffer_internal method
std::vector<std::shared_ptr<BufferHandle>>
RealClient::batch_get_buffer_internal(
    const std::vector<std::string>& keys,
    const std::optional<ReadConfig>& /*config*/) {
    std::vector<std::shared_ptr<BufferHandle>> final_results(keys.size(),
                                                             nullptr);

    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return final_results;
    }

    if (keys.empty()) {
        return final_results;
    }

    auto results =
        client_service_->BatchGet(keys, client_service_->GetBufferAllocator());

    for (size_t i = 0; i < keys.size(); ++i) {
        if (results[i]) {
            final_results[i] = results[i].value();
        } else if (results[i].error() != ErrorCode::OBJECT_NOT_FOUND &&
                   results[i].error() != ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "BatchGet failed for key '" << keys[i]
                       << "': " << toString(results[i].error());
        }
    }

    return final_results;
}

tl::expected<int64_t, ErrorCode> RealClient::get_into_internal(
    const std::string& key, void* buffer, size_t size,
    const std::optional<ReadConfig>& /*config*/) {
    // NOTE: The buffer address must be previously registered with
    // register_buffer() for zero-copy RDMA operations to work correctly
    if (!client_service_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = client_service_->Get(key, buffer, size);
    if (!result && result.error() != ErrorCode::OBJECT_NOT_FOUND &&
        result.error() != ErrorCode::REPLICA_IS_NOT_READY) {
        LOG(ERROR) << "Get failed, key=" << key << ", error=" << result.error();
    }
    return result;
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_internal(
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes,
    const std::optional<ReadConfig>& /*config*/) {
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

    auto results = client_service_->BatchGet(keys, buffers, sizes);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i] && results[i].error() != ErrorCode::OBJECT_NOT_FOUND &&
            results[i].error() != ErrorCode::REPLICA_IS_NOT_READY) {
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
    bool prefer_alloc_in_same_node,
    const std::optional<ReadConfig>& /*config*/) {
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
                                             prefer_alloc_in_same_node);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i] && results[i].error() != ErrorCode::OBJECT_NOT_FOUND &&
            results[i].error() != ErrorCode::REPLICA_IS_NOT_READY) {
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
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "Object not found for key: " << key;
        } else {
            LOG(ERROR) << "Query failed for key: " << key
                       << " with error: " << toString(query_result.error());
        }
        return replica_list;
    }
    const std::vector<ObjectDescriptor>& replica_list = query_result->replicas;
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
            replica_map[keys[i]] = query_results[i]->replicas;
        } else {
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
    LOG(ERROR) << "removeAllLocal_internal is not supported by this build";
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<void, ErrorCode> RealClient::removeLocal_internal(
    const std::string& key) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "removeLocal_internal failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    (void)key;
    LOG(ERROR) << "removeLocal_internal is not supported by this build";
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<UUID, ErrorCode> RealClient::create_copy_task(
    const std::string& key, const std::vector<std::string>& targets) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "create_copy_task failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = client_service_->CreateCopyTask(key, targets);
    if (!result) {
        LOG(ERROR) << "create_copy_task failed, error=" << result.error();
    }
    return result;
}

tl::expected<UUID, ErrorCode> RealClient::create_move_task(
    const std::string& key, const std::string& source,
    const std::string& target) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "create_move_task failed, error="
                   << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = client_service_->CreateMoveTask(key, source, target);
    if (!result) {
        LOG(ERROR) << "create_move_task failed, error=" << result.error();
    }
    return result;
}

tl::expected<QueryTaskResponse, ErrorCode> RealClient::query_task(
    const UUID& task_id) {
    const auto state = state_.load(std::memory_order_acquire);
    if (state == State::CLOSING || state == State::CLOSED) {
        LOG(ERROR) << "query_task failed, error=" << ErrorCode::SHUTTING_DOWN;
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = client_service_->QueryTask(task_id);
    if (!result) {
        LOG(ERROR) << "query_task failed, error=" << result.error();
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
        if (result.error() != ErrorCode::OBJECT_NOT_FOUND &&
            result.error() != ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "Query failed, key=" << key
                       << ", error=" << result.error();
        }
        return tl::unexpected(result.error());
    }
    const auto& entries = result->replicas;
    if (entries.empty()) {
        LOG(ERROR) << "Empty query result, key=" << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return static_cast<int64_t>(calculate_total_size(entries.front()));
}

}  // namespace mooncake
