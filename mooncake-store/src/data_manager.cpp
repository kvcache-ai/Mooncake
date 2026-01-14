#include "data_manager.h"

#include <glog/logging.h>
#include <thread>
#include <chrono>
#include "transfer_engine.h"
#include "transport/transport.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"
#include "utils/scoped_vlog_timer.h"
#include "utils.h"

namespace mooncake {

DataManager::DataManager(std::unique_ptr<TieredBackend> tiered_backend,
                         std::shared_ptr<TransferEngine> transfer_engine)
    : tiered_backend_(std::move(tiered_backend)),
      transfer_engine_(transfer_engine),
      lock_shard_count_(GetEnvOr<size_t>("MOONCAKE_DM_LOCK_SHARD_COUNT", 1024)),
      lock_shards_(lock_shard_count_) {
    if (!tiered_backend_) {
        LOG(FATAL) << "TieredBackend cannot be null";
    }
    if (!transfer_engine_) {
        LOG(FATAL) << "TransferEngine cannot be null";
    }

    LOG(INFO) << "DataManager initialized with " << lock_shard_count_
              << " lock shards (configured via MOONCAKE_DM_LOCK_SHARD_COUNT)";
}

tl::expected<void, ErrorCode> DataManager::Put(const std::string& key,
                                               std::unique_ptr<char[]> data,
                                               size_t size,
                                               std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::Put");
    timer.LogRequest("key=", key, "size=", size);

    std::unique_lock lock(GetKeyLock(key));

    // Allocate space in tiered backend
    auto handle = tiered_backend_->Allocate(size, tier_id);
    if (!handle.has_value()) {
        LOG(ERROR) << "Failed to allocate space for key: " << key;
        timer.LogResponse("error_code=", handle.error());
        return tl::make_unexpected(handle.error());
    }

    // Create DataSource from input data
    DataSource source;
    source.buffer = std::make_unique<TempDRAMBuffer>(std::move(data), size);
    source.type = MemoryType::DRAM;

    // Write data to allocated handle
    auto write_result = tiered_backend_->Write(source, handle.value());
    if (!write_result.has_value()) {
        LOG(ERROR) << "Failed to write data for key: " << key;
        timer.LogResponse("error_code=", write_result.error());
        return tl::make_unexpected(write_result.error());
    }

    // Commit the handle
    auto commit_result = tiered_backend_->Commit(key, handle.value());
    if (!commit_result.has_value()) {
        LOG(ERROR) << "Failed to commit data for key: " << key;
        timer.LogResponse("error_code=", commit_result.error());
        return tl::make_unexpected(commit_result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<AllocationHandle, ErrorCode> DataManager::Get(
    const std::string& key, std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::Get");
    timer.LogRequest("key=", key);

    // Get handle from tiered backend
    auto handle = tiered_backend_->Get(key, tier_id);
    if (!handle.has_value()) {
        LOG(ERROR) << "Failed to get data for key: " << key;
        timer.LogResponse("error_code=", handle.error());
        return tl::make_unexpected(handle.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return handle.value();
}

tl::expected<void, ErrorCode> DataManager::Delete(const std::string& key,
                                                  std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::Delete");
    timer.LogRequest("key=", key);

    std::unique_lock lock(GetKeyLock(key));

    auto result = tiered_backend_->Delete(key, tier_id);
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to delete key: " << key;
        timer.LogResponse("error_code=", result.error());
        return tl::make_unexpected(result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<void, ErrorCode> DataManager::ReadRemoteData(
    const std::string& key, const std::vector<RemoteBufferDesc>& dest_buffers) {
    ScopedVLogTimer timer(1, "DataManager::ReadRemoteData");
    timer.LogRequest("key=", key, "buffer_count=", dest_buffers.size());

    // Step 1: Get data handle from TieredBackend
    auto handle_result = tiered_backend_->Get(key);
    if (!handle_result.has_value()) {
        LOG(ERROR) << "ReadRemoteData: Failed to get data for key: " << key
                   << ", error: " << toString(handle_result.error());
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    auto handle = handle_result.value();
    return TransferDataToRemote(handle, dest_buffers);
}

tl::expected<void, ErrorCode> DataManager::WriteRemoteData(
    const std::string& key, const std::vector<RemoteBufferDesc>& src_buffers,
    std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::WriteRemoteData");
    timer.LogRequest("key=", key, "buffer_count=", src_buffers.size());

    // Calculate total size and validate buffers
    size_t total_size = 0;
    for (const auto& buffer : src_buffers) {
        if (buffer.size == 0 || buffer.addr == 0) {
            LOG(ERROR) << "WriteRemoteData: Invalid buffer (zero size or null "
                          "address)";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_size += buffer.size;
    }

    std::unique_lock lock(GetKeyLock(key));

    // Allocate space in tiered backend
    auto handle_result = tiered_backend_->Allocate(total_size, tier_id);
    if (!handle_result.has_value()) {
        LOG(ERROR) << "WriteRemoteData: Failed to allocate space for key: "
                   << key;
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    auto handle = handle_result.value();

    // Transfer data from remote
    auto transfer_result = TransferDataFromRemote(handle, src_buffers);
    if (!transfer_result.has_value()) {
        timer.LogResponse("error_code=", transfer_result.error());
        return transfer_result;
    }

    // Commit the handle
    auto commit_result = tiered_backend_->Commit(key, handle);
    if (!commit_result.has_value()) {
        LOG(ERROR) << "WriteRemoteData: Failed to commit data for key: " << key;
        timer.LogResponse("error_code=", commit_result.error());
        return tl::make_unexpected(commit_result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK,
                      "transferred_bytes=", total_size);
    return {};
}

tl::expected<void, ErrorCode> DataManager::TransferDataToRemote(
    AllocationHandle handle,
    const std::vector<RemoteBufferDesc>& dest_buffers) {
    // Validate handle
    if (!handle) {
        LOG(ERROR) << "TransferDataToRemote: Invalid handle";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers
    if (dest_buffers.empty()) {
        LOG(ERROR) << "TransferDataToRemote: Empty destination buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return {};
}

tl::expected<void, ErrorCode> DataManager::TransferDataFromRemote(
    AllocationHandle handle, const std::vector<RemoteBufferDesc>& src_buffers) {
    // Validate handle
    if (!handle) {
        LOG(ERROR) << "TransferDataFromRemote: Invalid handle";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Validate buffers
    if (src_buffers.empty()) {
        LOG(ERROR) << "TransferDataFromRemote: Empty source buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return {};
}

}  // namespace mooncake
