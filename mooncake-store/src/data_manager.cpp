#include "data_manager.h"

#include <glog/logging.h>
#include <thread>
#include <chrono>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <vector>
#include "transfer_engine.h"
#include "transport/transport.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"
#include "tiered_cache/data_copier.h"
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
        if (buffer.segment_name.empty()) {
            LOG(ERROR) << "WriteData: Empty segment name in source buffers";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.size == 0 || buffer.addr == 0) {
            LOG(ERROR) << "WriteRemoteData: Invalid buffer (zero size or null "
                          "address)";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_size += buffer.size;
    }

    std::unique_lock lock(GetKeyLock(key));

    auto handle_result = tiered_backend_->Allocate(total_size, tier_id);
    if (!handle_result.has_value()) {
        LOG(ERROR) << "WriteRemoteData: Failed to allocate space for key: "
                   << key;
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    auto handle = handle_result.value();
    auto transfer_result = TransferDataFromRemote(handle, src_buffers);

    if (!transfer_result.has_value()) {
        LOG(ERROR) << "WriteRemoteData: Transfer failed for key: " << key
                   << ", error: " << toString(transfer_result.error());
        timer.LogResponse("error_code=", transfer_result.error());
        return tl::make_unexpected(transfer_result.error());
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

tl::expected<void, ErrorCode> DataManager::ValidateRemoteBuffers(
    const std::vector<RemoteBufferDesc>& buffers,
    const std::string& function_name) {
    if (buffers.empty()) {
        LOG(ERROR) << function_name << ": Empty buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const auto& buffer : buffers) {
        if (buffer.segment_name.empty()) {
            LOG(ERROR) << function_name << ": Empty segment name in buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.addr == 0) {
            LOG(ERROR) << function_name
                       << ": Invalid buffer address (null) in buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.size == 0) {
            LOG(ERROR) << function_name
                       << ": Invalid buffer size (zero) in buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    return {};
}

tl::expected<std::pair<void*, std::unique_ptr<void, void (*)(void*)>>,
             ErrorCode>
DataManager::PrepareDRAMTransferBuffer(void* source_ptr, MemoryType source_type,
                                       size_t total_size,
                                       TieredBackend* backend) {
    if (source_type == MemoryType::DRAM) {
        // No conversion needed, return source pointer with empty deleter
        return std::make_pair(
            source_ptr,
            std::unique_ptr<void, void (*)(void*)>(nullptr, [](void*) {}));
    }

    VLOG(1) << "PrepareDRAMTransferBuffer: Source is non-DRAM (type="
            << static_cast<int>(source_type)
            << "), allocating temp DRAM buffer";

    auto temp_buffer_deleter = [](void* ptr) {
        if (ptr) free_memory("", ptr);
    };
    std::unique_ptr<void, void (*)(void*)> temp_buffer(
        allocate_buffer_allocator_memory(total_size), temp_buffer_deleter);

    if (!temp_buffer) {
        LOG(ERROR) << "PrepareDRAMTransferBuffer: Failed to allocate temporary "
                      "DRAM buffer";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Create source DataSource using RefBuffer (non-owning wrapper)
    DataSource temp_source;
    temp_source.buffer = std::make_unique<RefBuffer>(source_ptr, total_size);
    temp_source.type = source_type;

    // Create destination DataSource using RefBuffer
    DataSource temp_dst;
    temp_dst.buffer =
        std::make_unique<RefBuffer>(temp_buffer.get(), total_size);
    temp_dst.type = MemoryType::DRAM;

    // Copy from non-DRAM to DRAM
    const DataCopier& copier = backend->GetDataCopier();
    auto copy_result = copier.Copy(temp_source, temp_dst);
    if (!copy_result.has_value()) {
        LOG(ERROR)
            << "PrepareDRAMTransferBuffer: Failed to copy data from tier "
               "to temp buffer";
        return tl::make_unexpected(copy_result.error());
    }

    VLOG(1) << "PrepareDRAMTransferBuffer: Copied " << total_size
            << " bytes from non-DRAM to temp DRAM buffer";

    // Return temp buffer pointer and ownership
    void* transfer_ptr = temp_buffer.get();
    auto deleter = temp_buffer_deleter;
    return std::make_pair(transfer_ptr, std::unique_ptr<void, void (*)(void*)>(
                                            temp_buffer.release(), deleter));
}

tl::expected<std::pair<void*, std::unique_ptr<void, void (*)(void*)>>,
             ErrorCode>
DataManager::PrepareDRAMReceiveBuffer(void* dest_ptr, MemoryType dest_type,
                                      size_t total_size) {
    if (dest_type == MemoryType::DRAM) {
        // No conversion needed, return dest pointer with empty deleter
        return std::make_pair(dest_ptr, std::unique_ptr<void, void (*)(void*)>(
                                            nullptr, [](void*) {}));
    }

    VLOG(1) << "PrepareDRAMReceiveBuffer: Destination is non-DRAM (type="
            << static_cast<int>(dest_type) << "), allocating temp DRAM buffer";

    auto temp_buffer_deleter = [](void* ptr) {
        if (ptr) free_memory("", ptr);
    };
    std::unique_ptr<void, void (*)(void*)> temp_buffer(
        allocate_buffer_allocator_memory(total_size), temp_buffer_deleter);

    if (!temp_buffer) {
        LOG(ERROR) << "PrepareDRAMReceiveBuffer: Failed to allocate temporary "
                      "DRAM buffer";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    void* transfer_ptr = temp_buffer.get();
    auto deleter = temp_buffer_deleter;
    return std::make_pair(transfer_ptr, std::unique_ptr<void, void (*)(void*)>(
                                            temp_buffer.release(), deleter));
}

tl::expected<void, ErrorCode> DataManager::CopyFromDRAMBuffer(
    void* temp_buffer, void* dest_ptr, MemoryType dest_type, size_t total_size,
    TieredBackend* backend) {
    VLOG(1) << "CopyFromDRAMBuffer: Copying from temp DRAM to non-DRAM tier";

    // Create source DataSource from temp buffer
    DataSource temp_src;
    temp_src.buffer = std::make_unique<RefBuffer>(temp_buffer, total_size);
    temp_src.type = MemoryType::DRAM;

    // Create destination DataSource
    DataSource temp_dst;
    temp_dst.buffer = std::make_unique<RefBuffer>(dest_ptr, total_size);
    temp_dst.type = dest_type;

    // Copy from DRAM to non-DRAM tier
    const DataCopier& copier = backend->GetDataCopier();
    auto copy_result = copier.Copy(temp_src, temp_dst);
    if (!copy_result.has_value()) {
        LOG(ERROR)
            << "CopyFromDRAMBuffer: Failed to copy data from temp buffer "
               "to tier";
        return tl::make_unexpected(copy_result.error());
    }

    VLOG(1) << "CopyFromDRAMBuffer: Copied " << total_size
            << " bytes from temp DRAM buffer to non-DRAM tier";
    return {};
}

tl::expected<BatchID, ErrorCode> DataManager::SubmitTransferRequests(
    const std::string& segment_name, SegmentHandle seg,
    const std::vector<TransferRequest>& requests,
    const std::string& function_name) {
    // Allocate batch ID
    BatchID batch_id = transfer_engine_->allocateBatchID(requests.size());
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << function_name << ": Failed to allocate batch ID";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    // Submit transfers
    Status submit_status = transfer_engine_->submitTransfer(batch_id, requests);
    if (!submit_status.ok()) {
        LOG(ERROR) << function_name << ": Failed to submit transfers, error: "
                   << submit_status.message();
        transfer_engine_->freeBatchID(batch_id);
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    return batch_id;
}

tl::expected<void, ErrorCode> DataManager::WaitAllTransferBatches(
    const std::vector<std::tuple<BatchID, size_t, std::string>>& batches,
    const std::string& function_name) {
    for (size_t i = 0; i < batches.size(); ++i) {
        const auto& [batch_id, num_tasks, segment_name] = batches[i];
        auto wait_result =
            WaitTransferBatch(batch_id, num_tasks, segment_name, function_name);
        if (!wait_result.has_value()) {
            LOG(ERROR) << function_name << ": Transfer failed for segment '"
                       << segment_name
                       << "', error: " << toString(wait_result.error());

            // Free remaining batch IDs that haven't been processed yet
            // Note: WaitTransferBatch already freed the failed batch ID
            for (size_t j = i + 1; j < batches.size(); ++j) {
                const auto& [remaining_batch_id, _, __] = batches[j];
                transfer_engine_->freeBatchID(remaining_batch_id);
            }

            return tl::make_unexpected(wait_result.error());
        }
    }

    return {};
}

tl::expected<void, ErrorCode> DataManager::TransferDataToRemote(
    AllocationHandle handle,
    const std::vector<RemoteBufferDesc>& dest_buffers) {
    if (!handle) {
        LOG(ERROR) << "TransferDataToRemote: Invalid handle";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers
    auto validate_result =
        ValidateRemoteBuffers(dest_buffers, "TransferDataToRemote");
    if (!validate_result.has_value()) {
        return validate_result;
    }

    // Get data source from handle
    const auto& data_source = handle->loc.data;
    if (!data_source.buffer) {
        LOG(ERROR) << "TransferDataToRemote: Handle has no data buffer";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t total_data_size = data_source.buffer->size();

    // Validate total size matches destination buffer sizes
    size_t total_dest_size = 0;
    for (const auto& buffer : dest_buffers) {
        total_dest_size += buffer.size;
    }
    if (total_dest_size < total_data_size) {
        LOG(ERROR) << "TransferDataToRemote: Destination buffers total size ("
                   << total_dest_size << ") is less than source data size ("
                   << total_data_size << ")";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Check if TransferEngine is properly initialized
    if (!transfer_engine_->getMetadata()) {
        LOG(ERROR) << "TransferDataToRemote: TransferEngine not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Get source data pointer and type
    void* source_ptr = reinterpret_cast<void*>(data_source.buffer->data());
    MemoryType source_type = data_source.type;

    // Prepare DRAM buffer if needed
    auto buffer_result = PrepareDRAMTransferBuffer(
        source_ptr, source_type, total_data_size, handle->backend);
    if (!buffer_result.has_value()) {
        return tl::make_unexpected(buffer_result.error());
    }
    auto [transfer_source, temp_buffer] = std::move(buffer_result.value());

    // Group transfers by segment_name
    std::unordered_map<std::string, std::vector<size_t>> segment_buffers;
    for (size_t i = 0; i < dest_buffers.size(); ++i) {
        segment_buffers[dest_buffers[i].segment_name].push_back(i);
    }

    // Precompute cumulative offsets
    std::vector<size_t> buffer_offsets(dest_buffers.size());
    size_t running_offset = 0;
    for (size_t i = 0; i < dest_buffers.size(); ++i) {
        buffer_offsets[i] = running_offset;
        running_offset += dest_buffers[i].size;
    }

    // Phase 1: Submit all transfer requests (without waiting)
    std::vector<std::tuple<BatchID, size_t, std::string>> submitted_batches;
    for (const auto& [segment_name, buffer_indices] : segment_buffers) {
        SegmentHandle seg = transfer_engine_->openSegment(segment_name);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "TransferDataToRemote: Failed to open segment '"
                       << segment_name << "'";
            // Cleanup already submitted batches
            for (const auto& [batch_id, _, __] : submitted_batches) {
                transfer_engine_->freeBatchID(batch_id);
            }
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        std::vector<TransferRequest> requests;
        requests.reserve(buffer_indices.size());

        for (size_t idx : buffer_indices) {
            const auto& buffer = dest_buffers[idx];
            size_t offset_in_source = buffer_offsets[idx];

            if (offset_in_source >= total_data_size) {
                continue;
            }

            size_t length =
                std::min(buffer.size, total_data_size - offset_in_source);
            if (length == 0) {
                continue;
            }

            TransferRequest request;
            request.opcode = TransferRequest::WRITE;
            request.source =
                static_cast<char*>(transfer_source) + offset_in_source;
            request.target_id = seg;
            request.target_offset = buffer.addr;
            request.length = length;
            requests.emplace_back(request);
        }

        if (requests.empty()) {
            continue;
        }

        // Submit transfer requests
        auto batch_result = SubmitTransferRequests(segment_name, seg, requests,
                                                   "TransferDataToRemote");
        if (!batch_result.has_value()) {
            // Cleanup already submitted batches
            for (const auto& [batch_id, _, __] : submitted_batches) {
                transfer_engine_->freeBatchID(batch_id);
            }
            return tl::make_unexpected(batch_result.error());
        }

        submitted_batches.emplace_back(batch_result.value(), requests.size(),
                                       segment_name);
    }

    // Phase 2: Wait for all batches to complete
    if (!submitted_batches.empty()) {
        auto wait_result =
            WaitAllTransferBatches(submitted_batches, "TransferDataToRemote");
        if (!wait_result.has_value()) {
            return wait_result;
        }
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
    auto validate_result =
        ValidateRemoteBuffers(src_buffers, "TransferDataFromRemote");
    if (!validate_result.has_value()) {
        return tl::make_unexpected(validate_result.error());
    }

    // Get destination handle info
    const auto& data_source = handle->loc.data;
    if (!data_source.buffer) {
        LOG(ERROR) << "TransferDataFromRemote: Handle has no data buffer";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t total_data_size = data_source.buffer->size();

    // Validate source buffer sizes
    size_t total_src_size = 0;
    for (const auto& buffer : src_buffers) {
        total_src_size += buffer.size;
    }
    if (total_src_size < total_data_size) {
        LOG(ERROR) << "TransferDataFromRemote: Source buffers total size ("
                   << total_src_size << ") is less than destination data size ("
                   << total_data_size << ")";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Check if TransferEngine is properly initialized
    if (!transfer_engine_->getMetadata()) {
        LOG(ERROR) << "TransferDataFromRemote: TransferEngine not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Get destination pointer and type
    void* dest_ptr = reinterpret_cast<void*>(data_source.buffer->data());
    MemoryType dest_type = data_source.type;

    // Prepare DRAM buffer if needed
    auto buffer_result =
        PrepareDRAMReceiveBuffer(dest_ptr, dest_type, total_data_size);
    if (!buffer_result.has_value()) {
        return tl::make_unexpected(buffer_result.error());
    }
    auto [transfer_dest, temp_buffer] = std::move(buffer_result.value());

    // Group transfers by segment_name
    std::unordered_map<std::string, std::vector<size_t>> segment_buffers;
    for (size_t i = 0; i < src_buffers.size(); ++i) {
        segment_buffers[src_buffers[i].segment_name].push_back(i);
    }

    // Precompute cumulative offsets
    std::vector<size_t> buffer_offsets(src_buffers.size());
    size_t running_offset = 0;
    for (size_t i = 0; i < src_buffers.size(); ++i) {
        buffer_offsets[i] = running_offset;
        running_offset += src_buffers[i].size;
    }

    // Phase 1: Submit all transfer requests (without waiting)
    std::vector<std::tuple<BatchID, size_t, std::string>> submitted_batches;
    for (const auto& [segment_name, buffer_indices] : segment_buffers) {
        SegmentHandle seg = transfer_engine_->openSegment(segment_name);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "TransferDataFromRemote: Failed to open segment '"
                       << segment_name << "'";
            // Cleanup already submitted batches
            for (const auto& [batch_id, _, __] : submitted_batches) {
                transfer_engine_->freeBatchID(batch_id);
            }
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Build transfer requests for this segment
        std::vector<TransferRequest> requests;
        requests.reserve(buffer_indices.size());

        for (size_t idx : buffer_indices) {
            const auto& buffer = src_buffers[idx];
            size_t offset_in_dest = buffer_offsets[idx];

            if (offset_in_dest >= total_data_size) {
                continue;
            }

            size_t length =
                std::min(buffer.size, total_data_size - offset_in_dest);
            if (length == 0) {
                continue;
            }

            TransferRequest request;
            request.opcode = TransferRequest::READ;
            request.source = static_cast<char*>(transfer_dest) + offset_in_dest;
            request.target_id = seg;
            request.target_offset = buffer.addr;
            request.length = length;
            requests.emplace_back(request);
        }

        if (requests.empty()) {
            continue;
        }

        // Submit transfer requests
        auto batch_result = SubmitTransferRequests(segment_name, seg, requests,
                                                   "TransferDataFromRemote");
        if (!batch_result.has_value()) {
            // Cleanup already submitted batches
            for (const auto& [batch_id, _, __] : submitted_batches) {
                transfer_engine_->freeBatchID(batch_id);
            }
            return tl::make_unexpected(batch_result.error());
        }

        submitted_batches.emplace_back(batch_result.value(), requests.size(),
                                       segment_name);
    }

    // Phase 2: Wait for all batches to complete
    if (submitted_batches.empty()) {
        LOG(ERROR) << "TransferDataFromRemote: No batches were submitted";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    auto wait_result =
        WaitAllTransferBatches(submitted_batches, "TransferDataFromRemote");
    if (!wait_result.has_value()) {
        LOG(ERROR) << "TransferDataFromRemote: Transfer failed, error: "
                   << toString(wait_result.error());
        return wait_result;
    }

    if (dest_type != MemoryType::DRAM && temp_buffer) {
        auto copy_result =
            CopyFromDRAMBuffer(temp_buffer.get(), dest_ptr, dest_type,
                               total_data_size, handle->backend);
        if (!copy_result.has_value()) {
            LOG(ERROR)
                << "TransferDataFromRemote: Failed to copy from temp DRAM "
                   "buffer to destination tier";
            return tl::make_unexpected(copy_result.error());
        }
    }

    return {};
}

tl::expected<void, ErrorCode> DataManager::WaitTransferBatch(
    BatchID batch_id, size_t num_tasks, const std::string& segment_name,
    const std::string& function_name) {
    // Poll for completion
    constexpr int64_t timeout_seconds = 10;
    auto start_time = std::chrono::steady_clock::now();

    while (true) {
        // Check timeout
        auto now = std::chrono::steady_clock::now();
        auto elapsed =
            std::chrono::duration_cast<std::chrono::seconds>(now - start_time)
                .count();
        if (elapsed >= timeout_seconds) {
            LOG(ERROR) << function_name << ": Timeout after " << elapsed
                       << " seconds";
            transfer_engine_->freeBatchID(batch_id);
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Check status of all tasks in batch
        bool all_completed = true;
        bool has_failure = false;

        for (size_t i = 0; i < num_tasks; ++i) {
            TransferStatus status;
            Status s = transfer_engine_->getTransferStatus(batch_id, i, status);
            if (!s.ok()) {
                LOG(ERROR) << function_name << ": Failed to get "
                           << "transfer status for task " << i
                           << ", error: " << s.message();
                has_failure = true;
                break;
            }

            if (status.s == TransferStatusEnum::COMPLETED) {
                continue;
            } else if (status.s == TransferStatusEnum::FAILED ||
                       status.s == TransferStatusEnum::CANCELED ||
                       status.s == TransferStatusEnum::INVALID ||
                       status.s == TransferStatusEnum::TIMEOUT) {
                LOG(ERROR) << function_name << ": Transfer task " << i
                           << " failed with status "
                           << static_cast<int>(status.s);
                has_failure = true;
                break;
            } else {
                // Still pending
                all_completed = false;
            }
        }

        if (has_failure) {
            transfer_engine_->freeBatchID(batch_id);
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        if (all_completed) {
            VLOG(1) << function_name
                    << ": All transfers completed for segment '" << segment_name
                    << "'";
            break;
        }

        // Short sleep before next poll
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    // Free batch ID
    transfer_engine_->freeBatchID(batch_id);
    return {};
}

}  // namespace mooncake
