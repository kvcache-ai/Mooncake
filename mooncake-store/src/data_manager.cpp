#include "data_manager.h"

#include <glog/logging.h>
#include <thread>
#include <chrono>
#include "transfer_engine.h"
#include "transport/transport.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

DataManager::DataManager(std::unique_ptr<TieredBackend> tiered_backend,
                         std::shared_ptr<TransferEngine> transfer_engine)
    : tiered_backend_(std::move(tiered_backend)), transfer_engine_(transfer_engine) {
    if (!tiered_backend_) {
        LOG(FATAL) << "TieredBackend cannot be null";
    }
    if (!transfer_engine_) {
        LOG(FATAL) << "TransferEngine cannot be null";
    }
}

tl::expected<void, ErrorCode> DataManager::Put(const std::string& key,
                                                 const void* data,
                                                 size_t size,
                                                 std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::Put");
    timer.LogRequest("key=", key, "size=", size);

    std::unique_lock lock(GetKeyLock(key));  // 写锁

    // Allocate space in tiered backend
    auto handle = tiered_backend_->Allocate(size, tier_id);
    if (!handle.has_value()) {
        LOG(ERROR) << "Failed to allocate space for key: " << key;
        timer.LogResponse("error_code=", handle.error());
        return tl::make_unexpected(handle.error());
    }

    // Create DataSource from input data
    DataSource source;
    source.ptr = reinterpret_cast<uint64_t>(data);
    source.offset = 0;
    source.size = size;
    source.type = MemoryType::DRAM;  // Assuming input is in DRAM

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

tl::expected<DataSource, ErrorCode> DataManager::Get(const std::string& key,
                                                         std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::Get");
    timer.LogRequest("key=", key);

    std::shared_lock lock(GetKeyLock(key));  // 读锁

    // Get handle from tiered backend
    auto handle = tiered_backend_->Get(key, tier_id);
    if (!handle.has_value()) {
        LOG(ERROR) << "Failed to get data for key: " << key;
        timer.LogResponse("error_code=", handle.error());
        return tl::make_unexpected(handle.error());
    }

    // Extract DataSource from handle location
    DataSource source = handle.value()->loc.data;

    timer.LogResponse("error_code=", ErrorCode::OK);
    return source;
}

bool DataManager::Delete(const std::string& key, std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::Delete");
    timer.LogRequest("key=", key);

    std::unique_lock lock(GetKeyLock(key));  // 写锁

    auto result = tiered_backend_->Delete(key, tier_id);
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to delete key: " << key;
        timer.LogResponse("error_code=", result.error());
        return false;
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return true;
}

tl::expected<void, ErrorCode> DataManager::ReadData(
    const std::string& key,
    const std::vector<RemoteBufferDesc>& dest_buffers) {
    ScopedVLogTimer timer(1, "DataManager::ReadData");
    timer.LogRequest("key=", key, "buffer_count=", dest_buffers.size());

    std::shared_lock lock(GetKeyLock(key));  // 读锁

    // Step 1: Get data handle from TieredBackend
    auto handle_result = tiered_backend_->Get(key);
    if (!handle_result.has_value()) {
        LOG(ERROR) << "ReadData: Failed to get data for key: " << key
                   << ", error: " << toString(handle_result.error());
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    auto handle = handle_result.value();
    DataSource source = handle->loc.data;
    return TransferDataToRemote(source, dest_buffers);
}

tl::expected<void, ErrorCode> DataManager::WriteData(
    const std::string& key,
    const std::vector<RemoteBufferDesc>& src_buffers,
    std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::WriteData");
    timer.LogRequest("key=", key, "tier_id=", tier_id,
                     "buffer_count=", src_buffers.size());

    // Calculate total size and validate buffers
    size_t total_size = 0;
    for (const auto& buffer : src_buffers) {
        if (buffer.size == 0 || buffer.addr == 0) {
            LOG(ERROR) << "WriteData: Invalid buffer (zero size or null address)";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_size += buffer.size;
    }

    std::unique_lock lock(GetKeyLock(key));  // 写锁

    // Allocate space in tiered backend
    auto handle_result = tiered_backend_->Allocate(total_size, tier_id);
    if (!handle_result.has_value()) {
        LOG(ERROR) << "WriteData: Failed to allocate space for key: " << key;
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
        LOG(ERROR) << "WriteData: Failed to commit data for key: " << key;
        timer.LogResponse("error_code=", commit_result.error());
        return tl::make_unexpected(commit_result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK, "transferred_bytes=", total_size);
    return {};
}

tl::expected<void, ErrorCode> DataManager::TransferDataToRemote(
    const DataSource& source,
    const std::vector<RemoteBufferDesc>& dest_buffers) {
    // Validate buffers
    if (dest_buffers.empty()) {
        LOG(ERROR) << "TransferDataToRemote: Empty destination buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Extract and validate segment name (all buffers should have the same segment name)
    std::string remote_segment_name;
    size_t total_size = 0;
    for (const auto& buffer_desc : dest_buffers) {
        if (buffer_desc.size == 0 || buffer_desc.addr == 0) {
            LOG(ERROR) << "TransferDataToRemote: Invalid buffer (zero size or null address)";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        
        // All buffers should have the same segment name
        if (remote_segment_name.empty()) {
            remote_segment_name = buffer_desc.segment_name;
        } else if (remote_segment_name != buffer_desc.segment_name) {
            LOG(ERROR) << "TransferDataToRemote: Buffers have different segment names";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        
        total_size += buffer_desc.size;
    }

    if (total_size != source.size) {
        LOG(ERROR) << "TransferDataToRemote: Size mismatch. Source size: " << source.size
                   << ", Destination total size: " << total_size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Open remote segment
    SegmentHandle remote_segment_id = transfer_engine_->openSegment(remote_segment_name);
    if (remote_segment_id < 0) {
        LOG(ERROR) << "TransferDataToRemote: Failed to open remote segment: " << remote_segment_name;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    // Create TransferRequest for each destination buffer
    // For WRITE operation: data flows from local source to remote dest
    std::vector<TransferRequest> transfer_requests;
    transfer_requests.reserve(dest_buffers.size());

    size_t source_offset = 0;
    for (const auto& buffer_desc : dest_buffers) {
        TransferRequest req;
        req.opcode = TransferRequest::OpCode::WRITE;  // WRITE: from local to remote
        req.source = reinterpret_cast<void*>(source.ptr + source_offset);
        req.target_id = remote_segment_id;
        req.target_offset = buffer_desc.addr;
        req.length = buffer_desc.size;

        transfer_requests.push_back(req);
        source_offset += buffer_desc.size;
    }

    // Submit transfer via TransferEngine
    BatchID batch_id = transfer_engine_->allocateBatchID(transfer_requests.size());
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "TransferDataToRemote: Failed to allocate batch ID";
        transfer_engine_->closeSegment(remote_segment_id);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    Status transfer_status = transfer_engine_->submitTransfer(batch_id, transfer_requests);
    if (!transfer_status.ok()) {
        LOG(ERROR) << "TransferDataToRemote: Failed to submit transfer, error: " 
                   << transfer_status.ToString();
        transfer_engine_->freeBatchID(batch_id);
        transfer_engine_->closeSegment(remote_segment_id);
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    // Wait for transfer completion
    bool all_completed = false;
    const int max_wait_iterations = 1000;  // Timeout protection
    int iteration = 0;

    while (!all_completed && iteration < max_wait_iterations) {
        all_completed = true;
        for (size_t i = 0; i < transfer_requests.size(); ++i) {
            TransferStatus status;
            Status status_result = transfer_engine_->getTransferStatus(batch_id, i, status);
            
            if (!status_result.ok()) {
                LOG(ERROR) << "TransferDataToRemote: Failed to get transfer status for task " << i;
                transfer_engine_->freeBatchID(batch_id);
                transfer_engine_->closeSegment(remote_segment_id);
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }

            if (status.s == TransferStatusEnum::FAILED ||
                status.s == TransferStatusEnum::CANCELED ||
                status.s == TransferStatusEnum::TIMEOUT) {
                LOG(ERROR) << "TransferDataToRemote: Transfer failed for task " << i
                           << ", status: " << static_cast<int>(status.s);
                transfer_engine_->freeBatchID(batch_id);
                transfer_engine_->closeSegment(remote_segment_id);
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }

            if (status.s != TransferStatusEnum::COMPLETED) {
                all_completed = false;
                break;
            }
        }

        if (!all_completed) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            ++iteration;
        }
    }

    if (!all_completed) {
        LOG(ERROR) << "TransferDataToRemote: Transfer timeout after " << max_wait_iterations << " iterations";
        transfer_engine_->freeBatchID(batch_id);
        transfer_engine_->closeSegment(remote_segment_id);
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    // Cleanup
    transfer_engine_->freeBatchID(batch_id);
    transfer_engine_->closeSegment(remote_segment_id);

    return {};
}

tl::expected<void, ErrorCode> DataManager::TransferDataFromRemote(
    AllocationHandle handle,
    const std::vector<RemoteBufferDesc>& src_buffers) {
    // Validate buffers
    if (src_buffers.empty()) {
        LOG(ERROR) << "TransferDataFromRemote: Empty source buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Extract and validate segment name (all buffers should have the same segment name)
    std::string remote_segment_name;
    size_t total_size = 0;
    for (const auto& buffer_desc : src_buffers) {
        if (buffer_desc.size == 0 || buffer_desc.addr == 0) {
            LOG(ERROR) << "TransferDataFromRemote: Invalid buffer (zero size or null address)";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        
        // All buffers should have the same segment name
        if (remote_segment_name.empty()) {
            remote_segment_name = buffer_desc.segment_name;
        } else if (remote_segment_name != buffer_desc.segment_name) {
            LOG(ERROR) << "TransferDataFromRemote: Buffers have different segment names";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        
        total_size += buffer_desc.size;
    }

    // Get destination data location
    DataSource dest = handle->loc.data;
    if (total_size != dest.size) {
        LOG(ERROR) << "TransferDataFromRemote: Size mismatch. Destination size: " << dest.size
                   << ", Source total size: " << total_size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Open remote segment
    SegmentHandle remote_segment_id = transfer_engine_->openSegment(remote_segment_name);
    if (remote_segment_id < 0) {
        LOG(ERROR) << "TransferDataFromRemote: Failed to open remote segment: " << remote_segment_name;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    // Create TransferRequest for each source buffer
    // For READ operation: data flows from remote source to local dest
    std::vector<TransferRequest> transfer_requests;
    transfer_requests.reserve(src_buffers.size());

    size_t dest_offset = 0;
    for (const auto& buffer_desc : src_buffers) {
        TransferRequest req;
        req.opcode = TransferRequest::OpCode::READ;  // READ: from remote to local
        req.source = reinterpret_cast<void*>(dest.ptr + dest_offset);
        req.target_id = remote_segment_id;
        req.target_offset = buffer_desc.addr;
        req.length = buffer_desc.size;

        transfer_requests.push_back(req);
        dest_offset += buffer_desc.size;
    }

    // Submit transfer via TransferEngine
    BatchID batch_id = transfer_engine_->allocateBatchID(transfer_requests.size());
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "TransferDataFromRemote: Failed to allocate batch ID";
        transfer_engine_->closeSegment(remote_segment_id);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    Status transfer_status = transfer_engine_->submitTransfer(batch_id, transfer_requests);
    if (!transfer_status.ok()) {
        LOG(ERROR) << "TransferDataFromRemote: Failed to submit transfer, error: "
                   << transfer_status.ToString();
        transfer_engine_->freeBatchID(batch_id);
        transfer_engine_->closeSegment(remote_segment_id);
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    // Wait for transfer completion
    bool all_completed = false;
    const int max_wait_iterations = 1000;  // Timeout protection
    int iteration = 0;

    while (!all_completed && iteration < max_wait_iterations) {
        all_completed = true;
        for (size_t i = 0; i < transfer_requests.size(); ++i) {
            TransferStatus status;
            Status status_result = transfer_engine_->getTransferStatus(batch_id, i, status);
            
            if (!status_result.ok()) {
                LOG(ERROR) << "TransferDataFromRemote: Failed to get transfer status for task " << i;
                transfer_engine_->freeBatchID(batch_id);
                transfer_engine_->closeSegment(remote_segment_id);
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }

            if (status.s == TransferStatusEnum::FAILED ||
                status.s == TransferStatusEnum::CANCELED ||
                status.s == TransferStatusEnum::TIMEOUT) {
                LOG(ERROR) << "TransferDataFromRemote: Transfer failed for task " << i
                           << ", status: " << static_cast<int>(status.s);
                transfer_engine_->freeBatchID(batch_id);
                transfer_engine_->closeSegment(remote_segment_id);
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }

            if (status.s != TransferStatusEnum::COMPLETED) {
                all_completed = false;
                break;
            }
        }

        if (!all_completed) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            ++iteration;
        }
    }

    if (!all_completed) {
        LOG(ERROR) << "TransferDataFromRemote: Transfer timeout";
        transfer_engine_->freeBatchID(batch_id);
        transfer_engine_->closeSegment(remote_segment_id);
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    // Cleanup
    transfer_engine_->freeBatchID(batch_id);
    transfer_engine_->closeSegment(remote_segment_id);

    return {};
}

}  // namespace mooncake

