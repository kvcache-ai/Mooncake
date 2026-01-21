#include "data_manager.h"

#include <glog/logging.h>
#include <thread>
#include <chrono>
#include <memory>
#include "transfer_engine.h"
#include "transport/transport.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"
#include "tiered_cache/data_copier.h"
#include "utils/scoped_vlog_timer.h"
#include "utils.h"

namespace mooncake {

// Helper class to wrap a raw pointer as BufferBase without taking ownership
// This is used for DataCopier operations where the source memory is owned
// elsewhere
class RefBuffer : public BufferBase {
   public:
    explicit RefBuffer(void* ptr, size_t size) : ptr_(ptr), size_(size) {}

    uint64_t data() const override { return reinterpret_cast<uint64_t>(ptr_); }

    std::size_t size() const override { return size_; }

   private:
    void* ptr_;
    size_t size_;
};

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
    // Validate handle first
    if (!handle) {
        LOG(ERROR) << "TransferDataToRemote: Invalid handle";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers early
    if (dest_buffers.empty()) {
        LOG(ERROR) << "TransferDataToRemote: Empty destination buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate segment names and buffer parameters early
    for (const auto& buffer : dest_buffers) {
        if (buffer.segment_name.empty()) {
            LOG(ERROR) << "TransferDataToRemote: Empty segment name in "
                          "destination buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.addr == 0) {
            LOG(ERROR) << "TransferDataToRemote: Invalid buffer address (null) "
                          "in destination buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.size == 0) {
            LOG(ERROR) << "TransferDataToRemote: Invalid buffer size (zero) in "
                          "destination buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    // Get data source from handle
    const auto& data_source = handle->loc.data;
    if (!data_source.buffer) {
        LOG(ERROR) << "TransferDataToRemote: Handle has no data buffer";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Calculate total data size
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

    // Check for local loopback transfer (segment_name == "local")
    // This allows single-node testing without network handshaking
    // Must be checked BEFORE TransferEngine initialization check
    bool has_local_transfer = false;
    for (const auto& buffer : dest_buffers) {
        if (buffer.segment_name == "local") {
            has_local_transfer = true;
            break;
        }
    }

    if (has_local_transfer) {
        VLOG(1)
            << "TransferDataToRemote: Using local memcpy for loopback transfer";

        // Get source data pointer
        void* source_ptr = reinterpret_cast<void*>(data_source.buffer->data());
        MemoryType source_type = data_source.type;

        // For non-DRAM tiers, copy data to temporary DRAM buffer first
        auto temp_buffer_deleter = [](void* ptr) {
            if (ptr) free_memory("", ptr);
        };
        std::unique_ptr<void, decltype(temp_buffer_deleter)> temp_buffer(
            nullptr, temp_buffer_deleter);
        void* transfer_source = source_ptr;

        if (source_type != MemoryType::DRAM) {
            VLOG(1) << "TransferDataToRemote: Source is non-DRAM, allocating "
                       "temp buffer";
            temp_buffer.reset(
                allocate_buffer_allocator_memory(total_data_size));
            if (!temp_buffer) {
                LOG(ERROR) << "TransferDataToRemote: Failed to allocate "
                              "temporary buffer";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            transfer_source = temp_buffer.get();

            DataSource temp_src;
            temp_src.buffer =
                std::make_unique<RefBuffer>(source_ptr, total_data_size);
            temp_src.type = source_type;
            DataSource temp_dst;
            temp_dst.buffer =
                std::make_unique<RefBuffer>(temp_buffer.get(), total_data_size);
            temp_dst.type = MemoryType::DRAM;

            const DataCopier& copier = handle->backend->GetDataCopier();
            auto copy_result = copier.Copy(temp_src, temp_dst);
            if (!copy_result.has_value()) {
                return tl::make_unexpected(copy_result.error());
            }
        }

        size_t src_offset = 0;
        for (const auto& buffer : dest_buffers) {
            if (buffer.segment_name != "local") {
                LOG(ERROR) << "TransferDataToRemote: Mixed local/remote "
                              "buffers not supported";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            size_t copy_size =
                std::min(buffer.size, total_data_size - src_offset);
            if (copy_size > 0) {
                std::memcpy(
                    reinterpret_cast<void*>(buffer.addr),
                    static_cast<const char*>(transfer_source) + src_offset,
                    copy_size);
                src_offset += copy_size;
            }
        }
        LOG(INFO) << "TransferDataToRemote: Local loopback transfer completed ("
                  << total_data_size << " bytes)";
        return {};
    }

    // Check if TransferEngine is properly initialized (only for non-local
    // transfers)
    if (!transfer_engine_->getMetadata()) {
        LOG(ERROR) << "TransferDataToRemote: TransferEngine not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Get source data pointer and type
    void* source_ptr = reinterpret_cast<void*>(data_source.buffer->data());
    MemoryType source_type = data_source.type;

    // For non-DRAM tiers, copy data to temporary DRAM buffer first
    auto temp_buffer_deleter = [](void* ptr) { free_memory("", ptr); };
    std::unique_ptr<void, decltype(temp_buffer_deleter)> temp_buffer(
        nullptr, temp_buffer_deleter);
    void* transfer_source = source_ptr;

    if (source_type != MemoryType::DRAM) {
        VLOG(1) << "TransferDataToRemote: Source is non-DRAM (type="
                << static_cast<int>(source_type)
                << "), allocating temp DRAM buffer";

        temp_buffer.reset(allocate_buffer_allocator_memory(total_data_size));
        if (!temp_buffer) {
            LOG(ERROR) << "TransferDataToRemote: Failed to allocate temporary "
                          "DRAM buffer";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        transfer_source = temp_buffer.get();

        // Create source DataSource using RefBuffer (non-owning wrapper)
        DataSource temp_source;
        temp_source.buffer =
            std::make_unique<RefBuffer>(source_ptr, total_data_size);
        temp_source.type = source_type;

        // Create destination DataSource using RefBuffer (temp_buffer owns the
        // memory)
        DataSource temp_dst;
        temp_dst.buffer =
            std::make_unique<RefBuffer>(temp_buffer.get(), total_data_size);
        temp_dst.type = MemoryType::DRAM;

        // Get DataCopier from TieredBackend (not from CacheTier)
        const DataCopier& copier = handle->backend->GetDataCopier();
        auto copy_result = copier.Copy(temp_source, temp_dst);
        if (!copy_result.has_value()) {
            LOG(ERROR) << "TransferDataToRemote: Failed to copy data from tier "
                          "to temp buffer";
            return tl::make_unexpected(copy_result.error());
        }

        VLOG(1) << "TransferDataToRemote: Copied " << total_data_size
                << " bytes from non-DRAM to temp DRAM buffer";
    }

    // Group transfers by segment_name to minimize openSegment calls
    std::unordered_map<std::string, std::vector<size_t>> segment_buffers;
    for (size_t i = 0; i < dest_buffers.size(); ++i) {
        segment_buffers[dest_buffers[i].segment_name].push_back(i);
    }

    // Submit transfers for each segment
    // Precompute cumulative offsets for each buffer in dest_buffers
    std::vector<size_t> buffer_offsets(dest_buffers.size());
    size_t running_offset = 0;
    for (size_t i = 0; i < dest_buffers.size(); ++i) {
        buffer_offsets[i] = running_offset;
        running_offset += dest_buffers[i].size;
    }

    for (const auto& [segment_name, buffer_indices] : segment_buffers) {
        // Open remote segment
        SegmentHandle seg = transfer_engine_->openSegment(segment_name);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "TransferDataToRemote: Failed to open segment '"
                       << segment_name << "'";
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Prepare transfer requests for this segment
        std::vector<TransferRequest> requests;
        requests.reserve(buffer_indices.size());

        for (size_t idx : buffer_indices) {
            const auto& buffer = dest_buffers[idx];
            TransferRequest request;
            request.opcode = TransferRequest::WRITE;  // Write from local source
                                                      // to remote dest
            request.source =
                static_cast<char*>(transfer_source) + buffer_offsets[idx];
            request.target_id = seg;
            request.target_offset = buffer.addr;
            request.length = buffer.size;
            requests.emplace_back(request);
        }

        // Allocate batch ID
        BatchID batch_id = transfer_engine_->allocateBatchID(requests.size());
        if (batch_id == INVALID_BATCH_ID) {
            LOG(ERROR) << "TransferDataToRemote: Failed to allocate batch ID";
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Submit transfers
        Status submit_status =
            transfer_engine_->submitTransfer(batch_id, requests);
        if (!submit_status.ok()) {
            LOG(ERROR)
                << "TransferDataToRemote: Failed to submit transfers, error: "
                << submit_status.message();
            transfer_engine_->freeBatchID(batch_id);
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Poll for completion
        constexpr int64_t timeout_seconds = 10;
        auto start_time = std::chrono::steady_clock::now();
        bool all_completed = false;

        while (true) {
            // Check timeout
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               now - start_time)
                               .count();
            if (elapsed >= timeout_seconds) {
                LOG(ERROR) << "TransferDataToRemote: Timeout after " << elapsed
                           << " seconds";
                transfer_engine_->freeBatchID(batch_id);
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }

            // Check status of all tasks in batch
            all_completed = true;
            bool has_failure = false;

            for (size_t i = 0; i < requests.size(); ++i) {
                TransferStatus status;
                Status s =
                    transfer_engine_->getTransferStatus(batch_id, i, status);
                if (!s.ok()) {
                    LOG(ERROR) << "TransferDataToRemote: Failed to get "
                                  "transfer status for task "
                               << i << ", error: " << s.message();
                    has_failure = true;
                    break;
                }

                if (status.s == TransferStatusEnum::COMPLETED) {
                    continue;
                } else if (status.s == TransferStatusEnum::FAILED ||
                           status.s == TransferStatusEnum::CANCELED ||
                           status.s == TransferStatusEnum::INVALID) {
                    LOG(ERROR)
                        << "TransferDataToRemote: Transfer task " << i
                        << " failed with status " << static_cast<int>(status.s);
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
                VLOG(1) << "TransferDataToRemote: All transfers completed for "
                           "segment '"
                        << segment_name << "'";
                break;
            }

            // Short sleep before next poll
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }

        // Free batch ID
        transfer_engine_->freeBatchID(batch_id);
    }

    return {};
}

tl::expected<void, ErrorCode> DataManager::TransferDataFromRemote(
    AllocationHandle handle, const std::vector<RemoteBufferDesc>& src_buffers) {
    // Validate handle first
    if (!handle) {
        LOG(ERROR) << "TransferDataFromRemote: Invalid handle";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers early
    if (src_buffers.empty()) {
        LOG(ERROR) << "TransferDataFromRemote: Empty source buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate segment names and buffer parameters early
    for (const auto& buffer : src_buffers) {
        if (buffer.segment_name.empty()) {
            LOG(ERROR) << "TransferDataFromRemote: Empty segment name in "
                          "source buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.addr == 0) {
            LOG(ERROR) << "TransferDataFromRemote: Invalid buffer address "
                          "(null) in source buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.size == 0) {
            LOG(ERROR) << "TransferDataFromRemote: Invalid buffer size (zero) "
                          "in source buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
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

    // Check for local loopback transfer (segment_name == "local")
    // Must be checked BEFORE TransferEngine initialization check
    bool has_local_transfer = false;
    for (const auto& buffer : src_buffers) {
        if (buffer.segment_name == "local") {
            has_local_transfer = true;
            break;
        }
    }

    if (has_local_transfer) {
        VLOG(1) << "TransferDataFromRemote: Using local memcpy for loopback "
                   "transfer";

        // Get destination pointer and type
        void* dest_ptr = reinterpret_cast<void*>(data_source.buffer->data());
        MemoryType dest_type = data_source.type;

        // For non-DRAM destination, use temp buffer
        auto temp_buffer_deleter = [](void* ptr) {
            if (ptr) free_memory("", ptr);
        };
        std::unique_ptr<void, decltype(temp_buffer_deleter)> temp_buffer(
            nullptr, temp_buffer_deleter);
        void* transfer_dest = dest_ptr;

        if (dest_type != MemoryType::DRAM) {
            temp_buffer.reset(
                allocate_buffer_allocator_memory(total_data_size));
            if (!temp_buffer) {
                LOG(ERROR)
                    << "TransferDataFromRemote: Failed to allocate temp buffer";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            transfer_dest = temp_buffer.get();
        }

        size_t dest_offset = 0;
        for (const auto& buffer : src_buffers) {
            if (buffer.segment_name != "local") {
                LOG(ERROR) << "TransferDataFromRemote: Mixed local/remote "
                              "buffers not supported";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            size_t copy_size =
                std::min(buffer.size, total_data_size - dest_offset);
            if (copy_size > 0) {
                std::memcpy(static_cast<char*>(transfer_dest) + dest_offset,
                            reinterpret_cast<const void*>(buffer.addr),
                            copy_size);
                dest_offset += copy_size;
            }
        }

        // If destination is non-DRAM, copy from temp buffer to destination tier
        if (dest_type != MemoryType::DRAM && temp_buffer) {
            DataSource temp_src;
            temp_src.buffer =
                std::make_unique<RefBuffer>(temp_buffer.get(), total_data_size);
            temp_src.type = MemoryType::DRAM;
            DataSource temp_dst;
            temp_dst.buffer =
                std::make_unique<RefBuffer>(dest_ptr, total_data_size);
            temp_dst.type = dest_type;
            const DataCopier& copier = handle->backend->GetDataCopier();
            auto copy_result = copier.Copy(temp_src, temp_dst);
            if (!copy_result.has_value()) {
                return tl::make_unexpected(copy_result.error());
            }
        }

        LOG(INFO)
            << "TransferDataFromRemote: Local loopback transfer completed ("
            << total_data_size << " bytes)";
        return {};
    }

    // Check if TransferEngine is properly initialized (only for non-local
    // transfers)
    if (!transfer_engine_->getMetadata()) {
        LOG(ERROR) << "TransferDataFromRemote: TransferEngine not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Get destination pointer and type
    void* dest_ptr = reinterpret_cast<void*>(data_source.buffer->data());
    MemoryType dest_type = data_source.type;

    // For non-DRAM tiers, copy to temporary DRAM buffer first
    auto temp_buffer_deleter = [](void* ptr) { free_memory("", ptr); };
    std::unique_ptr<void, decltype(temp_buffer_deleter)> temp_buffer(
        nullptr, temp_buffer_deleter);
    void* transfer_dest = dest_ptr;

    if (dest_type != MemoryType::DRAM) {
        VLOG(1) << "TransferDataFromRemote: Destination is non-DRAM (type="
                << static_cast<int>(dest_type)
                << "), allocating temp DRAM buffer";

        temp_buffer.reset(allocate_buffer_allocator_memory(total_data_size));
        if (!temp_buffer) {
            LOG(ERROR) << "TransferDataFromRemote: Failed to allocate "
                          "temporary DRAM buffer";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        transfer_dest = temp_buffer.get();
    }

    // Group transfers by segment_name to minimize openSegment calls
    std::unordered_map<std::string, std::vector<size_t>> segment_buffers;
    for (size_t i = 0; i < src_buffers.size(); ++i) {
        segment_buffers[src_buffers[i].segment_name].push_back(i);
    }

    // Submit transfers for each segment
    // Precompute cumulative offsets for each buffer in src_buffers
    std::vector<size_t> buffer_offsets(src_buffers.size());
    size_t running_offset = 0;
    for (size_t i = 0; i < src_buffers.size(); ++i) {
        buffer_offsets[i] = running_offset;
        running_offset += src_buffers[i].size;
    }

    for (const auto& [segment_name, buffer_indices] : segment_buffers) {
        // Open remote segment
        SegmentHandle seg = transfer_engine_->openSegment(segment_name);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "TransferDataFromRemote: Failed to open segment '"
                       << segment_name << "'";
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Prepare transfer requests for this segment
        std::vector<TransferRequest> requests;
        requests.reserve(buffer_indices.size());

        for (size_t idx : buffer_indices) {
            const auto& buffer = src_buffers[idx];
            TransferRequest request;
            request.opcode =
                TransferRequest::READ;  // Read from remote to local dest
            request.source =
                static_cast<char*>(transfer_dest) + buffer_offsets[idx];
            request.target_id = seg;
            request.target_offset = buffer.addr;
            request.length = buffer.size;
            requests.emplace_back(request);
        }

        // Allocate batch ID
        BatchID batch_id = transfer_engine_->allocateBatchID(requests.size());
        if (batch_id == INVALID_BATCH_ID) {
            LOG(ERROR) << "TransferDataFromRemote: Failed to allocate batch ID";
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Submit transfers
        Status submit_status =
            transfer_engine_->submitTransfer(batch_id, requests);
        if (!submit_status.ok()) {
            LOG(ERROR)
                << "TransferDataFromRemote: Failed to submit transfers, error: "
                << submit_status.message();
            transfer_engine_->freeBatchID(batch_id);
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        // Poll for completion
        constexpr int64_t timeout_seconds = 10;
        auto start_time = std::chrono::steady_clock::now();
        bool all_completed = false;

        while (true) {
            // Check timeout
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               now - start_time)
                               .count();
            if (elapsed >= timeout_seconds) {
                LOG(ERROR) << "TransferDataFromRemote: Timeout after "
                           << elapsed << " seconds";
                transfer_engine_->freeBatchID(batch_id);
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }

            // Check status of all tasks in batch
            all_completed = true;
            bool has_failure = false;

            for (size_t i = 0; i < requests.size(); ++i) {
                TransferStatus status;
                Status s =
                    transfer_engine_->getTransferStatus(batch_id, i, status);
                if (!s.ok()) {
                    LOG(ERROR) << "TransferDataFromRemote: Failed to get "
                                  "transfer status for task "
                               << i << ", error: " << s.message();
                    has_failure = true;
                    break;
                }

                if (status.s == TransferStatusEnum::COMPLETED) {
                    continue;
                } else if (status.s == TransferStatusEnum::FAILED ||
                           status.s == TransferStatusEnum::CANCELED ||
                           status.s == TransferStatusEnum::INVALID) {
                    LOG(ERROR)
                        << "TransferDataFromRemote: Transfer task " << i
                        << " failed with status " << static_cast<int>(status.s);
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
                VLOG(1) << "TransferDataFromRemote: All transfers completed "
                           "for segment '"
                        << segment_name << "'";
                break;
            }

            // Short sleep before next poll
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }

        // Free batch ID
        transfer_engine_->freeBatchID(batch_id);
    }

    // If destination is non-DRAM, copy from temp DRAM buffer to destination
    // tier
    if (dest_type != MemoryType::DRAM) {
        VLOG(1) << "TransferDataFromRemote: Copying from temp DRAM to non-DRAM "
                   "tier";

        // Create source DataSource from temp buffer (using RefBuffer)
        DataSource temp_src;
        temp_src.buffer =
            std::make_unique<RefBuffer>(temp_buffer.get(), total_data_size);
        temp_src.type = MemoryType::DRAM;

        // Create destination DataSource (using RefBuffer for destination)
        DataSource temp_dst;
        temp_dst.buffer =
            std::make_unique<RefBuffer>(dest_ptr, total_data_size);
        temp_dst.type = dest_type;

        // Get DataCopier from TieredBackend
        const DataCopier& copier = handle->backend->GetDataCopier();
        auto copy_result = copier.Copy(temp_src, temp_dst);
        if (!copy_result.has_value()) {
            LOG(ERROR) << "TransferDataFromRemote: Failed to copy data from "
                          "temp buffer to tier";
            return tl::make_unexpected(copy_result.error());
        }

        VLOG(1) << "TransferDataFromRemote: Copied " << total_data_size
                << " bytes from temp DRAM buffer to non-DRAM tier";
    }

    return {};
}

}  // namespace mooncake
