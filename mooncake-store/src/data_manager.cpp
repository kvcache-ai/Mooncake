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
#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/data_copier.h"
#include "utils/scoped_vlog_timer.h"
#include "utils.h"

namespace mooncake {
// ================================================================
// Constructor
// ================================================================

DataManager::DataManager(std::unique_ptr<TieredBackend> tiered_backend,
                         std::shared_ptr<TransferEngine> transfer_engine,
                         size_t lock_shard_count,
                         const LocalTransferConfig& local_transfer_config)
    : tiered_backend_(std::move(tiered_backend)),
      transfer_engine_(transfer_engine),
      lock_shard_count_(lock_shard_count > 0 ? lock_shard_count : 1024),
      lock_shards_(lock_shard_count_),
      local_transfer_config_(local_transfer_config) {
    if (!tiered_backend_) {
        LOG(FATAL) << "TieredBackend cannot be null";
    }
    if (!transfer_engine_) {
        LOG(FATAL) << "TransferEngine cannot be null";
    }

    if (local_transfer_config_.mode == LocalTransferMode::MEMCPY &&
        local_transfer_config_.local_memcpy_async_worker_num > 0 &&
        local_transfer_config_.local_memcpy_async_queue_depth > 0) {
        async_memcpy_executor_ = std::make_unique<AsyncMemcpyExecutor>(
            local_transfer_config_.local_memcpy_async_worker_num,
            local_transfer_config_.local_memcpy_async_queue_depth);
    }

    LOG(INFO) << "DataManager initialized with " << lock_shard_count_
              << " lock shards, local_transfer_mode="
              << (local_transfer_config_.mode == LocalTransferMode::TE
                      ? "TE"
                      : "MEMCPY")
              << ", te_endpoint=" << local_transfer_config_.te_endpoint
              << ", async_memcpy_workers="
              << local_transfer_config_.local_memcpy_async_worker_num
              << ", async_memcpy_queue_depth="
              << local_transfer_config_.local_memcpy_async_queue_depth;
}

// ================================================================
// Put
// ================================================================

// TODO: wanyue-wy
// Currently, for performance optimization,
// we only take the shared key lock during the allocation and commit phases.
// This leads to a concurrent race issue. If multiple threads write to the same
// key simultaneously, each thread will be allocated storage and computing
// resources, but ultimately only one thread will succeed in writing, resulting
// in a waste of resources.
// In future, we will add a pre-occupation mechanism in the Allocation stage
// to optimize this issue.
tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> DataManager::Put(
    const std::string& key, std::vector<Slice>& slices) {
    switch (local_transfer_config_.mode) {
        case LocalTransferMode::TE:
            return PutViaTe(key, slices);
        case LocalTransferMode::MEMCPY:
            return PutViaMemcpy(key, slices);
    }
    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
DataManager::PutViaTe(const std::string& key, std::vector<Slice>& slices) {
    // using Te, treat local memory as remote memory
    size_t total_size = 0;
    for (const auto& s : slices) total_size += s.size;
    auto src_buffers = SlicesToRemoteBufferDescs(slices);
    auto validate_result = ValidateRemoteBuffers(src_buffers);
    if (!validate_result) {
        LOG(ERROR) << "PutViaTe: Buffer validation failed"
                   << ", error: " << toString(validate_result.error());
        return tl::unexpected(validate_result.error());
    }

    AllocationHandle alloc_handle;
    {
        std::unique_lock<std::shared_mutex> lock(GetKeyLock(key));

        if (tiered_backend_->Exist(key)) {
            LOG(WARNING) << "PutViaTe: key already exists: " << key;
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }

        auto alloc_result = tiered_backend_->Allocate(total_size);
        if (!alloc_result) {
            auto err = alloc_result.error();
            if (err == ErrorCode::REPLICA_NUM_EXCEEDED ||
                err == ErrorCode::OBJECT_ALREADY_EXISTS ||
                err == ErrorCode::REPLICA_ALREADY_EXISTS) {
                LOG(WARNING)
                    << "PutViaTe: object already exists for key: " << key
                    << ", error code: " << err;
            }
            return tl::unexpected(err);
        }
        alloc_handle = alloc_result.value();
    }

    auto submit_result = SubmitTeTransferInternal(
        alloc_handle, src_buffers, Transport::TransferRequest::READ);
    if (!submit_result) {
        LOG(ERROR) << "PutViaTe: SubmitTeTransferInternal failed"
                   << ", key=" << key
                   << ", error_code=" << toString(submit_result.error());
        return tl::unexpected(submit_result.error());
    }

    return CallableTaskHandle<void>::Create(
        [this, ctx = std::move(*submit_result), alloc_handle,
         key]() mutable -> tl::expected<void, ErrorCode> {
            ScopedVLogTimer timer(1, "DataManager::PutViaTe");
            timer.LogRequest("key=", key);

            auto wait_result = WaitAllTransferBatches(ctx.transfer_batches);
            if (!wait_result) {
                LOG(ERROR) << "PutViaTe: WaitAllTransferBatches failed"
                           << ", key=" << key
                           << ", error_code=" << toString(wait_result.error());
                return tl::unexpected(wait_result.error());
            }

            if (ctx.handle->loc.data.type != MemoryType::DRAM &&
                ctx.temp_buffer) {
                auto& loc_data = ctx.handle->loc.data;
                auto copy_result = CopyFromDRAMBuffer(
                    ctx.temp_buffer.get(),
                    reinterpret_cast<void*>(loc_data.buffer->data()),
                    loc_data.type, loc_data.buffer->size(),
                    ctx.handle->backend);
                if (!copy_result) {
                    LOG(ERROR)
                        << "PutViaTe: CopyFromDRAMBuffer failed"
                        << ", key=" << key
                        << ", error_code=" << toString(copy_result.error());
                    return tl::unexpected(copy_result.error());
                }
            }

            std::unique_lock<std::shared_mutex> lock(GetKeyLock(key));
            auto commit_result = tiered_backend_->Commit(key, alloc_handle);
            if (!commit_result) {
                LOG(WARNING) << "PutViaTe: commit race for key: " << key;
            }
            timer.LogResponse("error_code=", ErrorCode::OK);
            return {};
        });
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
DataManager::PutViaMemcpy(const std::string& key, std::vector<Slice>& slices) {
    if (slices.size() != 1) {
        LOG(ERROR) << "PutLocal in memcpy mode only supports a single slice";
        return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    Slice slice = slices[0];

    AllocationHandle alloc_handle;
    {
        std::unique_lock lock(GetKeyLock(key));

        if (tiered_backend_->Exist(key)) {
            LOG(WARNING) << "Key already exists: " << key;
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }

        auto handle = tiered_backend_->Allocate(slice.size);
        if (!handle.has_value()) {
            LOG(ERROR) << "Failed to allocate space for key: " << key
                       << ", error: " << handle.error();
            return tl::make_unexpected(handle.error());
        }
        alloc_handle = handle.value();
    }

    auto write_fn = [this, key, slice,
                     alloc_handle]() -> tl::expected<void, ErrorCode> {
        DataSource source;
        source.buffer = std::make_unique<RefBuffer>(slice.ptr, slice.size);
        source.type = MemoryType::DRAM;

        auto write_result = tiered_backend_->Write(source, alloc_handle);
        if (!write_result.has_value()) {
            LOG(ERROR) << "Failed to write data for key: " << key
                       << ", error: " << write_result.error();
            return tl::make_unexpected(write_result.error());
        }
        return {};
    };

    auto commit_fn = [this, key,
                      alloc_handle]() -> tl::expected<void, ErrorCode> {
        std::unique_lock lock(GetKeyLock(key));
        auto commit_result = tiered_backend_->Commit(key, alloc_handle);
        if (!commit_result.has_value()) {
            auto err = commit_result.error();
            if (err != ErrorCode::REPLICA_NUM_EXCEEDED &&
                err != ErrorCode::OBJECT_ALREADY_EXISTS &&
                err != ErrorCode::REPLICA_ALREADY_EXISTS) {
                LOG(ERROR) << "Failed to commit data for key: " << key
                           << ", error: " << commit_result.error();
                return tl::make_unexpected(commit_result.error());
            }
        }
        return {};
    };

    if (async_memcpy_executor_) {
        // async write + sync commit
        auto future = async_memcpy_executor_
                          ->SubmitSingleTask<tl::expected<void, ErrorCode>>(
                              std::move(write_fn));
        return CallableTaskHandle<void>::Create(
            [f = std::move(future), commit_fn = std::move(commit_fn),
             key]() mutable -> tl::expected<void, ErrorCode> {
                ScopedVLogTimer timer(1, "DataManager::PutViaMemcpy");
                timer.LogRequest("key=", key);
                auto write_result = f.get();
                if (!write_result) {
                    LOG(ERROR) << "Failed to write data, error: "
                               << write_result.error();
                    return tl::make_unexpected(write_result.error());
                }
                auto commit_result = commit_fn();
                if (!commit_result) {
                    LOG(ERROR) << "Failed to commit data, error: "
                               << commit_result.error();
                    return tl::make_unexpected(commit_result.error());
                }
                timer.LogResponse("error_code=", ErrorCode::OK);
                return {};
            });
    } else {
        // sync write + sync commit
        return CallableTaskHandle<void>::Create(
            [write_fn = std::move(write_fn),
             commit_fn = std::move(commit_fn),
             key]() mutable -> tl::expected<void, ErrorCode> {
                ScopedVLogTimer timer(1, "DataManager::PutViaMemcpy");
                timer.LogRequest("key=", key);
                auto write_result = write_fn();
                if (!write_result) {
                    LOG(ERROR) << "Failed to write data, error: "
                               << write_result.error();
                    return tl::make_unexpected(write_result.error());
                }
                auto commit_result = commit_fn();
                if (!commit_result) {
                    LOG(ERROR) << "Failed to commit data, error: "
                               << commit_result.error();
                    return tl::make_unexpected(commit_result.error());
                }
                timer.LogResponse("error_code=", ErrorCode::OK);
                return {};
            });
    }
}

// ================================================================
// Get
// ================================================================

tl::expected<ReadTaskHandle, ErrorCode> DataManager::Get(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator) {
    auto handle = tiered_backend_->Get(key);
    if (!handle) {
        if (handle.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to get data for key: " << key
                       << ", error_code=" << handle.error();
        }
        return tl::unexpected(handle.error());
    } else if (!handle.value()->loc.data.buffer) {
        LOG(ERROR) << "Failed to get data for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const size_t local_size = handle.value()->loc.data.buffer->size();

    auto alloc_result = allocator->allocate(local_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate space for key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    auto read_buf = std::make_shared<BufferHandle>(std::move(*alloc_result));
    const std::vector<Slice> slices = {{read_buf->ptr(), local_size}};

    auto result = BuildDataCopier(handle.value(), key, slices);
    if (!result) {
        LOG(ERROR) << "Failed to build data copier for key: " << key
                   << ", error_code=" << result.error();
    } else {
        result->read_buf = std::move(read_buf);
    }
    return result;
}

tl::expected<ReadTaskHandle, ErrorCode> DataManager::Get(
    const std::string& key, const std::vector<Slice>& slices) {
    auto handle = tiered_backend_->Get(key);
    if (!handle) {
        if (handle.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to get data for key: " << key
                       << ", error_code=" << handle.error();
        }
        return tl::unexpected(handle.error());
    }
    auto result = BuildDataCopier(handle.value(), key, slices);
    if (!result) {
        LOG(ERROR) << "Failed to build data copier for key: " << key
                   << ", error_code=" << result.error();
    }
    return result;
}

tl::expected<ReadTaskHandle, ErrorCode> DataManager::BuildDataCopier(
    const AllocationHandle& handle, const std::string& key,
    const std::vector<Slice>& slices) {
    if (!handle || !handle->loc.data.buffer) {
        LOG(ERROR) << "Failed to get data for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    switch (local_transfer_config_.mode) {
        case LocalTransferMode::TE:
            return BuildDataCopierViaTe(handle, slices);
        case LocalTransferMode::MEMCPY:
            return BuildDataCopierViaMemcpy(handle, key, slices);
    }
    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
}

tl::expected<ReadTaskHandle, ErrorCode> DataManager::BuildDataCopierViaTe(
    const AllocationHandle& handle, const std::vector<Slice>& slices) {
    // using Te, treat local memory as remote memory
    const size_t source_size = handle->loc.data.buffer->size();
    auto dest_buffers = SlicesToRemoteBufferDescs(slices);
    auto validate_result = ValidateRemoteBuffers(dest_buffers);
    if (!validate_result) {
        LOG(ERROR) << "BuildDataCopierViaTe: Buffer validation failed"
                   << ", error: " << toString(validate_result.error());
        return tl::unexpected(validate_result.error());
    }

    auto submit_result = SubmitTeTransferInternal(
        handle, dest_buffers, Transport::TransferRequest::WRITE);
    if (!submit_result) {
        LOG(ERROR) << "Failed to submit TE read transfer, error_code="
                   << submit_result.error();
        return tl::unexpected(submit_result.error());
    }

    ReadTaskHandle res;
    res.data_size = static_cast<int64_t>(source_size);
    res.task_handle = CallableTaskHandle<void>::Create(
        [this, ctx = std::move(submit_result.value()),
         h = handle]() mutable -> tl::expected<void, ErrorCode> {
            ScopedVLogTimer timer(1, "DataManager::BuildDataCopierViaTe");
            auto wait_result = WaitAllTransferBatches(ctx.transfer_batches);
            if (!wait_result) {
                LOG(ERROR) << "Failed to wait TE read transfer, error_code="
                           << wait_result.error();
                return tl::unexpected(wait_result.error());
            }
            timer.LogResponse("error_code=", ErrorCode::OK);
            return {};
        });
    return res;
}

tl::expected<ReadTaskHandle, ErrorCode> DataManager::BuildDataCopierViaMemcpy(
    const AllocationHandle& handle, const std::string& key,
    const std::vector<Slice>& slices) {
    auto plan_result = BuildLocalCopyPlan(key, handle, slices);
    if (!plan_result) {
        LOG(ERROR) << "Failed to build local copy plan for key: " << key
                   << ", error_code=" << plan_result.error();
        return tl::unexpected(plan_result.error());
    }

    ReadTaskHandle res;
    res.data_size = static_cast<int64_t>(plan_result.value().source_size);

    if (async_memcpy_executor_) {
        auto future = async_memcpy_executor_->SubmitSingleTask<ErrorCode>(
            [plan = plan_result.value()]() {
                return ExecuteLocalCopyPlan(plan);
            });
        res.task_handle = CallableTaskHandle<void>::Create(
            [f = std::move(future),
             key]() mutable -> tl::expected<void, ErrorCode> {
                ScopedVLogTimer timer(1, "DataManager::BuildDataCopierViaMemcpy");
                timer.LogRequest("key=", key);
                ErrorCode ec = f.get();
                if (ec != ErrorCode::OK) {
                    LOG(ERROR) << "Failed to execute local copy plan"
                               << ", key=" << key << ", error_code=" << ec;
                    return tl::unexpected(ec);
                }
                timer.LogResponse("error_code=", ErrorCode::OK);
                return {};
            });
    } else {
        res.task_handle = CallableTaskHandle<void>::Create(
            [plan = std::move(plan_result.value()),
             key]() mutable -> tl::expected<void, ErrorCode> {
                ScopedVLogTimer timer(1, "DataManager::BuildDataCopierViaMemcpy");
                timer.LogRequest("key=", key);
                ErrorCode ec = ExecuteLocalCopyPlan(plan);
                if (ec != ErrorCode::OK) {
                    LOG(ERROR) << "Failed to execute local copy plan"
                               << ", key=" << key << ", error_code=" << ec;
                    return tl::unexpected(ec);
                }
                timer.LogResponse("error_code=", ErrorCode::OK);
                return {};
            });
    }

    return res;
}

tl::expected<LocalCopyPlan, ErrorCode> DataManager::BuildLocalCopyPlan(
    const std::string& key, const AllocationHandle& handle,
    const std::vector<Slice>& slices) const {
    if (!handle) {
        LOG(ERROR) << "Invalid local allocation handle for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const auto& loc = handle->loc;
    if (!loc.data.buffer) {
        LOG(ERROR) << "Allocation handle has null buffer for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const char* src = reinterpret_cast<const char*>(loc.data.buffer->data());
    const size_t src_size = loc.data.buffer->size();
    size_t provided_size = 0;
    for (const auto& s : slices) provided_size += s.size;
    if (provided_size < src_size) {
        LOG(ERROR) << "Buffer too small for local key '" << key
                   << "': required=" << src_size
                   << ", provided=" << provided_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    LocalCopyPlan plan;
    plan.source_handle = handle;
    plan.source_ptr = src;
    plan.source_size = src_size;
    if (slices.size() == 1) {
        plan.use_single_dest = true;
        plan.single_dest_ptr = slices[0].ptr;
        plan.single_dest_size = slices[0].size;
    } else {
        plan.dest_slices = slices;
    }
    return plan;
}

// ================================================================
// Remote data transfer — called by RPC service layer
// ================================================================

// Attention!!!
// This method runs without key lock.
tl::expected<void, ErrorCode> DataManager::ReadRemoteData(
    const std::string& key, const std::vector<RemoteBufferDesc>& dest_buffers) {
    ScopedVLogTimer timer(1, "DataManager::ReadRemoteData");
    timer.LogRequest("key=", key, "buffer_count=", dest_buffers.size());

    auto validate_result = ValidateRemoteBuffers(dest_buffers);
    if (!validate_result) {
        LOG(ERROR) << "ReadRemoteData: Buffer validation failed for key: "
                   << key << ", error: " << toString(validate_result.error());
        timer.LogResponse("error_code=", validate_result.error());
        return tl::make_unexpected(validate_result.error());
    }

    auto handle_result = tiered_backend_->Get(key);
    if (!handle_result.has_value()) {
        LOG(ERROR) << "ReadRemoteData: Failed to get data for key: " << key
                   << ", error: " << toString(handle_result.error());
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    return TransferDataToRemote(handle_result.value(), dest_buffers);
}

tl::expected<void, ErrorCode> DataManager::TransferDataToRemote(
    AllocationHandle handle,
    const std::vector<RemoteBufferDesc>& dest_buffers) {
    auto submit_result = SubmitTeTransferInternal(
        handle, dest_buffers, Transport::TransferRequest::WRITE);
    if (!submit_result) return tl::unexpected(submit_result.error());

    auto wait_result = WaitAllTransferBatches(submit_result->transfer_batches);
    if (!wait_result) {
        LOG(ERROR) << "TransferDataToRemote: WaitAllTransferBatches failed: "
                   << toString(wait_result.error());
        return wait_result;
    }
    return {};
}

tl::expected<UUID, ErrorCode> DataManager::WriteRemoteData(
    const std::string& key, const std::vector<RemoteBufferDesc>& src_buffers,
    std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::WriteRemoteData");
    timer.LogRequest("key=", key, "buffer_count=", src_buffers.size());

    auto validate_result = ValidateRemoteBuffers(src_buffers);
    if (!validate_result) {
        LOG(ERROR) << "WriteRemoteData: Buffer validation failed for key: "
                   << key << ", error: " << toString(validate_result.error());
        timer.LogResponse("error_code=", validate_result.error());
        return tl::make_unexpected(validate_result.error());
    }

    size_t total_size = 0;
    for (const auto& buf : src_buffers) total_size += buf.size;

    AllocationHandle handle;
    {
        std::unique_lock lock(GetKeyLock(key));

        if (tiered_backend_->Exist(key)) {
            LOG(WARNING) << "Key already exists: " << key;
            timer.LogResponse("error_code=", ErrorCode::OBJECT_ALREADY_EXISTS);
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }

        auto handle_result = tiered_backend_->Allocate(total_size, tier_id);
        if (!handle_result.has_value()) {
            LOG(ERROR) << "WriteRemoteData: Failed to allocate space for key: "
                       << key;
            timer.LogResponse("error_code=", handle_result.error());
            return tl::make_unexpected(handle_result.error());
        }
        handle = handle_result.value();
    }

    // Transfer phase — no lock held, RDMA runs concurrently.
    auto transfer_result = TransferDataFromRemote(handle, src_buffers);
    if (!transfer_result.has_value()) {
        LOG(ERROR) << "WriteRemoteData: Transfer failed for key: " << key
                   << ", error: " << toString(transfer_result.error());
        timer.LogResponse("error_code=", transfer_result.error());
        return tl::make_unexpected(transfer_result.error());
    }

    // Commit phase: re-acquire lock, commit the handle.
    UUID result_tier_id;
    {
        std::unique_lock lock(GetKeyLock(key));
        auto commit_result = tiered_backend_->Commit(key, handle);
        if (!commit_result.has_value()) {
            LOG(ERROR) << "WriteRemoteData: Failed to commit data for key: "
                       << key;
            timer.LogResponse("error_code=", commit_result.error());
            return tl::make_unexpected(commit_result.error());
        }
        result_tier_id = handle->loc.tier->GetTierId();
    }

    timer.LogResponse("error_code=", ErrorCode::OK,
                      "transferred_bytes=", total_size);
    return result_tier_id;
}

tl::expected<void, ErrorCode> DataManager::TransferDataFromRemote(
    AllocationHandle handle, const std::vector<RemoteBufferDesc>& src_buffers) {
    auto submit_result = SubmitTeTransferInternal(
        handle, src_buffers, Transport::TransferRequest::READ);
    if (!submit_result) return tl::unexpected(submit_result.error());

    auto wait_result = WaitAllTransferBatches(submit_result->transfer_batches);
    if (!wait_result) {
        LOG(ERROR) << "TransferDataFromRemote: WaitAllTransferBatches failed: "
                   << toString(wait_result.error());
        return wait_result;
    }

    if (submit_result->handle->loc.data.type != MemoryType::DRAM &&
        submit_result->temp_buffer) {
        auto& loc_data = submit_result->handle->loc.data;
        void* local_ptr = reinterpret_cast<void*>(loc_data.buffer->data());
        MemoryType local_type = loc_data.type;
        size_t total_size = loc_data.buffer->size();
        auto backend = submit_result->handle->backend;

        auto copy_result =
            CopyFromDRAMBuffer(submit_result->temp_buffer.get(), local_ptr,
                               local_type, total_size, backend);
        if (!copy_result.has_value()) {
            LOG(ERROR)
                << "TransferDataFromRemote: Failed to copy from temp DRAM "
                   "buffer to destination tier";
            return tl::make_unexpected(copy_result.error());
        }
    }
    return {};
}

// ================================================================
// TE transfer internals
// ================================================================

tl::expected<DataManager::TeSubmitResult, ErrorCode>
DataManager::SubmitTeTransferInternal(
    const AllocationHandle& handle,
    const std::vector<RemoteBufferDesc>& remote_buffers,
    Transport::TransferRequest::OpCode opcode) {
    if (!handle) {
        LOG(ERROR) << "Invalid allocation handle";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto& loc_data = handle->loc.data;
    if (!loc_data.buffer) {
        LOG(ERROR) << "Allocation handle has null buffer";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t total_data_size = loc_data.buffer->size();
    size_t total_remote_size = 0;
    for (const auto& buf : remote_buffers) total_remote_size += buf.size;
    if (total_remote_size != total_data_size) {
        LOG(ERROR) << "Remote buffers total size (" << total_remote_size
                   << ") is not equal to local data size (" << total_data_size
                   << ")";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!transfer_engine_->getMetadata()) {
        LOG(ERROR) << "TransferEngine not initialized";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    void* local_ptr = reinterpret_cast<void*>(loc_data.buffer->data());
    MemoryType local_type = loc_data.type;

    void* transfer_ptr = nullptr;
    std::unique_ptr<void, void (*)(void*)> temp_buffer_owner(nullptr,
                                                             [](void*) {});

    if (opcode == Transport::TransferRequest::WRITE) {
        // WRITE opcode: TE writes local data to remote (outbound)
        auto buffer_result = PrepareDRAMTransferBuffer(
            local_ptr, local_type, total_data_size, handle->backend);
        if (!buffer_result) {
            LOG(ERROR) << "PrepareDRAMTransferBuffer failed, error: "
                       << toString(buffer_result.error());
            return tl::unexpected(buffer_result.error());
        }
        std::tie(transfer_ptr, temp_buffer_owner) =
            std::move(buffer_result.value());
    } else {
        // READ opcode: TE reads remote data into local (inbound)
        auto buffer_result =
            PrepareDRAMReceiveBuffer(local_ptr, local_type, total_data_size);
        if (!buffer_result) {
            LOG(ERROR) << "PrepareDRAMReceiveBuffer failed, error: "
                       << toString(buffer_result.error());
            return tl::unexpected(buffer_result.error());
        }
        std::tie(transfer_ptr, temp_buffer_owner) =
            std::move(buffer_result.value());
    }

    std::unordered_map<std::string, std::vector<size_t>> segment_buffers;
    for (size_t i = 0; i < remote_buffers.size(); ++i) {
        segment_buffers[remote_buffers[i].segment_endpoint].push_back(i);
    }
    std::vector<size_t> buffer_offsets(remote_buffers.size());
    size_t off = 0;
    for (size_t i = 0; i < remote_buffers.size(); ++i) {
        buffer_offsets[i] = off;
        off += remote_buffers[i].size;
    }

    std::vector<std::tuple<BatchID, size_t, std::string>> submitted_batches;
    for (const auto& [endpoint, indices] : segment_buffers) {
        SegmentHandle seg = transfer_engine_->openSegment(endpoint);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment '" << endpoint << "'";
            for (const auto& [bid, n, ep] : submitted_batches)
                transfer_engine_->freeBatchID(bid);
            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        }

        std::vector<Transport::TransferRequest> requests;
        requests.reserve(indices.size());
        for (size_t idx : indices) {
            const auto& buf = remote_buffers[idx];
            size_t offset = buffer_offsets[idx];
            if (offset >= total_data_size) {
                LOG(WARNING) << "Buffer offset " << offset
                             << " is out of range (total size "
                             << total_data_size << ")";
                continue;
            }
            size_t length = std::min(buf.size, total_data_size - offset);
            if (length == 0) {
                LOG(WARNING) << "Buffer length " << length << " is zero";
                continue;
            }

            Transport::TransferRequest req;
            req.opcode = opcode;
            req.source = static_cast<char*>(transfer_ptr) + offset;
            req.target_id = seg;
            req.target_offset = buf.addr;
            req.length = length;
            requests.emplace_back(req);
        }

        if (requests.empty()) {
            LOG(WARNING) << "No valid requests for segment " << endpoint;
            continue;
        }

        auto batch_result = SubmitTransferRequests(endpoint, seg, requests);
        if (!batch_result) {
            LOG(ERROR) << "Failed to submit transfer requests for segment "
                       << endpoint;
            for (const auto& [bid, n, ep] : submitted_batches)
                transfer_engine_->freeBatchID(bid);
            return tl::unexpected(batch_result.error());
        }
        submitted_batches.emplace_back(batch_result.value(), requests.size(),
                                       endpoint);
    }

    if (submitted_batches.empty()) {
        LOG(ERROR) << "No valid batches submitted";
        return tl::unexpected(ErrorCode::TRANSFER_FAIL);
    }

    TeSubmitResult result;
    result.transfer_batches = std::move(submitted_batches);
    result.temp_buffer = std::move(temp_buffer_owner);
    result.handle = handle;
    return result;
}

tl::expected<void, ErrorCode> DataManager::ValidateRemoteBuffers(
    const std::vector<RemoteBufferDesc>& buffers) {
    if (buffers.empty()) {
        LOG(ERROR) << "Empty buffers";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const auto& buffer : buffers) {
        if (buffer.segment_endpoint.empty()) {
            LOG(ERROR) << "Empty segment endpoint in buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.addr == 0) {
            LOG(ERROR) << "Invalid buffer address (null) in buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (buffer.size == 0) {
            LOG(ERROR) << "Invalid buffer size (zero) in buffers";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    return {};
}

static tl::expected<std::unique_ptr<void, void (*)(void*)>, ErrorCode>
AllocateTempDRAMBuffer(size_t total_size) {
    auto deleter = [](void* ptr) {
        if (ptr) free_memory("", ptr);
    };
    std::unique_ptr<void, void (*)(void*)> buf(
        allocate_buffer_allocator_memory(total_size), deleter);
    if (!buf) {
        LOG(ERROR) << "Failed to allocate temporary DRAM buffer";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return buf;
}

tl::expected<std::pair<void*, std::unique_ptr<void, void (*)(void*)>>,
             ErrorCode>
DataManager::PrepareDRAMTransferBuffer(void* source_ptr, MemoryType source_type,
                                       size_t total_size,
                                       TieredBackend* backend) {
    if (source_type == MemoryType::DRAM) {
        return std::make_pair(
            source_ptr,
            std::unique_ptr<void, void (*)(void*)>(nullptr, [](void*) {}));
    }

    VLOG(1) << "PrepareDRAMTransferBuffer: Source is non-DRAM (type="
            << static_cast<int>(source_type)
            << "), allocating temp DRAM buffer";

    auto buf_result = AllocateTempDRAMBuffer(total_size);
    if (!buf_result) {
        LOG(ERROR) << "Failed to allocate temp DRAM buffer";
        return tl::make_unexpected(buf_result.error());
    }
    auto temp_buffer = std::move(buf_result.value());

    DataSource temp_source;
    temp_source.buffer = std::make_unique<RefBuffer>(source_ptr, total_size);
    temp_source.type = source_type;

    DataSource temp_dst;
    temp_dst.buffer =
        std::make_unique<RefBuffer>(temp_buffer.get(), total_size);
    temp_dst.type = MemoryType::DRAM;

    const DataCopier& copier = backend->GetDataCopier();
    auto copy_result = copier.Copy(temp_source, temp_dst);
    if (!copy_result.has_value()) {
        LOG(ERROR) << "PrepareDRAMTransferBuffer: Failed to copy data from tier"
                      " to temp buffer";
        return tl::make_unexpected(copy_result.error());
    }

    VLOG(1) << "PrepareDRAMTransferBuffer: Copied " << total_size
            << " bytes from non-DRAM to temp DRAM buffer";

    void* transfer_ptr = temp_buffer.get();
    return std::make_pair(transfer_ptr, std::move(temp_buffer));
}

tl::expected<std::pair<void*, std::unique_ptr<void, void (*)(void*)>>,
             ErrorCode>
DataManager::PrepareDRAMReceiveBuffer(void* dest_ptr, MemoryType dest_type,
                                      size_t total_size) {
    if (dest_type == MemoryType::DRAM) {
        return std::make_pair(dest_ptr, std::unique_ptr<void, void (*)(void*)>(
                                            nullptr, [](void*) {}));
    }

    VLOG(1) << "PrepareDRAMReceiveBuffer: Destination is non-DRAM (type="
            << static_cast<int>(dest_type) << "), allocating temp DRAM buffer";

    auto buf_result = AllocateTempDRAMBuffer(total_size);
    if (!buf_result) {
        LOG(ERROR) << "Failed to allocate temp DRAM buffer";
        return tl::make_unexpected(buf_result.error());
    }
    auto temp_buffer = std::move(buf_result.value());

    void* transfer_ptr = temp_buffer.get();
    return std::make_pair(transfer_ptr, std::move(temp_buffer));
}

tl::expected<Transport::BatchID, ErrorCode> DataManager::SubmitTransferRequests(
    const std::string& segment_endpoint, Transport::SegmentHandle seg,
    const std::vector<Transport::TransferRequest>& requests) {
    BatchID batch_id = transfer_engine_->allocateBatchID(requests.size());
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    Status submit_status = transfer_engine_->submitTransfer(batch_id, requests);
    if (!submit_status.ok()) {
        LOG(ERROR) << "Failed to submit transfers, error: "
                   << submit_status.message();
        transfer_engine_->freeBatchID(batch_id);
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    return batch_id;
}

tl::expected<void, ErrorCode> DataManager::WaitAllTransferBatches(
    const std::vector<std::tuple<Transport::BatchID, size_t, std::string>>&
        batches) {
    for (size_t i = 0; i < batches.size(); ++i) {
        Transport::BatchID batch_id = std::get<0>(batches[i]);
        size_t num_tasks = std::get<1>(batches[i]);
        const std::string& segment_endpoint = std::get<2>(batches[i]);
        auto wait_result =
            WaitTransferBatch(batch_id, num_tasks, segment_endpoint);
        if (!wait_result.has_value()) {
            LOG(ERROR) << "Transfer failed for segment endpoint '"
                       << segment_endpoint << "'"
                       << ", error: " << toString(wait_result.error())
                       << ", batch_id: " << batch_id << ", index: " << i
                       << ", total_batches: " << batches.size()
                       << ", num_tasks: " << num_tasks;

            // Free remaining batch IDs that haven't been processed yet.
            // Note: WaitTransferBatch already freed the failed batch ID.
            for (size_t j = i + 1; j < batches.size(); ++j) {
                transfer_engine_->freeBatchID(std::get<0>(batches[j]));
            }

            return tl::make_unexpected(wait_result.error());
        }
    }

    return {};
}

tl::expected<void, ErrorCode> DataManager::WaitTransferBatch(
    Transport::BatchID batch_id, size_t num_tasks,
    const std::string& segment_endpoint) {
    constexpr int64_t timeout_seconds = 10;
    auto start_time = std::chrono::steady_clock::now();

    while (true) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed =
            std::chrono::duration_cast<std::chrono::seconds>(now - start_time)
                .count();
        if (elapsed >= timeout_seconds) {
            LOG(ERROR) << "WaitTransferBatch: Timeout after " << elapsed
                       << " seconds for batch " << batch_id
                       << " for segment endpoint '" << segment_endpoint << "'";
            transfer_engine_->freeBatchID(batch_id);
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        bool all_completed = true;
        bool has_failure = false;

        for (size_t i = 0; i < num_tasks; ++i) {
            TransferStatus status;
            Status s = transfer_engine_->getTransferStatus(batch_id, i, status);
            if (!s.ok()) {
                LOG(ERROR) << "Failed to get transfer status for task " << i
                           << " for batch " << batch_id
                           << " for segment endpoint '" << segment_endpoint
                           << "', error: " << s.message();
                has_failure = true;
                break;
            }

            if (status.s == TransferStatusEnum::COMPLETED) {
                continue;
            } else if (status.s == TransferStatusEnum::FAILED ||
                       status.s == TransferStatusEnum::CANCELED ||
                       status.s == TransferStatusEnum::INVALID ||
                       status.s == TransferStatusEnum::TIMEOUT) {
                LOG(ERROR) << "Transfer task " << i << " for batch " << batch_id
                           << " for segment endpoint '" << segment_endpoint
                           << "' failed with status "
                           << static_cast<int>(status.s);
                has_failure = true;
                break;
            } else {
                all_completed = false;
            }
        }  // end for

        if (has_failure) {
            LOG(ERROR) << "Transfer failed in batch_id " << batch_id;
            transfer_engine_->freeBatchID(batch_id);
            return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
        }

        if (all_completed) {
            VLOG(1) << "All transfers completed for batch " << batch_id
                    << " for segment endpoint '" << segment_endpoint << "'";
            break;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }  // end while

    transfer_engine_->freeBatchID(batch_id);
    return {};
}

tl::expected<void, ErrorCode> DataManager::CopyFromDRAMBuffer(
    void* temp_buffer, void* dest_ptr, MemoryType dest_type, size_t total_size,
    TieredBackend* backend) {
    VLOG(1) << "Copying from temp DRAM to non-DRAM tier";

    DataSource temp_src;
    temp_src.buffer = std::make_unique<RefBuffer>(temp_buffer, total_size);
    temp_src.type = MemoryType::DRAM;

    DataSource temp_dst;
    temp_dst.buffer = std::make_unique<RefBuffer>(dest_ptr, total_size);
    temp_dst.type = dest_type;

    const DataCopier& copier = backend->GetDataCopier();
    auto copy_result = copier.Copy(temp_src, temp_dst);
    if (!copy_result.has_value()) {
        LOG(ERROR) << "Failed to copy data from temp buffer to tier";
        return tl::make_unexpected(copy_result.error());
    }

    VLOG(1) << "Copied " << total_size
            << " bytes from temp DRAM buffer to non-DRAM tier";
    return {};
}

// ================================================================
// Buffer helpers
// ================================================================
std::vector<RemoteBufferDesc> DataManager::SlicesToRemoteBufferDescs(
    const std::vector<Slice>& slices) const {
    std::vector<RemoteBufferDesc> buffers;
    buffers.reserve(slices.size());
    for (const auto& s : slices) {
        RemoteBufferDesc buf;
        buf.segment_endpoint = local_transfer_config_.te_endpoint;
        buf.addr = reinterpret_cast<uintptr_t>(s.ptr);
        buf.size = s.size;
        buffers.push_back(std::move(buf));
    }
    return buffers;
}

// ================================================================
// Delete / Exist
// ================================================================

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

bool DataManager::Exist(const std::string& key,
                        std::optional<UUID> tier_id) const {
    return tiered_backend_->Exist(key, tier_id);
}

// ================================================================
// utilities
// ================================================================

// If a client attempts to access data via a read route and the data is not
// found locally, it calls this function to rectify the stale route in master.
void DataManager::RectifyReadRoute(const std::string& key,
                                   std::optional<UUID> tier_id) {
    if (!rectify_wrong_route_fn_) return;

    std::shared_lock lock(GetKeyLock(key));

    if (!tiered_backend_->Exist(key, tier_id)) {
        LOG(WARNING) << "RectifyReadRoute: key not found locally"
                     << (tier_id.has_value()
                             ? " on tier " +
                                   std::to_string(tier_id.value().first) + "_" +
                                   std::to_string(tier_id.value().second)
                             : "")
                     << ", removing replica from master for key: " << key;
        rectify_wrong_route_fn_(key, tier_id);
    }
}

void DataManager::SetRectifyCallback(
    std::function<void(const std::string&, std::optional<UUID>)> fn) {
    rectify_wrong_route_fn_ = std::move(fn);
}

std::vector<TierView> DataManager::GetTierViews() const {
    return tiered_backend_->GetTierViews();
}

}  // namespace mooncake
