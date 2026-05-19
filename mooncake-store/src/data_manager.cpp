#include "data_manager.h"

#include <algorithm>
#include <cstring>
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

namespace {

constexpr uint32_t kDefaultLeaseDurationMs = 5000;
constexpr uint32_t kDefaultLeaseScanIntervalMs = 1000;

struct LocalCopyPlan {
    AllocationHandle source_handle;
    const char* source_ptr = nullptr;
    size_t source_size = 0;
    bool use_single_dest = false;
    void* single_dest_ptr = nullptr;
    size_t single_dest_size = 0;
    std::vector<Slice> dest_slices;
};

tl::expected<LocalCopyPlan, ErrorCode> BuildLocalCopyPlan(
    std::string_view key, const AllocationHandle& handle,
    const std::vector<Slice>& slices) {
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

ErrorCode ExecuteLocalCopyPlan(const LocalCopyPlan& plan) {
    if (plan.use_single_dest) {
        if (!plan.single_dest_ptr) {
            LOG(ERROR) << "Local copy destination buffer is null";
            return ErrorCode::INVALID_PARAMS;
        }
        if (plan.single_dest_size < plan.source_size) {
            LOG(ERROR) << "Local copy destination is too small, required="
                       << plan.source_size
                       << ", provided=" << plan.single_dest_size;
            return ErrorCode::INVALID_PARAMS;
        }
        if (plan.source_size > 0) {
            std::memcpy(plan.single_dest_ptr, plan.source_ptr,
                        plan.source_size);
        }
        return ErrorCode::OK;
    }

    size_t offset = 0;
    for (const auto& slice : plan.dest_slices) {
        if (offset >= plan.source_size) break;
        const size_t copy_size =
            std::min(slice.size, plan.source_size - offset);
        if (copy_size == 0) continue;
        if (!slice.ptr) {
            LOG(ERROR) << "Local copy destination buffer is null";
            return ErrorCode::INVALID_PARAMS;
        }
        std::memcpy(slice.ptr, plan.source_ptr + offset, copy_size);
        offset += copy_size;
    }

    if (offset != plan.source_size) {
        LOG(ERROR) << "Local copy did not complete, copied=" << offset
                   << ", source_size=" << plan.source_size;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

bool IsZeroUUID(const UUID& uuid) {
    return uuid.first == 0 && uuid.second == 0;
}

}  // namespace

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
      pending_write_shards_(lock_shard_count_),
      pinned_key_shards_(lock_shard_count_),
      local_transfer_config_(local_transfer_config) {
    if (!tiered_backend_) {
        LOG(FATAL) << "TieredBackend cannot be null";
    }
    if (!transfer_engine_) {
        LOG(FATAL) << "TransferEngine cannot be null";
    }

    if (local_transfer_config_.mode == LocalTransferMode::MEMCPY &&
        local_transfer_config_.local_memcpy_async_worker_num > 0) {
        async_memcpy_executor_ = std::make_unique<AsyncMemcpyExecutor>(
            local_transfer_config_.local_memcpy_async_worker_num);
    }

    lease_duration_ = std::chrono::milliseconds(
        local_transfer_config_.p2p_key_lease_duration_ms > 0
            ? local_transfer_config_.p2p_key_lease_duration_ms
            : kDefaultLeaseDurationMs);
    const uint32_t scan_ms =
        local_transfer_config_.p2p_key_lease_scan_interval_ms > 0
            ? local_transfer_config_.p2p_key_lease_scan_interval_ms
            : kDefaultLeaseScanIntervalMs;
    lease_scan_interval_ = std::chrono::milliseconds(
        std::max<uint32_t>(1, scan_ms));

    LOG(INFO) << "DataManager initialized with " << lock_shard_count_
              << " lock shards, local_transfer_mode="
              << (local_transfer_config_.mode == LocalTransferMode::TE
                      ? "TE"
                      : "MEMCPY")
              << ", te_endpoint=" << local_transfer_config_.te_endpoint
              << ", async_memcpy_workers="
              << local_transfer_config_.local_memcpy_async_worker_num
              << ", p2p_key_lease_duration_ms=" << lease_duration_.count()
              << ", p2p_key_lease_scan_interval_ms=" << lease_scan_interval_.count();

    // Start background work only after synchronous initialization completes.
    lease_scanner_thread_ = std::thread(&DataManager::LeaseScannerMain, this);
}

DataManager::~DataManager() { Stop(); }

void DataManager::Stop() {
    ShutdownLeaseScanner();
    ClearLeaseRecords();
    if (async_memcpy_executor_) {
        async_memcpy_executor_->Shutdown();
    }
    if (tiered_backend_) {
        tiered_backend_->Stop();
    }
}

void DataManager::ClearLeaseRecords() {
    for (auto& shard : pending_write_shards_) {
        std::unique_lock lock(shard.mutex);
        shard.by_key.clear();
        shard.ordered_list.clear();
    }
    for (auto& shard : pinned_key_shards_) {
        std::unique_lock lock(shard.mutex);
        shard.by_key.clear();
        shard.ordered_list.clear();
    }
}

DataManager::KeyCtx DataManager::BuildKeyCtx(std::string_view key) const {
    KeyCtx ctx;
    // `key` may reference RPC/coro request storage; copy before returning.
    ctx.key.assign(key.data(), key.size());
    ctx.hash = StringHash{}(std::string_view(ctx.key));
    ctx.pending_write_shard_idx =
        pending_write_shards_.empty()
            ? 0
            : (ctx.hash % pending_write_shards_.size());
    ctx.pinned_key_shard_idx =
        pinned_key_shards_.empty() ? 0 : (ctx.hash % pinned_key_shards_.size());
    return ctx;
}

DataManager::PendingWriteShard& DataManager::GetPendingWriteShard(
    const KeyCtx& ctx) {
    return pending_write_shards_[ctx.pending_write_shard_idx];
}

DataManager::PinnedKeyShard& DataManager::GetPinnedKeyShard(const KeyCtx& ctx) {
    return pinned_key_shards_[ctx.pinned_key_shard_idx];
}

bool DataManager::IsExpired(TimePoint deadline) const {
    return deadline <= std::chrono::steady_clock::now();
}

tl::expected<RemoteBufferDesc, ErrorCode> DataManager::BuildRemoteBufferDesc(
    const AllocationHandle& handle) const {
    const auto& loc_data = handle->loc.data;
    if (!loc_data.buffer) {
        LOG(ERROR) << "BuildRemoteBufferDesc: allocation handle has no buffer";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    RemoteBufferDesc remote_buffer;
    remote_buffer.segment_endpoint = local_transfer_config_.te_endpoint;
    remote_buffer.addr =
        reinterpret_cast<uintptr_t>(loc_data.buffer->data());
    remote_buffer.size = loc_data.buffer->size();
    return remote_buffer;
}

size_t DataManager::ScanExpiredPendingWrites(PendingWriteShard& shard,
                                             TimePoint now) {
    size_t removed = 0;
    while (!shard.ordered_list.empty()) {
        auto list_it = shard.ordered_list.begin();
        if (list_it->second > now) {
            break;
        }
        auto record_it = shard.by_key.find(list_it->first);
        if (record_it == shard.by_key.end() ||
            record_it->second.list_it != list_it) {
            shard.ordered_list.erase(list_it);
            continue;
        }
        shard.by_key.erase(record_it);
        shard.ordered_list.erase(list_it);
        ++removed;
    }
    return removed;
}

size_t DataManager::ScanExpiredPinnedKeys(PinnedKeyShard& shard,
                                          TimePoint now) {
    size_t removed = 0;
    while (!shard.ordered_list.empty()) {
        auto list_it = shard.ordered_list.begin();
        if (list_it->second > now) {
            break;
        }
        auto record_it = shard.by_key.find(list_it->first);
        if (record_it == shard.by_key.end() ||
            record_it->second.list_it != list_it) {
            shard.ordered_list.erase(list_it);
            continue;
        }
        shard.by_key.erase(record_it);
        shard.ordered_list.erase(list_it);
        ++removed;
    }
    return removed;
}

tl::expected<AllocationHandle, ErrorCode>
DataManager::LookupPendingWriteHandleInternal(const KeyCtx& ctx,
                                              const UUID& write_operation_id) {
    const auto now = std::chrono::steady_clock::now();
    auto& pending_write_shard = GetPendingWriteShard(ctx);
    std::shared_lock pending_write_shard_lock(pending_write_shard.mutex);
    auto it = pending_write_shard.by_key.find(ctx.key);
    if (it == pending_write_shard.by_key.end()) {
        LOG(ERROR) << "LookupPendingWriteHandle: no pending write for key: "
                   << ctx.key;
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (it->second.deadline <= now) {
        LOG(ERROR) << "LookupPendingWriteHandle: lease expired for key: "
                   << ctx.key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    if (it->second.write_operation_id != write_operation_id) {
        LOG(ERROR) << "LookupPendingWriteHandle: write_operation_id mismatch "
                      "for key: "
                   << ctx.key;
        return tl::unexpected(ErrorCode::INVALID_WRITE);
    }
    return it->second.handle;
}

void DataManager::AbortPendingWriteInternal(const KeyCtx& ctx,
                                            const UUID& write_operation_id) {
    if (ctx.key.empty() || IsZeroUUID(write_operation_id)) return;
    auto& pending_write_shard = GetPendingWriteShard(ctx);
    std::unique_lock pending_write_shard_lock(pending_write_shard.mutex);
    auto it = pending_write_shard.by_key.find(ctx.key);
    if (it == pending_write_shard.by_key.end()) {
        LOG(WARNING) << "AbortPendingWrite: no pending write record for key: "
                     << ctx.key;
        return;
    }
    if (it->second.write_operation_id != write_operation_id) {
        LOG(ERROR) << "AbortPendingWrite: write_operation_id mismatch for key: "
                   << ctx.key;
        return;
    }
    pending_write_shard.ordered_list.erase(it->second.list_it);
    pending_write_shard.by_key.erase(it);
}

void DataManager::ShutdownLeaseScanner() {
    lease_scanner_stop_requested_.store(true);
    lease_scanner_cv_.notify_all();
    if (lease_scanner_thread_.joinable()) {
        lease_scanner_thread_.join();
    }
}

void DataManager::LeaseScannerMain() {
    std::unique_lock<std::mutex> wait_lock(lease_scanner_mutex_);
    while (!lease_scanner_stop_requested_.load()) {
        lease_scanner_cv_.wait_for(wait_lock, lease_scan_interval_, [this]() {
            return lease_scanner_stop_requested_.load();
        });
        if (lease_scanner_stop_requested_.load()) {
            break;
        }
        const auto now = std::chrono::steady_clock::now();
        wait_lock.unlock();
        for (auto& shard : pending_write_shards_) {
            std::unique_lock shard_lock(shard.mutex);
            ScanExpiredPendingWrites(shard, now);
        }
        for (auto& shard : pinned_key_shards_) {
            std::unique_lock shard_lock(shard.mutex);
            ScanExpiredPinnedKeys(shard, now);
        }
        wait_lock.lock();
    }
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
    std::string_view key, std::vector<Slice>& slices) {
    switch (local_transfer_config_.mode) {
        case LocalTransferMode::TE:
            return PutViaTe(key, slices);
        case LocalTransferMode::MEMCPY:
            return PutViaMemcpy(key, slices);
    }
    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
}

// TODO: The returned CallableTaskHandle's WaitAsync() falls back to a
// synchronous Wait() on the coroutine's current thread, because the
// WaitAllTransferBatches() is a loop with no async completion notification.
// Possible optimizations:
//   (1) run a polling coroutine on yalantinglibs coro_io's io_context (via
//       co_await coro_io::sleep_for(100us) + getTransferStatus), no new thread;
//   (2) introduce a lightweight timer service to bridge cv-poll to
//       async_simple::Promise;
//   (3) introduce a completion callback from transfer_engine itself.
// Once any of these lands, switch the return type to FutureHandle.
tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
DataManager::PutViaTe(std::string_view key, std::vector<Slice>& slices) {
    // using Te, treat local memory as remote memory
    const KeyCtx kctx = BuildKeyCtx(key);
    size_t total_size = 0;
    for (const auto& s : slices) total_size += s.size;
    auto src_buffers = SlicesToRemoteBufferDescs(slices);
    auto validate_result = ValidateRemoteBuffers(src_buffers);
    if (!validate_result) {
        LOG(ERROR) << "Buffer validation failed"
                   << ", error: " << toString(validate_result.error());
        return tl::unexpected(validate_result.error());
    }

    auto prewrite_result = PreWriteInternal(kctx, total_size, std::nullopt);
    if (!prewrite_result) {
        LOG(ERROR) << "PutViaTe: PreWrite failed"
                   << ", key=" << key
                   << ", error=" << toString(prewrite_result.error());
        return tl::unexpected(prewrite_result.error());
    }
    const UUID write_operation_id = prewrite_result->write_operation_id;

    auto handle_result =
        LookupPendingWriteHandleInternal(kctx, write_operation_id);
    if (!handle_result) {
        AbortPendingWriteInternal(kctx, write_operation_id);
        return tl::unexpected(handle_result.error());
    }
    AllocationHandle alloc_handle = handle_result.value();

    auto submit_result = SubmitTeTransferInternal(
        alloc_handle, src_buffers, Transport::TransferRequest::READ);
    if (!submit_result) {
        LOG(ERROR) << "SubmitTeTransferInternal failed"
                   << ", key=" << key
                   << ", error_code=" << toString(submit_result.error());
        AbortPendingWriteInternal(kctx, write_operation_id);
        return tl::unexpected(submit_result.error());
    }

    return CallableTaskHandle<void>::Create(
        [this, ctx = std::move(*submit_result), alloc_handle, kctx,
         write_operation_id]() mutable -> tl::expected<void, ErrorCode> {
            ScopedVLogTimer timer(1, "DataManager::PutViaTe");
            timer.LogRequest("key=", kctx.key);

            auto wait_result = WaitAllTransferBatches(ctx.transfer_batches);
            if (!wait_result) {
                LOG(ERROR) << "WaitAllTransferBatches failed"
                           << ", key=" << kctx.key
                           << ", error_code=" << toString(wait_result.error());
                AbortPendingWriteInternal(kctx, write_operation_id);
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
                        << "CopyFromDRAMBuffer failed"
                        << ", key=" << kctx.key
                        << ", error_code=" << toString(copy_result.error());
                    AbortPendingWriteInternal(kctx, write_operation_id);
                    return tl::unexpected(copy_result.error());
                }
            }

            auto commit_result = WriteCommitInternal(kctx, write_operation_id);
            if (!commit_result) {
                LOG(ERROR) << "PutViaTe: WriteCommit failed"
                           << ", key=" << kctx.key
                           << ", error=" << toString(commit_result.error());
                return tl::unexpected(commit_result.error());
            }
            timer.LogResponse("error_code=", ErrorCode::OK);
            return {};
        });
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
DataManager::PutViaMemcpy(std::string_view key, std::vector<Slice>& slices) {
    if (slices.size() != 1) {
        LOG(ERROR) << "PutLocal in memcpy mode only supports a single slice";
        return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    const KeyCtx kctx = BuildKeyCtx(key);
    Slice slice = slices[0];

    auto prewrite_result = PreWriteInternal(kctx, slice.size, std::nullopt);
    if (!prewrite_result) {
        LOG(ERROR) << "PutViaMemcpy: PreWrite failed"
                   << ", key=" << key
                   << ", error=" << toString(prewrite_result.error());
        return tl::unexpected(prewrite_result.error());
    }
    const UUID write_operation_id = prewrite_result->write_operation_id;

    auto handle_result =
        LookupPendingWriteHandleInternal(kctx, write_operation_id);
    if (!handle_result) {
        AbortPendingWriteInternal(kctx, write_operation_id);
        return tl::unexpected(handle_result.error());
    }
    AllocationHandle alloc_handle = handle_result.value();

    auto write_fn = [this, kctx, slice, alloc_handle,
                     write_operation_id]() -> tl::expected<void, ErrorCode> {
        DataSource source;
        source.buffer = std::make_unique<RefBuffer>(slice.ptr, slice.size);
        source.type = MemoryType::DRAM;

        auto write_result = tiered_backend_->Write(source, alloc_handle);
        if (!write_result.has_value()) {
            LOG(ERROR) << "Failed to write data for key: " << kctx.key
                       << ", error: " << write_result.error();
            AbortPendingWriteInternal(kctx, write_operation_id);
            return tl::make_unexpected(write_result.error());
        }
        return {};
    };

    auto commit_fn = [this, kctx,
                      write_operation_id]() -> tl::expected<void, ErrorCode> {
        auto commit_result = WriteCommitInternal(kctx, write_operation_id);
        if (!commit_result) {
            LOG(ERROR) << "Failed to commit data for key: " << kctx.key
                       << ", error: " << commit_result.error();
            return tl::make_unexpected(commit_result.error());
        }
        return {};
    };

    auto write_and_commit = [write_fn = std::move(write_fn),
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
    };

    if (async_memcpy_executor_) {
        auto future = async_memcpy_executor_
                          ->SubmitSingleTask<tl::expected<void, ErrorCode>>(
                              std::move(write_and_commit));
        return FutureHandle<void>::Create(std::shared_ptr<void>{},
                                          std::move(future));
    }
    // No async executor: run synchronously when Wait() is called.
    return CallableTaskHandle<void>::Create(std::move(write_and_commit));
}

// ================================================================
// Get
// ================================================================

tl::expected<ReadTaskHandle, ErrorCode> DataManager::Get(
    std::string_view key, std::shared_ptr<ClientBufferAllocator> allocator) {
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
    std::string_view key, const std::vector<Slice>& slices) {
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
    const AllocationHandle& handle, std::string_view key,
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
    const AllocationHandle& handle, std::string_view key,
    const std::vector<Slice>& slices) {
    auto plan_result = BuildLocalCopyPlan(key, handle, slices);
    if (!plan_result) {
        LOG(ERROR) << "Failed to build local copy plan for key: " << key
                   << ", error_code=" << plan_result.error();
        return tl::unexpected(plan_result.error());
    }

    ReadTaskHandle res;
    res.data_size = static_cast<int64_t>(plan_result.value().source_size);

    auto read_fn = [plan = std::move(plan_result.value()),
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
    };

    if (async_memcpy_executor_) {
        auto future = async_memcpy_executor_
                          ->SubmitSingleTask<tl::expected<void, ErrorCode>>(
                              std::move(read_fn));
        res.task_handle = FutureHandle<void>::Create(std::shared_ptr<void>{},
                                                     std::move(future));
    } else {
        res.task_handle = CallableTaskHandle<void>::Create(std::move(read_fn));
    }

    return res;
}

// ================================================================
// Remote data transfer — called by RPC service layer
// ================================================================

tl::expected<void, ErrorCode> DataManager::ReadRemoteData(
    std::string_view key, const std::vector<RemoteBufferDesc>& dest_buffers) {
    ScopedVLogTimer timer(1, "DataManager::ReadRemoteData");
    timer.LogRequest("key=", key, "buffer_count=", dest_buffers.size());

    auto validate_result = ValidateRemoteBuffers(dest_buffers);
    if (!validate_result) {
        LOG(ERROR) << "ReadRemoteData: Buffer validation failed for key: "
                   << key << ", error: " << toString(validate_result.error());
        timer.LogResponse("error_code=", validate_result.error());
        return tl::make_unexpected(validate_result.error());
    }

    // Reverse RDMA read stays on the direct object-handle path. Only forward
    // RDMA read uses the 3-phase PinKey -> TE Read -> UnPinKey flow.
    auto handle_result = tiered_backend_->Get(key);
    if (!handle_result) {
        LOG(ERROR) << "ReadRemoteData: Get failed"
                   << ", key=" << key
                   << ", error=" << toString(handle_result.error());
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    auto transfer_result =
        TransferDataToRemote(handle_result.value(), dest_buffers);
    if (!transfer_result) {
        LOG(ERROR) << "ReadRemoteData: TransferDataToRemote failed"
                   << ", key=" << key
                   << ", error=" << toString(transfer_result.error());
        timer.LogResponse("error_code=", transfer_result.error());
        return tl::make_unexpected(transfer_result.error());
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
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
    std::string_view key, const std::vector<RemoteBufferDesc>& src_buffers,
    std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::WriteRemoteData");
    timer.LogRequest("key=", key, "buffer_count=", src_buffers.size());
    const KeyCtx kctx = BuildKeyCtx(key);

    auto validate_result = ValidateRemoteBuffers(src_buffers);
    if (!validate_result) {
        LOG(ERROR) << "WriteRemoteData: Buffer validation failed for key: "
                   << key << ", error: " << toString(validate_result.error());
        timer.LogResponse("error_code=", validate_result.error());
        return tl::make_unexpected(validate_result.error());
    }

    size_t total_size = 0;
    for (const auto& buf : src_buffers) total_size += buf.size;

    // Reverse RDMA path: still one RPC, but internally use the 3-phase write
    // model (PreWrite -> transfer -> WriteCommit).
    auto prewrite_result = PreWriteInternal(kctx, total_size, tier_id);
    if (!prewrite_result) {
        timer.LogResponse("error_code=", prewrite_result.error());
        return tl::make_unexpected(prewrite_result.error());
    }
    const UUID write_operation_id = prewrite_result->write_operation_id;

    auto handle_result =
        LookupPendingWriteHandleInternal(kctx, write_operation_id);
    if (!handle_result) {
        AbortPendingWriteInternal(kctx, write_operation_id);
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }
    AllocationHandle handle = handle_result.value();
    UUID result_tier_id = handle->loc.tier->GetTierId();

    // Transfer phase — no long key lock held.
    auto transfer_result = TransferDataFromRemote(handle, src_buffers);
    if (!transfer_result) {
        AbortPendingWriteInternal(kctx, write_operation_id);
        timer.LogResponse("error_code=", transfer_result.error());
        return tl::make_unexpected(transfer_result.error());
    }

    auto commit_result = WriteCommitInternal(kctx, write_operation_id);
    if (!commit_result) {
        timer.LogResponse("error_code=", commit_result.error());
        return tl::make_unexpected(commit_result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK,
                      "transferred_bytes=", total_size);
    return result_tier_id;
}

tl::expected<DataManager::PreWriteResult, ErrorCode> DataManager::PreWrite(
    std::string_view key, size_t size_bytes, std::optional<UUID> tier_id) {
    return PreWriteInternal(BuildKeyCtx(key), size_bytes, tier_id);
}

tl::expected<DataManager::PreWriteResult, ErrorCode>
DataManager::PreWriteInternal(const KeyCtx& ctx, size_t size_bytes,
                              std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::PreWrite");
    timer.LogRequest("key=", ctx.key, "size_bytes=", size_bytes);

    if (ctx.key.empty() || size_bytes == 0) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto now = std::chrono::steady_clock::now();
    const auto deadline = now + lease_duration_;
    const UUID write_operation_id = generate_uuid();

    auto& pending_write_shard = GetPendingWriteShard(ctx);

    const auto rollback_reservation = [&]() {
        std::unique_lock pending_write_shard_lock(pending_write_shard.mutex);
        auto it = pending_write_shard.by_key.find(ctx.key);
        if (it != pending_write_shard.by_key.end() &&
            it->second.write_operation_id == write_operation_id) {
            pending_write_shard.ordered_list.erase(it->second.list_it);
            pending_write_shard.by_key.erase(it);
        }
    };

    // Reserve a pending-write slot (handle filled after Allocate).
    {
        std::unique_lock pending_write_shard_lock(pending_write_shard.mutex);
        auto pending_it = pending_write_shard.by_key.find(ctx.key);
        if (pending_it != pending_write_shard.by_key.end()) {
            if (pending_it->second.deadline <= now) {
                pending_write_shard.ordered_list.erase(
                    pending_it->second.list_it);
                pending_write_shard.by_key.erase(pending_it);
            } else {
                LOG(ERROR) << "PreWrite: key has active pending write lease"
                           << ", key=" << ctx.key
                           << ", error=" << toString(ErrorCode::OBJECT_HAS_LEASE);
                timer.LogResponse("error_code=", ErrorCode::OBJECT_HAS_LEASE);
                return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
            }
        }
        auto list_it = pending_write_shard.ordered_list.emplace(
            pending_write_shard.ordered_list.end(), ctx.key, deadline);
        PendingWriteRecord record;
        record.write_operation_id = write_operation_id;
        record.deadline = deadline;
        record.list_it = list_it;
        pending_write_shard.by_key.insert_or_assign(ctx.key, std::move(record));
    }

    // Slow path: no pending_write_shard_lock during tier allocation.
    if (tiered_backend_->Exist(ctx.key)) {
        LOG(ERROR) << "PreWrite: key already exists"
                   << ", key=" << ctx.key
                   << ", error=" << toString(ErrorCode::OBJECT_ALREADY_EXISTS);
        rollback_reservation();
        timer.LogResponse("error_code=", ErrorCode::OBJECT_ALREADY_EXISTS);
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    auto handle_result = tiered_backend_->Allocate(size_bytes, tier_id);
    if (!handle_result) {
        LOG(ERROR) << "PreWrite: Allocate failed"
                   << ", key=" << ctx.key
                   << ", error=" << toString(handle_result.error());
        rollback_reservation();
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    auto handle = std::move(handle_result.value());
    {
        std::unique_lock pending_write_shard_lock(pending_write_shard.mutex);
        auto it = pending_write_shard.by_key.find(ctx.key);
        if (it == pending_write_shard.by_key.end()) {
            LOG(ERROR) << "PreWrite: reserved pending write record missing"
                       << ", key=" << ctx.key
                       << ", error=" << toString(ErrorCode::OBJECT_NOT_FOUND);
            timer.LogResponse("error_code=", ErrorCode::OBJECT_NOT_FOUND);
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        if (it->second.write_operation_id != write_operation_id) {
            LOG(ERROR) << "PreWrite: write_operation_id mismatch"
                       << ", key=" << ctx.key
                       << ", error=" << toString(ErrorCode::INVALID_WRITE);
            timer.LogResponse("error_code=", ErrorCode::INVALID_WRITE);
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        it->second.handle = handle;
    }

    auto remote_buffer_result = BuildRemoteBufferDesc(handle);
    if (!remote_buffer_result) {
        rollback_reservation();
        timer.LogResponse("error_code=", remote_buffer_result.error());
        return tl::make_unexpected(remote_buffer_result.error());
    }

    PreWriteResult result;
    result.remote_buffer = std::move(remote_buffer_result.value());
    result.write_operation_id = write_operation_id;
    timer.LogResponse("error_code=", ErrorCode::OK);
    return result;
}

tl::expected<void, ErrorCode> DataManager::WriteCommit(
    std::string_view key, const UUID& write_operation_id) {
    return WriteCommitInternal(BuildKeyCtx(key), write_operation_id);
}

tl::expected<void, ErrorCode> DataManager::WriteCommitInternal(
    const KeyCtx& ctx, const UUID& write_operation_id) {
    ScopedVLogTimer timer(1, "DataManager::WriteCommit");
    timer.LogRequest("key=", ctx.key);

    if (ctx.key.empty() || IsZeroUUID(write_operation_id)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto now = std::chrono::steady_clock::now();

    auto& pending_write_shard = GetPendingWriteShard(ctx);
    AllocationHandle handle;

    // Pending: validate token, copy handle, remove lease (always, before Commit).
    {
        std::unique_lock pending_write_shard_lock(pending_write_shard.mutex);
        auto record_it = pending_write_shard.by_key.find(ctx.key);
        if (record_it == pending_write_shard.by_key.end()) {
            LOG(ERROR) << "WriteCommit: no pending write record for key: "
                       << ctx.key;
            timer.LogResponse("error_code=", ErrorCode::OBJECT_NOT_FOUND);
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        if (record_it->second.deadline <= now) {
            pending_write_shard.ordered_list.erase(record_it->second.list_it);
            pending_write_shard.by_key.erase(record_it);
            LOG(ERROR) << "WriteCommit: pending write lease expired"
                       << ", key=" << ctx.key
                       << ", error=" << toString(ErrorCode::LEASE_EXPIRED);
            timer.LogResponse("error_code=", ErrorCode::LEASE_EXPIRED);
            return tl::make_unexpected(ErrorCode::LEASE_EXPIRED);
        }
        if (record_it->second.write_operation_id != write_operation_id) {
            LOG(ERROR) << "WriteCommit: write_operation_id mismatch"
                       << ", key=" << ctx.key
                       << ", error=" << toString(ErrorCode::INVALID_WRITE);
            timer.LogResponse("error_code=", ErrorCode::INVALID_WRITE);
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        if (!record_it->second.handle) {
            LOG(ERROR) << "WriteCommit: pending write has no allocation handle"
                       << ", key=" << ctx.key
                       << ", error=" << toString(ErrorCode::INVALID_WRITE);
            timer.LogResponse("error_code=", ErrorCode::INVALID_WRITE);
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        handle = record_it->second.handle;
        pending_write_shard.ordered_list.erase(record_it->second.list_it);
        pending_write_shard.by_key.erase(record_it);
    }

    // Commit under key_lock to avoid concurrent Delete. Most commit failures
    // are not recoverable by retrying WriteCommit alone; caller must restart
    // PreWrite -> transfer -> WriteCommit. Duplicate WriteCommit -> OBJECT_NOT_FOUND.
    tl::expected<void, ErrorCode> commit_result;
    {
        std::unique_lock<std::shared_mutex> key_lock(GetKeyLock(ctx.key));
        commit_result = tiered_backend_->Commit(ctx.key, handle);
    }

    if (!commit_result) {
        LOG(ERROR) << "WriteCommit: tier commit failed"
                   << ", key=" << ctx.key
                   << ", error=" << toString(commit_result.error());
        timer.LogResponse("error_code=", commit_result.error(),
                          "record_erased=", true);
        return tl::make_unexpected(commit_result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK, "record_erased=", true);
    return {};
}

tl::expected<DataManager::PinKeyResult, ErrorCode> DataManager::PinKey(
    std::string_view key, std::optional<UUID> tier_id) {
    return PinKeyInternal(BuildKeyCtx(key), tier_id);
}

tl::expected<DataManager::PinKeyResult, ErrorCode> DataManager::PinKeyInternal(
    const KeyCtx& ctx, std::optional<UUID> tier_id) {
    ScopedVLogTimer timer(1, "DataManager::PinKey");
    timer.LogRequest("key=", ctx.key);

    if (ctx.key.empty()) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto now = std::chrono::steady_clock::now();
    const auto deadline = now + lease_duration_;

    auto& pin_shard = GetPinnedKeyShard(ctx);

    // Active pin: bump ref_count / renew lease; no tiered_backend::Get.
    const auto bump_existing_pin =
        [&](auto it) -> tl::expected<PinKeyResult, ErrorCode> {
        auto remote_buffer_result = BuildRemoteBufferDesc(it->second.handle);
        if (!remote_buffer_result) {
            timer.LogResponse("error_code=", remote_buffer_result.error());
            return tl::make_unexpected(remote_buffer_result.error());
        }
        it->second.ref_count++;
        it->second.deadline = deadline;
        auto list_it = it->second.list_it;
        list_it->first.assign(ctx.key.data(), ctx.key.size());
        list_it->second = deadline;
        pin_shard.ordered_list.splice(pin_shard.ordered_list.end(),
                                      pin_shard.ordered_list, list_it);

        PinKeyResult result;
        result.remote_buffer = std::move(remote_buffer_result.value());
        result.read_operation_id = it->second.read_operation_id;
        timer.LogResponse("error_code=", ErrorCode::OK,
                          "ref_count=", it->second.ref_count);
        return result;
    };

    // Fast path: valid pin record -> return without tiered_backend::Get.
    {
        std::unique_lock pin_shard_lock(pin_shard.mutex);
        auto record_it = pin_shard.by_key.find(ctx.key);
        if (record_it != pin_shard.by_key.end()) {
            if (record_it->second.deadline <= now) {
                pin_shard.ordered_list.erase(record_it->second.list_it);
                pin_shard.by_key.erase(record_it);
            } else {
                return bump_existing_pin(record_it);
            }
        }
    }

    // Slow path: Get committed object (no pin_shard_lock).
    auto handle_result = tiered_backend_->Get(ctx.key, tier_id);
    if (!handle_result) {
        LOG(ERROR) << "PinKey: Get failed"
                   << ", key=" << ctx.key
                   << ", error=" << toString(handle_result.error());
        timer.LogResponse("error_code=", handle_result.error());
        return tl::make_unexpected(handle_result.error());
    }

    auto handle = std::move(handle_result.value());
    auto remote_buffer_result = BuildRemoteBufferDesc(handle);
    if (!remote_buffer_result) {
        timer.LogResponse("error_code=", remote_buffer_result.error());
        return tl::make_unexpected(remote_buffer_result.error());
    }

    // Re-check after Get: concurrent pin -> bump; else insert new record.
    std::unique_lock pin_shard_lock(pin_shard.mutex);
    auto record_it = pin_shard.by_key.find(ctx.key);
    if (record_it != pin_shard.by_key.end()) {
        if (record_it->second.deadline <= now) {
            pin_shard.ordered_list.erase(record_it->second.list_it);
            pin_shard.by_key.erase(record_it);
        } else {
            return bump_existing_pin(record_it);
        }
    }

    auto list_it = pin_shard.ordered_list.emplace(pin_shard.ordered_list.end(),
                                                  ctx.key, deadline);

    const UUID read_operation_id_value = generate_uuid();
    PinnedKeyRecord record;
    record.read_operation_id = read_operation_id_value;
    record.deadline = deadline;
    record.handle = handle;
    record.ref_count = 1;
    record.list_it = list_it;
    pin_shard.by_key.insert_or_assign(ctx.key, std::move(record));

    PinKeyResult result;
    result.remote_buffer = std::move(remote_buffer_result.value());
    result.read_operation_id = read_operation_id_value;
    timer.LogResponse("error_code=", ErrorCode::OK);
    return result;
}

tl::expected<void, ErrorCode> DataManager::UnPinKey(std::string_view key,
                                                    const UUID& read_operation_id) {
    return UnPinKeyInternal(BuildKeyCtx(key), read_operation_id);
}

tl::expected<void, ErrorCode> DataManager::UnPinKeyInternal(
    const KeyCtx& ctx, const UUID& read_operation_id) {
    ScopedVLogTimer timer(1, "DataManager::UnPinKey");
    timer.LogRequest("key=", ctx.key);

    if (ctx.key.empty() || IsZeroUUID(read_operation_id)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto now = std::chrono::steady_clock::now();

    auto& pin_shard = GetPinnedKeyShard(ctx);
    std::unique_lock pin_shard_lock(pin_shard.mutex);

    auto record_it = pin_shard.by_key.find(ctx.key);
    if (record_it == pin_shard.by_key.end()) {
        LOG(WARNING) << "UnPinKey: no pinned key record for key: " << ctx.key;
        timer.LogResponse("error_code=", ErrorCode::OK, "idempotent=", true);
        return {};
    }
    if (record_it->second.deadline <= now) {
        pin_shard.ordered_list.erase(record_it->second.list_it);
        pin_shard.by_key.erase(record_it);
        timer.LogResponse("error_code=", ErrorCode::LEASE_EXPIRED);
        return tl::make_unexpected(ErrorCode::LEASE_EXPIRED);
    }
    if (record_it->second.read_operation_id != read_operation_id) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_READ);
        return tl::make_unexpected(ErrorCode::INVALID_READ);
    }

    if (record_it->second.ref_count > 1) {
        record_it->second.ref_count--;
        timer.LogResponse("error_code=", ErrorCode::OK,
                          "ref_count=", record_it->second.ref_count);
        return {};
    }

    pin_shard.ordered_list.erase(record_it->second.list_it);
    pin_shard.by_key.erase(record_it);
    timer.LogResponse("error_code=", ErrorCode::OK, "ref_count=", 0);
    return {};
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

    std::vector<std::tuple<Transport::BatchID, size_t, std::string>>
        submitted_batches;
    for (const auto& [endpoint, indices] : segment_buffers) {
        SegmentHandle seg = transfer_engine_->openSegment(endpoint);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment '" << endpoint << "'";
            for (const auto& [bid, n, ep] : submitted_batches) {
                CancelBatchTETask(bid, n);
            }
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
            for (const auto& [bid, n, ep] : submitted_batches) {
                CancelBatchTETask(bid, n);
            }
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
    Transport::BatchID batch_id =
        transfer_engine_->allocateBatchID(requests.size());
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    Status submit_status = transfer_engine_->submitTransfer(batch_id, requests);
    if (!submit_status.ok()) {
        LOG(ERROR) << "Failed to submit transfers, error: "
                   << submit_status.message();
        // Tasks were never dispatched to hardware and will remain in WAITING
        // state indefinitely, so CancelBatchTETask cannot help here.
        auto free_status = transfer_engine_->freeBatchID(batch_id);
        if (!free_status.ok()) {
            LOG(WARNING) << "freeBatchID failed after submitTransfer error"
                         << ", BatchDesc may leak: " << free_status.message();
        }
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

            // Cancel remaining batches that haven't been processed yet.
            // Note: WaitTransferBatch already freed the failed batch ID.
            for (size_t j = i + 1; j < batches.size(); ++j) {
                CancelBatchTETask(std::get<0>(batches[j]),
                                  std::get<1>(batches[j]));
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
            CancelBatchTETask(batch_id, num_tasks);
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
            CancelBatchTETask(batch_id, num_tasks);
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

// freeBatchID() only releases BatchDesc when is_finished=true on every task.
// Then, only COMPLETED and FAILED status of task will trigger to set
// is_finished=true. And there is no cancel API in TransferEngine. Thus, we must
// poll until all tasks reach a terminal state before calling freeBatchID.
void DataManager::CancelBatchTETask(Transport::BatchID batch_id,
                                    size_t num_tasks) {
    constexpr int64_t kDrainTimeoutSeconds = 10;
    auto start = std::chrono::steady_clock::now();
    while (true) {
        bool all_finished = true;
        for (size_t i = 0; i < num_tasks; ++i) {
            TransferStatus status;
            Status s = transfer_engine_->getTransferStatus(batch_id, i, status);
            if (!s.ok() || (status.s != TransferStatusEnum::COMPLETED &&
                            status.s != TransferStatusEnum::FAILED)) {
                all_finished = false;
                break;
            }
        }
        if (all_finished) {
            break;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
        if (elapsed >= kDrainTimeoutSeconds) {
            LOG(WARNING) << "CancelBatchTETask: timed out after " << elapsed
                         << "s for batch " << batch_id
                         << " — BatchDesc may leak";
            return;  // freeBatchID would fail; accept the leak
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    transfer_engine_->freeBatchID(batch_id);
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

tl::expected<size_t, ErrorCode> DataManager::QueryObjectSize(
    std::string_view key) {
    auto handle = tiered_backend_->Get(key);
    if (!handle) {
        return tl::unexpected(handle.error());
    }
    auto& loc = handle.value()->loc;
    size_t size = loc.data.buffer ? loc.data.buffer->size() : 0;
    return size;
}

tl::expected<void, ErrorCode> DataManager::Delete(std::string_view key,
                                                  std::optional<UUID> tier_id,
                                                  bool notify_master) {
    ScopedVLogTimer timer(1, "DataManager::Delete");
    timer.LogRequest("key=", key);

    // NOTE (weak delete semantics):
    // TieredBackend::Delete only removes the metadata entry (or a replica
    // entry) from the in-memory index. It does NOT directly free underlying
    // memory. The actual buffer lifetime is still governed by
    // AllocationHandle's RAII reference counting.
    //
    // We still guard Delete against in-flight 3-phase contexts:
    // - PendingWriteRecord holds a strong handle reference until WriteCommit or
    //   lease cleanup.
    // - PinnedKeyRecord holds a strong handle reference until UnPinKey reaches
    //   ref_count==0 or lease cleanup.
    std::unique_lock lock(GetKeyLock(key));

    auto result = tiered_backend_->Delete(key, tier_id, notify_master);
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to delete key: " << key;
        timer.LogResponse("error_code=", result.error());
        return tl::make_unexpected(result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

bool DataManager::Exist(std::string_view key,
                        std::optional<UUID> tier_id) const {
    return tiered_backend_->Exist(key, tier_id);
}

void DataManager::ForEachKeyBatch(
    const std::function<bool(std::vector<ReplicaLocation>&&)>& callback) const {
    if (tiered_backend_) {
        tiered_backend_->ForEachKeyBatch(callback);
    }
}

AccessStats DataManager::GetHotKeyStats() const {
    if (!tiered_backend_) return {};
    return tiered_backend_->GetHotKeyStats();
}

std::vector<UUID> DataManager::GetReplicaTierIds(std::string_view key) const {
    if (!tiered_backend_) return {};
    return tiered_backend_->GetReplicaTierIds(key);
}

// ================================================================
// utilities
// ================================================================

// If a client attempts to access data via a read route and the data is not
// found locally, it calls this function to rectify the stale route in master.
void DataManager::RectifyReadRoute(std::string_view key,
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
        // Callback signature pins const std::string&; copy at the boundary.
        rectify_wrong_route_fn_(key, tier_id);
    }
}

void DataManager::SetRectifyCallback(
    std::function<void(std::string_view, std::optional<UUID>)> fn) {
    rectify_wrong_route_fn_ = std::move(fn);
}

std::vector<TierView> DataManager::GetTierViews() const {
    return tiered_backend_->GetTierViews();
}

}  // namespace mooncake
