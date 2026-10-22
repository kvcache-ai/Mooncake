// Copyright 2025 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/ascend_transport/ascend_direct_transport/local_copy_engine.h"

#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include "transport/transport.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

namespace mooncake {

namespace {
// aclrtPointerGetAttributes reports an imported memfd as UNREGISTERED.
// That memory is still host memory. Copy() classifies a whole slice list
// from the first pointer, so leaving this as DEFAULT sends every slice
// through aclrtMemcpy.
uint32_t HostLikeLocation(uint32_t type) {
    if (type == ACL_MEM_LOCATION_TYPE_UNREGISTERED) {
        return ACL_MEM_LOCATION_TYPE_HOST;
    }
    return type;
}

// Slices per aclrtMemcpyBatch call when a copy is split across threads.
constexpr size_t kParallelMemcpyBatchSize = 122U;

struct ParallelCopyState {
    std::function<void(size_t)> copy_chunk;
    size_t chunk_count = 0;
    size_t worker_count = 0;
    std::atomic<size_t> next_chunk{0};
    std::atomic<size_t> workers_failed{0};
    std::mutex mutex;
    std::condition_variable cv;
    size_t finished_chunks = 0;
    // Set when every memcpy worker failed to enter the caller's ACL context.
    // The caller then copies the remaining chunks itself.
    bool run_on_caller = false;
};

// copy_chunk references the caller's slices. The caller does not return
// until every claimed chunk has finished, so a worker that starts later
// still sees a live slice list.
void RunChunks(ParallelCopyState& state) {
    while (true) {
        const size_t chunk = state.next_chunk.fetch_add(1);
        if (chunk >= state.chunk_count) {
            return;
        }
        state.copy_chunk(chunk);
        std::lock_guard<std::mutex> lock(state.mutex);
        if (++state.finished_chunks == state.chunk_count) {
            state.cv.notify_all();
        }
    }
}
}  // namespace

namespace {
constexpr uint32_t kStreamFlags = ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC;
constexpr size_t kAsyncCopyStreamCount = 6U;

bool UseMemcpyBatchFromEnv() {
    const char* value = std::getenv("ASCEND_USE_MEMCPY_BATCH");
    if (value == nullptr) {
        return false;
    }
    return std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 ||
           std::strcmp(value, "TRUE") == 0;
}
}  // namespace

LocalCopyEngine::LocalCopyEngine()
    : transfer_timeout_(10000), initialized_(false) {}

LocalCopyEngine::~LocalCopyEngine() { Finalize(); }

int LocalCopyEngine::Initialize(int32_t transfer_timeout) {
    transfer_timeout_ = transfer_timeout;
    use_memcpy_batch_ = UseMemcpyBatchFromEnv();
    if (use_memcpy_batch_) {
        LOG(INFO) << "ASCEND_USE_MEMCPY_BATCH is enabled";
    }
    // Create the async streams here when a context is already current, and
    // again on first use for any other device.
    int32_t device_id = 0;
    aclrtContext context = nullptr;
    if (aclrtGetDevice(&device_id) == ACL_ERROR_NONE &&
        aclrtGetCurrentContext(&context) == ACL_ERROR_NONE &&
        context != nullptr) {
        if (EnsureAsyncCopyStreams(device_id, context).size() !=
            kAsyncCopyStreamCount) {
            LOG(ERROR) << "Failed to initialize " << kAsyncCopyStreamCount
                       << " async copy streams";
            return -1;
        }
    }
    initialized_ = true;
    return 0;
}

void LocalCopyEngine::Finalize() {
    std::lock_guard<std::mutex> lock(streams_mu_);
    aclrtContext saved_ctx = nullptr;
    if (aclrtGetCurrentContext(&saved_ctx) != ACL_ERROR_NONE) {
        saved_ctx = nullptr;
    }
    // Capture only saved_ctx: MAKE_GUARD is a 2-arg macro, so a
    // multi-capture lambda would be split on the comma in [].
    MAKE_GUARD(ctx_restore, [saved_ctx]() {
        if (saved_ctx != nullptr) {
            (void)aclrtSetCurrentContext(saved_ctx);
        }
    });

    for (auto& kv : async_stream_groups_) {
        auto& group = kv.second;
        aclError ret = ACL_ERROR_NONE;
        if (group.context != nullptr) {
            ret = aclrtSetCurrentContext(group.context);
        } else {
            ret = aclrtSetDevice(kv.first);
        }
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to switch to device " << kv.first
                       << " context before destroying async copy streams, ret:"
                       << ret << ", errmsg:" << aclGetRecentErrMsg();
        }
        for (aclrtStream stream : group.streams) {
            if (stream != nullptr) {
                (void)aclrtDestroyStream(stream);
            }
        }
        group.streams.clear();
    }
    async_stream_groups_.clear();
    initialized_ = false;
}

void LocalCopyEngine::SetTaskRunner(TaskRunner runner, size_t max_parallelism) {
    task_runner_ = std::move(runner);
    max_parallelism_ = std::max<size_t>(max_parallelism, 1);
}

void* LocalCopyEngine::DestAddr(const std::vector<Slice*>& slice_list,
                                const std::vector<void*>& dest_addrs,
                                size_t index) {
    if (!dest_addrs.empty()) {
        return dest_addrs[index];
    }
    return reinterpret_cast<void*>(slice_list[index]->ascend_direct.dest_addr);
}

void LocalCopyEngine::Copy(TransferRequest::OpCode opcode,
                           const std::vector<Slice*>& slice_list,
                           const std::vector<void*>& dest_addrs) {
    if (slice_list.empty()) {
        return;
    }

    if (!initialized_) {
        LOG(ERROR) << "LocalCopyEngine not initialized";
        for (auto& slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    if (!dest_addrs.empty() && dest_addrs.size() != slice_list.size()) {
        LOG(ERROR) << "Rebased destination count " << dest_addrs.size()
                   << " does not match slice count " << slice_list.size();
        for (auto& slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    auto& first_slice = slice_list[0];
    auto remote_ptr = DestAddr(slice_list, dest_addrs, 0);

    aclrtPtrAttributes src_attrs;
    auto ret = aclrtPointerGetAttributes(first_slice->source_addr, &src_attrs);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed for source, ret:"
                   << ret;
        for (auto& slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    aclrtPtrAttributes dst_attrs;
    ret = aclrtPointerGetAttributes(remote_ptr, &dst_attrs);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed for dest, ret:" << ret;
        for (auto& slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    auto kind = GetMemcpyKind(opcode, src_attrs, dst_attrs);

    // Host-to-host stays synchronous. Every other kind uses 6 async streams.
    // ASCEND_USE_MEMCPY_BATCH opts back into parallel aclrtMemcpyBatch.
    if (kind == ACL_MEMCPY_HOST_TO_HOST || kind == ACL_MEMCPY_DEFAULT) {
        CopyWithSync(opcode, slice_list, dest_addrs, kind);
        return;
    }
    if (use_memcpy_batch_ && task_runner_ && max_parallelism_ > 1) {
        CopyWithParallelBatch(opcode, slice_list, dest_addrs, kind);
        return;
    }
    CopyWithMultiStreamAsync(opcode, slice_list, dest_addrs, kind);
}

aclrtMemcpyKind LocalCopyEngine::GetMemcpyKind(TransferRequest::OpCode opcode,
                                               aclrtPtrAttributes& src_attrs,
                                               aclrtPtrAttributes& dst_attrs) {
    const uint32_t src_type = HostLikeLocation(src_attrs.location.type);
    const uint32_t dst_type = HostLikeLocation(dst_attrs.location.type);
    if (src_type == ACL_MEM_LOCATION_TYPE_HOST &&
        dst_type == ACL_MEM_LOCATION_TYPE_HOST) {
        return ACL_MEMCPY_HOST_TO_HOST;
    } else if (src_type == ACL_MEM_LOCATION_TYPE_DEVICE &&
               dst_type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return ACL_MEMCPY_DEVICE_TO_DEVICE;
    } else if (src_type == ACL_MEM_LOCATION_TYPE_HOST &&
               dst_type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return (opcode == TransferRequest::WRITE) ? ACL_MEMCPY_HOST_TO_DEVICE
                                                  : ACL_MEMCPY_DEVICE_TO_HOST;
    } else if (src_type == ACL_MEM_LOCATION_TYPE_DEVICE &&
               dst_type == ACL_MEM_LOCATION_TYPE_HOST) {
        return (opcode == TransferRequest::WRITE) ? ACL_MEMCPY_DEVICE_TO_HOST
                                                  : ACL_MEMCPY_HOST_TO_DEVICE;
    }
    return ACL_MEMCPY_DEFAULT;
}

aclError LocalCopyEngine::CopyWithBatch(TransferRequest::OpCode opcode,
                                        const std::vector<Slice*>& slice_list,
                                        const std::vector<void*>& dest_addrs,
                                        aclrtMemcpyKind kind, size_t batch_num,
                                        size_t slice_index) const {
    std::vector<void*> void_remote_addrs(batch_num);
    std::vector<void*> void_local_addrs(batch_num);
    std::vector<aclrtMemcpyBatchAttr> attrs(batch_num);
    std::vector<size_t> attrsIds(batch_num);
    std::vector<size_t> sizes(batch_num);
    size_t idx = 0;
    // caller make sure slice_list is not empty
    int32_t device_logic_id = 0;
    CHECK_ACL(aclrtGetDevice(&device_logic_id));
    for (size_t i = 0; i < batch_num; i++) {
        auto device_loc = aclrtMemLocation{
            static_cast<uint32_t>(device_logic_id),
            aclrtMemLocationType::ACL_MEM_LOCATION_TYPE_DEVICE};
        auto host_loc = aclrtMemLocation{
            0, aclrtMemLocationType::ACL_MEM_LOCATION_TYPE_HOST};

        if (kind == ACL_MEMCPY_DEVICE_TO_HOST) {
            attrs[i] = aclrtMemcpyBatchAttr{host_loc, device_loc, {}};
        } else {
            attrs[i] = aclrtMemcpyBatchAttr{device_loc, host_loc, {}};
        }
        attrsIds[i] = idx++;

        auto& slice = slice_list[slice_index + i];
        void_local_addrs[i] = slice->source_addr;
        void_remote_addrs[i] =
            DestAddr(slice_list, dest_addrs, slice_index + i);
        sizes[i] = slice->length;
    }

    size_t fail_idx;
    aclError ret;
    if (opcode == TransferRequest::WRITE) {
        ret = aclrtMemcpyBatch(void_remote_addrs.data(), sizes.data(),
                               void_local_addrs.data(), sizes.data(),
                               sizes.size(), attrs.data(), attrsIds.data(),
                               attrs.size(), &fail_idx);
    } else {
        ret = aclrtMemcpyBatch(void_local_addrs.data(), sizes.data(),
                               void_remote_addrs.data(), sizes.data(),
                               sizes.size(), attrs.data(), attrsIds.data(),
                               attrs.size(), &fail_idx);
    }

    if (ret != ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
        if (ret == ACL_ERROR_NONE) {
            VLOG(1) << "Copy with aclrtMemcpyBatch suc.";
            for (size_t i = 0; i < batch_num; i++) {
                auto& slice = slice_list[slice_index + i];
                slice->markSuccess();
            }
        } else {
            LOG(ERROR) << "aclrtMemcpyBatch failed, ret:" << ret;
            for (size_t i = 0; i < batch_num; i++) {
                auto& slice = slice_list[slice_index + i];
                slice->markFailed();
            }
        }
    }

    return ret;
}

void LocalCopyEngine::CopyBatchChunk(TransferRequest::OpCode opcode,
                                     const std::vector<Slice*>& slice_list,
                                     const std::vector<void*>& dest_addrs,
                                     aclrtMemcpyKind kind, size_t slice_index,
                                     size_t batch_num) {
    auto ret = CopyWithBatch(opcode, slice_list, dest_addrs, kind, batch_num,
                             slice_index);
    if (ret != ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
        return;
    }
    VLOG(1) << "aclrtMemcpyBatch FEATURE_NOT_SUPPORT (207000); "
               "falling back to CopyWithMultiStreamAsync";
    const auto begin =
        slice_list.begin() + static_cast<std::ptrdiff_t>(slice_index);
    std::vector<Slice*> chunk_slices(
        begin, begin + static_cast<std::ptrdiff_t>(batch_num));
    std::vector<void*> chunk_dest_addrs;
    if (!dest_addrs.empty()) {
        const auto dest_begin =
            dest_addrs.begin() + static_cast<std::ptrdiff_t>(slice_index);
        chunk_dest_addrs.assign(
            dest_begin, dest_begin + static_cast<std::ptrdiff_t>(batch_num));
    }
    // Already on a memcpy worker. Submitting back to the pool would wait
    // for a thread that is inside this copy.
    CopyWithMultiStreamAsync(opcode, chunk_slices, chunk_dest_addrs, kind,
                             /*use_pool=*/false);
}

void LocalCopyEngine::CopyWithParallelBatch(
    TransferRequest::OpCode opcode, const std::vector<Slice*>& slice_list,
    const std::vector<void*>& dest_addrs, aclrtMemcpyKind kind) {
    auto state = std::make_shared<ParallelCopyState>();
    state->chunk_count = (slice_list.size() + kParallelMemcpyBatchSize - 1) /
                         kParallelMemcpyBatchSize;
    state->copy_chunk = [this, opcode, &slice_list, &dest_addrs,
                         kind](size_t chunk) {
        const size_t slice_index = chunk * kParallelMemcpyBatchSize;
        const size_t batch_num =
            std::min(kParallelMemcpyBatchSize, slice_list.size() - slice_index);
        CopyBatchChunk(opcode, slice_list, dest_addrs, kind, slice_index,
                       batch_num);
    };

    // The caller is a hixl dispatcher thread. Keep aclrtMemcpyBatch on the
    // memcpy pool and only wait here.
    aclrtContext context = nullptr;
    const bool have_context =
        aclrtGetCurrentContext(&context) == ACL_ERROR_NONE &&
        context != nullptr;
    if (!have_context) {
        LOG(WARNING) << "aclrtGetCurrentContext failed, copying "
                     << slice_list.size() << " slices on the caller, errmsg:"
                     << aclGetRecentErrMsg();
        RunChunks(*state);
        return;
    }

    state->worker_count = std::min(state->chunk_count, max_parallelism_);
    for (size_t i = 0; i < state->worker_count; ++i) {
        task_runner_([state, context] {
            if (aclrtSetCurrentContext(context) != ACL_ERROR_NONE) {
                LOG(ERROR) << "aclrtSetCurrentContext failed for parallel "
                              "local copy, errmsg:"
                           << aclGetRecentErrMsg();
                const size_t failed = state->workers_failed.fetch_add(1) + 1;
                if (failed == state->worker_count) {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    state->run_on_caller = true;
                    state->cv.notify_all();
                }
                return;
            }
            RunChunks(*state);
        });
    }

    {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->cv.wait(lock, [&state] {
            return state->finished_chunks == state->chunk_count ||
                   state->run_on_caller;
        });
    }
    if (state->finished_chunks < state->chunk_count) {
        LOG(WARNING) << "Memcpy pool could not enter the ACL context; "
                        "copying "
                     << slice_list.size() << " slices on the caller";
        RunChunks(*state);
    }
    VLOG(1) << "Copied " << slice_list.size() << " slices with "
            << state->chunk_count << " parallel aclrtMemcpyBatch calls";
}

void LocalCopyEngine::CopyWithSync(TransferRequest::OpCode opcode,
                                   const std::vector<Slice*>& slice_list,
                                   const std::vector<void*>& dest_addrs,
                                   aclrtMemcpyKind kind) {
    for (size_t i = 0; i < slice_list.size(); ++i) {
        auto& slice = slice_list[i];
        auto local_ptr = slice->source_addr;
        auto remote_ptr = DestAddr(slice_list, dest_addrs, i);
        auto len = slice->length;

        aclError ret;
        if (opcode == TransferRequest::WRITE) {
            ret = aclrtMemcpy(remote_ptr, len, local_ptr, len, kind);
        } else {
            ret = aclrtMemcpy(local_ptr, len, remote_ptr, len, kind);
        }

        if (ret == ACL_ERROR_NONE) {
            VLOG(1) << "Copy with aclrtMemcpy suc.";
            slice->markSuccess();
        } else {
            LOG(ERROR) << "aclrtMemcpy failed, ret:" << ret;
            slice->markFailed();
        }
    }
}

std::vector<aclrtStream> LocalCopyEngine::EnsureAsyncCopyStreams(
    int32_t device_id, aclrtContext context) {
    std::lock_guard<std::mutex> lock(streams_mu_);
    auto it = async_stream_groups_.find(device_id);
    if (it != async_stream_groups_.end() &&
        it->second.streams.size() == kAsyncCopyStreamCount) {
        return it->second.streams;
    }

    AsyncStreamGroup group;
    group.context = context;
    group.streams.reserve(kAsyncCopyStreamCount);
    for (size_t i = 0; i < kAsyncCopyStreamCount; ++i) {
        aclrtStream stream = nullptr;
        auto ret = aclrtCreateStreamWithConfig(&stream, 0, kStreamFlags);
        if (ret != ACL_ERROR_NONE || stream == nullptr) {
            LOG(ERROR) << "aclrtCreateStreamWithConfig failed for async copy "
                          "stream "
                       << i << " on device " << device_id << ", ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            for (aclrtStream created : group.streams) {
                (void)aclrtDestroyStream(created);
            }
            return {};
        }
        group.streams.push_back(stream);
    }
    async_stream_groups_[device_id] = group;
    return group.streams;
}

void LocalCopyEngine::CopyWithMultiStreamAsync(
    TransferRequest::OpCode opcode, const std::vector<Slice*>& slice_list,
    const std::vector<void*>& dest_addrs, aclrtMemcpyKind kind, bool use_pool) {
    // One copy owns the streams. Another dispatcher thread waits here
    // instead of submitting onto streams that this copy is synchronizing.
    std::lock_guard<std::mutex> copy_lock(async_copy_mu_);

    int32_t device_id = 0;
    aclrtContext context = nullptr;
    if (aclrtGetDevice(&device_id) != ACL_ERROR_NONE ||
        aclrtGetCurrentContext(&context) != ACL_ERROR_NONE ||
        context == nullptr) {
        LOG(ERROR) << "Async copy cannot read the current device, errmsg:"
                   << aclGetRecentErrMsg();
        for (auto& slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    const auto streams = EnsureAsyncCopyStreams(device_id, context);
    if (streams.size() != kAsyncCopyStreamCount) {
        for (auto& slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    struct StreamWait {
        std::mutex mutex;
        std::condition_variable cv;
        size_t finished = 0;
    };
    auto wait_state = std::make_shared<StreamWait>();
    auto finish_stream = [wait_state]() {
        std::lock_guard<std::mutex> lock(wait_state->mutex);
        if (++wait_state->finished == kAsyncCopyStreamCount) {
            wait_state->cv.notify_all();
        }
    };
    // One pool task per stream, so a single worker cannot take every stream.
    auto submit_stream = [this, opcode, &slice_list, &dest_addrs, kind,
                          streams](size_t stream_index) {
        aclrtStream stream = streams[stream_index];
        std::vector<Slice*> queued;
        queued.reserve(slice_list.size() / kAsyncCopyStreamCount + 1);
        for (size_t index = stream_index; index < slice_list.size();
             index += kAsyncCopyStreamCount) {
            auto* slice = slice_list[index];
            void* local_ptr = slice->source_addr;
            void* remote_ptr = DestAddr(slice_list, dest_addrs, index);
            size_t len = slice->length;
            aclError ret = ACL_ERROR_NONE;
            if (opcode == TransferRequest::WRITE) {
                ret = aclrtMemcpyAsync(remote_ptr, len, local_ptr, len, kind,
                                       stream);
            } else {
                ret = aclrtMemcpyAsync(local_ptr, len, remote_ptr, len, kind,
                                       stream);
            }
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret;
                slice->markFailed();
                continue;
            }
            queued.push_back(slice);
        }
        if (queued.empty()) {
            return;
        }
        aclError ret =
            aclrtSynchronizeStreamWithTimeout(stream, transfer_timeout_);
        if (ret == ACL_ERROR_NONE) {
            for (auto* slice : queued) {
                slice->markSuccess();
            }
            return;
        }
        LOG(ERROR) << "Async copy stream sync failed, ret:" << ret
                   << ", stream_index:" << stream_index;
        (void)aclrtStreamAbort(stream);
        for (auto* slice : queued) {
            slice->markFailed();
        }
    };

    aclrtContext current = nullptr;
    const bool have_pool = use_pool && static_cast<bool>(task_runner_) &&
                           aclrtGetCurrentContext(&current) == ACL_ERROR_NONE &&
                           current != nullptr;
    if (!have_pool) {
        if (use_pool) {
            LOG(WARNING) << "Async copy has no memcpy pool, submitting "
                         << slice_list.size() << " slices on the caller";
        }
        for (size_t stream_index = 0; stream_index < kAsyncCopyStreamCount;
             ++stream_index) {
            submit_stream(stream_index);
        }
        return;
    }

    for (size_t stream_index = 0; stream_index < kAsyncCopyStreamCount;
         ++stream_index) {
        task_runner_([submit_stream, finish_stream, current, stream_index,
                      &slice_list]() {
            auto fail_stream = [&]() {
                for (size_t index = stream_index; index < slice_list.size();
                     index += kAsyncCopyStreamCount) {
                    slice_list[index]->markFailed();
                }
            };
            if (aclrtSetCurrentContext(current) != ACL_ERROR_NONE) {
                LOG(ERROR) << "aclrtSetCurrentContext failed for async copy "
                              "stream "
                           << stream_index
                           << ", errmsg:" << aclGetRecentErrMsg();
                fail_stream();
                finish_stream();
                return;
            }
            submit_stream(stream_index);
            finish_stream();
        });
    }

    {
        std::unique_lock<std::mutex> lock(wait_state->mutex);
        wait_state->cv.wait(lock, [&wait_state] {
            return wait_state->finished == kAsyncCopyStreamCount;
        });
    }
    VLOG(1) << "Async copy submitted " << slice_list.size() << " slices across "
            << kAsyncCopyStreamCount << " streams";
}

}  // namespace mooncake
