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

#include <condition_variable>
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

constexpr uint32_t kStreamFlags = ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC;
constexpr size_t kAsyncCopyStreamCount = 6U;
}  // namespace

LocalCopyEngine::LocalCopyEngine()
    : transfer_timeout_(10000), initialized_(false) {}

LocalCopyEngine::~LocalCopyEngine() { Finalize(); }

int LocalCopyEngine::Initialize(int32_t transfer_timeout, size_t device_count) {
    transfer_timeout_ = transfer_timeout;
    // One device keeps the streams ready for the first copy. Several devices
    // each get a group on the first copy that runs on that device.
    if (device_count <= 1) {
        int32_t device_id = 0;
        aclrtContext context = nullptr;
        if (aclrtGetDevice(&device_id) == ACL_ERROR_NONE &&
            aclrtGetCurrentContext(&context) == ACL_ERROR_NONE &&
            context != nullptr) {
            AsyncStreamGroup* group = GroupFor(device_id);
            std::lock_guard<std::mutex> copy_lock(group->copy_mu);
            if (!EnsureAsyncCopyStreams(group, device_id, context)) {
                LOG(ERROR) << "Failed to initialize " << kAsyncCopyStreamCount
                           << " async copy streams";
                return -1;
            }
        }
    }
    initialized_ = true;
    return 0;
}

void LocalCopyEngine::Finalize() {
    std::lock_guard<std::mutex> lock(groups_mu_);
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
        auto& group = *kv.second;
        std::lock_guard<std::mutex> copy_lock(group.copy_mu);
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

LocalCopyEngine::AsyncStreamGroup* LocalCopyEngine::GroupFor(
    int32_t device_id) {
    std::lock_guard<std::mutex> lock(groups_mu_);
    auto& slot = async_stream_groups_[device_id];
    if (!slot) {
        slot = std::make_unique<AsyncStreamGroup>();
    }
    return slot.get();
}

void LocalCopyEngine::SetTaskRunner(TaskRunner runner) {
    task_runner_ = std::move(runner);
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
    if (kind == ACL_MEMCPY_HOST_TO_HOST || kind == ACL_MEMCPY_DEFAULT) {
        CopyWithSync(opcode, slice_list, dest_addrs, kind);
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

bool LocalCopyEngine::EnsureAsyncCopyStreams(AsyncStreamGroup* group,
                                             int32_t device_id,
                                             aclrtContext context) {
    if (group->streams.size() == kAsyncCopyStreamCount) {
        return true;
    }

    group->context = context;
    group->streams.clear();
    group->streams.reserve(kAsyncCopyStreamCount);
    for (size_t i = 0; i < kAsyncCopyStreamCount; ++i) {
        aclrtStream stream = nullptr;
        auto ret = aclrtCreateStreamWithConfig(&stream, 0, kStreamFlags);
        if (ret != ACL_ERROR_NONE || stream == nullptr) {
            LOG(ERROR) << "aclrtCreateStreamWithConfig failed for async copy "
                          "stream "
                       << i << " on device " << device_id << ", ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            for (aclrtStream created : group->streams) {
                (void)aclrtDestroyStream(created);
            }
            group->streams.clear();
            return false;
        }
        group->streams.push_back(stream);
    }
    return true;
}

void LocalCopyEngine::CopyWithMultiStreamAsync(
    TransferRequest::OpCode opcode, const std::vector<Slice*>& slice_list,
    const std::vector<void*>& dest_addrs, aclrtMemcpyKind kind) {
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
    // One copy owns this device's streams. A copy on another device uses its
    // own group and does not wait here.
    AsyncStreamGroup* group = GroupFor(device_id);
    std::lock_guard<std::mutex> copy_lock(group->copy_mu);
    if (!EnsureAsyncCopyStreams(group, device_id, context)) {
        for (auto& slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    const auto streams = group->streams;

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
    const bool have_pool = static_cast<bool>(task_runner_) &&
                           aclrtGetCurrentContext(&current) == ACL_ERROR_NONE &&
                           current != nullptr;
    if (!have_pool) {
        LOG(WARNING) << "Async copy has no memcpy pool, submitting "
                     << slice_list.size() << " slices on the caller";
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
