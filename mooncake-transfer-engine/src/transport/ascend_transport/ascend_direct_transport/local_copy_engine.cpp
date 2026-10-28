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
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "transport/transport.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

namespace mooncake {

namespace {

constexpr uint32_t kStreamFlags = ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC;
constexpr size_t kFlagBytes = sizeof(uint32_t);
constexpr size_t kFlagStride = 64;
constexpr size_t kCopyStreamCount = 6;

// Only ACL_MEM_LOCATION_TYPE_DEVICE is device memory.
// ACL_MEM_LOCATION_TYPE_HOST is reserved in current CANN; imported memfd
// mappings come back as UNREGISTERED/MANAGED/HOST_NUMA, which must classify as
// host or the copy kind falls back to DEFAULT and takes the slow synchronous
// aclrtMemcpy path.
uint32_t HostLikeLocation(uint32_t type) {
    return type == ACL_MEM_LOCATION_TYPE_DEVICE ? ACL_MEM_LOCATION_TYPE_DEVICE
                                                : ACL_MEM_LOCATION_TYPE_HOST;
}

}  // namespace

volatile uint32_t* LocalCopyEngine::FlagAt(PendingGroup& group, size_t index) {
    auto* base = static_cast<char*>(group.host_flags);
    return reinterpret_cast<volatile uint32_t*>(base + index * kFlagStride);
}

void* LocalCopyEngine::DestAddr(const std::vector<Slice*>& slice_list,
                                const std::vector<void*>& dest_addrs,
                                size_t index) {
    if (!dest_addrs.empty()) {
        return dest_addrs[index];
    }
    return reinterpret_cast<void*>(slice_list[index]->ascend_direct.dest_addr);
}

LocalCopyEngine::LocalCopyEngine() = default;

LocalCopyEngine::~LocalCopyEngine() { Finalize(); }

bool LocalCopyEngine::EnsureStreamsLocked(DeviceState& device) {
    if (device.streams.size() == kCopyStreamCount &&
        device.device_one != nullptr) {
        return true;
    }
    DestroyStreamsLocked(device);
    aclrtContext saved = nullptr;
    (void)aclrtGetCurrentContext(&saved);
    if (aclrtSetCurrentContext(device.context) != ACL_ERROR_NONE) {
        LOG(ERROR) << "Local copy cannot set device context, errmsg:"
                   << aclGetRecentErrMsg();
        return false;
    }
    bool ok = true;
    device.streams.reserve(kCopyStreamCount);
    for (size_t i = 0; i < kCopyStreamCount; ++i) {
        aclrtStream stream = nullptr;
        auto ret = aclrtCreateStreamWithConfig(&stream, 0, kStreamFlags);
        if (ret != ACL_ERROR_NONE || stream == nullptr) {
            LOG(ERROR) << "aclrtCreateStreamWithConfig failed for copy stream "
                       << i << ", ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            ok = false;
            break;
        }
        device.streams.push_back(stream);
    }
    if (ok) {
        // The completion value must live in device memory so the D2H flag
        // write is ordered behind the copies on that stream. A host source
        // would let ACL complete the H2H copy early.
        auto ret = aclrtMalloc(&device.device_one, kFlagBytes,
                               ACL_MEM_MALLOC_HUGE_FIRST);
        if (ret != ACL_ERROR_NONE || device.device_one == nullptr) {
            LOG(ERROR) << "aclrtMalloc device completion flag failed, ret:"
                       << ret;
            device.device_one = nullptr;
            ok = false;
        } else {
            uint32_t one = 1;
            ret = aclrtMemcpy(device.device_one, kFlagBytes, &one, kFlagBytes,
                              ACL_MEMCPY_HOST_TO_DEVICE);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "aclrtMemcpy device completion flag failed, ret:"
                           << ret;
                ok = false;
            }
        }
    }
    if (!ok) {
        DestroyStreamsLocked(device);
    }
    if (saved != nullptr) {
        (void)aclrtSetCurrentContext(saved);
    }
    return ok;
}

void LocalCopyEngine::DestroyStreamsLocked(DeviceState& device) {
    for (aclrtStream stream : device.streams) {
        if (stream != nullptr) {
            (void)aclrtStreamAbort(stream);
            (void)aclrtDestroyStream(stream);
        }
    }
    device.streams.clear();
    if (device.device_one != nullptr) {
        (void)aclrtFree(device.device_one);
        device.device_one = nullptr;
    }
}

void LocalCopyEngine::ReleaseGroupLocked(DeviceState& device, void* handle,
                                         bool success) {
    auto it = device.pending.find(handle);
    if (it == device.pending.end()) {
        return;
    }
    auto& group = it->second;
    for (auto* slice : group.slices) {
        if (success) {
            slice->markSuccess();
        } else {
            slice->markFailed();
        }
    }
    if (group.host_flags != nullptr) {
        (void)aclrtFreeHost(group.host_flags);
        group.host_flags = nullptr;
    }
    device.pending.erase(it);
    delete static_cast<int*>(handle);
}

void LocalCopyEngine::FailAllLocked(DeviceState& device) {
    // Abort the streams before releasing the flags: a running stream may still
    // be writing a flag, and freeing it first would make that write a UAF.
    for (aclrtStream stream : device.streams) {
        if (stream != nullptr) {
            (void)aclrtStreamAbort(stream);
        }
    }
    while (!device.pending.empty()) {
        ReleaseGroupLocked(device, device.pending.begin()->first, false);
    }
    DestroyStreamsLocked(device);
}

void LocalCopyEngine::Poll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& device_entry : devices_) {
        DeviceState& device = *device_entry.second;
        for (auto it = device.pending.begin(); it != device.pending.end();) {
            void* handle = it->first;
            PendingGroup& group = it->second;
            if (!group.issued) {
                // The issue task has not run yet. Do not fail it here: the
                // task still needs to read this group and its flags.
                ++it;
                continue;
            }
            bool failed = group.failed ||
                          std::chrono::steady_clock::now() >= group.deadline;
            bool done = !failed;
            if (done) {
                for (size_t i = 0; i < group.stream_count; ++i) {
                    if (*FlagAt(group, i) != kDoneFlag) {
                        done = false;
                        break;
                    }
                }
            }
            if (done) {
                ReleaseGroupLocked(device, handle, true);
                it = device.pending.begin();
            } else if (failed) {
                ReleaseGroupLocked(device, handle, false);
                FailAllLocked(device);
                break;
            } else {
                ++it;
            }
        }
    }
}

int LocalCopyEngine::Initialize(int32_t transfer_timeout, size_t device_count) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        transfer_timeout_ = transfer_timeout;
        initialized_ = true;
        if (device_count <= 1) {
            int32_t device_id = 0;
            aclrtContext context = nullptr;
            if (aclrtGetDevice(&device_id) == ACL_ERROR_NONE &&
                aclrtGetCurrentContext(&context) == ACL_ERROR_NONE &&
                context != nullptr) {
                auto& device = devices_[device_id];
                if (!device) {
                    device = std::make_unique<DeviceState>();
                    device->context = context;
                }
                if (!EnsureStreamsLocked(*device)) {
                    initialized_ = false;
                    return -1;
                }
            }
        }
    }
    return 0;
}

void LocalCopyEngine::Finalize() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : devices_) {
        DeviceState& device = *entry.second;
        for (aclrtStream stream : device.streams) {
            if (stream != nullptr) {
                (void)aclrtStreamAbort(stream);
            }
        }
        while (!device.pending.empty()) {
            ReleaseGroupLocked(device, device.pending.begin()->first, false);
        }
        DestroyStreamsLocked(device);
    }
    devices_.clear();
    task_runner_ = nullptr;
    initialized_ = false;
}

void LocalCopyEngine::SetTaskRunner(TaskRunner runner) {
    std::lock_guard<std::mutex> lock(mutex_);
    task_runner_ = std::move(runner);
}

aclrtMemcpyKind LocalCopyEngine::GetMemcpyKind(TransferRequest::OpCode opcode,
                                               aclrtPtrAttributes& src_attrs,
                                               aclrtPtrAttributes& dst_attrs) {
    const uint32_t src_type = HostLikeLocation(src_attrs.location.type);
    const uint32_t dst_type = HostLikeLocation(dst_attrs.location.type);
    if (src_type == ACL_MEM_LOCATION_TYPE_HOST &&
        dst_type == ACL_MEM_LOCATION_TYPE_HOST) {
        return ACL_MEMCPY_HOST_TO_HOST;
    }
    if (src_type == ACL_MEM_LOCATION_TYPE_DEVICE &&
        dst_type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return ACL_MEMCPY_DEVICE_TO_DEVICE;
    }
    if (src_type == ACL_MEM_LOCATION_TYPE_HOST &&
        dst_type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return (opcode == TransferRequest::WRITE) ? ACL_MEMCPY_HOST_TO_DEVICE
                                                  : ACL_MEMCPY_DEVICE_TO_HOST;
    }
    if (src_type == ACL_MEM_LOCATION_TYPE_DEVICE &&
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
        aclError ret;
        if (opcode == TransferRequest::WRITE) {
            ret = aclrtMemcpy(remote_ptr, slice->length, local_ptr,
                              slice->length, kind);
        } else {
            ret = aclrtMemcpy(local_ptr, slice->length, remote_ptr,
                              slice->length, kind);
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

void LocalCopyEngine::Copy(TransferRequest::OpCode opcode,
                           const std::vector<Slice*>& slice_list,
                           const std::vector<void*>& dest_addrs) {
    if (slice_list.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!initialized_) {
            for (auto* slice : slice_list) {
                slice->markFailed();
            }
            return;
        }
    }
    if (!dest_addrs.empty() && dest_addrs.size() != slice_list.size()) {
        LOG(ERROR) << "Rebased destination count " << dest_addrs.size()
                   << " does not match slice count " << slice_list.size();
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    aclrtPtrAttributes src_attrs;
    if (aclrtPointerGetAttributes(slice_list[0]->source_addr, &src_attrs) !=
        ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed for source";
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    aclrtPtrAttributes dst_attrs;
    auto* first_dest = DestAddr(slice_list, dest_addrs, 0);
    if (aclrtPointerGetAttributes(first_dest, &dst_attrs) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed for destination";
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    const auto kind = GetMemcpyKind(opcode, src_attrs, dst_attrs);
    struct MergedOp {
        void* local;
        void* remote;
        size_t length;
    };
    std::vector<MergedOp> sorted_ops;
    sorted_ops.reserve(slice_list.size());
    for (size_t i = 0; i < slice_list.size(); ++i) {
        sorted_ops.push_back({slice_list[i]->source_addr,
                              DestAddr(slice_list, dest_addrs, i),
                              slice_list[i]->length});
    }
    // TENT sorts merged candidates by target offset before merging. Do the same
    // so slices that arrive interleaved (for example K/V pairs) still coalesce.
    std::sort(sorted_ops.begin(), sorted_ops.end(),
              [](const MergedOp& a, const MergedOp& b) {
                  if (a.remote != b.remote) {
                      return reinterpret_cast<uintptr_t>(a.remote) <
                             reinterpret_cast<uintptr_t>(b.remote);
                  }
                  return reinterpret_cast<uintptr_t>(a.local) <
                         reinterpret_cast<uintptr_t>(b.local);
              });
    std::vector<MergedOp> merged_ops;
    merged_ops.reserve(sorted_ops.size());
    for (const auto& op : sorted_ops) {
        if (!merged_ops.empty()) {
            auto& last = merged_ops.back();
            if (reinterpret_cast<uintptr_t>(last.local) + last.length ==
                    reinterpret_cast<uintptr_t>(op.local) &&
                reinterpret_cast<uintptr_t>(last.remote) + last.length ==
                    reinterpret_cast<uintptr_t>(op.remote)) {
                last.length += op.length;
                continue;
            }
        }
        merged_ops.push_back(op);
    }
    if (VLOG_IS_ON(1)) {
        size_t total_bytes = 0;
        for (auto* slice : slice_list) {
            total_bytes += slice->length;
        }
        VLOG(1) << "LocalCopy kind=" << static_cast<int>(kind)
                << " slices=" << slice_list.size()
                << " ops=" << merged_ops.size() << " bytes=" << total_bytes
                << " src_type=" << src_attrs.location.type
                << " dst_type=" << dst_attrs.location.type
                << " opcode=" << static_cast<int>(opcode);
    }
    if (kind == ACL_MEMCPY_HOST_TO_HOST || kind == ACL_MEMCPY_DEFAULT) {
        CopyWithSync(opcode, slice_list, dest_addrs, kind);
        return;
    }

    int32_t device_id = 0;
    aclrtContext context = nullptr;
    if (aclrtGetDevice(&device_id) != ACL_ERROR_NONE ||
        aclrtGetCurrentContext(&context) != ACL_ERROR_NONE ||
        context == nullptr) {
        LOG(ERROR) << "Async copy cannot read the current device, errmsg:"
                   << aclGetRecentErrMsg();
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    void* handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& slot = devices_[device_id];
        if (!slot) {
            slot = std::make_unique<DeviceState>();
        }
        slot->context = context;
        if (!EnsureStreamsLocked(*slot)) {
            for (auto* slice : slice_list) {
                slice->markFailed();
            }
            return;
        }
        void* flags = nullptr;
        if (aclrtMallocHost(&flags, kCopyStreamCount * kFlagStride) !=
                ACL_ERROR_NONE ||
            flags == nullptr) {
            LOG(ERROR) << "Alloc local copy host flags failed";
            for (auto* slice : slice_list) {
                slice->markFailed();
            }
            return;
        }
        PendingGroup group;
        group.host_flags = flags;
        group.stream_count = kCopyStreamCount;
        group.slices = slice_list;
        group.deadline = std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(transfer_timeout_);
        for (size_t i = 0; i < kCopyStreamCount; ++i) {
            *FlagAt(group, i) = 0;
        }
        handle = new int(0);
        slot->pending.emplace(handle, std::move(group));
    }

    auto issue = [this, device_id, context, handle, opcode,
                  ops = std::move(merged_ops), kind]() {
        aclrtContext saved = nullptr;
        (void)aclrtGetCurrentContext(&saved);
        if (aclrtSetCurrentContext(context) != ACL_ERROR_NONE) {
            LOG(ERROR) << "Local copy cannot set context for issue, errmsg:"
                       << aclGetRecentErrMsg();
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = devices_.find(device_id);
            if (it != devices_.end()) {
                auto git = it->second->pending.find(handle);
                if (git != it->second->pending.end()) {
                    git->second.failed = true;
                    git->second.issued = true;
                }
            }
            if (saved != nullptr) {
                (void)aclrtSetCurrentContext(saved);
            }
            return;
        }
        DeviceState* device = nullptr;
        PendingGroup* group = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = devices_.find(device_id);
            if (it == devices_.end()) {
                return;
            }
            device = it->second.get();
            auto git = device->pending.find(handle);
            if (git == device->pending.end()) {
                return;
            }
            group = &git->second;
        }
        for (size_t stream_index = 0; stream_index < kCopyStreamCount;
             ++stream_index) {
            aclrtStream stream = device->streams[stream_index];
            for (size_t index = stream_index; index < ops.size();
                 index += kCopyStreamCount) {
                const auto& op = ops[index];
                aclError ret;
                if (opcode == TransferRequest::WRITE) {
                    ret = aclrtMemcpyAsync(op.remote, op.length, op.local,
                                           op.length, kind, stream);
                } else {
                    ret = aclrtMemcpyAsync(op.local, op.length, op.remote,
                                           op.length, kind, stream);
                }
                if (ret != ACL_ERROR_NONE) {
                    std::lock_guard<std::mutex> lock(mutex_);
                    group->failed = true;
                }
            }
            auto* host_flag =
                const_cast<uint32_t*>(FlagAt(*group, stream_index));
            auto ret =
                aclrtMemcpyAsync(host_flag, kFlagBytes, device->device_one,
                                 kFlagBytes, ACL_MEMCPY_DEVICE_TO_HOST, stream);
            if (ret != ACL_ERROR_NONE) {
                std::lock_guard<std::mutex> lock(mutex_);
                group->failed = true;
            }
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            group->issued = true;
        }
        if (saved != nullptr) {
            (void)aclrtSetCurrentContext(saved);
        }
    };

    TaskRunner runner;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        runner = task_runner_;
    }
    if (runner) {
        runner(issue);
    } else {
        issue();
    }
}

}  // namespace mooncake
