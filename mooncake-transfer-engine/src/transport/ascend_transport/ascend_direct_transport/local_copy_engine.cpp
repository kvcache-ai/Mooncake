// Copyright 2025 Huawei Co., Ltd
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
#include <condition_variable>
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
constexpr uint32_t kDoneFlag = 1;

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

LocalCopyEngine::ContextGuard::ContextGuard(aclrtContext target) {
    if (aclrtGetCurrentContext(&saved_) != ACL_ERROR_NONE) {
        saved_ = nullptr;
    }
    if (target != nullptr && aclrtSetCurrentContext(target) != ACL_ERROR_NONE) {
        LOG(ERROR) << "Local copy cannot bind the device context, errmsg:"
                   << aclGetRecentErrMsg();
        return;
    }
    // A thread with no context of its own still works under target; a thread
    // that already had one keeps it for the restore below.
    ok_ = target != nullptr || saved_ != nullptr;
}

LocalCopyEngine::ContextGuard::~ContextGuard() {
    if (ok_ && saved_ != nullptr) {
        (void)aclrtSetCurrentContext(saved_);
    }
}

volatile uint32_t* LocalCopyEngine::FlagAt(void* host_flags, size_t index) {
    auto* base = static_cast<char*>(host_flags);
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

bool LocalCopyEngine::CreateStreamsLocked() {
    DestroyStreamsLocked();
    streams_.reserve(kCopyStreamCount);
    for (size_t i = 0; i < kCopyStreamCount; ++i) {
        aclrtStream stream = nullptr;
        auto ret = aclrtCreateStreamWithConfig(&stream, 0, kStreamFlags);
        if (ret != ACL_ERROR_NONE || stream == nullptr) {
            LOG(ERROR) << "aclrtCreateStreamWithConfig failed for copy stream "
                       << i << ", ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            DestroyStreamsLocked();
            return false;
        }
        streams_.push_back(stream);
    }
    return true;
}

void LocalCopyEngine::DestroyStreamsLocked() {
    // Abort before destroying so queued work is canceled here rather than
    // surfacing on a later generation of streams.
    for (aclrtStream stream : streams_) {
        if (stream != nullptr) {
            (void)aclrtStreamAbort(stream);
            (void)aclrtDestroyStream(stream);
        }
    }
    streams_.clear();
}

void LocalCopyEngine::ReleaseGroupLocked(void* handle, bool success) {
    auto it = groups_.find(handle);
    if (it == groups_.end()) {
        return;
    }
    Group& group = *it->second;
    for (auto* slice : group.slices) {
        if (success) {
            slice->markSuccess();
        } else {
            slice->markFailed();
        }
    }
    if (group.host_flags != nullptr) {
        // The caller either saw every flag done or aborted the streams first,
        // so ACL can no longer be writing this buffer. Free it under the device
        // context, like every other ACL call.
        (void)aclrtFreeHost(group.host_flags);
        group.host_flags = nullptr;
    }
    groups_.erase(it);
}

void LocalCopyEngine::FailAllGroupsLocked() {
    for (auto& [handle, group] : groups_) {
        (void)handle;
        for (auto* slice : group->slices) {
            slice->markFailed();
        }
        if (group->host_flags != nullptr) {
            (void)aclrtFreeHost(group->host_flags);
            group->host_flags = nullptr;
        }
    }
    groups_.clear();
}

void LocalCopyEngine::FailAndRecreateLocked() {
    // Abort and drop the streams first: the aborts cancel any queued D2H flag
    // write, which is what makes releasing the groups' host flags safe. The
    // queued copies of every pending group are canceled with them, so those
    // groups can never complete and are failed here instead of being reported
    // as successes that never happened.
    DestroyStreamsLocked();
    FailAllGroupsLocked();
    if (!CreateStreamsLocked()) {
        LOG(ERROR) << "Local copy cannot recreate its streams; later copies "
                      "will fail";
    }
}

int LocalCopyEngine::Initialize(aclrtContext context, int32_t transfer_timeout,
                                size_t local_engine_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    context_ = context;
    transfer_timeout_ = transfer_timeout;
    // One engine keeps its streams ready for the first copy. A process with
    // several engines copies locally only on the devices it serves, so each of
    // those engines creates its six streams on the first copy instead of
    // leaving them unused from install on.
    const bool lazy_streams = local_engine_count > 1;
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return -1;
    }
    if (!lazy_streams && !CreateStreamsLocked()) {
        return -1;
    }
    // The completion value must live in device memory so the D2H flag write is
    // ordered behind the copies on that stream. A host source would let ACL
    // complete the H2H copy early.
    auto ret = aclrtMalloc(&device_one_, kFlagBytes, ACL_MEM_MALLOC_HUGE_FIRST);
    if (ret != ACL_ERROR_NONE || device_one_ == nullptr) {
        LOG(ERROR) << "aclrtMalloc device completion flag failed, ret:" << ret;
        device_one_ = nullptr;
        DestroyStreamsLocked();
        return -1;
    }
    uint32_t one = kDoneFlag;
    ret = aclrtMemcpy(device_one_, kFlagBytes, &one, kFlagBytes,
                      ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtMemcpy device completion flag failed, ret:" << ret;
        DestroyStreamsLocked();
        (void)aclrtFree(device_one_);
        device_one_ = nullptr;
        return -1;
    }
    initialized_ = true;
    return 0;
}

void LocalCopyEngine::Finalize() {
    std::lock_guard<std::mutex> lock(mutex_);
    ContextGuard guard(context_);
    task_runner_ = nullptr;
    // Abort and destroy the streams before releasing the flags: a stream that
    // is still running may still be writing its group's host flag, and freeing
    // the flag first would leave that write pointing at released memory.
    DestroyStreamsLocked();
    FailAllGroupsLocked();
    if (device_one_ != nullptr) {
        (void)aclrtFree(device_one_);
        device_one_ = nullptr;
    }
    initialized_ = false;
}

void LocalCopyEngine::SetTaskRunner(TaskRunner runner) {
    std::lock_guard<std::mutex> lock(mutex_);
    task_runner_ = std::move(runner);
}

void LocalCopyEngine::Poll() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || groups_.empty()) {
        return;
    }
    ContextGuard guard(context_);
    const auto now = std::chrono::steady_clock::now();
    bool expired = false;
    for (auto it = groups_.begin(); it != groups_.end();) {
        Group& group = *it->second;
        bool done = true;
        for (size_t i = 0; i < group.stream_count; ++i) {
            if (*FlagAt(group.host_flags, i) != kDoneFlag) {
                done = false;
                break;
            }
        }
        if (done) {
            // The flag writes have landed, so ACL is no longer touching this
            // group's buffers.
            void* handle = it->first;
            ReleaseGroupLocked(handle, true);
            it = groups_.begin();
            continue;
        }
        if (now >= group.deadline) {
            expired = true;
            break;
        }
        ++it;
    }
    if (expired) {
        // A group missed its deadline. Recreating the streams drops whatever is
        // still queued on them; every group that shared those streams loses its
        // completion flags, so they all fail together instead of hanging until
        // their own deadline.
        LOG(ERROR) << "Local copy timed out after " << transfer_timeout_
                   << " ms, aborting and recreating the copy streams";
        FailAndRecreateLocked();
    }
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

bool LocalCopyEngine::IssueGroupLocked(
    size_t stream_count, const std::function<bool(size_t)>& issue_stream) {
    if (!task_runner_ || stream_count <= 1) {
        bool ok = true;
        for (size_t i = 0; i < stream_count; ++i) {
            ok = issue_stream(i) && ok;
        }
        return ok;
    }
    // One task per stream, so a worker that is slow to pick up its task cannot
    // hold back the streams the others own. Every task is done issuing before
    // this returns, and none of them waits on a stream; that is why the caller
    // can hold mutex_ across the whole group.
    struct IssueState {
        std::mutex mutex;
        std::condition_variable cv;
        size_t done = 0;
        bool failed = false;
    };
    auto state = std::make_shared<IssueState>();
    for (size_t i = 0; i < stream_count; ++i) {
        task_runner_([this, &issue_stream, state, i]() {
            // The worker issues on this engine's streams, so it needs the
            // engine's context, not whatever the pool thread last had.
            bool ok = false;
            try {
                ContextGuard worker_guard(context_);
                ok = worker_guard.ok() && issue_stream(i);
            } catch (...) {
                // The submitter waits on this counter, so a thrown task must
                // still report itself instead of leaving it blocked forever.
                ok = false;
            }
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->failed = state->failed || !ok;
                ++state->done;
            }
            state->cv.notify_one();
        });
    }
    std::unique_lock<std::mutex> wait_lock(state->mutex);
    state->cv.wait(wait_lock, [&state, stream_count] {
        return state->done == stream_count;
    });
    return !state->failed;
}

void LocalCopyEngine::Copy(TransferRequest::OpCode opcode,
                           const std::vector<Slice*>& slice_list,
                           const std::vector<void*>& dest_addrs) {
    if (slice_list.empty()) {
        return;
    }
    if (!dest_addrs.empty() && dest_addrs.size() != slice_list.size()) {
        LOG(ERROR) << "Rebased destination count " << dest_addrs.size()
                   << " does not match slice count " << slice_list.size();
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    // Classify and coalesce outside the engine lock: this is pure CPU work on
    // the caller's slice list, and holding the lock here would stall Poll().
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

    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || device_one_ == nullptr) {
        LOG(ERROR) << "Local copy engine is not initialized";
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    // An engine of a multi-engine process creates its six streams on its first
    // copy, under the lock and with the device context already bound.
    if (streams_.empty() && !CreateStreamsLocked()) {
        LOG(ERROR) << "Local copy cannot create its streams on first use";
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    auto group = std::make_unique<Group>();
    // An empty group still takes one stream, so that its flag reports the
    // copies that preceded it on that stream.
    group->stream_count =
        std::min(streams_.size(), std::max<size_t>(merged_ops.size(), 1));
    group->slices = slice_list;
    group->deadline = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(transfer_timeout_);
    if (aclrtMallocHost(&group->host_flags,
                        group->stream_count * kFlagStride) != ACL_ERROR_NONE ||
        group->host_flags == nullptr) {
        LOG(ERROR) << "Alloc local copy host flags failed";
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    for (size_t i = 0; i < group->stream_count; ++i) {
        *FlagAt(group->host_flags, i) = 0;
    }

    const size_t stream_count = group->stream_count;
    auto issue_stream = [&](size_t stream_index) {
        aclrtStream stream = streams_[stream_index];
        for (size_t index = stream_index; index < merged_ops.size();
             index += stream_count) {
            const auto& op = merged_ops[index];
            aclError ret;
            if (opcode == TransferRequest::WRITE) {
                ret = aclrtMemcpyAsync(op.remote, op.length, op.local,
                                       op.length, kind, stream);
            } else {
                ret = aclrtMemcpyAsync(op.local, op.length, op.remote,
                                       op.length, kind, stream);
            }
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Local copy aclrtMemcpyAsync failed, ret:" << ret
                           << ", errmsg:" << aclGetRecentErrMsg();
                return false;
            }
        }
        auto* host_flag =
            const_cast<uint32_t*>(FlagAt(group->host_flags, stream_index));
        auto ret =
            aclrtMemcpyAsync(host_flag, kFlagBytes, device_one_, kFlagBytes,
                             ACL_MEMCPY_DEVICE_TO_HOST, stream);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Local copy aclrtMemcpyAsync host flag failed, ret:"
                       << ret << ", errmsg:" << aclGetRecentErrMsg();
            return false;
        }
        return true;
    };

    if (!IssueGroupLocked(stream_count, issue_stream)) {
        // The group has work queued on the streams; the abort in
        // FailAndRecreateLocked cancels it, so it is safe to release the flags
        // and fail the slices after it. This group is not in groups_ yet, so
        // FailAllGroupsLocked does not touch it.
        FailAndRecreateLocked();
        for (auto* slice : slice_list) {
            slice->markFailed();
        }
        (void)aclrtFreeHost(group->host_flags);
        return;
    }
    void* handle = group.get();
    groups_[handle] = std::move(group);
}

}  // namespace mooncake
