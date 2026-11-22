// Copyright 2026 KVCache.AI
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

#include "tent/transport/ascend/local_copy_engine.h"

#include <glog/logging.h>

#include <algorithm>
#include <condition_variable>

#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {
namespace {
constexpr uint32_t kStreamFlags = ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC;
constexpr uint32_t kDoneFlag = 1;
constexpr size_t kFlagBytes = sizeof(uint32_t);
// One flag per stream, each on its own cache line.
constexpr size_t kFlagStride = 64;
// Matches the classic Ascend transport, whose copy bandwidth needs more than
// one stream to saturate a device link.
constexpr size_t kCopyStreamCount = 6;
}  // namespace

LocalCopyEngine::ContextGuard::ContextGuard(aclrtContext target) {
    // Same as the HIXL engine guard: a caller without a bound context, such as
    // the thread that releases the engine during shutdown, leaves nothing to
    // restore but can still run under the context passed in here.
    if (aclrtGetCurrentContext(&saved_) != ACL_ERROR_NONE) {
        saved_ = nullptr;
    }
    if (target != nullptr && aclrtSetCurrentContext(target) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtSetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return;
    }
    ok_ = target != nullptr || saved_ != nullptr;
}

LocalCopyEngine::ContextGuard::~ContextGuard() {
    if (ok_ && saved_ != nullptr) {
        (void)aclrtSetCurrentContext(saved_);
    }
}

LocalCopyEngine::~LocalCopyEngine() { finalize(); }

void LocalCopyEngine::destroyStreamsLocked() {
    for (size_t i = 0; i < streams_.size(); ++i) {
        auto& stream = streams_[i];
        if (!stream) continue;
        const bool aborted = aclrtStreamAbort(stream) == ACL_ERROR_NONE;
        if (aclrtDestroyStream(stream) == ACL_ERROR_NONE) stream = nullptr;
        if (!aborted && stream) continue;
        // A partial submission may never have queued its completion flag.
        for (auto& entry : groups_) {
            auto& group = *entry.second;
            if (i < group.stream_count) *flagAt(group, i) = kDoneFlag;
        }
    }
    retiring_ = std::any_of(streams_.begin(), streams_.end(),
                            [](aclrtStream s) { return s != nullptr; });
    if (!retiring_) streams_.clear();
}

Status LocalCopyEngine::createStreamsLocked() {
    streams_.reserve(kCopyStreamCount);
    for (size_t i = 0; i < kCopyStreamCount; ++i) {
        aclrtStream stream = nullptr;
        auto ret = aclrtCreateStreamWithConfig(&stream, 0, kStreamFlags);
        if (ret != ACL_ERROR_NONE || stream == nullptr) {
            LOG(ERROR) << "aclrtCreateStreamWithConfig failed for copy stream "
                       << i << ", ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            destroyStreamsLocked();
            return Status::InternalError(
                "Create local copy stream failed" LOC_MARK);
        }
        streams_.push_back(stream);
    }
    return Status::OK();
}

Status LocalCopyEngine::initialize(aclrtContext context, int32_t device_id,
                                   size_t local_engine_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    context_ = context;
    device_id_ = device_id;
    // One engine keeps its streams ready for the first copy. A process with
    // several engines copies locally only on the devices it serves, so each of
    // those engines creates its six streams on the first submit instead of
    // leaving them unused from install on.
    const bool lazy_streams = local_engine_count > 1;
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    if (!lazy_streams) {
        CHECK_STATUS(createStreamsLocked());
    }
    auto tear_down = [this]() {
        destroyStreamsLocked();
        if (device_one_ != nullptr) {
            (void)aclrtFree(device_one_);
            device_one_ = nullptr;
        }
    };
    auto ret = aclrtMalloc(&device_one_, kFlagBytes, ACL_MEM_MALLOC_HUGE_FIRST);
    if (ret != ACL_ERROR_NONE || device_one_ == nullptr) {
        device_one_ = nullptr;
        LOG(ERROR) << "aclrtMalloc device flag failed, ret:" << ret;
        tear_down();
        return Status::InternalError(
            "Alloc local copy device flag failed" LOC_MARK);
    }
    uint32_t one = kDoneFlag;
    ret = aclrtMemcpy(device_one_, kFlagBytes, &one, kFlagBytes,
                      ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtMemcpy device flag failed, ret:" << ret;
        tear_down();
        return Status::InternalError(
            "Init local copy device flag failed" LOC_MARK);
    }
    initialized_ = true;
    return Status::OK();
}

void LocalCopyEngine::finalize() {
    std::lock_guard<std::mutex> lock(mutex_);
    ContextGuard guard(context_);
    task_runner_ = nullptr;
    // Abort and destroy the streams before releasing the flags: a stream that
    // is still running may still be writing its group's host flag, and freeing
    // the flag first would leave that write pointing at released memory.
    initialized_ = false;
    if (!guard.ok()) return;
    destroyStreamsLocked();
    if (retiring_) {
        LOG(ERROR) << "Local copy streams still own their completion flags";
        return;
    }
    for (auto& [handle, group] : groups_) {
        (void)handle;
        if (group) {
            freeHostFlags(*group);
        }
    }
    groups_.clear();
    if (device_one_ != nullptr) {
        (void)aclrtFree(device_one_);
        device_one_ = nullptr;
    }
}

void LocalCopyEngine::setTaskRunner(TaskRunner runner) {
    std::lock_guard<std::mutex> lock(mutex_);
    task_runner_ = std::move(runner);
}

volatile uint32_t* LocalCopyEngine::flagAt(const Group& group, size_t index) {
    auto* base = static_cast<char*>(group.host_flags);
    return reinterpret_cast<volatile uint32_t*>(base + index * kFlagStride);
}

Status LocalCopyEngine::allocHostFlags(Group& group) {
    void* ptr = nullptr;
    auto ret = aclrtMallocHost(&ptr, group.stream_count * kFlagStride);
    if (ret != ACL_ERROR_NONE || ptr == nullptr) {
        group.host_flags = nullptr;
        return Status::InternalError(
            "Alloc local copy host flag failed" LOC_MARK);
    }
    group.host_flags = ptr;
    for (size_t i = 0; i < group.stream_count; ++i) {
        *flagAt(group, i) = kDoneFlag;
    }
    return Status::OK();
}

void LocalCopyEngine::freeHostFlags(Group& group) {
    if (group.host_flags != nullptr) {
        (void)aclrtFreeHost(group.host_flags);
        group.host_flags = nullptr;
    }
}

aclrtMemcpyKind LocalCopyEngine::memcpyKind(void* src, void* dst) {
    aclrtPtrAttributes src_attrs{};
    aclrtPtrAttributes dst_attrs{};
    if (aclrtPointerGetAttributes(src, &src_attrs) != ACL_ERROR_NONE ||
        aclrtPointerGetAttributes(dst, &dst_attrs) != ACL_ERROR_NONE) {
        return ACL_MEMCPY_DEFAULT;
    }
    // ACL_MEM_LOCATION_TYPE_HOST is reserved in current CANN; real host
    // buffers report UNREGISTERED/MANAGED/HOST_NUMA. Only DEVICE is device.
    const bool src_host =
        src_attrs.location.type != ACL_MEM_LOCATION_TYPE_DEVICE;
    const bool dst_host =
        dst_attrs.location.type != ACL_MEM_LOCATION_TYPE_DEVICE;
    if (src_host && dst_host) {
        return ACL_MEMCPY_HOST_TO_HOST;
    }
    if (!src_host && !dst_host) {
        return ACL_MEMCPY_DEVICE_TO_DEVICE;
    }
    if (src_host) {
        return ACL_MEMCPY_HOST_TO_DEVICE;
    }
    return ACL_MEMCPY_DEVICE_TO_HOST;
}

void LocalCopyEngine::failStreamsLocked(TransferStatusEnum failure) {
    for (auto& entry : groups_) {
        auto& group = *entry.second;
        if (group.failure == PENDING) group.failure = failure;
    }
    retiring_ = true;
}

Status LocalCopyEngine::submit(bool write, const std::vector<HixlOpDesc>& ops,
                               void*& req) {
    req = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || device_one_ == nullptr) {
        return Status::InternalError(
            "Local copy engine is not initialized" LOC_MARK);
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    // An engine of a multi-engine process creates its six streams on its first
    // submit, under the lock and with the device context already bound.
    if (retiring_) {
        destroyStreamsLocked();
        if (retiring_)
            return Status::InternalError(
                "Local copy streams still retiring" LOC_MARK);
    }
    if (streams_.empty()) {
        CHECK_STATUS(createStreamsLocked());
    }

    auto owned = std::make_unique<Group>();
    auto* group = owned.get();
    groups_.emplace(group, std::move(owned));
    // An empty group still takes one stream, so that its flag reports the
    // copies that preceded it on that stream.
    group->stream_count =
        std::min(streams_.size(), std::max<size_t>(ops.size(), 1));
    auto allocated = allocHostFlags(*group);
    if (!allocated.ok()) {
        groups_.erase(group);
        return allocated;
    }

    // Each stream takes every stream_count-th op, then closes with the flag
    // copy. The stream keeps those in order, so the flag landing on the host
    // means this stream finished every op it was given.
    const size_t stream_count = group->stream_count;
    auto issue_stream = [&](size_t stream_index) {
        aclrtStream stream = streams_[stream_index];
        auto* flag = flagAt(*group, stream_index);
        *flag = 0;
        for (size_t i = stream_index; i < ops.size(); i += stream_count) {
            const auto& op = ops[i];
            void* local = reinterpret_cast<void*>(op.local_addr);
            void* remote = reinterpret_cast<void*>(op.remote_addr);
            void* src = write ? local : remote;
            void* dst = write ? remote : local;
            auto ret = aclrtMemcpyAsync(dst, op.len, src, op.len,
                                        memcpyKind(src, dst), stream);
            if (ret != ACL_ERROR_NONE) {
                if (i == stream_index) *flag = kDoneFlag;
                LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret
                           << ", errmsg:" << aclGetRecentErrMsg();
                return false;
            }
        }
        auto* host_flag = const_cast<uint32_t*>(flag);
        auto ret =
            aclrtMemcpyAsync(host_flag, kFlagBytes, device_one_, kFlagBytes,
                             ACL_MEMCPY_DEVICE_TO_HOST, stream);
        if (ret != ACL_ERROR_NONE) {
            if (stream_index >= ops.size()) *flag = kDoneFlag;
            LOG(ERROR) << "aclrtMemcpyAsync host flag failed, ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            return false;
        }
        return true;
    };

    bool issued = false;
    try {
        issued = issueGroupLocked(stream_count, issue_stream);
    } catch (const std::bad_alloc&) {
        // Allocating the dispatch state can fail before the first worker runs.
    }
    if (!issued) {
        failStreamsLocked(FAILED);
        destroyStreamsLocked();
        if (!retiring_) {
            freeHostFlags(*group);
            groups_.erase(group);
            return Status::InternalError("aclrtMemcpyAsync failed" LOC_MARK);
        }
    }

    req = group;
    return Status::OK();
}

bool LocalCopyEngine::issueGroupLocked(
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
    // this returns, and none of them waits on a stream.
    struct IssueState {
        std::mutex mutex;
        std::condition_variable cv;
        size_t done = 0;
        bool failed = false;
    };
    auto state = std::make_shared<IssueState>();
    for (size_t i = 0; i < stream_count; ++i) {
        auto issue = [this, &issue_stream, state, i]() {
            // The worker issues on this engine's streams, so it needs the
            // engine's context, not whatever the pool thread last had.
            bool ok = false;
            try {
                ContextGuard worker_guard(context_);
                ok = worker_guard.ok() && issue_stream(i);
            } catch (...) {
                // The submitter waits on this counter, so a thrown task
                // must still report itself instead of leaving it blocked
                // forever.
                ok = false;
            }
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->failed = state->failed || !ok;
                ++state->done;
            }
            state->cv.notify_one();
        };
        try {
            task_runner_(std::move(issue));
        } catch (...) {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->failed = true;
            state->done += stream_count - i;
            break;
        }
    }
    std::unique_lock<std::mutex> wait_lock(state->mutex);
    state->cv.wait(wait_lock, [&state, stream_count] {
        return state->done == stream_count;
    });
    return !state->failed;
}

TransferStatusEnum LocalCopyEngine::poll(void* req, bool timed_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = groups_.find(req);
    if (it == groups_.end() || !it->second) {
        return TransferStatusEnum::FAILED;
    }
    const auto& group = *it->second;
    if (timed_out && group.failure == PENDING) failStreamsLocked(TIMEOUT);
    if (retiring_) {
        ContextGuard guard(context_);
        if (guard.ok()) destroyStreamsLocked();
    }
    for (size_t i = 0; i < group.stream_count; ++i) {
        if (*flagAt(group, i) != kDoneFlag) {
            return TransferStatusEnum::PENDING;
        }
    }
    return group.failure == PENDING ? COMPLETED : group.failure;
}

void LocalCopyEngine::release(void* req) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = groups_.find(req);
    if (it == groups_.end()) {
        return;
    }
    if (it->second) {
        freeHostFlags(*it->second);
    }
    groups_.erase(it);
}

}  // namespace tent
}  // namespace mooncake
