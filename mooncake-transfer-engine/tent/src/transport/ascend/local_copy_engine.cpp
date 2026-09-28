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

#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {
namespace {
constexpr uint32_t kStreamFlags = ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC;
constexpr uint32_t kDoneFlag = 1;
constexpr size_t kFlagBytes = sizeof(uint32_t);
}  // namespace

LocalCopyEngine::ContextGuard::ContextGuard(aclrtContext target) {
    ok_ = (aclrtGetCurrentContext(&saved_) == ACL_ERROR_NONE);
    if (!ok_) {
        LOG(ERROR) << "aclrtGetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        return;
    }
    if (target != nullptr && aclrtSetCurrentContext(target) != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtSetCurrentContext failed, errmsg: "
                   << aclGetRecentErrMsg();
        ok_ = false;
    }
}

LocalCopyEngine::ContextGuard::~ContextGuard() {
    if (ok_ && saved_ != nullptr) {
        (void)aclrtSetCurrentContext(saved_);
    }
}

LocalCopyEngine::~LocalCopyEngine() { finalize(); }

Status LocalCopyEngine::createStreamLocked() {
    if (stream_ != nullptr) {
        // A failed submit or timeout can leave work queued on the old stream.
        // Abort it before destroying the stream so a later device copy cannot
        // observe stale work from the previous generation.
        (void)aclrtStreamAbort(stream_);
        (void)aclrtDestroyStream(stream_);
        stream_ = nullptr;
    }
    auto ret = aclrtCreateStreamWithConfig(&stream_, 0, kStreamFlags);
    if (ret != ACL_ERROR_NONE || stream_ == nullptr) {
        stream_ = nullptr;
        LOG(ERROR) << "aclrtCreateStreamWithConfig failed, ret:" << ret
                   << ", errmsg:" << aclGetRecentErrMsg();
        return Status::InternalError(
            "Create local copy stream failed" LOC_MARK);
    }
    return Status::OK();
}

Status LocalCopyEngine::initialize(aclrtContext context, int32_t device_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    context_ = context;
    device_id_ = device_id;
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }
    CHECK_STATUS(createStreamLocked());
    auto tear_down = [this]() {
        if (stream_ != nullptr) {
            (void)aclrtStreamAbort(stream_);
            (void)aclrtDestroyStream(stream_);
            stream_ = nullptr;
        }
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
    for (auto& [handle, group] : groups_) {
        (void)handle;
        if (group) {
            freeHostFlag(group->host_flag);
        }
    }
    groups_.clear();
    if (stream_ != nullptr) {
        (void)aclrtStreamAbort(stream_);
        (void)aclrtDestroyStream(stream_);
        stream_ = nullptr;
    }
    if (device_one_ != nullptr) {
        (void)aclrtFree(device_one_);
        device_one_ = nullptr;
    }
    initialized_ = false;
}

Status LocalCopyEngine::allocHostFlag(volatile uint32_t*& flag) {
    void* ptr = nullptr;
    auto ret = aclrtMallocHost(&ptr, kFlagBytes);
    if (ret != ACL_ERROR_NONE || ptr == nullptr) {
        flag = nullptr;
        return Status::InternalError(
            "Alloc local copy host flag failed" LOC_MARK);
    }
    flag = static_cast<volatile uint32_t*>(ptr);
    *flag = 0;
    return Status::OK();
}

void LocalCopyEngine::freeHostFlag(volatile uint32_t* flag) {
    if (flag != nullptr) {
        (void)aclrtFreeHost(const_cast<uint32_t*>(flag));
    }
}

aclrtMemcpyKind LocalCopyEngine::memcpyKind(void* src, void* dst) {
    aclrtPtrAttributes src_attrs{};
    aclrtPtrAttributes dst_attrs{};
    if (aclrtPointerGetAttributes(src, &src_attrs) != ACL_ERROR_NONE ||
        aclrtPointerGetAttributes(dst, &dst_attrs) != ACL_ERROR_NONE) {
        return ACL_MEMCPY_DEFAULT;
    }
    const bool src_host = src_attrs.location.type == ACL_MEM_LOCATION_TYPE_HOST;
    const bool dst_host = dst_attrs.location.type == ACL_MEM_LOCATION_TYPE_HOST;
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

std::vector<void*> LocalCopyEngine::failAndRecreateLocked() {
    std::vector<void*> handles;
    handles.reserve(groups_.size());
    for (auto& [handle, group] : groups_) {
        if (group) {
            group->failed = true;
        }
        handles.push_back(handle);
    }
    auto status = createStreamLocked();
    if (!status.ok()) {
        LOG(ERROR) << "Recreate local copy stream failed: "
                   << status.ToString();
    }
    return handles;
}

std::vector<void*> LocalCopyEngine::failAndRecreate() {
    std::lock_guard<std::mutex> lock(mutex_);
    ContextGuard guard(context_);
    return failAndRecreateLocked();
}

Status LocalCopyEngine::submit(bool write, const std::vector<HixlOpDesc>& ops,
                               void*& req, std::vector<void*>& failed_handles) {
    req = nullptr;
    failed_handles.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || stream_ == nullptr || device_one_ == nullptr) {
        return Status::InternalError(
            "Local copy engine is not initialized" LOC_MARK);
    }
    ContextGuard guard(context_);
    if (!guard.ok()) {
        return Status::InternalError("Set ACL context failed" LOC_MARK);
    }

    auto group = std::make_unique<Group>();
    CHECK_STATUS(allocHostFlag(group->host_flag));

    auto fail_stream = [&]() {
        freeHostFlag(group->host_flag);
        group->host_flag = nullptr;
        failed_handles = failAndRecreateLocked();
    };

    for (const auto& op : ops) {
        void* local = reinterpret_cast<void*>(op.local_addr);
        void* remote = reinterpret_cast<void*>(op.remote_addr);
        void* src = write ? local : remote;
        void* dst = write ? remote : local;
        auto ret = aclrtMemcpyAsync(dst, op.len, src, op.len,
                                    memcpyKind(src, dst), stream_);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            fail_stream();
            return Status::InternalError("aclrtMemcpyAsync failed" LOC_MARK);
        }
    }

    auto* host_flag = const_cast<uint32_t*>(group->host_flag);
    auto ret = aclrtMemcpyAsync(host_flag, kFlagBytes, device_one_, kFlagBytes,
                                ACL_MEMCPY_DEVICE_TO_HOST, stream_);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtMemcpyAsync host flag failed, ret:" << ret
                   << ", errmsg:" << aclGetRecentErrMsg();
        fail_stream();
        return Status::InternalError(
            "aclrtMemcpyAsync host flag failed" LOC_MARK);
    }

    req = group.get();
    groups_[req] = std::move(group);
    return Status::OK();
}

TransferStatusEnum LocalCopyEngine::poll(void* req) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = groups_.find(req);
    if (it == groups_.end() || !it->second) {
        return TransferStatusEnum::FAILED;
    }
    if (it->second->failed) {
        return TransferStatusEnum::FAILED;
    }
    if (it->second->host_flag != nullptr &&
        *it->second->host_flag == kDoneFlag) {
        return TransferStatusEnum::COMPLETED;
    }
    return TransferStatusEnum::PENDING;
}

void LocalCopyEngine::release(void* req) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = groups_.find(req);
    if (it == groups_.end()) {
        return;
    }
    if (it->second) {
        freeHostFlag(it->second->host_flag);
    }
    groups_.erase(it);
}

}  // namespace tent
}  // namespace mooncake
