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
#include "transport/transport.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

namespace mooncake {

namespace {
constexpr size_t kMemcpyBatchLimit = 4096U;
}  // namespace

namespace {
constexpr uint32_t kStreamFlags = ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC;
}  // namespace

LocalCopyEngine::LocalCopyEngine()
    : stream_(nullptr), transfer_timeout_(10000), initialized_(false) {}

LocalCopyEngine::~LocalCopyEngine() { Finalize(); }

int LocalCopyEngine::Initialize(int32_t transfer_timeout) {
    transfer_timeout_ = transfer_timeout;
    auto ret = aclrtCreateStreamWithConfig(&stream_, 0, kStreamFlags);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtCreateStreamWithConfig failed, ret:" << ret
                   << ", errmsg:" << aclGetRecentErrMsg();
        return -1;
    }
    initialized_ = true;
    return 0;
}

void LocalCopyEngine::Finalize() {
    if (initialized_ && stream_ != nullptr) {
        (void)aclrtDestroyStream(stream_);
        stream_ = nullptr;
    }
    initialized_ = false;
}

void LocalCopyEngine::Copy(TransferRequest::OpCode opcode,
                           const std::vector<Slice *> &slice_list) {
    if (slice_list.empty()) {
        return;
    }

    if (!initialized_) {
        LOG(ERROR) << "LocalCopyEngine not initialized";
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    auto &first_slice = slice_list[0];
    auto remote_ptr =
        reinterpret_cast<void *>(first_slice->ascend_direct.dest_addr);

    aclrtPtrAttributes src_attrs;
    auto ret = aclrtPointerGetAttributes(first_slice->source_addr, &src_attrs);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed for source, ret:"
                   << ret;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    aclrtPtrAttributes dst_attrs;
    ret = aclrtPointerGetAttributes(remote_ptr, &dst_attrs);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtPointerGetAttributes failed for dest, ret:" << ret;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }

    auto kind = GetMemcpyKind(opcode, src_attrs, dst_attrs);

    // For host-to-host or default cases, use sync copy
    if (kind == ACL_MEMCPY_HOST_TO_HOST || kind == ACL_MEMCPY_DEFAULT) {
        CopyWithSync(opcode, slice_list, kind);
        return;
    }

    // For device-to-device, use async copy
    if (kind == ACL_MEMCPY_DEVICE_TO_DEVICE) {
        CopyWithAsync(opcode, slice_list, kind);
        return;
    }

    // For host-device or device-host transfers, try batch copy first
    auto left_num = slice_list.size();
    size_t slice_index = 0;
    while (left_num > 0) {
        auto batch_num = std::min(left_num, kMemcpyBatchLimit);
        ret = CopyWithBatch(opcode, slice_list, kind, batch_num, slice_index);
        if (ret == ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
            // Fallback to async copy if batch copy is not supported
            CopyWithAsync(opcode, slice_list, kind);
            return;
        }
        left_num -= batch_num;
        slice_index += batch_num;
    }
}

aclrtMemcpyKind LocalCopyEngine::GetMemcpyKind(TransferRequest::OpCode opcode,
                                               aclrtPtrAttributes &src_attrs,
                                               aclrtPtrAttributes &dst_attrs) {
    if (src_attrs.location.type == ACL_MEM_LOCATION_TYPE_HOST &&
        dst_attrs.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        return ACL_MEMCPY_HOST_TO_HOST;
    } else if (src_attrs.location.type == ACL_MEM_LOCATION_TYPE_DEVICE &&
               dst_attrs.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return ACL_MEMCPY_DEVICE_TO_DEVICE;
    } else if (src_attrs.location.type == ACL_MEM_LOCATION_TYPE_HOST &&
               dst_attrs.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        return (opcode == TransferRequest::WRITE) ? ACL_MEMCPY_HOST_TO_DEVICE
                                                  : ACL_MEMCPY_DEVICE_TO_HOST;
    } else if (src_attrs.location.type == ACL_MEM_LOCATION_TYPE_DEVICE &&
               dst_attrs.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
        return (opcode == TransferRequest::WRITE) ? ACL_MEMCPY_DEVICE_TO_HOST
                                                  : ACL_MEMCPY_HOST_TO_DEVICE;
    }
    return ACL_MEMCPY_DEFAULT;
}

aclError LocalCopyEngine::CopyWithBatch(TransferRequest::OpCode opcode,
                                        const std::vector<Slice *> &slice_list,
                                        aclrtMemcpyKind kind, size_t batch_num,
                                        size_t slice_index) const {
    std::vector<void *> void_remote_addrs(batch_num);
    std::vector<void *> void_local_addrs(batch_num);
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

        auto &slice = slice_list[slice_index + i];
        void_local_addrs[i] = slice->source_addr;
        void_remote_addrs[i] =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
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
                auto &slice = slice_list[slice_index + i];
                slice->markSuccess();
            }
        } else {
            LOG(ERROR) << "aclrtMemcpyBatch failed, ret:" << ret;
            for (size_t i = 0; i < batch_num; i++) {
                auto &slice = slice_list[slice_index + i];
                slice->markFailed();
            }
        }
    }

    return ret;
}

void LocalCopyEngine::CopyWithSync(TransferRequest::OpCode opcode,
                                   const std::vector<Slice *> &slice_list,
                                   aclrtMemcpyKind kind) {
    for (auto &slice : slice_list) {
        auto local_ptr = slice->source_addr;
        auto remote_ptr =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
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

void LocalCopyEngine::CopyWithAsync(TransferRequest::OpCode opcode,
                                    const std::vector<Slice *> &slice_list,
                                    aclrtMemcpyKind kind) {
    std::vector<Slice *> async_list;
    async_list.reserve(slice_list.size());

    for (auto &slice : slice_list) {
        auto local_ptr = slice->source_addr;
        auto remote_ptr =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
        auto len = slice->length;

        aclError ret;
        if (opcode == TransferRequest::WRITE) {
            ret = aclrtMemcpyAsync(remote_ptr, len, local_ptr, len, kind,
                                   stream_);
        } else {
            ret = aclrtMemcpyAsync(local_ptr, len, remote_ptr, len, kind,
                                   stream_);
        }

        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret;
            slice->markFailed();
            continue;
        }
        async_list.emplace_back(slice);
    }

    if (async_list.empty()) {
        return;
    }

    aclError ret =
        aclrtSynchronizeStreamWithTimeout(stream_, transfer_timeout_);
    if (ret == ACL_ERROR_NONE) {
        VLOG(1) << "Copy with aclrtMemcpyAsync suc.";
        for (auto &slice : async_list) {
            slice->markSuccess();
        }
    } else {
        LOG(ERROR) << "Memory copy failed, ret:" << ret;
        (void)aclrtStreamAbort(stream_);
        for (auto &slice : async_list) {
            slice->markFailed();
        }
    }
}

}  // namespace mooncake
