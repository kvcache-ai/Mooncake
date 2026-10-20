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
#include <cstdlib>
#include "transport/transport.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"

namespace mooncake {

namespace {
constexpr size_t kMemcpyBatchLimit = 4096U;
constexpr size_t kMaxLocalCopyStreams = 32U;
constexpr size_t kDefaultLocalCopyStreams = 1U;

size_t ResolveNumLocalCopyStreams() {
    const char *env = std::getenv("MC_LOCAL_COPY_STREAMS");
    if (env == nullptr || *env == '\0') {
        return kDefaultLocalCopyStreams;
    }
    char *end = nullptr;
    const long parsed = std::strtol(env, &end, 10);
    if (end == env || *end != '\0' || parsed < 1) {
        LOG(WARNING) << "Invalid MC_LOCAL_COPY_STREAMS value: '" << env
                     << "', using default " << kDefaultLocalCopyStreams;
        return kDefaultLocalCopyStreams;
    }
    return static_cast<size_t>(std::min<long>(parsed, kMaxLocalCopyStreams));
}
}  // namespace

namespace {
constexpr uint32_t kStreamFlags = ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC;
}  // namespace

LocalCopyEngine::LocalCopyEngine()
    : transfer_timeout_(10000),
      num_streams_(kDefaultLocalCopyStreams),
      initialized_(false) {}

LocalCopyEngine::~LocalCopyEngine() { Finalize(); }

int LocalCopyEngine::Initialize(int32_t transfer_timeout) {
    transfer_timeout_ = transfer_timeout;
    num_streams_ = ResolveNumLocalCopyStreams();
    if (num_streams_ > 1) {
        LOG(INFO) << "LocalCopyEngine striping local copies across "
                  << num_streams_ << " streams per device (MC_LOCAL_COPY_STREAMS)";
    }
    // Do not create streams here: initEngines() may still run under a
    // different device context than the worker threads that later call Copy.
    // Streams are created lazily in GetOrCreateStreamsForCurrentDevice().
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

    for (auto &kv : streams_) {
        auto &info = kv.second;
        for (aclrtStream stream : info.streams) {
            if (stream == nullptr) {
                continue;
            }
            aclError ret = ACL_ERROR_NONE;
            if (info.context != nullptr) {
                ret = aclrtSetCurrentContext(info.context);
            } else {
                ret = aclrtSetDevice(kv.first);
            }
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR)
                    << "Failed to switch to device " << kv.first
                    << " context before destroying LocalCopyEngine stream, ret:"
                    << ret << ", errmsg:" << aclGetRecentErrMsg();
            }
            (void)aclrtDestroyStream(stream);
        }
        info.streams.clear();
        info.context = nullptr;
    }
    streams_.clear();
    initialized_ = false;
}

const std::vector<aclrtStream> *LocalCopyEngine::GetOrCreateStreamsForCurrentDevice() {
    int32_t device_id = 0;
    auto ret = aclrtGetDevice(&device_id);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtGetDevice failed, ret:" << ret
                   << ", errmsg:" << aclGetRecentErrMsg();
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(streams_mu_);
    auto it = streams_.find(device_id);
    if (it != streams_.end() && !it->second.streams.empty() &&
        it->second.streams[0] != nullptr) {
        return &it->second.streams;
    }

    std::vector<aclrtStream> created;
    created.reserve(num_streams_);
    for (size_t i = 0; i < num_streams_; ++i) {
        aclrtStream stream = nullptr;
        ret = aclrtCreateStreamWithConfig(&stream, 0, kStreamFlags);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtCreateStreamWithConfig failed for device "
                       << device_id << " stream " << i << ", ret:" << ret
                       << ", errmsg:" << aclGetRecentErrMsg();
            for (aclrtStream s : created) {
                (void)aclrtDestroyStream(s);
            }
            return nullptr;
        }
        created.push_back(stream);
    }

    aclrtContext ctx = nullptr;
    ret = aclrtGetCurrentContext(&ctx);
    if (ret != ACL_ERROR_NONE || ctx == nullptr) {
        LOG(ERROR) << "aclrtGetCurrentContext failed after creating streams for "
                      "device "
                   << device_id << ", ret:" << ret
                   << ", errmsg:" << aclGetRecentErrMsg();
        ctx = nullptr;
    }
    DeviceStreams &entry = streams_[device_id];
    entry.context = ctx;
    entry.streams = std::move(created);
    VLOG(1) << "Created " << entry.streams.size()
            << " LocalCopyEngine stream(s) for device " << device_id;
    return &entry.streams;
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
            VLOG(1) << "aclrtMemcpyBatch FEATURE_NOT_SUPPORT (207000); "
                       "falling back to CopyWithAsync";
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
    const std::vector<aclrtStream> *streams =
        GetOrCreateStreamsForCurrentDevice();
    if (streams == nullptr) {
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    const size_t num_streams = streams->size();

    // Per-slice stream index so completion can be tracked per stream.
    // Note: slices are striped round-robin. Key-level striping is not possible
    // at this layer: one TransferTask maps to one slice in the ascend direct
    // transport, so no key identity reaches LocalCopyEngine. Splitting a key's
    // pages across streams is harmless: each slice is an independent D2D copy
    // with no ordering requirement.
    std::vector<std::pair<Slice *, size_t>> async_list;
    async_list.reserve(slice_list.size());

    for (size_t i = 0; i < slice_list.size(); ++i) {
        auto slice = slice_list[i];
        const size_t stream_idx = i % num_streams;
        aclrtStream stream = (*streams)[stream_idx];
        auto local_ptr = slice->source_addr;
        auto remote_ptr =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
        auto len = slice->length;

        aclError ret;
        if (opcode == TransferRequest::WRITE) {
            ret = aclrtMemcpyAsync(remote_ptr, len, local_ptr, len, kind, stream);
        } else {
            ret = aclrtMemcpyAsync(local_ptr, len, remote_ptr, len, kind, stream);
        }

        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret;
            slice->markFailed();
            continue;
        }
        async_list.emplace_back(slice, stream_idx);
    }

    if (async_list.empty()) {
        return;
    }

    // Sync each stream once, then resolve slices per stream result.
    std::vector<aclError> sync_results(num_streams, ACL_ERROR_NONE);
    std::vector<bool> stream_used(num_streams, false);
    for (const auto &entry : async_list) {
        stream_used[entry.second] = true;
    }
    for (size_t i = 0; i < num_streams; ++i) {
        if (!stream_used[i]) {
            continue;
        }
        sync_results[i] = aclrtSynchronizeStreamWithTimeout(
            (*streams)[i], transfer_timeout_);
        if (sync_results[i] != ACL_ERROR_NONE) {
            LOG(ERROR) << "Memory copy failed on stream " << i
                       << ", ret:" << sync_results[i];
            (void)aclrtStreamAbort((*streams)[i]);
        }
    }
    size_t success_count = 0;
    for (const auto &entry : async_list) {
        if (sync_results[entry.second] == ACL_ERROR_NONE) {
            entry.first->markSuccess();
            ++success_count;
        } else {
            entry.first->markFailed();
        }
    }
    VLOG(1) << "Copy with aclrtMemcpyAsync done. success:" << success_count
            << " total:" << async_list.size();
}

}  // namespace mooncake
