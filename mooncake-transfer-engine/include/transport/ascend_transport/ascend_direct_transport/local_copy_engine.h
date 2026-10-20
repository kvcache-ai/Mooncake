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

#ifndef LOCAL_COPY_ENGINE_H
#define LOCAL_COPY_ENGINE_H

#include <mutex>
#include <unordered_map>
#include <vector>

#include <acl/acl.h>
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
using Slice = Transport::Slice;

// LocalCopyEngine handles local memory copy operations for Ascend devices.
// It provides optimized copy methods for different memory types and transfer
// patterns including sync, async, and batch copy operations.
class LocalCopyEngine {
   public:
    LocalCopyEngine();

    ~LocalCopyEngine();

    // Initialize the local copy engine. Streams are created lazily per device
    // under that device's current ACL context (not at Initialize time).
    // transfer_timeout: timeout in milliseconds for sync operations
    // Returns 0 on success, non-zero on failure
    int Initialize(int32_t transfer_timeout);

    // Finalize and cleanup resources
    void Finalize();

    // Perform local memory copy for a list of slices.
    // This is the main entry point that determines the optimal copy method
    // based on memory types and transfer size.
    // opcode: WRITE for local->remote, READ for remote->local
    // slice_list: list of slices to copy
    void Copy(TransferRequest::OpCode opcode,
              const std::vector<Slice *> &slice_list);

   private:
    // Perform synchronous memory copy for each slice individually
    void CopyWithSync(TransferRequest::OpCode opcode,
                      const std::vector<Slice *> &slice_list,
                      aclrtMemcpyKind kind);

    // Perform asynchronous memory copy with stream synchronization
    void CopyWithAsync(TransferRequest::OpCode opcode,
                       const std::vector<Slice *> &slice_list,
                       aclrtMemcpyKind kind);

    // Perform batched memory copy using aclrtMemcpyBatch
    // Returns ACL_ERROR_RT_FEATURE_NOT_SUPPORT if batch copy is not supported
    aclError CopyWithBatch(TransferRequest::OpCode opcode,
                           const std::vector<Slice *> &slice_list,
                           aclrtMemcpyKind kind, size_t batch_num,
                           size_t slice_index) const;

    // Determine the appropriate memcpy kind based on source and dest memory
    // types
    static aclrtMemcpyKind GetMemcpyKind(TransferRequest::OpCode opcode,
                                         aclrtPtrAttributes &src_attrs,
                                         aclrtPtrAttributes &dst_attrs);

    // Return (or lazily create) the stream set belonging to the current ACL
    // device context. Streams must be created while that device's context is
    // current so aclrtMemcpyAsync does not return 107003. The set holds
    // MC_LOCAL_COPY_STREAMS streams (default 1) so local D2D copies can be
    // striped across copy engines.
    const std::vector<aclrtStream> *GetOrCreateStreamsForCurrentDevice();

   private:
    // Streams plus the ACL context they were created under. Destroying a
    // stream requires that same context to be current, otherwise
    // aclrtDestroyStream can return 107003 (ACL_ERROR_RT_STREAM_CONTEXT).
    struct DeviceStreams {
        std::vector<aclrtStream> streams;
        aclrtContext context = nullptr;
    };

    // Per-device stream sets keyed by ACL device id. A single shared stream is
    // unsafe across devices because Initialize() may run under device0
    // context while workers later copy under other device contexts.
    std::unordered_map<int32_t, DeviceStreams> streams_;
    mutable std::mutex streams_mu_;
    int32_t transfer_timeout_;
    // Number of streams per device for local copy striping. Parsed once from
    // MC_LOCAL_COPY_STREAMS at Initialize (clamped to [1, 32]). 1 keeps the
    // historical single-stream behavior.
    size_t num_streams_;
    bool initialized_;
};

}  // namespace mooncake

#endif  // LOCAL_COPY_ENGINE_H
