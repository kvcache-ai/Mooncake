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

#include <functional>
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
    // Runs a task on another thread of the pool that calls Copy().
    using TaskRunner = std::function<void(std::function<void()>)>;

    LocalCopyEngine();

    ~LocalCopyEngine();

    // Initialize the local copy engine. Streams are created lazily per device
    // under that device's current ACL context (not at Initialize time).
    // transfer_timeout: timeout in milliseconds for sync operations
    // Returns 0 on success, non-zero on failure
    int Initialize(int32_t transfer_timeout);

    // Finalize and cleanup resources
    void Finalize();

    // Lets a large host-device copy split its aclrtMemcpyBatch calls across
    // `runner`, using at most `max_parallelism` threads including the caller.
    // Pass an empty runner to keep every copy on the calling thread. Must not
    // be called while a Copy() is in flight.
    void SetTaskRunner(TaskRunner runner, size_t max_parallelism);

    // Perform local memory copy for a list of slices.
    // This is the main entry point that determines the optimal copy method
    // based on memory types and transfer size.
    // opcode: WRITE for local->remote, READ for remote->local
    // slice_list: list of slices to copy
    // dest_addrs: either empty, meaning each slice's own dest_addr is already
    //   a valid local address, or exactly one entry per slice holding that
    //   slice's destination rebased onto this process's mapping. The latter is
    //   needed for a segment owned by another process on this machine, whose
    //   virtual addresses mean nothing here.
    void Copy(TransferRequest::OpCode opcode,
              const std::vector<Slice*>& slice_list,
              const std::vector<void*>& dest_addrs = {});

   private:
    // Destination for slice_list[index], honouring the rebased addresses when
    // the caller supplied them.
    static void* DestAddr(const std::vector<Slice*>& slice_list,
                          const std::vector<void*>& dest_addrs, size_t index);

    // Perform synchronous memory copy for each slice individually
    void CopyWithSync(TransferRequest::OpCode opcode,
                      const std::vector<Slice*>& slice_list,
                      const std::vector<void*>& dest_addrs,
                      aclrtMemcpyKind kind);

    // Create 6 streams for this device, then submit aclrtMemcpyAsync through
    // the memcpy pool and synchronize each stream. use_pool is false when the
    // caller is already a memcpy worker.
    std::vector<aclrtStream> EnsureAsyncCopyStreams(int32_t device_id,
                                                    aclrtContext context);
    void CopyWithMultiStreamAsync(TransferRequest::OpCode opcode,
                                  const std::vector<Slice*>& slice_list,
                                  const std::vector<void*>& dest_addrs,
                                  aclrtMemcpyKind kind, bool use_pool = true);

    // Perform batched memory copy using aclrtMemcpyBatch
    // Returns ACL_ERROR_RT_FEATURE_NOT_SUPPORT if batch copy is not supported
    aclError CopyWithBatch(TransferRequest::OpCode opcode,
                           const std::vector<Slice*>& slice_list,
                           const std::vector<void*>& dest_addrs,
                           aclrtMemcpyKind kind, size_t batch_num,
                           size_t slice_index) const;

    // Copies slices [slice_index, slice_index + batch_num) with one
    // aclrtMemcpyBatch call, falling back to async copy for just these slices
    // when batch copy is not supported. dest_addrs follows Copy().
    void CopyBatchChunk(TransferRequest::OpCode opcode,
                        const std::vector<Slice*>& slice_list,
                        const std::vector<void*>& dest_addrs,
                        aclrtMemcpyKind kind, size_t slice_index,
                        size_t batch_num);

    // Splits the slices into fixed-size aclrtMemcpyBatch calls and runs them
    // on task_runner_. The caller waits. It copies only when the pool cannot
    // enter the caller's ACL context.
    void CopyWithParallelBatch(TransferRequest::OpCode opcode,
                               const std::vector<Slice*>& slice_list,
                               const std::vector<void*>& dest_addrs,
                               aclrtMemcpyKind kind);

    // Determine the appropriate memcpy kind based on source and dest memory
    // types
    static aclrtMemcpyKind GetMemcpyKind(TransferRequest::OpCode opcode,
                                         aclrtPtrAttributes& src_attrs,
                                         aclrtPtrAttributes& dst_attrs);

   private:
    // Six streams per device for the default async copy path.
    struct AsyncStreamGroup {
        aclrtContext context = nullptr;
        std::vector<aclrtStream> streams;
    };
    std::unordered_map<int32_t, AsyncStreamGroup> async_stream_groups_;
    // Serializes multi-stream copies so they do not share the 6 streams.
    std::mutex async_copy_mu_;
    mutable std::mutex streams_mu_;
    int32_t transfer_timeout_;
    bool initialized_;
    bool use_memcpy_batch_ = false;
    TaskRunner task_runner_;
    size_t max_parallelism_ = 1;
};

}  // namespace mooncake

#endif  // LOCAL_COPY_ENGINE_H
