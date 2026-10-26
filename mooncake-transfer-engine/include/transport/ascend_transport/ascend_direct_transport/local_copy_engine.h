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
#include <memory>
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
// patterns, including synchronous host copies and async multi-stream copies.
class LocalCopyEngine {
   public:
    // Runs a task on another thread of the pool that calls Copy().
    using TaskRunner = std::function<void(std::function<void()>)>;

    LocalCopyEngine();

    ~LocalCopyEngine();

    // Initialize the local copy engine.
    // transfer_timeout: timeout in milliseconds for sync operations.
    // device_count: local NPUs this engine serves. One device creates its 6
    // streams now, when a context is already current. More than one device
    // leaves every stream group uncreated until the first copy on that device.
    // Returns 0 on success, non-zero on failure.
    int Initialize(int32_t transfer_timeout, size_t device_count);

    // Finalize and cleanup resources
    void Finalize();

    // Runs each async-copy stream task on `runner`. Pass an empty runner to
    // keep those tasks on the calling thread. Must not be called while a
    // Copy() is in flight.
    void SetTaskRunner(TaskRunner runner);

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

    // Six streams per device. copy_mu keeps two copies on that device off the
    // same streams. Copies on different devices do not share a lock.
    struct AsyncStreamGroup {
        std::mutex copy_mu;
        aclrtContext context = nullptr;
        std::vector<aclrtStream> streams;
    };

    // Finds or creates the group for device_id. The group stays put until
    // Finalize. The caller locks group->copy_mu before using the streams.
    AsyncStreamGroup* GroupFor(int32_t device_id);

    // Creates this device's 6 streams when the group does not have them yet.
    // The caller holds group->copy_mu. Returns false when creation fails.
    bool EnsureAsyncCopyStreams(AsyncStreamGroup* group, int32_t device_id,
                                aclrtContext context);

    // Submit aclrtMemcpyAsync through the memcpy pool and synchronize each
    // stream. With no pool, the caller submits and synchronizes the streams.
    void CopyWithMultiStreamAsync(TransferRequest::OpCode opcode,
                                  const std::vector<Slice*>& slice_list,
                                  const std::vector<void*>& dest_addrs,
                                  aclrtMemcpyKind kind);

    // Determine the appropriate memcpy kind based on source and dest memory
    // types
    static aclrtMemcpyKind GetMemcpyKind(TransferRequest::OpCode opcode,
                                         aclrtPtrAttributes& src_attrs,
                                         aclrtPtrAttributes& dst_attrs);

   private:
    // Protects async_stream_groups_. A copy releases this before taking the
    // group's copy_mu, so Finalize can wait for an in-flight copy.
    std::mutex groups_mu_;
    std::unordered_map<int32_t, std::unique_ptr<AsyncStreamGroup>>
        async_stream_groups_;
    int32_t transfer_timeout_;
    bool initialized_;
    TaskRunner task_runner_;
};

}  // namespace mooncake

#endif  // LOCAL_COPY_ENGINE_H
