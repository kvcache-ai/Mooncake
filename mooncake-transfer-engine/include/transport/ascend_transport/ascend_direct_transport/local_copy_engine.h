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

#include <acl/acl.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
using Slice = Transport::Slice;

// Same-host copies through the ACL copy engine. Copies are submitted
// asynchronously: six streams carry aclrtMemcpyAsync work and each stream ends
// with a D2H write of a host completion flag. The transport polls the flags
// from its status path and marks slices complete, so the transfer worker does
// not wait for the streams (the TENT local-copy engine uses the same model).
class LocalCopyEngine {
   public:
    // Runs the issue work on a bound pool thread. Empty keeps it on the caller.
    using TaskRunner = std::function<void(std::function<void()>)>;

    LocalCopyEngine();
    ~LocalCopyEngine();

    // transfer_timeout: per-group timeout in milliseconds.
    // device_count: local NPUs this engine serves. One device creates its
    // streams and flag slot eagerly; more than one defers them to the first
    // copy on that device.
    int Initialize(int32_t transfer_timeout, size_t device_count);
    void Finalize();
    void SetTaskRunner(TaskRunner runner);

    // Advances completion of every pending group. The transport calls this
    // from getTransferStatus(), matching the TENT local-copy engine, which has
    // no completion thread of its own.
    void Poll();

    // Submit the copy and return. Poll() marks every slice success/failure when
    // the streams finish (or the group times out).
    void Copy(TransferRequest::OpCode opcode,
              const std::vector<Slice*>& slice_list,
              const std::vector<void*>& dest_addrs = {});

   private:
    static constexpr size_t kCopyStreamCount = 6;
    static constexpr size_t kFlagBytes = sizeof(uint32_t);
    static constexpr size_t kFlagStride = 64;
    static constexpr uint32_t kDoneFlag = 1;

    struct PendingGroup {
        void* host_flags = nullptr;
        size_t stream_count = 0;
        std::vector<Slice*> slices;
        std::chrono::steady_clock::time_point deadline;
        bool issued = false;
        bool failed = false;
    };

    struct DeviceState {
        aclrtContext context = nullptr;
        std::vector<aclrtStream> streams;
        void* device_one = nullptr;
        std::unordered_map<void*, PendingGroup> pending;
    };

    static void* DestAddr(const std::vector<Slice*>& slice_list,
                          const std::vector<void*>& dest_addrs, size_t index);
    static aclrtMemcpyKind GetMemcpyKind(TransferRequest::OpCode opcode,
                                         aclrtPtrAttributes& src_attrs,
                                         aclrtPtrAttributes& dst_attrs);
    static volatile uint32_t* FlagAt(PendingGroup& group, size_t index);

    // H2H copies are small and stay synchronous.
    void CopyWithSync(TransferRequest::OpCode opcode,
                      const std::vector<Slice*>& slice_list,
                      const std::vector<void*>& dest_addrs,
                      aclrtMemcpyKind kind);

    // Creates the six streams and the device-side completion value for one
    // device. Caller holds mutex_ and has switched to this device's context.
    bool EnsureStreamsLocked(DeviceState& device);
    void DestroyStreamsLocked(DeviceState& device);
    void ReleaseGroupLocked(DeviceState& device, void* handle, bool success);
    void FailAllLocked(DeviceState& device);

    std::mutex mutex_;
    std::unordered_map<int32_t, std::unique_ptr<DeviceState>> devices_;
    TaskRunner task_runner_;
    int32_t transfer_timeout_ = 10000;
    bool initialized_ = false;
};

}  // namespace mooncake

#endif  // LOCAL_COPY_ENGINE_H
