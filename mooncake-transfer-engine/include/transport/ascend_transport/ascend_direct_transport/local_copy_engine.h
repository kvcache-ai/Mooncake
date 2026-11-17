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

// Same-host copies through the ACL copy engine, one instance per local engine.
// Copies are submitted asynchronously: six streams carry aclrtMemcpyAsync work
// and each stream ends with a D2H write of a host completion flag. The
// transport polls the flags from its status path and marks slices complete, so
// the transfer worker does not wait for the streams.
//
// This is the TENT local-copy engine's model, kept on the classic transport:
//
//  * one instance owns one device's context and streams, so there is no device
//    map and no caller-side device lookup;
//  * Copy() issues the whole group before it returns, so a submitter cannot be
//    between its copies and its completion flag while Poll() aborts the
//    streams. That is what makes a single mutex sufficient: no per-stream
//    lock, no submission gate and no in-flight issue counter is needed;
//  * a failed submission or a timed-out group aborts and destroys the streams,
//    recreates them, and fails every group that was still pending on them.
//    Aborting drops the queued work of those groups, so their flags can no
//    longer complete and failing them is the only honest answer.
class LocalCopyEngine {
   public:
    // Runs one stream's issue work. The transport supplies a pool so a group
    // spread over several streams does not issue one stream at a time.
    using TaskRunner = std::function<void(std::function<void()>)>;

    LocalCopyEngine();
    ~LocalCopyEngine();

    LocalCopyEngine(const LocalCopyEngine&) = delete;
    LocalCopyEngine& operator=(const LocalCopyEngine&) = delete;

    // context: the ACL context of the local engine this instance copies for.
    // transfer_timeout: per-group timeout in milliseconds.
    // local_engine_count: how many local engines this process runs. With more
    // than one, the six copy streams are created on the first copy that needs
    // them instead of at Initialize: a multi-engine process (a store driving
    // one engine per NPU) copies locally only on the devices it actually
    // serves, so creating six streams for every engine at install would leave
    // most of them unused.
    int Initialize(aclrtContext context, int32_t transfer_timeout,
                   size_t local_engine_count);
    void Finalize();
    void SetTaskRunner(TaskRunner runner);

    // Advances completion of every pending group. The transport calls this
    // from getTransferStatus(), matching the TENT local-copy engine, which has
    // no completion thread of its own.
    void Poll();

    // Issues the whole group and returns. Poll() marks every slice
    // success/failure when the streams finish (or the group times out).
    void Copy(TransferRequest::OpCode opcode,
              const std::vector<Slice*>& slice_list,
              const std::vector<void*>& dest_addrs = {});

   private:
    static constexpr size_t kCopyStreamCount = 6;
    static constexpr size_t kFlagBytes = sizeof(uint32_t);
    static constexpr size_t kFlagStride = 64;
    static constexpr uint32_t kDoneFlag = 1;

    struct Group {
        // One flag per stream this group used, padded apart so two streams
        // never land their completion write in the same cache line.
        void* host_flags = nullptr;
        size_t stream_count = 0;
        std::vector<Slice*> slices;
        std::chrono::steady_clock::time_point deadline;
    };

    // Binds target as the calling thread's ACL context for the lifetime of the
    // guard and restores the caller's context afterwards. ACL stream and
    // memory APIs only work on the thread that has the owning device's context
    // bound, while Poll() runs on whichever thread calls getTransferStatus().
    class ContextGuard {
       public:
        explicit ContextGuard(aclrtContext target);
        ~ContextGuard();
        bool ok() const { return ok_; }

        ContextGuard(const ContextGuard&) = delete;
        ContextGuard& operator=(const ContextGuard&) = delete;

       private:
        aclrtContext saved_{nullptr};
        bool ok_{false};
    };

    static void* DestAddr(const std::vector<Slice*>& slice_list,
                          const std::vector<void*>& dest_addrs, size_t index);
    static aclrtMemcpyKind GetMemcpyKind(TransferRequest::OpCode opcode,
                                         aclrtPtrAttributes& src_attrs,
                                         aclrtPtrAttributes& dst_attrs);
    static volatile uint32_t* FlagAt(void* host_flags, size_t index);

    // H2H copies are small and stay synchronous.
    void CopyWithSync(TransferRequest::OpCode opcode,
                      const std::vector<Slice*>& slice_list,
                      const std::vector<void*>& dest_addrs,
                      aclrtMemcpyKind kind);

    // All of these run with mutex_ held and bind the device context while they
    // touch ACL state. Stream creation/destruction requires that no Copy() is
    // in progress, which mutex_ guarantees because Copy() holds it from the
    // first stream call to the last.
    bool CreateStreamsLocked();
    void DestroyStreamsLocked();
    // Aborts and destroys the streams, recreates them, and fails every pending
    // group. The streams are dropped first so the aborts cancel any queued D2H
    // flag write before a group's host flags are released.
    void FailAndRecreateLocked();
    bool IssueGroupLocked(size_t stream_count,
                          const std::function<bool(size_t)>& issue_stream);
    void ReleaseGroupLocked(void* handle, bool success);
    void FailAllGroupsLocked();

    std::mutex mutex_;
    aclrtContext context_{nullptr};
    std::vector<aclrtStream> streams_;
    void* device_one_{nullptr};
    std::unordered_map<void*, std::unique_ptr<Group>> groups_;
    TaskRunner task_runner_;
    int32_t transfer_timeout_ = 10000;
    bool initialized_ = false;
};

}  // namespace mooncake

#endif  // LOCAL_COPY_ENGINE_H
